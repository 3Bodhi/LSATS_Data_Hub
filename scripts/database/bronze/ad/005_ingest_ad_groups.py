#!/usr/bin/env python3
"""
Active Directory LDAP Group Ingestion Service

This service ingests group data from the University of Michigan Active Directory LDAP
directory (adsroot.itcs.umich.edu) into the bronze layer for cross-referencing and analysis.

Active Directory provides authoritative directory information for all LSA groups including:
- Group membership (member attribute for users and nested groups)
- Group identifiers (objectGUID, sAMAccountName, objectSid)
- Group descriptions and metadata
- Group type and scope (groupType attribute)
- Distinguished names and organizational structure

All group records are stored in the LSA OU structure with objectGUID as the unique
external identifier. Groups are filtered by objectClass=group.

IMPORTANT: Many LDAP attributes (member, memberOf, description, etc.) can be either
strings or lists of strings depending on the group record. The normalization functions
handle this appropriately.
"""

import argparse
import base64
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# Core Python imports for PostgreSQL operations
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.pool import QueuePool

# Add your LSATS project to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter, create_postgres_adapter
from ldap.adapters.ldap_adapter import LDAPAdapter

script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "/var/log/lsats/bronze"
os.makedirs(log_dir, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"{log_dir}/{script_name}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class ActiveDirectoryGroupIngestionService:
    """
    Group ingestion service for University of Michigan Active Directory LDAP directory.

    Uses content hashing for change detection since LDAP doesn't provide
    modification timestamps in a consistent way. This approach:

    1. Fetches current group data from Active Directory LDAP (LSA OU structure)
    2. Calculates content hashes for each group
    3. Compares against stored hashes from previous ingestions
    4. Only creates new bronze records when group content has actually changed
    5. Preserves complete change history for group analysis

    Key Features:
    - Efficient change detection without requiring timestamps
    - Group membership tracking (member attribute)
    - Handles multi-value LDAP attributes (member, memberOf, etc.)
    - Tracks group types and scopes (security, distribution, etc.)
    - Comprehensive audit trail for group changes
    - Detailed ingestion statistics and monitoring
    """

    def __init__(
        self,
        database_url: str,
        ldap_config: Dict[str, Any],
        force_full_sync: bool = False,
        dry_run: bool = False,
        batch_size: int = 500,
    ):
        """
        Initialize the Active Directory group ingestion service.

        Args:
            database_url: PostgreSQL connection string
            ldap_config: LDAP connection configuration dictionary
            force_full_sync: If True, bypass timestamp filtering and perform full sync
            dry_run: If True, preview changes without committing to database
            batch_size: Batch size for processing
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        # Initialize LDAP adapter for Active Directory
        self.ldap_adapter = LDAPAdapter(ldap_config)

        # Store configuration
        self.force_full_sync = force_full_sync
        self.dry_run = dry_run
        self.batch_size = batch_size

        # Test LDAP connection
        if not self.ldap_adapter.test_connection():
            raise Exception("Failed to connect to Active Directory LDAP")

        logger.info(
            "✅ Active Directory group ingestion service initialized with content hashing"
        )

    def _normalize_ldap_attribute(self, value: Any) -> Any:
        """
        Normalize LDAP attribute values for consistent hashing and JSON serialization.

        LDAP attributes can be single values, lists, bytes, or None. Many Active Directory
        group attributes like member, memberOf, description can be either strings
        or lists depending on the group.

        Binary fields (sIDHistory, proxiedObjectName, etc.) are converted to base64
        to avoid issues with null bytes in PostgreSQL JSON/JSONB fields.

        Args:
            value: Raw LDAP attribute value

        Returns:
            Normalized value suitable for JSON serialization
        """
        if value is None:
            return ""
        elif isinstance(value, bytes):
            # Handle binary attributes using base64 encoding
            # This avoids null byte issues with PostgreSQL JSON/JSONB
            # Fields like sIDHistory, proxiedObjectName contain binary data
            try:
                # Try to decode as UTF-8 for string-like fields (objectGUID)
                decoded = value.decode("utf-8")
                # Check if it contains null bytes or other problematic characters
                if "\x00" in decoded or any(
                    ord(c) < 32 and c not in "\t\n\r" for c in decoded
                ):
                    # Contains binary data, use base64
                    return base64.b64encode(value).decode("ascii")
                else:
                    return decoded.strip()
            except UnicodeDecodeError:
                # Not valid UTF-8, definitely binary data
                return base64.b64encode(value).decode("ascii")
        elif isinstance(value, datetime):
            # Handle datetime objects
            return value.isoformat()
        elif isinstance(value, list):
            if len(value) == 0:
                return ""
            elif len(value) == 1:
                item = value[0]
                if isinstance(item, bytes):
                    # Same binary handling for list items
                    try:
                        decoded = item.decode("utf-8")
                        if "\x00" in decoded or any(
                            ord(c) < 32 and c not in "\t\n\r" for c in decoded
                        ):
                            return base64.b64encode(item).decode("ascii")
                        else:
                            return decoded.strip()
                    except UnicodeDecodeError:
                        return base64.b64encode(item).decode("ascii")
                elif isinstance(item, datetime):
                    return item.isoformat()
                else:
                    return str(item).strip()
            else:
                # Sort multi-value attributes for consistent hashing
                normalized_items = []
                for item in value:
                    if isinstance(item, bytes):
                        try:
                            decoded = item.decode("utf-8")
                            if "\x00" in decoded or any(
                                ord(c) < 32 and c not in "\t\n\r" for c in decoded
                            ):
                                normalized_items.append(
                                    base64.b64encode(item).decode("ascii")
                                )
                            else:
                                normalized_items.append(decoded.strip())
                        except UnicodeDecodeError:
                            normalized_items.append(
                                base64.b64encode(item).decode("ascii")
                            )
                    elif isinstance(item, datetime):
                        normalized_items.append(item.isoformat())
                    else:
                        normalized_items.append(str(item).strip())
                return sorted(normalized_items)
        else:
            return str(value).strip()

    def _normalize_raw_data_for_json(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively normalize all values in a dictionary for JSON serialization.

        This ensures that datetime objects, bytes, and other non-JSON-serializable
        types are converted before inserting into the database.

        Args:
            data: Dictionary with raw LDAP data

        Returns:
            Dictionary with all values normalized for JSON serialization
        """
        normalized = {}
        for key, value in data.items():
            normalized[key] = self._normalize_ldap_attribute(value)
        return normalized

    def _calculate_group_content_hash(self, group_data: Dict[str, Any]) -> str:
        """
        Calculate a content hash for Active Directory group data to detect meaningful changes.

        This hash represents the "content fingerprint" of the group record.
        We include all fields that would represent meaningful group changes based on
        the Active Directory LDAP schema.

        Args:
            group_data: Raw group data from Active Directory LDAP

        Returns:
            SHA-256 hash of the normalized group content
        """
        # Extract significant fields for change detection
        # Based on Active Directory LDAP schema for groups
        significant_fields = {
            # Core identifiers
            "name": self._normalize_ldap_attribute(group_data.get("name")),
            "cn": self._normalize_ldap_attribute(group_data.get("cn")),
            "sAMAccountName": self._normalize_ldap_attribute(
                group_data.get("sAMAccountName")
            ),
            "distinguishedName": self._normalize_ldap_attribute(
                group_data.get("distinguishedName")
            ),
            "objectGUID": self._normalize_ldap_attribute(group_data.get("objectGUID")),
            "objectSid": self._normalize_ldap_attribute(group_data.get("objectSid")),
            # Group membership
            "member": self._normalize_ldap_attribute(group_data.get("member")),
            "memberOf": self._normalize_ldap_attribute(group_data.get("memberOf")),
            # Group metadata
            "description": self._normalize_ldap_attribute(
                group_data.get("description")
            ),
            "groupType": self._normalize_ldap_attribute(group_data.get("groupType")),
            "sAMAccountType": self._normalize_ldap_attribute(
                group_data.get("sAMAccountType")
            ),
            # Group category and object class
            "objectCategory": self._normalize_ldap_attribute(
                group_data.get("objectCategory")
            ),
            "objectClass": self._normalize_ldap_attribute(
                group_data.get("objectClass")
            ),
            # Instance type
            "instanceType": self._normalize_ldap_attribute(
                group_data.get("instanceType")
            ),
            # Historical data
            "sIDHistory": self._normalize_ldap_attribute(group_data.get("sIDHistory")),
            "proxiedObjectName": self._normalize_ldap_attribute(
                group_data.get("proxiedObjectName")
            ),
        }
        
        # EXCLUDED FIELDS (Volatile metadata that changes without content updates)
        # - uSNChanged, uSNCreated: Change on every replication/update
        # - whenChanged: Timestamp of last change
        # - dSCorePropagationData: Replication metadata
        # - lastLogonTimestamp: Updates periodically

        # Create normalized JSON for consistent hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )

        # Generate SHA-256 hash
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        name = group_data.get("name", "unknown")
        object_guid = self._normalize_ldap_attribute(group_data.get("objectGUID"))
        # logger.debug(
        #     f"Content hash for group {name} (objectGUID: {object_guid}): {content_hash}"
        # )

        return content_hash

    def _get_existing_group_hashes(self) -> Dict[str, str]:
        """
        Retrieve the latest content hash for each Active Directory group from the bronze layer.

        This uses a window function to get only the most recent record for each
        group, allowing efficient comparison with new data.

        Returns:
            Dictionary mapping objectGUID -> latest_content_hash
        """
        try:
            # Query to get the most recent record for each group
            query = """
            WITH latest_groups AS (
                SELECT
                    external_id,
                    raw_data,
                    ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'group'
                AND source_system = 'active_directory'
            )
            SELECT
                external_id,
                raw_data
            FROM latest_groups
            WHERE row_num = 1
            """

            results_df = self.db_adapter.query_to_dataframe(query)

            # Calculate content hashes for existing records
            existing_hashes = {}
            for _, row in results_df.iterrows():
                object_guid = row["external_id"]
                raw_data = row["raw_data"]  # JSONB comes back as dict
                content_hash = self._calculate_group_content_hash(raw_data)
                existing_hashes[object_guid] = content_hash

            logger.info(
                f"📊 Retrieved content hashes for {len(existing_hashes)} existing Active Directory groups"
            )
            return existing_hashes

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to retrieve existing group hashes: {e}")
            raise

    def create_ingestion_run(self, source_system: str, entity_type: str, metadata: Dict[str, Any] = None) -> str:
        """Create a new ingestion run record for tracking purposes."""
        try:
            run_id = str(uuid.uuid4())

            # Metadata specific to Active Directory LDAP content hashing approach
            base_metadata = {
                "ingestion_type": "content_hash_based",
                "source_api": "active_directory_ldap",
                "ldap_server": "adsroot.itcs.umich.edu",
                "search_base": "OU=Users and Groups,OU=LSA,OU=Organizations,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
                "search_filter": "(objectClass=group)",
                "change_detection_method": "sha256_content_hash",
                "includes_group_membership": True,
                "includes_nested_groups": True,
            }
            
            if metadata:
                base_metadata.update(metadata)

            with self.db_adapter.engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status, metadata
                    ) VALUES (
                        :run_id, :source_system, :entity_type, :started_at, 'running', :metadata
                    )
                """)

                conn.execute(
                    insert_query,
                    {
                        "run_id": run_id,
                        "source_system": source_system,
                        "entity_type": entity_type,
                        "started_at": datetime.now(timezone.utc),
                        "metadata": json.dumps(base_metadata),
                    },
                )

                conn.commit()

            logger.info(
                f"🚀 Created Active Directory ingestion run {run_id} for {source_system}/{entity_type}"
            )
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create ingestion run: {e}")
            raise

    def complete_ingestion_run(
        self,
        run_id: str,
        records_processed: int,
        records_created: int,
        records_skipped: int = 0,
        error_message: Optional[str] = None,
    ):
        """Mark an ingestion run as completed with comprehensive statistics."""
        try:
            status = "failed" if error_message else "completed"

            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = :completed_at,
                        status = :status,
                        records_processed = :records_processed,
                        records_created = :records_created,
                        records_updated = :records_skipped,
                        error_message = :error_message
                    WHERE run_id = :run_id
                """)

                conn.execute(
                    update_query,
                    {
                        "run_id": run_id,
                        "completed_at": datetime.now(timezone.utc),
                        "status": status,
                        "records_processed": records_processed,
                        "records_created": records_created,
                        "records_skipped": records_skipped,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"🏁 Completed Active Directory ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete ingestion run: {e}")

    def ingest_ad_groups_with_change_detection(self, full_sync: bool = False, dry_run: bool = False, batch_size: int = 500) -> Dict[str, Any]:
        """
        Ingest University of Michigan Active Directory groups using intelligent content hashing.

        This method:
        1. Fetches all group data from the Active Directory LDAP (LSA OU structure)
        2. Calculates content hashes for each group
        3. Compares against existing bronze records
        4. Only creates new records when content has actually changed
        5. Provides detailed statistics about group changes detected

        Args:
            full_sync: If True, process all records regardless of change status (not fully applicable here as we always fetch all, but signals intent)
            dry_run: If True, do not write to database
            batch_size: Number of records to process in a batch (not used for LDAP fetch, but could be for DB inserts)

        Returns:
            Dictionary with comprehensive ingestion statistics
        """
        # Create ingestion run for tracking
        run_metadata = {"full_sync": full_sync, "dry_run": dry_run}
        if not dry_run:
            run_id = self.create_ingestion_run("active_directory", "group", run_metadata)
        else:
            run_id = "DRY_RUN_" + str(uuid.uuid4())
            logger.info(f"🧪 Dry run mode enabled. Run ID: {run_id}")

        ingestion_stats = {
            "run_id": run_id,
            "records_processed": 0,
            "records_created": 0,
            "records_skipped_unchanged": 0,
            "new_groups": 0,
            "changed_groups": 0,
            "total_members": 0,
            "groups_with_members": 0,
            "groups_with_memberof": 0,
            "security_groups": 0,
            "distribution_groups": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info(
                "Starting Active Directory group ingestion with content hash change detection..."
            )

            # Step 1: Get existing group content hashes from bronze layer
            existing_hashes = {}
            if not full_sync:
                existing_hashes = self._get_existing_group_hashes()
            else:
                logger.info("🔄 Full sync requested - ignoring existing hashes")

            # Step 2: Fetch current data from Active Directory LDAP
            search_base = "OU=Users and Groups,OU=LSA,OU=Organizations,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu"
            logger.info(
                f"📥 Fetching group data from Active Directory LDAP ({search_base})..."
            )

            # Request all attributes for comprehensive group data
            # Note: using attributes=None returns all available attributes
            raw_groups = self.ldap_adapter.search_as_dicts(
                search_filter="(objectClass=group)",
                search_base=search_base,
                scope="subtree",
                attributes=None,  # Return all attributes
                use_pagination=True,
            )

            if not raw_groups:
                logger.warning("⚠️ No groups found in Active Directory LDAP")
                return ingestion_stats

            logger.info(
                f"📥 Retrieved {len(raw_groups)} groups from Active Directory LDAP"
            )

            # Step 3: Process each group with content hash change detection
            for group_data in raw_groups:
                try:
                    # Extract group identifiers
                    name = self._normalize_ldap_attribute(group_data.get("name"))
                    object_guid = self._normalize_ldap_attribute(
                        group_data.get("objectGUID")
                    )
                    sam_account_name = self._normalize_ldap_attribute(
                        group_data.get("sAMAccountName")
                    )

                    # Skip if no objectGUID (required as external_id)
                    if not object_guid:
                        logger.warning(
                            f"⚠️ Skipping group {name} - missing objectGUID attribute"
                        )
                        continue

                    # Track analytics for reporting
                    # Count members
                    members = group_data.get("member")
                    if members:
                        if isinstance(members, list):
                            member_count = len(members)
                        else:
                            member_count = 1
                        ingestion_stats["total_members"] += member_count
                        ingestion_stats["groups_with_members"] += 1

                    # Count memberOf (groups this group belongs to)
                    member_of = group_data.get("memberOf")
                    if member_of:
                        ingestion_stats["groups_with_memberof"] += 1

                    # Track group type (security vs distribution)
                    group_type = group_data.get("groupType")
                    if group_type:
                        # In AD, groupType is a bitmask
                        # -2147483646 = security group, universal scope
                        # -2147483644 = security group, domain local scope
                        # -2147483640 = security group, global scope
                        # Positive values are distribution groups
                        if isinstance(group_type, int):
                            if group_type < 0:
                                ingestion_stats["security_groups"] += 1
                            else:
                                ingestion_stats["distribution_groups"] += 1

                    # Calculate content hash for this group
                    current_hash = self._calculate_group_content_hash(group_data)

                    # Check if this group is new or has changed
                    existing_hash = existing_hashes.get(object_guid)

                    should_insert = False
                    if existing_hash is None:
                        # This is a completely new group
                        logger.info(
                            f"🆕 New group detected: {name} ({sam_account_name})"
                        )
                        should_insert = True
                        ingestion_stats["new_groups"] += 1

                    elif existing_hash != current_hash:
                        # This group exists but has changed
                        logger.info(
                            f"📝 Group changed: {name} ({sam_account_name})"
                        )
                        # logger.debug(f"   Old hash: {existing_hash}")
                        # logger.debug(f"   New hash: {current_hash}")
                        should_insert = True
                        ingestion_stats["changed_groups"] += 1

                    else:
                        # This group exists and hasn't changed - skip it
                        # logger.debug(
                        #     f"⏭️ Group unchanged, skipping: {name} ({sam_account_name})"
                        # )
                        should_insert = False
                        ingestion_stats["records_skipped_unchanged"] += 1

                    # Only insert if the group is new or changed
                    if should_insert:
                        if dry_run:
                            logger.info(f"[DRY RUN] Would insert group: {name} ({object_guid})")
                        else:
                            # Normalize all raw data for JSON serialization
                            # This converts datetime, bytes, and other non-JSON types
                            normalized_data = self._normalize_raw_data_for_json(group_data)

                            # Enhance with metadata for future reference
                            normalized_data["_content_hash"] = current_hash
                            normalized_data["_change_detection"] = "content_hash_based"
                            normalized_data["_ldap_server"] = "adsroot.itcs.umich.edu"
                            normalized_data["_search_base"] = search_base

                            # Insert into bronze layer using objectGUID as external_id
                            entity_id = self.db_adapter.insert_raw_entity(
                                entity_type="group",
                                source_system="active_directory",
                                external_id=object_guid,
                                raw_data=normalized_data,
                                ingestion_run_id=run_id,
                            )

                        ingestion_stats["records_created"] += 1

                    # Log progress periodically
                    if (
                        ingestion_stats["records_processed"] % 100 == 0
                        and ingestion_stats["records_processed"] > 0
                    ):
                        logger.info(
                            f"⏳ Progress: {ingestion_stats['records_processed']} groups processed "
                            f"({ingestion_stats['records_created']} new/changed, "
                            f"{ingestion_stats['records_skipped_unchanged']} unchanged)"
                        )

                except Exception as record_error:
                    name_safe = (
                        group_data.get("name", "unknown")
                        if "name" in group_data
                        else "unknown"
                    )
                    guid_safe = (
                        group_data.get("objectGUID", "unknown")
                        if "objectGUID" in group_data
                        else "unknown"
                    )
                    error_msg = f"Failed to process group {name_safe} (objectGUID: {guid_safe}): {record_error}"
                    logger.error(f"❌ {error_msg}")
                    ingestion_stats["errors"].append(error_msg)

                ingestion_stats["records_processed"] += 1

            # Complete the ingestion run
            error_summary = None
            if ingestion_stats["errors"]:
                error_summary = f"{len(ingestion_stats['errors'])} individual record errors occurred"

            if not dry_run:
                self.complete_ingestion_run(
                    run_id=run_id,
                    records_processed=ingestion_stats["records_processed"],
                    records_created=ingestion_stats["records_created"],
                    records_skipped=ingestion_stats["records_skipped_unchanged"],
                    error_message=error_summary,
                )

            ingestion_stats["completed_at"] = datetime.now(timezone.utc)
            duration = (
                ingestion_stats["completed_at"] - ingestion_stats["started_at"]
            ).total_seconds()

            # Log comprehensive results
            logger.info(
                f"✅ Active Directory group ingestion completed in {duration:.2f} seconds"
            )
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Total Processed: {ingestion_stats['records_processed']}")
            logger.info(f"   New Records Created: {ingestion_stats['records_created']}")
            logger.info(f"   ├─ New Groups: {ingestion_stats['new_groups']}")
            logger.info(f"   └─ Changed Groups: {ingestion_stats['changed_groups']}")
            logger.info(
                f"   Skipped (Unchanged): {ingestion_stats['records_skipped_unchanged']}"
            )
            logger.info(f"   Group Analytics:")
            logger.info(
                f"   ├─ Total Members (across all groups): {ingestion_stats['total_members']}"
            )
            logger.info(
                f"   ├─ Groups with Members: {ingestion_stats['groups_with_members']}"
            )
            logger.info(
                f"   ├─ Groups with MemberOf: {ingestion_stats['groups_with_memberof']}"
            )
            logger.info(f"   ├─ Security Groups: {ingestion_stats['security_groups']}")
            logger.info(
                f"   └─ Distribution Groups: {ingestion_stats['distribution_groups']}"
            )
            logger.info(f"   Errors: {len(ingestion_stats['errors'])}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"Active Directory group ingestion failed: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)

            if not dry_run:
                self.complete_ingestion_run(
                    run_id=run_id,
                    records_processed=ingestion_stats["records_processed"],
                    records_created=ingestion_stats["records_created"],
                    records_skipped=ingestion_stats["records_skipped_unchanged"],
                    error_message=error_msg,
                )

            raise

    def get_group_analytics(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze Active Directory group data from bronze layer.

        This provides insights into the group structure and can help
        identify patterns or anomalies in the group data.

        Returns:
            Dictionary containing DataFrames for different group analyses
        """
        try:
            # Query for group analytics using Active Directory LDAP fields
            analytics_query = """
            WITH latest_groups AS (
                SELECT
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'group'
                AND source_system = 'active_directory'
            )
            SELECT
                raw_data->>'name' as name,
                raw_data->>'sAMAccountName' as sam_account_name,
                raw_data->>'objectGUID' as object_guid,
                raw_data->>'description' as description,
                raw_data->>'groupType' as group_type,
                raw_data->>'distinguishedName' as distinguished_name,
                CASE
                    WHEN jsonb_typeof(raw_data->'member') = 'array'
                    THEN jsonb_array_length(raw_data->'member')
                    WHEN raw_data->>'member' IS NOT NULL AND raw_data->>'member' != ''
                    THEN 1
                    ELSE 0
                END as member_count,
                CASE
                    WHEN jsonb_typeof(raw_data->'memberOf') = 'array'
                    THEN jsonb_array_length(raw_data->'memberOf')
                    WHEN raw_data->>'memberOf' IS NOT NULL AND raw_data->>'memberOf' != ''
                    THEN 1
                    ELSE 0
                END as member_of_count
            FROM latest_groups
            WHERE row_num = 1
            ORDER BY name
            """

            analytics_df = self.db_adapter.query_to_dataframe(analytics_query)

            # Create summary analyses
            analyses = {}

            # Group size summary (by members)
            if not analytics_df.empty and "member_count" in analytics_df.columns:
                size_summary = (
                    analytics_df.groupby("member_count")
                    .size()
                    .reset_index(name="group_count")
                )
                analyses["size_summary"] = size_summary.sort_values(
                    "member_count", ascending=False
                )

            # Group type summary
            if not analytics_df.empty and "group_type" in analytics_df.columns:
                type_summary = (
                    analytics_df.groupby("group_type")
                    .size()
                    .reset_index(name="group_count")
                )
                analyses["type_summary"] = type_summary

            # Group features summary
            if not analytics_df.empty:
                features_summary = {
                    "total_groups": len(analytics_df),
                    "groups_with_members": (analytics_df["member_count"] > 0).sum(),
                    "groups_with_memberof": (analytics_df["member_of_count"] > 0).sum(),
                    "total_members": analytics_df["member_count"].sum(),
                    "avg_members_per_group": analytics_df["member_count"].mean(),
                    "max_members": analytics_df["member_count"].max(),
                }
                analyses["features_summary"] = pd.DataFrame([features_summary])

            # Full group list
            analyses["full_group_list"] = analytics_df

            logger.info(
                f"📊 Generated group analytics with {len(analytics_df)} groups from Active Directory"
            )
            return analyses

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to generate group analytics: {e}")
            raise


def main():
    """Main entry point for Active Directory group ingestion."""
    load_dotenv()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Ingest Active Directory groups into bronze layer')
    parser.add_argument('--full-sync', action='store_true', help='Force full sync (ignore existing hashes)')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without writing to database')
    parser.add_argument('--batch-size', type=int, default=500, help='Batch size for processing')
    args = parser.parse_args()

    # Get configuration from environment variables
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    # Active Directory LDAP configuration — sourced from environment variables.
    # On the production server, AD_PASSWORD is injected by systemd via
    # LoadCredential= and exported by the orchestrator shell script.
    # In local dev, set AD_PASSWORD (and optionally AD_KEYRING_SERVICE) in .env.
    ad_config = {
        "server": os.getenv("AD_SERVER", "adsroot.itcs.umich.edu"),
        "search_base": os.getenv(
            "AD_SEARCH_BASE",
            "OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
        ),
        "user": os.getenv("AD_USER"),
        "password": os.getenv("AD_PASSWORD"),
        "keyring_service": os.getenv("AD_KEYRING_SERVICE", "ldap_umich"),
        "port": int(os.getenv("AD_PORT", "636")),
        "use_ssl": True,
    }

    try:
        # Validate configuration
        if not database_url:
            raise ValueError("Missing required environment variable: DATABASE_URL")

        if not ad_config["user"]:
            raise ValueError("Missing required environment variable: AD_USER")

        # Create and run Active Directory ingestion service
        ingestion_service = ActiveDirectoryGroupIngestionService(
            database_url=database_url,
            ldap_config=ad_config,
            force_full_sync=args.full_sync,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
        )
        ingestion_service.ingest_ad_groups_with_change_detection(
            full_sync=args.full_sync,
            dry_run=args.dry_run,
            batch_size=args.batch_size
        )
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
