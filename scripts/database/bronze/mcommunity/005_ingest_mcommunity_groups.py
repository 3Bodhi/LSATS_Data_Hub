#!/usr/bin/env python3
"""
MCommunity LDAP Group Ingestion Service

This service ingests group data from the University of Michigan MCommunity LDAP
directory (ldap.umich.edu) into the bronze layer for cross-referencing and analysis.

MCommunity provides authoritative directory information for all U-M groups including:
- Group membership (member, groupMember attributes for people and nested groups)
- Group identifiers (gidNumber, cn)
- Group descriptions and metadata
- Email group settings (membersonly, joinable, etc.)
- Group ownership and administration

All group records are stored in ou=User Groups,ou=Groups,dc=umich,dc=edu with gidNumber
as the unique external identifier. Only groups with members are ingested.

IMPORTANT: Many LDAP attributes (member, groupMember, cn, owner, etc.) can be either
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


class MCommunityGroupIngestionService:
    """
    Group ingestion service for University of Michigan MCommunity LDAP directory.

    Uses content hashing for change detection since LDAP doesn't provide
    modification timestamps in a consistent way. This approach:

    1. Fetches current group data from MCommunity LDAP (ou=User Groups,ou=Groups)
    2. Calculates content hashes for each group
    3. Compares against stored hashes from previous ingestions
    4. Only creates new bronze records when group content has actually changed
    5. Preserves complete change history for group analysis

    Key Features:
    - Efficient change detection without requiring timestamps
    - Group membership tracking (member and groupMember attributes)
    - Handles multi-value LDAP attributes (member, groupMember, owner, etc.)
    - Tracks email group settings and permissions
    - Comprehensive audit trail for group changes
    - Detailed ingestion statistics and monitoring
    """

    def __init__(
        self,
        database_url: str,
        ldap_config: Dict[str, Any],
        force_full_sync: bool = False,
        dry_run: bool = False,
    ):
        """
        Initialize the MCommunity group ingestion service.

        Args:
            database_url: PostgreSQL connection string
            ldap_config: LDAP connection configuration dictionary
            force_full_sync: If True, bypass timestamp filtering and perform full sync
            dry_run: If True, preview changes without committing to database
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        # Initialize LDAP adapter for MCommunity
        self.ldap_adapter = LDAPAdapter(ldap_config)

        # Store full sync and dry run flags
        self.force_full_sync = force_full_sync
        self.dry_run = dry_run

        # Test LDAP connection
        if not self.ldap_adapter.test_connection():
            raise Exception("Failed to connect to MCommunity LDAP")

        logger.info(
            f"MCommunity group ingestion service initialized with content hashing "
            f"(force_full_sync={'enabled' if force_full_sync else 'disabled'}, "
            f"dry_run={'enabled' if dry_run else 'disabled'})"
        )

    def _normalize_ldap_attribute(self, value: Any) -> Any:
        """
        Normalize LDAP attribute values for consistent hashing and JSON serialization.

        LDAP attributes can be single values, lists, bytes, or None. Many MCommunity
        attributes like cn, ou, umichPostalAddress can be either strings or
        lists depending on the user.

        Binary fields are converted to base64 to avoid issues with null bytes in
        PostgreSQL JSON/JSONB fields.

        Args:
            value: Raw LDAP attribute value

        Returns:
            Normalized value suitable for JSON serialization
        """
        if value is None:
            return ""
        elif isinstance(value, bytes):
            # Handle binary attributes using base64 encoding
            try:
                # Try to decode as UTF-8 for string-like fields
                decoded = value.decode("utf-8")
                # Check if it contains null bytes or other problematic characters
                if "\x00" in decoded or any(
                    ord(c) < 32 and c not in "\t\n\r" for c in decoded
                ):
                    return base64.b64encode(value).decode("ascii")
                else:
                    return decoded.strip()
            except UnicodeDecodeError:
                return base64.b64encode(value).decode("ascii")
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, list):
            if len(value) == 0:
                return ""
            elif len(value) == 1:
                item = value[0]
                if isinstance(item, bytes):
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

    def _calculate_group_content_hash(self, group_data: Dict[str, Any]) -> str:
        """
        Calculate a content hash for MCommunity group data to detect meaningful changes.

        This hash represents the "content fingerprint" of the group record.
        We include all fields that would represent meaningful group changes based on
        the MCommunity LDAP schema.

        Args:
            group_data: Raw group data from MCommunity LDAP

        Returns:
            SHA-256 hash of the normalized group content
        """
        # Extract significant fields for change detection
        # Based on MCommunity LDAP schema for groups
        significant_fields = {
            # Core identifiers
            "cn": self._normalize_ldap_attribute(group_data.get("cn")),
            "gidNumber": self._normalize_ldap_attribute(group_data.get("gidNumber")),
            # Membership (people and nested groups)
            "member": self._normalize_ldap_attribute(group_data.get("member")),
            "groupMember": self._normalize_ldap_attribute(
                group_data.get("groupMember")
            ),
            "rfc822mail": self._normalize_ldap_attribute(
                group_data.get("rfc822mail")
            ),  # External email members
            # Group metadata
            "description": self._normalize_ldap_attribute(
                group_data.get("description")
            ),
            "postalAddress": self._normalize_ldap_attribute(
                group_data.get("postalAddress")
            ),
            "labeledUri": self._normalize_ldap_attribute(group_data.get("labeledUri")),
            # Group ownership and administration
            "owner": self._normalize_ldap_attribute(group_data.get("owner")),
            "errorsTo": self._normalize_ldap_attribute(group_data.get("errorsTo")),
            "requestsTo": self._normalize_ldap_attribute(group_data.get("requestsTo")),
            "rfc822ErrorsTo": self._normalize_ldap_attribute(
                group_data.get("rfc822ErrorsTo")
            ),
            "rfc822RequestsTo": self._normalize_ldap_attribute(
                group_data.get("rfc822RequestsTo")
            ),
            # Email group settings
            "umichGroupEmail": self._normalize_ldap_attribute(
                group_data.get("umichGroupEmail")
            ),
            "membersonly": self._normalize_ldap_attribute(
                group_data.get("membersonly")
            ),
            "joinable": self._normalize_ldap_attribute(group_data.get("joinable")),
            "RealtimeBlockList": self._normalize_ldap_attribute(
                group_data.get("RealtimeBlockList")
            ),
            "supressNoEmailError": self._normalize_ldap_attribute(
                group_data.get("supressNoEmailError")
            ),
            "permittedGroup": self._normalize_ldap_attribute(
                group_data.get("permittedGroup")
            ),
            "umichPermittedSenders": self._normalize_ldap_attribute(
                group_data.get("umichPermittedSenders")
            ),
            "umichPermittedSendersDomains": self._normalize_ldap_attribute(
                group_data.get("umichPermittedSendersDomains")
            ),
            # Privacy and access control
            "umichPrivate": self._normalize_ldap_attribute(
                group_data.get("umichPrivate")
            ),
            # Auto-reply settings
            "umichAutoReply": self._normalize_ldap_attribute(
                group_data.get("umichAutoReply")
            ),
            "umichAutoReplyStart": self._normalize_ldap_attribute(
                group_data.get("umichAutoReplyStart")
            ),
            "umichAutoReplyEnd": self._normalize_ldap_attribute(
                group_data.get("umichAutoReplyEnd")
            ),
            # Service entitlements
            "umichServiceEntitlement": self._normalize_ldap_attribute(
                group_data.get("umichServiceEntitlement")
            ),
            # Expiry and disabled status
            "umichExpiryTimestamp": self._normalize_ldap_attribute(
                group_data.get("umichExpiryTimestamp")
            ),
            "umichEntryDisabled": self._normalize_ldap_attribute(
                group_data.get("umichEntryDisabled")
            ),
            "umichDisabledTimestamp": self._normalize_ldap_attribute(
                group_data.get("umichDisabledTimestamp")
            ),
        }

        # Create normalized JSON for consistent hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )

        # Generate SHA-256 hash
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        cn = group_data.get("cn", "unknown")
        gid_number = self._normalize_ldap_attribute(group_data.get("gidNumber"))
        logger.debug(
            f"Content hash for group {cn} (gidNumber: {gid_number}): {content_hash}"
        )

        return content_hash

    def _get_existing_group_hashes(self) -> Dict[str, str]:
        """
        Retrieve the latest content hash for each MCommunity group from the bronze layer.

        This uses a window function to get only the most recent record for each
        group, allowing efficient comparison with new data.

        Returns:
            Dictionary mapping gidNumber -> latest_content_hash
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
                AND source_system = 'mcommunity_ldap'
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
                gid_number = row["external_id"]
                raw_data = row["raw_data"]  # JSONB comes back as dict
                content_hash = self._calculate_group_content_hash(raw_data)
                existing_hashes[gid_number] = content_hash

            logger.info(
                f"Retrieved content hashes for {len(existing_hashes)} existing MCommunity groups"
            )
            return existing_hashes

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve existing group hashes: {e}")
            raise

    def create_ingestion_run(self, source_system: str, entity_type: str) -> str:
        """Create a new ingestion run record for tracking purposes."""
        if self.dry_run:
            run_id = f"dry-run-{uuid.uuid4()}"
            logger.info(f"[DRY RUN] Would create ingestion run {run_id}")
            return run_id
        try:
            run_id = str(uuid.uuid4())

            # Metadata specific to MCommunity LDAP content hashing approach
            metadata = {
                "ingestion_type": "content_hash_based",
                "source_api": "mcommunity_ldap",
                "ldap_server": "ldap.umich.edu",
                "search_base": "ou=User Groups,ou=Groups,dc=umich,dc=edu",
                "search_filter": "(&(member=*)(gidNumber=*))",
                "change_detection_method": "sha256_content_hash",
                "includes_group_membership": True,
                "includes_nested_groups": True,
            }

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
                        "metadata": json.dumps(metadata),
                    },
                )

                conn.commit()

            logger.info(
                f"Created MCommunity ingestion run {run_id} for {source_system}/{entity_type}"
            )
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create ingestion run: {e}")
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
        if self.dry_run:
            logger.info(f"[DRY RUN] Would complete ingestion run {run_id}")
            return
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

            logger.info(f"Completed MCommunity ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete ingestion run: {e}")

    def ingest_mcommunity_groups_with_change_detection(self) -> Dict[str, Any]:
        """
        Ingest University of Michigan MCommunity groups using intelligent content hashing.

        This method:
        1. Fetches all group data from the MCommunity LDAP (ou=User Groups,ou=Groups)
        2. Calculates content hashes for each group
        3. Compares against existing bronze records
        4. Only creates new records when content has actually changed
        5. Provides detailed statistics about group changes detected

        Returns:
            Dictionary with comprehensive ingestion statistics
        """
        # Create ingestion run for tracking
        run_id = self.create_ingestion_run("mcommunity_ldap", "group")

        ingestion_stats = {
            "run_id": run_id,
            "records_processed": 0,
            "records_created": 0,
            "records_skipped_unchanged": 0,
            "new_groups": 0,
            "changed_groups": 0,
            "total_direct_members": 0,
            "total_group_members": 0,
            "total_external_members": 0,
            "groups_with_email": 0,
            "private_groups": 0,
            "joinable_groups": 0,
            "members_only_groups": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info(
                "Starting MCommunity group ingestion with content hash change detection..."
            )

            # Step 1: Get existing group content hashes from bronze layer
            existing_hashes = self._get_existing_group_hashes()

            # Step 2: Fetch current data from MCommunity LDAP
            logger.info(
                "Fetching group data from MCommunity LDAP (ou=User Groups,ou=Groups,dc=umich,dc=edu)..."
            )

            # Request all attributes for comprehensive group data
            # Note: using attributes=None returns all available attributes
            raw_groups = self.ldap_adapter.search_as_dicts(
                search_filter="(&(member=*)(gidNumber=*))",
                search_base="ou=User Groups,ou=Groups,dc=umich,dc=edu",
                scope="subtree",
                attributes=None,  # Return all attributes
                use_pagination=True,
            )

            if not raw_groups:
                logger.warning("No groups found in MCommunity LDAP")
                return ingestion_stats

            logger.info(f"Retrieved {len(raw_groups)} groups from MCommunity LDAP")

            # Step 3: Process each group with content hash change detection
            for group_data in raw_groups:
                try:
                    # Extract group identifiers
                    cn = self._normalize_ldap_attribute(group_data.get("cn"))
                    gid_number = self._normalize_ldap_attribute(
                        group_data.get("gidNumber")
                    )

                    # Skip if no gidNumber (required as external_id)
                    if not gid_number:
                        logger.warning(
                            f"Skipping group {cn} - missing gidNumber attribute"
                        )
                        continue

                    # Track analytics for reporting
                    # Count direct members (people)
                    members = group_data.get("member")
                    if members:
                        if isinstance(members, list):
                            member_count = len(members)
                        else:
                            member_count = 1
                        ingestion_stats["total_direct_members"] += member_count

                    # Count group members (nested groups)
                    group_members = group_data.get("groupMember")
                    if group_members:
                        if isinstance(group_members, list):
                            group_member_count = len(group_members)
                        else:
                            group_member_count = 1
                        ingestion_stats["total_group_members"] += group_member_count

                    # Count external email members
                    external_members = group_data.get("rfc822mail")
                    if external_members:
                        if isinstance(external_members, list):
                            external_count = len(external_members)
                        else:
                            external_count = 1
                        ingestion_stats["total_external_members"] += external_count

                    # Track group features
                    if group_data.get("umichGroupEmail"):
                        ingestion_stats["groups_with_email"] += 1

                    if (
                        self._normalize_ldap_attribute(group_data.get("umichPrivate"))
                        == "TRUE"
                    ):
                        ingestion_stats["private_groups"] += 1

                    if (
                        self._normalize_ldap_attribute(group_data.get("joinable"))
                        == "TRUE"
                    ):
                        ingestion_stats["joinable_groups"] += 1

                    if (
                        self._normalize_ldap_attribute(group_data.get("membersonly"))
                        == "TRUE"
                    ):
                        ingestion_stats["members_only_groups"] += 1

                    # Calculate content hash for this group
                    current_hash = self._calculate_group_content_hash(group_data)

                    # Check if this group is new or has changed
                    existing_hash = existing_hashes.get(gid_number)

                    if existing_hash is None:
                        # This is a completely new group
                        logger.info(
                            f"🆕 New group detected: {cn} (gidNumber: {gid_number})"
                        )
                        should_insert = True
                        ingestion_stats["new_groups"] += 1

                    elif existing_hash != current_hash:
                        # This group exists but has changed
                        logger.info(f"📝 Group changed: {cn} (gidNumber: {gid_number})")
                        logger.debug(f"   Old hash: {existing_hash}")
                        logger.debug(f"   New hash: {current_hash}")
                        should_insert = True
                        ingestion_stats["changed_groups"] += 1

                    else:
                        # This group exists and hasn't changed - skip it
                        logger.debug(
                            f"⏭️  Group unchanged, skipping: {cn} (gidNumber: {gid_number})"
                        )
                        should_insert = False
                        ingestion_stats["records_skipped_unchanged"] += 1

                    # Only insert if the group is new or changed
                    if should_insert:
                        if self.dry_run:
                            logger.info(f"[DRY RUN] Would insert group {gid_number}")
                        else:
                            # Enhance raw data with metadata for future reference
                            enhanced_raw_data = group_data.copy()
                            enhanced_raw_data["_content_hash"] = current_hash
                            enhanced_raw_data["_change_detection"] = "content_hash_based"
                            enhanced_raw_data["_ldap_server"] = "ldap.umich.edu"
                            enhanced_raw_data["_search_base"] = (
                                "ou=User Groups,ou=Groups,dc=umich,dc=edu"
                            )

                            # Insert into bronze layer using gidNumber as external_id
                            entity_id = self.db_adapter.insert_raw_entity(
                                entity_type="group",
                                source_system="mcommunity_ldap",
                                external_id=gid_number,
                                raw_data=enhanced_raw_data,
                                ingestion_run_id=run_id,
                            )

                        ingestion_stats["records_created"] += 1

                    # Log progress periodically
                    if (
                        ingestion_stats["records_processed"] % 100 == 0
                        and ingestion_stats["records_processed"] > 0
                    ):
                        logger.info(
                            f"Progress: {ingestion_stats['records_processed']} groups processed "
                            f"({ingestion_stats['records_created']} new/changed, "
                            f"{ingestion_stats['records_skipped_unchanged']} unchanged)"
                        )

                except Exception as record_error:
                    cn_safe = (
                        group_data.get("cn", "unknown")
                        if "cn" in group_data
                        else "unknown"
                    )
                    gid_safe = (
                        group_data.get("gidNumber", "unknown")
                        if "gidNumber" in group_data
                        else "unknown"
                    )
                    error_msg = f"Failed to process group {cn_safe} (gidNumber: {gid_safe}): {record_error}"
                    logger.error(error_msg)
                    ingestion_stats["errors"].append(error_msg)

                ingestion_stats["records_processed"] += 1

            # Complete the ingestion run
            error_summary = None
            if ingestion_stats["errors"]:
                error_summary = f"{len(ingestion_stats['errors'])} individual record errors occurred"

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
                f"🎉 MCommunity group ingestion completed in {duration:.2f} seconds"
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
                f"   ├─ Total Direct Members (people): {ingestion_stats['total_direct_members']}"
            )
            logger.info(
                f"   ├─ Total Group Members (nested): {ingestion_stats['total_group_members']}"
            )
            logger.info(
                f"   ├─ Total External Email Members: {ingestion_stats['total_external_members']}"
            )
            logger.info(
                f"   ├─ Groups with Email: {ingestion_stats['groups_with_email']}"
            )
            logger.info(f"   ├─ Private Groups: {ingestion_stats['private_groups']}")
            logger.info(f"   ├─ Joinable Groups: {ingestion_stats['joinable_groups']}")
            logger.info(
                f"   └─ Members-Only Groups: {ingestion_stats['members_only_groups']}"
            )
            logger.info(f"   Errors: {len(ingestion_stats['errors'])}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"MCommunity group ingestion failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

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
        Analyze MCommunity group data from bronze layer.

        This provides insights into the group structure and can help
        identify patterns or anomalies in the group data.

        Returns:
            Dictionary containing DataFrames for different group analyses
        """
        try:
            # Query for group analytics using MCommunity LDAP fields
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
                AND source_system = 'mcommunity_ldap'
            )
            SELECT
                raw_data->>'cn' as cn,
                raw_data->>'gidNumber' as gid_number,
                raw_data->>'description' as description,
                raw_data->>'umichGroupEmail' as email,
                raw_data->>'umichPrivate' as is_private,
                raw_data->>'joinable' as is_joinable,
                raw_data->>'membersonly' as is_members_only,
                CASE
                    WHEN jsonb_typeof(raw_data->'member') = 'array'
                    THEN jsonb_array_length(raw_data->'member')
                    WHEN raw_data->>'member' IS NOT NULL AND raw_data->>'member' != ''
                    THEN 1
                    ELSE 0
                END as direct_member_count,
                CASE
                    WHEN jsonb_typeof(raw_data->'groupMember') = 'array'
                    THEN jsonb_array_length(raw_data->'groupMember')
                    WHEN raw_data->>'groupMember' IS NOT NULL AND raw_data->>'groupMember' != ''
                    THEN 1
                    ELSE 0
                END as group_member_count,
                CASE
                    WHEN jsonb_typeof(raw_data->'rfc822mail') = 'array'
                    THEN jsonb_array_length(raw_data->'rfc822mail')
                    WHEN raw_data->>'rfc822mail' IS NOT NULL AND raw_data->>'rfc822mail' != ''
                    THEN 1
                    ELSE 0
                END as external_member_count
            FROM latest_groups
            WHERE row_num = 1
            ORDER BY cn
            """

            analytics_df = self.db_adapter.query_to_dataframe(analytics_query)

            # Create summary analyses
            analyses = {}

            # Group size summary (by direct members)
            if not analytics_df.empty and "direct_member_count" in analytics_df.columns:
                size_summary = (
                    analytics_df.groupby("direct_member_count")
                    .size()
                    .reset_index(name="group_count")
                )
                analyses["size_summary"] = size_summary.sort_values(
                    "direct_member_count", ascending=False
                )

            # Group features summary
            if not analytics_df.empty:
                features_summary = {
                    "total_groups": len(analytics_df),
                    "groups_with_email": analytics_df["email"].notna().sum(),
                    "private_groups": (analytics_df["is_private"] == "TRUE").sum(),
                    "joinable_groups": (analytics_df["is_joinable"] == "TRUE").sum(),
                    "members_only_groups": (
                        analytics_df["is_members_only"] == "TRUE"
                    ).sum(),
                    "total_direct_members": analytics_df["direct_member_count"].sum(),
                    "total_group_members": analytics_df["group_member_count"].sum(),
                    "total_external_members": analytics_df[
                        "external_member_count"
                    ].sum(),
                }
                analyses["features_summary"] = pd.DataFrame([features_summary])

            # Full group list
            analyses["full_group_list"] = analytics_df

            logger.info(
                f"Generated group analytics with {len(analytics_df)} groups from MCommunity"
            )
            return analyses

        except SQLAlchemyError as e:
            logger.error(f"Failed to generate group analytics: {e}")
            raise

    def get_group_change_history(self, gid_number: str) -> pd.DataFrame:
        """
        Get the complete change history for a specific MCommunity group.

        Args:
            gid_number: The MCommunity gidNumber

        Returns:
            DataFrame with all historical versions of the group
        """
        try:
            query = """
            SELECT
                raw_id,
                raw_data->>'cn' as cn,
                raw_data->>'description' as description,
                raw_data->>'umichGroupEmail' as email,
                CASE
                    WHEN jsonb_typeof(raw_data->'member') = 'array'
                    THEN jsonb_array_length(raw_data->'member')
                    WHEN raw_data->>'member' IS NOT NULL AND raw_data->>'member' != ''
                    THEN 1
                    ELSE 0
                END as direct_member_count,
                CASE
                    WHEN jsonb_typeof(raw_data->'groupMember') = 'array'
                    THEN jsonb_array_length(raw_data->'groupMember')
                    WHEN raw_data->>'groupMember' IS NOT NULL AND raw_data->>'groupMember' != ''
                    THEN 1
                    ELSE 0
                END as group_member_count,
                raw_data->>'_content_hash' as content_hash,
                ingested_at,
                ingestion_run_id
            FROM bronze.raw_entities
            WHERE entity_type = 'group'
            AND source_system = 'mcommunity_ldap'
            AND external_id = :gid_number
            ORDER BY ingested_at DESC
            """

            history_df = self.db_adapter.query_to_dataframe(
                query, {"gid_number": gid_number}
            )

            logger.info(
                f"Retrieved {len(history_df)} historical records for MCommunity group {gid_number}"
            )
            return history_df

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve group history: {e}")
            raise

    def close(self):
        """Clean up database and LDAP connections."""
        if self.db_adapter:
            self.db_adapter.close()
        if self.ldap_adapter:
            # LDAPAdapter doesn't have explicit close, connection is managed internally
            pass
        logger.info("MCommunity group ingestion service closed")


def main():
    """
    Main function to run MCommunity group ingestion from command line.
    """
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(
            description="Ingest MCommunity groups into bronze layer"
        )
        parser.add_argument(
            "--full-sync",
            action="store_true",
            help="Force full sync (bypass timestamp filtering - not applicable for MCommunity but kept for consistency)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without committing to database",
        )
        args = parser.parse_args()

        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv("DATABASE_URL")

        # MCommunity LDAP configuration.
        # On the production server, MCOMMUNITY_LDAP_PASSWORD is injected by systemd
        # via LoadCredential= and exported by the orchestrator shell script.
        # In local dev, set MCOMMUNITY_LDAP_PASSWORD in .env.
        ldap_config = {
            "server": os.getenv("MCOMMUNITY_LDAP_SERVER", "ldap.umich.edu"),
            "search_base": os.getenv("MCOMMUNITY_LDAP_BASE", "dc=umich,dc=edu"),
            "user": os.getenv("MCOMMUNITY_LDAP_USER"),
            "password": os.getenv("MCOMMUNITY_LDAP_PASSWORD"),
            "keyring_service": os.getenv("MCOMMUNITY_KEYRING_SERVICE", "Mcom_umich"),
            "port": int(os.getenv("MCOMMUNITY_LDAP_PORT", "636")),
            "use_ssl": os.getenv("MCOMMUNITY_LDAP_USE_SSL", "true").lower() == "true",
            "timeout": int(os.getenv("MCOMMUNITY_LDAP_TIMEOUT", "90")),
        }
        # Validate configuration
        if not database_url:
            raise ValueError("Missing required environment variable: DATABASE_URL")

        if not ldap_config["user"]:
            raise ValueError(
                "Missing required environment variable: MCOMMUNITY_LDAP_USER"
            )

        # Create and run MCommunity ingestion service
        ingestion_service = MCommunityGroupIngestionService(
            database_url=database_url,
            ldap_config=ldap_config,
            force_full_sync=args.full_sync,
            dry_run=args.dry_run,
        )

        # Run the content hash-based ingestion process
        print("👥 Starting MCommunity group ingestion with content hashing...")
        results = ingestion_service.ingest_mcommunity_groups_with_change_detection()

        # Display comprehensive summary
        print(f"\n📊 MCommunity Group Ingestion Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Total Groups Processed: {results['records_processed']}")
        print(f"   New Records Created: {results['records_created']}")
        print(f"     ├─ Brand New Groups: {results['new_groups']}")
        print(f"     └─ Groups with Changes: {results['changed_groups']}")
        print(f"   Skipped (No Changes): {results['records_skipped_unchanged']}")
        print(f"   Group Analytics:")
        print(
            f"     ├─ Total Direct Members (people): {results['total_direct_members']}"
        )
        print(f"     ├─ Total Group Members (nested): {results['total_group_members']}")
        print(
            f"     ├─ Total External Email Members: {results['total_external_members']}"
        )
        print(f"     ├─ Groups with Email: {results['groups_with_email']}")
        print(f"     ├─ Private Groups: {results['private_groups']}")
        print(f"     ├─ Joinable Groups: {results['joinable_groups']}")
        print(f"     └─ Members-Only Groups: {results['members_only_groups']}")
        print(f"   Errors: {len(results['errors'])}")

        if results["records_skipped_unchanged"] > 0:
            efficiency_percentage = (
                results["records_skipped_unchanged"] / results["records_processed"]
            ) * 100
            print(
                f"\n⚡ Efficiency: {efficiency_percentage:.1f}% of groups were unchanged and skipped"
            )

        # Show group analytics
        print("\n🏗️  Analyzing group data...")
        group_analyses = ingestion_service.get_group_analytics()

        # Group size distribution
        if "size_summary" in group_analyses:
            print("\n👥 Top 20 Group Sizes by Member Count:")
            size_summary = group_analyses["size_summary"]
            for _, row in size_summary.head(20).iterrows():
                print(
                    f"   - {row['direct_member_count']} members: {row['group_count']} groups"
                )

            if len(size_summary) > 20:
                remaining_count = size_summary.iloc[20:]["group_count"].sum()
                print(f"   - ... and {remaining_count} more size categories")

        # Features summary
        if "features_summary" in group_analyses:
            print("\n📈 Overall Group Statistics:")
            features = group_analyses["features_summary"].iloc[0]
            print(f"   - Total Groups: {features['total_groups']}")
            print(f"   - Groups with Email: {features['groups_with_email']}")
            print(f"   - Private Groups: {features['private_groups']}")
            print(f"   - Joinable Groups: {features['joinable_groups']}")
            print(f"   - Members-Only Groups: {features['members_only_groups']}")
            print(
                f"   - Total Direct Members: {features['total_direct_members']} (across all groups)"
            )
            print(
                f"   - Total Group Members: {features['total_group_members']} (nested groups)"
            )
            print(
                f"   - Total External Members: {features['total_external_members']} (email addresses)"
            )

        # Clean up
        ingestion_service.close()

        print("\n✅ MCommunity group ingestion completed successfully!")

    except Exception as e:
        logger.error(f"MCommunity group ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
