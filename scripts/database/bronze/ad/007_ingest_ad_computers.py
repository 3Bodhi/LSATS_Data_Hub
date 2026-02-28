#!/usr/bin/env python3
"""
Active Directory LDAP Computer Ingestion Service

This service ingests computer data from the University of Michigan Active Directory LDAP
directory (adsroot.itcs.umich.edu) into the bronze layer for cross-referencing and analysis.

Active Directory provides authoritative directory information for all LSA computers including:
- Computer identifiers (objectGUID, sAMAccountName, objectSid, dNSHostName)
- Computer operating system and version
- Computer group memberships (memberOf attribute)
- Computer timestamps (pwdLastSet, lastLogon, lastLogonTimestamp)
- Computer metadata (distinguishedName, organizational unit structure)

All computer records are stored in the LSA OU structure with objectGUID as the unique
external identifier. Computers are filtered by objectClass=computer.

IMPORTANT: Many LDAP attributes can be either strings or lists of strings depending on
the computer record. The normalization functions handle this appropriately.
"""

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

# Set up logging (will be reconfigured in main() with proper directory)
logger = logging.getLogger(__name__)


class ActiveDirectoryComputerIngestionService:
    """
    Computer ingestion service for University of Michigan Active Directory LDAP directory.

    Uses content hashing for change detection since LDAP doesn't provide
    modification timestamps in a consistent way. This approach:

    1. Fetches current computer data from Active Directory LDAP (LSA OU structure)
    2. Calculates content hashes for each computer
    3. Compares against stored hashes from previous ingestions
    4. Only creates new bronze records when computer content has actually changed
    5. Preserves complete change history for computer analysis

    Key Features:
    - Efficient change detection without requiring timestamps
    - Computer group membership tracking (memberOf attribute)
    - Handles multi-value LDAP attributes
    - Tracks operating systems and versions
    - Tracks last logon and password set times
    - Comprehensive audit trail for computer changes
    - Detailed ingestion statistics and monitoring
    """

    def __init__(self, database_url: str, ldap_config: Dict[str, Any]):
        """
        Initialize the Active Directory computer ingestion service.

        Args:
            database_url: PostgreSQL connection string
            ldap_config: LDAP connection configuration dictionary
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        # Initialize LDAP adapter for Active Directory
        self.ldap_adapter = LDAPAdapter(ldap_config)

        # Test LDAP connection
        if not self.ldap_adapter.test_connection():
            raise Exception("Failed to connect to Active Directory LDAP")

        logger.info(
            "Active Directory computer ingestion service initialized with content hashing"
        )

    def _normalize_ldap_attribute(self, value: Any) -> Any:
        """
        Normalize LDAP attribute values for consistent hashing and JSON serialization.

        LDAP attributes can be single values, lists, bytes, or None. Many Active Directory
        computer attributes like memberOf, servicePrincipalName can be either strings
        or lists depending on the computer.

        Binary fields (userCertificate, msDS-KeyCredentialLink, etc.) are converted to base64
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
            # Fields like userCertificate, msDS-KeyCredentialLink contain binary data
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

    def _calculate_computer_content_hash(self, computer_data: Dict[str, Any]) -> str:
        """
        Calculate a content hash for Active Directory computer data to detect meaningful changes.

        This hash represents the "content fingerprint" of the computer record.
        We include all fields that would represent meaningful computer changes based on
        the Active Directory LDAP schema.

        Args:
            computer_data: Raw computer data from Active Directory LDAP

        Returns:
            SHA-256 hash of the normalized computer content
        """
        # Extract significant fields for change detection
        # Based on Active Directory LDAP schema for computers
        significant_fields = {
            # Core identifiers
            "name": self._normalize_ldap_attribute(computer_data.get("name")),
            "cn": self._normalize_ldap_attribute(computer_data.get("cn")),
            "sAMAccountName": self._normalize_ldap_attribute(
                computer_data.get("sAMAccountName")
            ),
            "distinguishedName": self._normalize_ldap_attribute(
                computer_data.get("distinguishedName")
            ),
            "objectGUID": self._normalize_ldap_attribute(
                computer_data.get("objectGUID")
            ),
            "objectSid": self._normalize_ldap_attribute(computer_data.get("objectSid")),
            "dNSHostName": self._normalize_ldap_attribute(
                computer_data.get("dNSHostName")
            ),
            # Computer group memberships
            "memberOf": self._normalize_ldap_attribute(computer_data.get("memberOf")),
            # Operating system information
            "operatingSystem": self._normalize_ldap_attribute(
                computer_data.get("operatingSystem")
            ),
            "operatingSystemVersion": self._normalize_ldap_attribute(
                computer_data.get("operatingSystemVersion")
            ),
            # Computer account control and type
            "userAccountControl": self._normalize_ldap_attribute(
                computer_data.get("userAccountControl")
            ),
            "sAMAccountType": self._normalize_ldap_attribute(
                computer_data.get("sAMAccountType")
            ),
            # Timestamps (ONLY include meaningful business timestamps)
            # EXCLUDED: lastLogon, lastLogonTimestamp, badPasswordTime (change on every login)
            # EXCLUDED: whenChanged (changes automatically without meaningful updates)
            "pwdLastSet": self._normalize_ldap_attribute(
                computer_data.get("pwdLastSet")
            ),
            "whenCreated": self._normalize_ldap_attribute(
                computer_data.get("whenCreated")
            ),
            "accountExpires": self._normalize_ldap_attribute(
                computer_data.get("accountExpires")
            ),
            # Service principal names
            "servicePrincipalName": self._normalize_ldap_attribute(
                computer_data.get("servicePrincipalName")
            ),
            # Computer category and object class
            "objectCategory": self._normalize_ldap_attribute(
                computer_data.get("objectCategory")
            ),
            "objectClass": self._normalize_ldap_attribute(
                computer_data.get("objectClass")
            ),
            # EXCLUDED: uSNCreated, uSNChanged (auto-increment on every AD change)
            # EXCLUDED: dSCorePropagationData (replication metadata, not business data)
            # EXCLUDED: logonCount, badPwdCount (change frequently without meaningful updates)
            # Instance type
            "instanceType": self._normalize_ldap_attribute(
                computer_data.get("instanceType")
            ),
            # Primary group
            "primaryGroupID": self._normalize_ldap_attribute(
                computer_data.get("primaryGroupID")
            ),
            # Certificate and key credentials
            "userCertificate": self._normalize_ldap_attribute(
                computer_data.get("userCertificate")
            ),
            "msDS-KeyCredentialLink": self._normalize_ldap_attribute(
                computer_data.get("msDS-KeyCredentialLink")
            ),
            # Additional metadata
            "isCriticalSystemObject": self._normalize_ldap_attribute(
                computer_data.get("isCriticalSystemObject")
            ),
            "localPolicyFlags": self._normalize_ldap_attribute(
                computer_data.get("localPolicyFlags")
            ),
            "msDS-SupportedEncryptionTypes": self._normalize_ldap_attribute(
                computer_data.get("msDS-SupportedEncryptionTypes")
            ),
            "countryCode": self._normalize_ldap_attribute(
                computer_data.get("countryCode")
            ),
            "codePage": self._normalize_ldap_attribute(computer_data.get("codePage")),
            # LAPS password expiration (if used)
            "ms-Mcs-AdmPwdExpirationTime": self._normalize_ldap_attribute(
                computer_data.get("ms-Mcs-AdmPwdExpirationTime")
            ),
        }

        # Create normalized JSON for consistent hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )

        # Generate SHA-256 hash
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        name = computer_data.get("name", "unknown")
        object_guid = self._normalize_ldap_attribute(computer_data.get("objectGUID"))
        logger.debug(
            f"Content hash for computer {name} (objectGUID: {object_guid}): {content_hash}"
        )

        return content_hash

    def _get_last_ingestion_timestamp(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful ingestion run.

        Returns:
            Timestamp of last successful run, or None if no previous runs
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'active_directory'
            AND entity_type = 'computer'
            AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last successful ingestion: {last_timestamp}")
                return last_timestamp
            else:
                logger.info("📅 No previous successful ingestion found")
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not retrieve last ingestion timestamp: {e}")
            return None

    def _get_existing_computer_hashes(self) -> Dict[str, str]:
        """
        Retrieve the latest content hash for each Active Directory computer from the bronze layer.

        This uses a window function to get only the most recent record for each
        computer, allowing efficient comparison with new data.

        Returns:
            Dictionary mapping objectGUID -> latest_content_hash
        """
        try:
            # Query to get the most recent record for each computer
            query = """
            WITH latest_computers AS (
                SELECT
                    external_id,
                    raw_data,
                    ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'computer'
                AND source_system = 'active_directory'
            )
            SELECT
                external_id,
                raw_data
            FROM latest_computers
            WHERE row_num = 1
            """

            results_df = self.db_adapter.query_to_dataframe(query)

            # Calculate content hashes for existing records
            existing_hashes = {}
            for _, row in results_df.iterrows():
                object_guid = row["external_id"]
                raw_data = row["raw_data"]  # JSONB comes back as dict
                content_hash = self._calculate_computer_content_hash(raw_data)
                existing_hashes[object_guid] = content_hash

            logger.info(
                f"Retrieved content hashes for {len(existing_hashes)} existing Active Directory computers"
            )
            return existing_hashes

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve existing computer hashes: {e}")
            raise

    def create_ingestion_run(self, source_system: str, entity_type: str) -> str:
        """Create a new ingestion run record for tracking purposes."""
        try:
            run_id = str(uuid.uuid4())

            # Metadata specific to Active Directory LDAP content hashing approach
            metadata = {
                "ingestion_type": "content_hash_based",
                "source_api": "active_directory_ldap",
                "ldap_server": "adsroot.itcs.umich.edu",
                "search_base": "OU=LSA,OU=Organizations,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
                "search_filter": "(objectClass=computer)",
                "change_detection_method": "sha256_content_hash",
                "includes_group_membership": True,
                "includes_os_information": True,
            }

            with self.db_adapter.engine.connect() as conn:
                # Mark any stale 'running' runs as failed before starting a new one.
                # Stale runs occur when a process is OOM-killed or force-stopped before
                # it can update its own status.
                conn.execute(text("""
                    UPDATE meta.ingestion_runs
                    SET status = 'failed',
                        completed_at = NOW(),
                        error_message = 'stale - process terminated before completing (OOM kill or force stop)'
                    WHERE source_system = :source_system
                      AND entity_type = :entity_type
                      AND status = 'running'
                """), {"source_system": source_system, "entity_type": entity_type})

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
                f"Created Active Directory ingestion run {run_id} for {source_system}/{entity_type}"
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

            logger.info(f"Completed Active Directory ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete ingestion run: {e}")

    def ingest_incremental(
        self, full_sync: bool = False, dry_run: bool = False, batch_size: int = 500
    ) -> Dict[str, Any]:
        """
        Ingest University of Michigan Active Directory computers using intelligent content hashing.

        This method:
        1. Determines incremental vs full sync mode
        2. Fetches computer data from Active Directory LDAP (LSA OU structure)
        3. Calculates content hashes for each computer
        4. Compares against existing bronze records
        5. Only creates new records when content has actually changed
        6. Supports dry-run mode for preview without commits

        Args:
            full_sync: If True, process all computers. If False, use incremental mode.
            dry_run: If True, preview changes without committing to database.
            batch_size: Number of records to process per batch.

        Returns:
            Dictionary with comprehensive ingestion statistics
        """
        # Get last ingestion timestamp (unless full sync)
        last_timestamp = None if full_sync else self._get_last_ingestion_timestamp()

        if full_sync:
            logger.info("🔄 Full sync mode: Processing ALL computers")
        elif last_timestamp:
            logger.info(
                f"⚡ Incremental mode: Processing computers since {last_timestamp}"
            )
        else:
            logger.info("🆕 First run: Processing ALL computers")

        # Create ingestion run for tracking
        run_id = self.create_ingestion_run("active_directory", "computer")

        ingestion_stats = {
            "run_id": run_id,
            "records_processed": 0,
            "records_created": 0,
            "records_skipped_unchanged": 0,
            "new_computers": 0,
            "changed_computers": 0,
            "computers_with_groups": 0,
            "total_group_memberships": 0,
            "windows_10_count": 0,
            "windows_11_count": 0,
            "windows_server_count": 0,
            "other_os_count": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info(
                "Starting Active Directory computer ingestion with content hash change detection..."
            )

            # Step 1: Get existing computer content hashes from bronze layer
            existing_hashes = self._get_existing_computer_hashes()

            # Step 2: Fetch current data from Active Directory LDAP
            search_base = (
                "OU=LSA,OU=Organizations,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu"
            )
            logger.info(
                f"Fetching computer data from Active Directory LDAP ({search_base})..."
            )

            # Request all attributes for comprehensive computer data
            # Note: using attributes=None returns all available attributes
            raw_computers = self.ldap_adapter.search_as_dicts(
                search_filter="(objectClass=computer)",
                search_base=search_base,
                scope="subtree",
                attributes=None,  # Return all attributes
                use_pagination=True,
            )

            if not raw_computers:
                logger.warning("No computers found in Active Directory LDAP")
                return ingestion_stats

            logger.info(
                f"Retrieved {len(raw_computers)} computers from Active Directory LDAP"
            )

            # Step 3: Process each computer with content hash change detection
            # Collect entities for batch insertion
            entities_to_insert = []

            for computer_data in raw_computers:
                try:
                    # Extract computer identifiers
                    name = self._normalize_ldap_attribute(computer_data.get("name"))
                    object_guid = self._normalize_ldap_attribute(
                        computer_data.get("objectGUID")
                    )
                    sam_account_name = self._normalize_ldap_attribute(
                        computer_data.get("sAMAccountName")
                    )
                    dns_hostname = self._normalize_ldap_attribute(
                        computer_data.get("dNSHostName")
                    )

                    # Skip if no objectGUID (required as external_id)
                    if not object_guid:
                        logger.warning(
                            f"Skipping computer {name} - missing objectGUID attribute"
                        )
                        continue

                    # Track analytics for reporting
                    # Count group memberships
                    member_of = computer_data.get("memberOf")
                    if member_of:
                        if isinstance(member_of, list):
                            membership_count = len(member_of)
                        else:
                            membership_count = 1
                        ingestion_stats["total_group_memberships"] += membership_count
                        ingestion_stats["computers_with_groups"] += 1

                    # Track operating system types
                    operating_system = computer_data.get("operatingSystem")
                    if operating_system:
                        os_str = str(operating_system).lower()
                        if "windows 10" in os_str:
                            ingestion_stats["windows_10_count"] += 1
                        elif "windows 11" in os_str:
                            ingestion_stats["windows_11_count"] += 1
                        elif "server" in os_str:
                            ingestion_stats["windows_server_count"] += 1
                        else:
                            ingestion_stats["other_os_count"] += 1

                    # Calculate content hash for this computer
                    current_hash = self._calculate_computer_content_hash(computer_data)

                    # Check if this computer is new or has changed
                    existing_hash = existing_hashes.get(object_guid)

                    if existing_hash is None:
                        # This is a completely new computer
                        logger.info(
                            f"New computer detected: {name} ({sam_account_name}, objectGUID: {object_guid})"
                        )
                        should_insert = True
                        ingestion_stats["new_computers"] += 1

                    elif existing_hash != current_hash:
                        # This computer exists but has changed
                        logger.info(
                            f"Computer changed: {name} ({sam_account_name}, objectGUID: {object_guid})"
                        )
                        logger.debug(f"   Old hash: {existing_hash}")
                        logger.debug(f"   New hash: {current_hash}")
                        should_insert = True
                        ingestion_stats["changed_computers"] += 1

                    else:
                        # This computer exists and hasn't changed - skip it
                        logger.debug(
                            f"Computer unchanged, skipping: {name} ({sam_account_name}, objectGUID: {object_guid})"
                        )
                        should_insert = False
                        ingestion_stats["records_skipped_unchanged"] += 1

                    # Only insert if the computer is new or changed
                    if should_insert:
                        # Normalize all raw data for JSON serialization
                        # This converts datetime, bytes, and other non-JSON types
                        normalized_data = self._normalize_raw_data_for_json(
                            computer_data
                        )

                        # Enhance with metadata for future reference
                        normalized_data["_content_hash"] = current_hash
                        normalized_data["_change_detection"] = "content_hash_based"
                        normalized_data["_ldap_server"] = "adsroot.itcs.umich.edu"
                        normalized_data["_search_base"] = search_base

                        if dry_run:
                            # Dry-run mode: log what would be inserted
                            logger.info(
                                f"[DRY RUN] Would insert: {name} ({sam_account_name}, objectGUID: {object_guid})"
                            )
                            logger.debug(f"[DRY RUN] Content hash: {current_hash}")
                            ingestion_stats["records_created"] += 1
                        else:
                            # Collect entity for batch insertion
                            entities_to_insert.append(
                                {
                                    "entity_type": "computer",
                                    "source_system": "active_directory",
                                    "external_id": object_guid,
                                    "raw_data": normalized_data,
                                    "ingestion_run_id": run_id,
                                }
                            )

                            # Perform batch insert when we reach batch_size
                            if len(entities_to_insert) >= batch_size:
                                inserted_count = (
                                    self.db_adapter.bulk_insert_raw_entities(
                                        entities_to_insert, batch_size=batch_size
                                    )
                                )
                                ingestion_stats["records_created"] += inserted_count
                                logger.info(
                                    f"💾 Batch inserted {inserted_count} computer records"
                                )
                                entities_to_insert = []

                    # Log progress periodically
                    if (
                        ingestion_stats["records_processed"] % 100 == 0
                        and ingestion_stats["records_processed"] > 0
                    ):
                        logger.info(
                            f"Progress: {ingestion_stats['records_processed']} computers processed "
                            f"({ingestion_stats['records_created']} new/changed, "
                            f"{ingestion_stats['records_skipped_unchanged']} unchanged)"
                        )

                except Exception as record_error:
                    name_safe = (
                        computer_data.get("name", "unknown")
                        if "name" in computer_data
                        else "unknown"
                    )
                    guid_safe = (
                        computer_data.get("objectGUID", "unknown")
                        if "objectGUID" in computer_data
                        else "unknown"
                    )
                    error_msg = f"Failed to process computer {name_safe} (objectGUID: {guid_safe}): {record_error}"
                    logger.error(error_msg)
                    ingestion_stats["errors"].append(error_msg)

                ingestion_stats["records_processed"] += 1

            # Insert any remaining entities in the final batch
            if not dry_run and entities_to_insert:
                inserted_count = self.db_adapter.bulk_insert_raw_entities(
                    entities_to_insert, batch_size=batch_size
                )
                ingestion_stats["records_created"] += inserted_count
                logger.info(
                    f"💾 Final batch inserted {inserted_count} computer records"
                )

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
                f"Active Directory computer ingestion completed in {duration:.2f} seconds"
            )
            logger.info(f"Results Summary:")
            logger.info(f"   Total Processed: {ingestion_stats['records_processed']}")
            logger.info(f"   New Records Created: {ingestion_stats['records_created']}")
            logger.info(f"   ├─ New Computers: {ingestion_stats['new_computers']}")
            logger.info(
                f"   └─ Changed Computers: {ingestion_stats['changed_computers']}"
            )
            logger.info(
                f"   Skipped (Unchanged): {ingestion_stats['records_skipped_unchanged']}"
            )
            logger.info(f"   Computer Analytics:")
            logger.info(
                f"   ├─ Total Group Memberships: {ingestion_stats['total_group_memberships']}"
            )
            logger.info(
                f"   ├─ Computers with Groups: {ingestion_stats['computers_with_groups']}"
            )
            logger.info(f"   ├─ Windows 10: {ingestion_stats['windows_10_count']}")
            logger.info(f"   ├─ Windows 11: {ingestion_stats['windows_11_count']}")
            logger.info(
                f"   ├─ Windows Server: {ingestion_stats['windows_server_count']}"
            )
            logger.info(f"   └─ Other OS: {ingestion_stats['other_os_count']}")
            logger.info(f"   Errors: {len(ingestion_stats['errors'])}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"Active Directory computer ingestion failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats["records_processed"],
                records_created=ingestion_stats["records_created"],
                records_skipped=ingestion_stats["records_skipped_unchanged"],
                error_message=error_msg,
            )

            raise

    def get_computer_analytics(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze Active Directory computer data from bronze layer.

        This provides insights into the computer fleet and can help
        identify patterns or anomalies in the computer data.

        Returns:
            Dictionary containing DataFrames for different computer analyses
        """
        try:
            # Query for computer analytics using Active Directory LDAP fields
            analytics_query = """
            WITH latest_computers AS (
                SELECT
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'computer'
                AND source_system = 'active_directory'
            )
            SELECT
                raw_data->>'name' as name,
                raw_data->>'sAMAccountName' as sam_account_name,
                raw_data->>'objectGUID' as object_guid,
                raw_data->>'dNSHostName' as dns_hostname,
                raw_data->>'operatingSystem' as operating_system,
                raw_data->>'operatingSystemVersion' as os_version,
                raw_data->>'distinguishedName' as distinguished_name,
                raw_data->>'lastLogonTimestamp' as last_logon_timestamp,
                raw_data->>'pwdLastSet' as pwd_last_set,
                CASE
                    WHEN jsonb_typeof(raw_data->'memberOf') = 'array'
                    THEN jsonb_array_length(raw_data->'memberOf')
                    WHEN raw_data->>'memberOf' IS NOT NULL AND raw_data->>'memberOf' != ''
                    THEN 1
                    ELSE 0
                END as group_membership_count
            FROM latest_computers
            WHERE row_num = 1
            ORDER BY name
            """

            analytics_df = self.db_adapter.query_to_dataframe(analytics_query)

            # Create summary analyses
            analyses = {}

            # Operating system distribution
            if not analytics_df.empty and "operating_system" in analytics_df.columns:
                os_summary = (
                    analytics_df.groupby("operating_system")
                    .size()
                    .reset_index(name="computer_count")
                )
                analyses["os_summary"] = os_summary.sort_values(
                    "computer_count", ascending=False
                )

            # Group membership distribution
            if (
                not analytics_df.empty
                and "group_membership_count" in analytics_df.columns
            ):
                membership_summary = (
                    analytics_df.groupby("group_membership_count")
                    .size()
                    .reset_index(name="computer_count")
                )
                analyses["membership_summary"] = membership_summary.sort_values(
                    "group_membership_count", ascending=False
                )

            # Computer features summary
            if not analytics_df.empty:
                features_summary = {
                    "total_computers": len(analytics_df),
                    "computers_with_groups": (
                        analytics_df["group_membership_count"] > 0
                    ).sum(),
                    "total_group_memberships": analytics_df[
                        "group_membership_count"
                    ].sum(),
                    "avg_groups_per_computer": analytics_df[
                        "group_membership_count"
                    ].mean(),
                    "max_groups": analytics_df["group_membership_count"].max(),
                    "computers_with_dns": analytics_df["dns_hostname"].notna().sum(),
                }
                analyses["features_summary"] = pd.DataFrame([features_summary])

            # Full computer list
            analyses["full_computer_list"] = analytics_df

            logger.info(
                f"Generated computer analytics with {len(analytics_df)} computers from Active Directory"
            )
            return analyses

        except SQLAlchemyError as e:
            logger.error(f"Failed to generate computer analytics: {e}")
            raise

    def get_computer_change_history(self, object_guid: str) -> pd.DataFrame:
        """
        Get the complete change history for a specific Active Directory computer.

        Args:
            object_guid: The Active Directory objectGUID

        Returns:
            DataFrame with all historical versions of the computer
        """
        try:
            query = """
            SELECT
                raw_id,
                raw_data->>'name' as name,
                raw_data->>'sAMAccountName' as sam_account_name,
                raw_data->>'dNSHostName' as dns_hostname,
                raw_data->>'operatingSystem' as operating_system,
                raw_data->>'operatingSystemVersion' as os_version,
                CASE
                    WHEN jsonb_typeof(raw_data->'memberOf') = 'array'
                    THEN jsonb_array_length(raw_data->'memberOf')
                    WHEN raw_data->>'memberOf' IS NOT NULL AND raw_data->>'memberOf' != ''
                    THEN 1
                    ELSE 0
                END as group_membership_count,
                raw_data->>'_content_hash' as content_hash,
                ingested_at,
                ingestion_run_id
            FROM bronze.raw_entities
            WHERE entity_type = 'computer'
            AND source_system = 'active_directory'
            AND external_id = :object_guid
            ORDER BY ingested_at DESC
            """

            history_df = self.db_adapter.query_to_dataframe(
                query, {"object_guid": object_guid}
            )

            logger.info(
                f"Retrieved {len(history_df)} historical records for Active Directory computer {object_guid}"
            )
            return history_df

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve computer history: {e}")
            raise

    def close(self):
        """Clean up database and LDAP connections."""
        if self.db_adapter:
            self.db_adapter.close()
        if self.ldap_adapter:
            # LDAPAdapter doesn't have explicit close, connection is managed internally
            pass
        logger.info("Active Directory computer ingestion service closed")


def main():
    """
    Main function to run Active Directory computer ingestion from command line.
    """
    # Parse command-line arguments
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest Active Directory computer data into bronze layer"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all computers (ignore incremental timestamp)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of records to process per batch (default: 500)",
    )
    args = parser.parse_args()

    try:
        # Ensure logs directory exists
        log_dir = "/var/log/lsats/bronze"
        os.makedirs(log_dir, exist_ok=True)

        # Configure logging with layer-specific directory
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(f"{log_dir}/007_ingest_ad_computers.log"),
                logging.StreamHandler(sys.stdout),
            ],
            force=True,  # Override any existing configuration
        )

        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv("DATABASE_URL")

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

        # Validate configuration
        if not database_url:
            raise ValueError("Missing required environment variable: DATABASE_URL")

        if not ad_config["user"]:
            raise ValueError("Missing required environment variable: AD_USER")

        # Create and run Active Directory ingestion service
        ingestion_service = ActiveDirectoryComputerIngestionService(
            database_url=database_url, ldap_config=ad_config
        )

        # Run the ingestion process
        logger.info("=" * 80)
        logger.info("🚀 Starting Active Directory computer ingestion")
        logger.info(f"   Mode: {'FULL SYNC' if args.full_sync else 'INCREMENTAL'}")
        logger.info(f"   Dry Run: {args.dry_run}")
        logger.info(f"   Batch Size: {args.batch_size}")
        logger.info("=" * 80)

        results = ingestion_service.ingest_incremental(
            full_sync=args.full_sync, dry_run=args.dry_run, batch_size=args.batch_size
        )

        # Calculate duration
        duration = (
            results.get("completed_at", datetime.now(timezone.utc))
            - results["started_at"]
        ).total_seconds()

        # Display comprehensive summary
        print("\n" + "=" * 80)
        print("📊 ACTIVE DIRECTORY COMPUTER INGESTION SUMMARY")
        print("=" * 80)
        print(f"Run ID:              {results['run_id']}")
        print(
            f"Mode:                {'FULL SYNC' if args.full_sync else 'INCREMENTAL'}"
        )
        print(f"Records Processed:   {results['records_processed']:>6,}")
        print(f"Records Created:     {results['records_created']:>6,}")
        print(f"  ├─ New Computers:  {results['new_computers']:>6,}")
        print(f"  └─ Changed:        {results['changed_computers']:>6,}")
        print(f"Skipped (Unchanged): {results['records_skipped_unchanged']:>6,}")
        print(f"")
        print(f"Computer Analytics:")
        print(f"  ├─ Total Group Memberships: {results['total_group_memberships']:>6,}")
        print(f"  ├─ Computers with Groups:   {results['computers_with_groups']:>6,}")
        print(f"  ├─ Windows 10:              {results['windows_10_count']:>6,}")
        print(f"  ├─ Windows 11:              {results['windows_11_count']:>6,}")
        print(f"  ├─ Windows Server:          {results['windows_server_count']:>6,}")
        print(f"  └─ Other OS:                {results['other_os_count']:>6,}")
        print(f"")
        print(f"Errors:              {len(results['errors']):>6,}")
        print(f"Duration:            {duration:.2f}s")
        print("=" * 80)

        if (
            results["records_skipped_unchanged"] > 0
            and results["records_processed"] > 0
        ):
            efficiency_percentage = (
                results["records_skipped_unchanged"] / results["records_processed"]
            ) * 100
            print(
                f"\n⚡ Efficiency: {efficiency_percentage:.1f}% of computers were unchanged and skipped"
            )

        # Show computer analytics
        print("\n🏗️  Analyzing computer data...")
        computer_analyses = ingestion_service.get_computer_analytics()

        # Operating system distribution
        if "os_summary" in computer_analyses:
            print("\n🖥️  Top 20 Operating Systems:")
            os_summary = computer_analyses["os_summary"]
            for _, row in os_summary.head(20).iterrows():
                print(
                    f"   - {row['operating_system']}: {row['computer_count']} computers"
                )

            if len(os_summary) > 20:
                remaining_count = os_summary.iloc[20:]["computer_count"].sum()
                print(f"   - ... and {remaining_count} more computers with other OS")

        # Group membership distribution
        if "membership_summary" in computer_analyses:
            print("\n👥 Top 20 Group Membership Counts:")
            membership_summary = computer_analyses["membership_summary"]
            for _, row in membership_summary.head(20).iterrows():
                print(
                    f"   - {row['group_membership_count']} groups: {row['computer_count']} computers"
                )

            if len(membership_summary) > 20:
                remaining_count = membership_summary.iloc[20:]["computer_count"].sum()
                print(f"   - ... and {remaining_count} more membership categories")

        # Features summary
        if "features_summary" in computer_analyses:
            print("\n📈 Overall Computer Statistics:")
            features = computer_analyses["features_summary"].iloc[0]
            print(f"   - Total Computers: {features['total_computers']}")
            print(f"   - Computers with Groups: {features['computers_with_groups']}")
            print(
                f"   - Total Group Memberships: {features['total_group_memberships']}"
            )
            print(
                f"   - Avg Groups per Computer: {features['avg_groups_per_computer']:.2f}"
            )
            print(f"   - Max Groups on a Computer: {features['max_groups']}")
            print(f"   - Computers with DNS: {features['computers_with_dns']}")

        # Clean up
        ingestion_service.close()

        if args.dry_run:
            print("\n⚠️  DRY RUN MODE - No changes committed to database")
        else:
            print("\n✅ Active Directory computer ingestion completed successfully!")

    except Exception as e:
        logger.error(f"Active Directory computer ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
