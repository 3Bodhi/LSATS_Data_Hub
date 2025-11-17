#!/usr/bin/env python3
"""
Active Directory LDAP User Ingestion Service

This service ingests user data from the University of Michigan Active Directory LDAP
directory (adsroot.itcs.umich.edu) into the bronze layer for cross-referencing and analysis.

Active Directory provides authoritative directory information for all LSA users including:
- User identifiers (objectGUID, sAMAccountName, objectSid, uid)
- User attributes (name, mail, displayName, title, department)
- Account status (userAccountControl, accountExpires, lastLogon)
- Group memberships (memberOf attribute)
- UMich-specific attributes (umichadOU, umichadRole, umichDirectoryID)

All user records are stored in the LSA OU structure with objectGUID as the unique
external identifier. Users are filtered by objectClass=person.

IMPORTANT: Many LDAP attributes (memberOf, proxyAddresses, umichadOU, etc.) can be either
strings or lists of strings depending on the user record. The normalization functions
handle this appropriately.
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
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter, create_postgres_adapter
from ldap.adapters.ldap_adapter import LDAPAdapter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/ad_user_ingestion.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class ActiveDirectoryUserIngestionService:
    """
    User ingestion service for University of Michigan Active Directory LDAP directory.

    Uses content hashing for change detection since LDAP doesn't provide
    modification timestamps in a consistent way. This approach:

    1. Fetches current user data from Active Directory LDAP (LSA OU structure)
    2. Calculates content hashes for each user
    3. Compares against stored hashes from previous ingestions
    4. Only creates new bronze records when user content has actually changed
    5. Preserves complete change history for user analysis

    Key Features:
    - Efficient change detection without requiring timestamps
    - User account tracking (account status, login times, etc.)
    - Handles multi-value LDAP attributes (memberOf, proxyAddresses, umichadOU, etc.)
    - Tracks UMich-specific attributes (umichadOU, umichadRole, umichDirectoryID)
    - Comprehensive audit trail for user changes
    - Detailed ingestion statistics and monitoring
    """

    def __init__(self, database_url: str, ldap_config: Dict[str, Any]):
        """
        Initialize the Active Directory user ingestion service.

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
            "Active Directory user ingestion service initialized with content hashing"
        )

    def _normalize_ldap_attribute(self, value: Any) -> Any:
        """
        Normalize LDAP attribute values for consistent hashing and JSON serialization.

        LDAP attributes can be single values, lists, bytes, or None. Many Active Directory
        user attributes like memberOf, proxyAddresses, umichadOU can be either strings
        or lists depending on the user.

        Binary fields (userCertificate, objectGUID bytes, objectSid, etc.) are converted
        to base64 to avoid issues with null bytes in PostgreSQL JSON/JSONB fields.

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
            # Fields like userCertificate, objectSid, mS-DS-ConsistencyGuid contain binary data
            try:
                # Try to decode as UTF-8 for string-like fields
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

    def _calculate_user_content_hash(self, user_data: Dict[str, Any]) -> str:
        """
        Calculate a content hash for Active Directory user data to detect meaningful changes.

        This hash represents the "content fingerprint" of the user record.
        We include all fields that would represent meaningful user changes based on
        the Active Directory LDAP schema and UMich-specific attributes.

        IMPORTANT: Volatile fields (timestamps that change on every query, login counters,
        etc.) are EXCLUDED from the hash to avoid detecting spurious "changes" when the
        user's actual data hasn't changed. These include:
        - whenChanged: Updates on every AD sync/replication
        - uSNChanged: Update Sequence Number, increments on any attribute change
        - lastLogon/lastLogonTimestamp: Updates on every user login
        - logonCount: Increments on every login
        - badPwdCount/badPasswordTime: Changes on failed login attempts

        Args:
            user_data: Raw user data from Active Directory LDAP

        Returns:
            SHA-256 hash of the normalized user content
        """
        # Extract significant fields for change detection
        # Based on Active Directory LDAP schema for users and UMich-specific attributes
        significant_fields = {
            # Core identifiers
            "name": self._normalize_ldap_attribute(user_data.get("name")),
            "cn": self._normalize_ldap_attribute(user_data.get("cn")),
            "sAMAccountName": self._normalize_ldap_attribute(
                user_data.get("sAMAccountName")
            ),
            "uid": self._normalize_ldap_attribute(user_data.get("uid")),
            "distinguishedName": self._normalize_ldap_attribute(
                user_data.get("distinguishedName")
            ),
            "objectGUID": self._normalize_ldap_attribute(user_data.get("objectGUID")),
            "objectSid": self._normalize_ldap_attribute(user_data.get("objectSid")),
            "userPrincipalName": self._normalize_ldap_attribute(
                user_data.get("userPrincipalName")
            ),
            # Personal information
            "givenName": self._normalize_ldap_attribute(user_data.get("givenName")),
            "sn": self._normalize_ldap_attribute(user_data.get("sn")),
            "middleName": self._normalize_ldap_attribute(user_data.get("middleName")),
            "initials": self._normalize_ldap_attribute(user_data.get("initials")),
            "displayName": self._normalize_ldap_attribute(user_data.get("displayName")),
            "description": self._normalize_ldap_attribute(user_data.get("description")),
            # Contact information
            "mail": self._normalize_ldap_attribute(user_data.get("mail")),
            "mailNickname": self._normalize_ldap_attribute(
                user_data.get("mailNickname")
            ),
            "telephoneNumber": self._normalize_ldap_attribute(
                user_data.get("telephoneNumber")
            ),
            "proxyAddresses": self._normalize_ldap_attribute(
                user_data.get("proxyAddresses")
            ),
            # Organizational information
            "title": self._normalize_ldap_attribute(user_data.get("title")),
            "umichadOU": self._normalize_ldap_attribute(user_data.get("umichadOU")),
            "umichadRole": self._normalize_ldap_attribute(user_data.get("umichadRole")),
            # UMich-specific identifiers
            "umichDirectoryID": self._normalize_ldap_attribute(
                user_data.get("umichDirectoryID")
            ),
            "uidNumber": self._normalize_ldap_attribute(user_data.get("uidNumber")),
            # Account status and control
            "userAccountControl": self._normalize_ldap_attribute(
                user_data.get("userAccountControl")
            ),
            "accountExpires": self._normalize_ldap_attribute(
                user_data.get("accountExpires")
            ),
            "pwdLastSet": self._normalize_ldap_attribute(user_data.get("pwdLastSet")),
            # Group membership
            "memberOf": self._normalize_ldap_attribute(user_data.get("memberOf")),
            "primaryGroupID": self._normalize_ldap_attribute(
                user_data.get("primaryGroupID")
            ),
            # Managed objects (computers/resources managed by this user)
            "managedObjects": self._normalize_ldap_attribute(
                user_data.get("managedObjects")
            ),
            # Object metadata
            "objectCategory": self._normalize_ldap_attribute(
                user_data.get("objectCategory")
            ),
            "objectClass": self._normalize_ldap_attribute(user_data.get("objectClass")),
            # Timestamps - ONLY include creation time (stable), EXCLUDE change timestamps (volatile)
            "whenCreated": self._normalize_ldap_attribute(user_data.get("whenCreated")),
            # EXCLUDED: "whenChanged" - updates on every AD sync/replication
            # EXCLUDED: "uSNChanged" - Update Sequence Number, increments constantly
            # USN Creation (stable after creation)
            "uSNCreated": self._normalize_ldap_attribute(user_data.get("uSNCreated")),
            # EXCLUDED: Activity tracking fields (these change constantly and don't represent user data changes)
            # - "lastLogon" - updates on every login
            # - "lastLogonTimestamp" - updates on logins (replicated less frequently than lastLogon)
            # - "logonCount" - increments on every login
            # - "badPwdCount" - changes on failed login attempts
            # - "badPasswordTime" - changes on failed login attempts
            # Exchange attributes (email-related)
            "legacyExchangeDN": self._normalize_ldap_attribute(
                user_data.get("legacyExchangeDN")
            ),
            "msExchRecipientTypeDetails": self._normalize_ldap_attribute(
                user_data.get("msExchRecipientTypeDetails")
            ),
            "targetAddress": self._normalize_ldap_attribute(
                user_data.get("targetAddress")
            ),
            # UMich-specific attributes
            "umichadNoBatchUpdates": self._normalize_ldap_attribute(
                user_data.get("umichadNoBatchUpdates")
            ),
            "umichadHidePersonalInfo": self._normalize_ldap_attribute(
                user_data.get("umichadHidePersonalInfo")
            ),
            "umichadUMDirToADSyncFlag": self._normalize_ldap_attribute(
                user_data.get("umichadUMDirToADSyncFlag")
            ),
            # Extension attributes (often used for custom data)
            "extensionAttribute5": self._normalize_ldap_attribute(
                user_data.get("extensionAttribute5")
            ),
            "extensionAttribute6": self._normalize_ldap_attribute(
                user_data.get("extensionAttribute6")
            ),
            "extensionAttribute9": self._normalize_ldap_attribute(
                user_data.get("extensionAttribute9")
            ),
            # Directory replication metadata
            "dSCorePropagationData": self._normalize_ldap_attribute(
                user_data.get("dSCorePropagationData")
            ),
            # Instance type
            "instanceType": self._normalize_ldap_attribute(
                user_data.get("instanceType")
            ),
            # Account type
            "sAMAccountType": self._normalize_ldap_attribute(
                user_data.get("sAMAccountType")
            ),
        }

        # Create normalized JSON for consistent hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )

        # Generate SHA-256 hash
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        name = user_data.get("name", "unknown")
        object_guid = self._normalize_ldap_attribute(user_data.get("objectGUID"))
        logger.debug(
            f"Content hash for user {name} (objectGUID: {object_guid}): {content_hash}"
        )

        return content_hash

    def _get_existing_user_hashes(self) -> Dict[str, str]:
        """
        Retrieve the latest content hash for each Active Directory user from the bronze layer.

        This uses a window function to get only the most recent record for each
        user, allowing efficient comparison with new data.

        Returns:
            Dictionary mapping objectGUID -> latest_content_hash
        """
        try:
            # Query to get the most recent record for each user
            query = """
            WITH latest_users AS (
                SELECT
                    external_id,
                    raw_data,
                    ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'user'
                AND source_system = 'active_directory'
            )
            SELECT
                external_id,
                raw_data
            FROM latest_users
            WHERE row_num = 1
            """

            results_df = self.db_adapter.query_to_dataframe(query)

            # Retrieve stored content hashes from existing records
            existing_hashes = {}
            recalculated_count = 0
            for _, row in results_df.iterrows():
                object_guid = row["external_id"]
                raw_data = row["raw_data"]  # JSONB comes back as dict

                # Use the stored hash that was calculated at ingestion time
                # This avoids recalculating with potentially different timestamp values
                content_hash = raw_data.get("_content_hash")

                if not content_hash:
                    # Fallback for old records without stored hash (from before this fix)
                    content_hash = self._calculate_user_content_hash(raw_data)
                    recalculated_count += 1
                    logger.debug(
                        f"User {object_guid} missing stored _content_hash, recalculating"
                    )

                existing_hashes[object_guid] = content_hash

            if recalculated_count > 0:
                logger.warning(
                    f"Recalculated hashes for {recalculated_count} users missing stored _content_hash. "
                    f"This is normal for records created before the hash fix."
                )

            logger.info(
                f"Retrieved content hashes for {len(existing_hashes)} existing Active Directory users"
            )
            return existing_hashes

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve existing user hashes: {e}")
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
                "search_base": "OU=Accounts,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
                "search_filter": "(&(objectClass=person)(cn=*))",
                "change_detection_method": "sha256_content_hash",
                "includes_group_membership": True,
                "includes_umich_attributes": True,
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

    def ingest_ad_users_with_change_detection(self) -> Dict[str, Any]:
        """
        Ingest University of Michigan Active Directory users using intelligent content hashing.

        This method:
        1. Fetches all user data from the Active Directory LDAP (LSA OU structure)
        2. Calculates content hashes for each user
        3. Compares against existing bronze records
        4. Only creates new records when content has actually changed
        5. Provides detailed statistics about user changes detected

        Returns:
            Dictionary with comprehensive ingestion statistics
        """
        # Create ingestion run for tracking
        run_id = self.create_ingestion_run("active_directory", "user")

        ingestion_stats = {
            "run_id": run_id,
            "records_processed": 0,
            "records_created": 0,
            "records_skipped_unchanged": 0,
            "new_users": 0,
            "changed_users": 0,
            "users_with_email": 0,
            "users_with_memberof": 0,
            "active_accounts": 0,
            "disabled_accounts": 0,
            "faculty_staff": 0,
            "total_group_memberships": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info(
                "Starting Active Directory user ingestion with content hash change detection..."
            )

            # Step 1: Get existing user content hashes from bronze layer
            existing_hashes = self._get_existing_user_hashes()

            # Step 2: Fetch current data from Active Directory LDAP
            search_base = "OU=Accounts,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu"
            logger.info(
                f"Fetching user data from Active Directory LDAP ({search_base})..."
            )

            # Request all attributes for comprehensive user data
            # Note: using attributes=None returns all available attributes
            raw_users = self.ldap_adapter.search_as_dicts(
                search_filter="(&(objectClass=person)(cn=*))",
                search_base=search_base,
                scope="subtree",
                attributes=None,  # Return all attributes
                use_pagination=True,
            )

            if not raw_users:
                logger.warning("No users found in Active Directory LDAP")
                return ingestion_stats

            logger.info(f"Retrieved {len(raw_users)} users from Active Directory LDAP")

            # Step 3: Process each user with content hash change detection
            for user_data in raw_users:
                try:
                    # Extract user identifiers
                    name = self._normalize_ldap_attribute(user_data.get("name"))
                    object_guid = self._normalize_ldap_attribute(
                        user_data.get("objectGUID")
                    )
                    sam_account_name = self._normalize_ldap_attribute(
                        user_data.get("sAMAccountName")
                    )

                    # Skip if no objectGUID (required as external_id)
                    if not object_guid:
                        logger.warning(
                            f"Skipping user {name} - missing objectGUID attribute"
                        )
                        continue

                    # Track analytics for reporting
                    # Email address
                    if user_data.get("mail"):
                        ingestion_stats["users_with_email"] += 1

                    # Group memberships
                    member_of = user_data.get("memberOf")
                    if member_of:
                        ingestion_stats["users_with_memberof"] += 1
                        if isinstance(member_of, list):
                            ingestion_stats["total_group_memberships"] += len(member_of)
                        else:
                            ingestion_stats["total_group_memberships"] += 1

                    # Account status (based on userAccountControl)
                    # Bit 2 (0x2) = Account disabled
                    user_account_control = user_data.get("userAccountControl")
                    if user_account_control:
                        if isinstance(user_account_control, int):
                            if user_account_control & 0x2:
                                ingestion_stats["disabled_accounts"] += 1
                            else:
                                ingestion_stats["active_accounts"] += 1

                    # UMich role (Faculty and Staff vs other)
                    umichad_role = user_data.get("umichadRole")
                    if umichad_role and "Faculty and Staff" in str(umichad_role):
                        ingestion_stats["faculty_staff"] += 1

                    # Calculate content hash for this user
                    current_hash = self._calculate_user_content_hash(user_data)

                    # Check if this user is new or has changed
                    existing_hash = existing_hashes.get(object_guid)

                    if existing_hash is None:
                        # This is a completely new user
                        logger.info(
                            f"New user detected: {name} ({sam_account_name}, objectGUID: {object_guid})"
                        )
                        should_insert = True
                        ingestion_stats["new_users"] += 1

                    elif existing_hash != current_hash:
                        # This user exists but has changed
                        logger.info(
                            f"User changed: {name} ({sam_account_name}, objectGUID: {object_guid})"
                        )
                        logger.debug(f"   Old hash: {existing_hash}")
                        logger.debug(f"   New hash: {current_hash}")
                        should_insert = True
                        ingestion_stats["changed_users"] += 1

                    else:
                        # This user exists and hasn't changed - skip it
                        logger.debug(
                            f"User unchanged, skipping: {name} ({sam_account_name}, objectGUID: {object_guid})"
                        )
                        should_insert = False
                        ingestion_stats["records_skipped_unchanged"] += 1

                    # Only insert if the user is new or changed
                    if should_insert:
                        # Normalize all raw data for JSON serialization
                        # This converts datetime, bytes, and other non-JSON types
                        normalized_data = self._normalize_raw_data_for_json(user_data)

                        # Enhance with metadata for future reference
                        normalized_data["_content_hash"] = current_hash
                        normalized_data["_change_detection"] = "content_hash_based"
                        normalized_data["_ldap_server"] = "adsroot.itcs.umich.edu"
                        normalized_data["_search_base"] = search_base

                        # Insert into bronze layer using objectGUID as external_id
                        entity_id = self.db_adapter.insert_raw_entity(
                            entity_type="user",
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
                            f"Progress: {ingestion_stats['records_processed']} users processed "
                            f"({ingestion_stats['records_created']} new/changed, "
                            f"{ingestion_stats['records_skipped_unchanged']} unchanged)"
                        )

                except Exception as record_error:
                    name_safe = (
                        user_data.get("name", "unknown")
                        if "name" in user_data
                        else "unknown"
                    )
                    guid_safe = (
                        user_data.get("objectGUID", "unknown")
                        if "objectGUID" in user_data
                        else "unknown"
                    )
                    error_msg = f"Failed to process user {name_safe} (objectGUID: {guid_safe}): {record_error}"
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
                f"Active Directory user ingestion completed in {duration:.2f} seconds"
            )
            logger.info(f"Results Summary:")
            logger.info(f"   Total Processed: {ingestion_stats['records_processed']}")
            logger.info(f"   New Records Created: {ingestion_stats['records_created']}")
            logger.info(f"   ├─ New Users: {ingestion_stats['new_users']}")
            logger.info(f"   └─ Changed Users: {ingestion_stats['changed_users']}")
            logger.info(
                f"   Skipped (Unchanged): {ingestion_stats['records_skipped_unchanged']}"
            )
            logger.info(f"   User Analytics:")
            logger.info(
                f"   ├─ Users with Email: {ingestion_stats['users_with_email']}"
            )
            logger.info(
                f"   ├─ Users with Group Memberships: {ingestion_stats['users_with_memberof']}"
            )
            logger.info(
                f"   ├─ Total Group Memberships: {ingestion_stats['total_group_memberships']}"
            )
            logger.info(f"   ├─ Active Accounts: {ingestion_stats['active_accounts']}")
            logger.info(
                f"   ├─ Disabled Accounts: {ingestion_stats['disabled_accounts']}"
            )
            logger.info(f"   └─ Faculty/Staff: {ingestion_stats['faculty_staff']}")
            logger.info(f"   Errors: {len(ingestion_stats['errors'])}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"Active Directory user ingestion failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats["records_processed"],
                records_created=ingestion_stats["records_created"],
                records_skipped=ingestion_stats["records_skipped_unchanged"],
                error_message=error_msg,
            )

            raise

    def get_user_analytics(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze Active Directory user data from bronze layer.

        This provides insights into the user structure and can help
        identify patterns or anomalies in the user data.

        Returns:
            Dictionary containing DataFrames for different user analyses
        """
        try:
            # Query for user analytics using Active Directory LDAP fields
            analytics_query = """
            WITH latest_users AS (
                SELECT
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'user'
                AND source_system = 'active_directory'
            )
            SELECT
                raw_data->>'name' as name,
                raw_data->>'sAMAccountName' as sam_account_name,
                raw_data->>'objectGUID' as object_guid,
                raw_data->>'mail' as email,
                raw_data->>'displayName' as display_name,
                raw_data->>'title' as title,
                raw_data->>'umichadRole' as umichad_role,
                raw_data->>'userAccountControl' as user_account_control,
                CASE
                    WHEN jsonb_typeof(raw_data->'memberOf') = 'array'
                    THEN jsonb_array_length(raw_data->'memberOf')
                    WHEN raw_data->>'memberOf' IS NOT NULL AND raw_data->>'memberOf' != ''
                    THEN 1
                    ELSE 0
                END as memberof_count,
                CASE
                    WHEN jsonb_typeof(raw_data->'umichadOU') = 'array'
                    THEN jsonb_array_length(raw_data->'umichadOU')
                    WHEN raw_data->>'umichadOU' IS NOT NULL AND raw_data->>'umichadOU' != ''
                    THEN 1
                    ELSE 0
                END as ou_count
            FROM latest_users
            WHERE row_num = 1
            ORDER BY name
            """

            analytics_df = self.db_adapter.query_to_dataframe(analytics_query)

            # Create summary analyses
            analyses = {}

            # Group membership summary
            if not analytics_df.empty and "memberof_count" in analytics_df.columns:
                membership_summary = (
                    analytics_df.groupby("memberof_count")
                    .size()
                    .reset_index(name="user_count")
                )
                analyses["membership_summary"] = membership_summary.sort_values(
                    "memberof_count", ascending=False
                )

            # UMich role summary
            if not analytics_df.empty and "umichad_role" in analytics_df.columns:
                role_summary = (
                    analytics_df.groupby("umichad_role")
                    .size()
                    .reset_index(name="user_count")
                )
                analyses["role_summary"] = role_summary

            # Account status summary
            if (
                not analytics_df.empty
                and "user_account_control" in analytics_df.columns
            ):
                # Parse userAccountControl to determine account status
                analytics_df["account_status"] = analytics_df[
                    "user_account_control"
                ].apply(
                    lambda x: "Disabled"
                    if (isinstance(x, str) and int(x) & 0x2)
                    else "Active"
                    if x
                    else "Unknown"
                )
                status_summary = (
                    analytics_df.groupby("account_status")
                    .size()
                    .reset_index(name="user_count")
                )
                analyses["status_summary"] = status_summary

            # User features summary
            if not analytics_df.empty:
                features_summary = {
                    "total_users": len(analytics_df),
                    "users_with_email": analytics_df["email"].notna().sum(),
                    "users_with_memberof": (analytics_df["memberof_count"] > 0).sum(),
                    "total_memberships": analytics_df["memberof_count"].sum(),
                    "avg_memberships_per_user": analytics_df["memberof_count"].mean(),
                    "max_memberships": analytics_df["memberof_count"].max(),
                }
                analyses["features_summary"] = pd.DataFrame([features_summary])

            # Full user list
            analyses["full_user_list"] = analytics_df

            logger.info(
                f"Generated user analytics with {len(analytics_df)} users from Active Directory"
            )
            return analyses

        except SQLAlchemyError as e:
            logger.error(f"Failed to generate user analytics: {e}")
            raise

    def get_user_change_history(self, object_guid: str) -> pd.DataFrame:
        """
        Get the complete change history for a specific Active Directory user.

        Args:
            object_guid: The Active Directory objectGUID

        Returns:
            DataFrame with all historical versions of the user
        """
        try:
            query = """
            SELECT
                raw_id,
                raw_data->>'name' as name,
                raw_data->>'sAMAccountName' as sam_account_name,
                raw_data->>'mail' as email,
                raw_data->>'displayName' as display_name,
                raw_data->>'title' as title,
                raw_data->>'umichadRole' as umichad_role,
                CASE
                    WHEN jsonb_typeof(raw_data->'memberOf') = 'array'
                    THEN jsonb_array_length(raw_data->'memberOf')
                    WHEN raw_data->>'memberOf' IS NOT NULL AND raw_data->>'memberOf' != ''
                    THEN 1
                    ELSE 0
                END as memberof_count,
                raw_data->>'_content_hash' as content_hash,
                ingested_at,
                ingestion_run_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
            AND source_system = 'active_directory'
            AND external_id = :object_guid
            ORDER BY ingested_at DESC
            """

            history_df = self.db_adapter.query_to_dataframe(
                query, {"object_guid": object_guid}
            )

            logger.info(
                f"Retrieved {len(history_df)} historical records for Active Directory user {object_guid}"
            )
            return history_df

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve user history: {e}")
            raise

    def close(self):
        """Clean up database and LDAP connections."""
        if self.db_adapter:
            self.db_adapter.close()
        if self.ldap_adapter:
            # LDAPAdapter doesn't have explicit close, connection is managed internally
            pass
        logger.info("Active Directory user ingestion service closed")


def main():
    """
    Main function to run Active Directory user ingestion from command line.
    """
    try:
        # Ensure logs directory exists
        os.makedirs("logs", exist_ok=True)

        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv("DATABASE_URL")

        # Active Directory LDAP configuration
        ad_config = {
            "server": "adsroot.itcs.umich.edu",
            "search_base": "OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
            "user": "umroot\\myodhes1",
            "keyring_service": "ldap_umich",
            "port": 636,
            "use_ssl": True,
        }

        # Validate configuration
        if not database_url:
            raise ValueError("Missing required environment variable: DATABASE_URL")

        if not ad_config["user"]:
            raise ValueError("Missing LDAP user configuration")

        # Create and run Active Directory ingestion service
        ingestion_service = ActiveDirectoryUserIngestionService(
            database_url=database_url, ldap_config=ad_config
        )

        # Run the content hash-based ingestion process
        print("👤 Starting Active Directory user ingestion with content hashing...")
        results = ingestion_service.ingest_ad_users_with_change_detection()

        # Display comprehensive summary
        print(f"\n📊 Active Directory User Ingestion Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Total Users Processed: {results['records_processed']}")
        print(f"   New Records Created: {results['records_created']}")
        print(f"     ├─ Brand New Users: {results['new_users']}")
        print(f"     └─ Users with Changes: {results['changed_users']}")
        print(f"   Skipped (No Changes): {results['records_skipped_unchanged']}")
        print(f"   User Analytics:")
        print(f"     ├─ Users with Email: {results['users_with_email']}")
        print(f"     ├─ Users with Group Memberships: {results['users_with_memberof']}")
        print(f"     ├─ Total Group Memberships: {results['total_group_memberships']}")
        print(f"     ├─ Active Accounts: {results['active_accounts']}")
        print(f"     ├─ Disabled Accounts: {results['disabled_accounts']}")
        print(f"     └─ Faculty/Staff: {results['faculty_staff']}")
        print(f"   Errors: {len(results['errors'])}")

        if results["records_skipped_unchanged"] > 0:
            efficiency_percentage = (
                results["records_skipped_unchanged"] / results["records_processed"]
            ) * 100
            print(
                f"\n⚡ Efficiency: {efficiency_percentage:.1f}% of users were unchanged and skipped"
            )

        # Show user analytics
        print("\n🔍 Analyzing user data...")
        user_analyses = ingestion_service.get_user_analytics()

        # Group membership distribution
        if "membership_summary" in user_analyses:
            print("\n👥 Top 20 Group Membership Counts:")
            membership_summary = user_analyses["membership_summary"]
            for _, row in membership_summary.head(20).iterrows():
                print(f"   - {row['memberof_count']} groups: {row['user_count']} users")

            if len(membership_summary) > 20:
                remaining_count = membership_summary.iloc[20:]["user_count"].sum()
                print(f"   - ... and {remaining_count} more membership counts")

        # UMich role distribution
        if "role_summary" in user_analyses:
            print("\n🎓 UMich Role Distribution:")
            role_summary = user_analyses["role_summary"]
            for _, row in role_summary.iterrows():
                role = row["umichad_role"] if row["umichad_role"] else "No Role"
                print(f"   - {role}: {row['user_count']} users")

        # Account status distribution
        if "status_summary" in user_analyses:
            print("\n🔐 Account Status Distribution:")
            status_summary = user_analyses["status_summary"]
            for _, row in status_summary.iterrows():
                print(f"   - {row['account_status']}: {row['user_count']} users")

        # Features summary
        if "features_summary" in user_analyses:
            print("\n📈 Overall User Statistics:")
            features = user_analyses["features_summary"].iloc[0]
            print(f"   - Total Users: {features['total_users']}")
            print(f"   - Users with Email: {features['users_with_email']}")
            print(
                f"   - Users with Group Memberships: {features['users_with_memberof']}"
            )
            print(
                f"   - Total Memberships: {features['total_memberships']} (across all users)"
            )
            print(
                f"   - Avg Memberships per User: {features['avg_memberships_per_user']:.2f}"
            )
            print(f"   - Max Memberships for a User: {features['max_memberships']}")

        # Clean up
        ingestion_service.close()

        print("\n✅ Active Directory user ingestion completed successfully!")

    except Exception as e:
        logger.error(f"Active Directory user ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
