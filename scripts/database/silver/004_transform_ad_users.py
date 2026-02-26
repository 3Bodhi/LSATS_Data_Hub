#!/usr/bin/env python3
"""
AD Users Source-Specific Silver Layer Transformation Service

This service transforms bronze Active Directory user records into the source-specific
silver.ad_users table. This is TIER 1 of the two-tier silver architecture.

Key features:
- Extracts AD user fields from JSONB to typed columns
- Stores multi-value fields (proxyAddresses, memberOf, etc.) as JSONB arrays
- Content hash-based change detection
- Incremental processing (only transform users with new bronze data)
- Comprehensive logging with emoji standards
- Dry-run mode for validation
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from pathlib import Path
import dateutil.parser
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add LSATS project to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# LSATS imports
from dotenv import load_dotenv
from database.adapters.postgres_adapter import PostgresAdapter

# Set up logging
script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "/var/log/lsats/silver"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"{log_dir}/{script_name}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class ADUserTransformationService:
    """
    Service for transforming bronze AD user records into source-specific silver layer.

    This service creates silver.ad_users records from bronze.raw_entities where:
    - entity_type = 'user'
    - source_system = 'active_directory'

    Transformation Logic:
    - Extract AD fields from JSONB to typed columns
    - Normalize lists (proxyAddresses, memberOf, etc.) to JSONB arrays
    - Calculate entity_hash for change detection
    - Track raw_id for traceability back to bronze
    """

    def __init__(self, database_url: str):
        """
        Initialize the transformation service.

        Args:
            database_url: PostgreSQL connection string
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        logger.info("🔌 AD users silver transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful AD users transformation.

        Returns:
            Timestamp of last completed run, or None if this is the first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'ad_user'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info("🆕 No previous transformation found - processing all users")
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last transformation timestamp: {e}")
            return None

    def _get_users_needing_transformation(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Set[str]:
        """
        Find AD user GUIDs that have new/updated bronze records.

        Args:
            since_timestamp: Only include users with bronze records after this time
            full_sync: If True, return ALL AD users regardless of timestamp

        Returns:
            Set of AD ObjectGUIDs that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                external_id as ad_user_guid
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
              AND source_system = 'active_directory'
              {time_filter}
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            user_guids = set(result_df["ad_user_guid"].tolist())

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"🔍 Found {len(user_guids)} AD users needing transformation ({sync_mode} mode)"
            )
            return user_guids

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get users needing transformation: {e}")
            raise

    def _fetch_latest_bronze_record(self, ad_user_guid: str) -> Optional[Tuple[Dict, str]]:
        """
        Fetch the latest bronze record for an AD user.

        Args:
            ad_user_guid: The AD ObjectGUID

        Returns:
            Tuple of (raw_data dict, raw_id UUID) or None if not found
        """
        try:
            query = """
            SELECT raw_data, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
              AND source_system = 'active_directory'
              AND external_id = :ad_user_guid
            ORDER BY ingested_at DESC
            LIMIT 1
            """

            result_df = self.db_adapter.query_to_dataframe(
                query, {"ad_user_guid": ad_user_guid}
            )

            if result_df.empty:
                return None

            return result_df.iloc[0]["raw_data"], result_df.iloc[0]["raw_id"]

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch bronze record for GUID {ad_user_guid}: {e}")
            raise

    def _calculate_content_hash(self, silver_record: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 content hash for change detection.

        Args:
            silver_record: The prepared silver record

        Returns:
            SHA-256 hash string
        """
        # Exclude metadata fields from hash calculation
        exclude_fields = {
            "raw_id", "entity_hash", "ingestion_run_id", 
            "created_at", "updated_at", "source_system"
        }
        
        content_to_hash = {
            k: v for k, v in silver_record.items() 
            if k not in exclude_fields
        }

        normalized_json = json.dumps(
            content_to_hash, sort_keys=True, separators=(",", ":"), default=str
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _parse_ad_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Parse AD timestamp strings into Python datetime objects.
        
        AD timestamps in bronze are ISO formatted strings (e.g., "2023-10-26T14:30:00+00:00")
        """
        if not timestamp_str:
            return None

        try:
            # Handle the specific 1601-01-01 date which means "never" or "null" in AD
            if timestamp_str.startswith("1601-01-01"):
                return None
                
            parsed_dt = dateutil.parser.isoparse(timestamp_str)
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            return parsed_dt
        except (ValueError, TypeError) as e:
            # logger.debug(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None

    def _normalize_list_field(self, value: Any) -> List[str]:
        """
        Normalize a field that might be a string or a list of strings into a list of strings.
        """
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item) for item in value]
        return [str(value)]

    def _parse_ou_hierarchy(self, distinguished_name: str) -> Dict[str, Any]:
        """
        Parse OU hierarchy from distinguishedName using universal extraction method.
        
        Adapted for CN objects (Users) to match the schema of silver.ad_organizational_units.
        
        Args:
            distinguished_name: Full AD DN string
            
        Returns:
            Dictionary with parsed OU fields
        """
        import re

        try:
            # Remove CN and DC parts (Standardize to OU path)
            # Users are CN objects, so we strip the CN part to get the parent OU path
            dn_no_cn = re.sub(r"^CN=[^,]+,", "", distinguished_name)
            dn_no_dc = re.sub(r",DC=.*$", "", dn_no_cn)

            # Split on ",OU=" to get OU components (leaf → root order)
            ou_parts = [part.strip() for part in dn_no_dc.split(",OU=")]

            # Remove leading "OU=" from first element if present
            if ou_parts and ou_parts[0].startswith("OU="):
                ou_parts[0] = ou_parts[0][3:]

            ou_count = len(ou_parts)

            # Helper function to safely get OU at position from end
            def get_ou_from_end(offset: int) -> Optional[str]:
                idx = ou_count - offset
                if 0 <= idx < ou_count:
                    return ou_parts[idx] if ou_parts[idx] else None
                return None

            # Helper function to safely get OU at position from start
            # For CN objects, the immediate parent is the first OU in the list (index 0)
            def get_ou_from_start(offset: int) -> Optional[str]:
                if 0 <= offset < ou_count:
                    return ou_parts[offset] if ou_parts[offset] else None
                return None

            return {
                "ou_root": get_ou_from_end(1),
                "ou_organization_type": get_ou_from_end(2),
                "ou_organization": get_ou_from_end(3),
                "ou_category": get_ou_from_end(4),
                "ou_status": None,  # Users don't have a status level in their path
                "ou_division": get_ou_from_end(5),
                "ou_department": get_ou_from_end(6),
                "ou_subdepartment": get_ou_from_end(7),
                "ou_immediate_parent": get_ou_from_start(0),
                "ou_full_path": ou_parts,
                "ou_depth": ou_count,
            }

        except Exception as e:
            logger.warning(
                f"⚠️  Failed to parse OU hierarchy for DN: {distinguished_name[:100]}... Error: {e}"
            )
            return {
                "ou_root": None,
                "ou_organization_type": None,
                "ou_organization": None,
                "ou_category": None,
                "ou_status": None,
                "ou_division": None,
                "ou_department": None,
                "ou_subdepartment": None,
                "ou_immediate_parent": None,
                "ou_full_path": [],
                "ou_depth": 0,
            }

    def _parse_parent_ou_dn(self, distinguished_name: str) -> Optional[str]:
        """
        Extract the immediate parent OU distinguished name.
        
        Args:
            distinguished_name: Full DN of the current user
            
        Returns:
            DN of parent OU, or None if no parent
        """
        if not distinguished_name:
            return None

        # For users (CN=...), the parent is everything after the first comma
        parts = distinguished_name.split(",", 1)
        if len(parts) == 2:
            return parts[1]
        return None

    def _extract_ad_fields(
        self, raw_data: Dict[str, Any], raw_id: str
    ) -> Dict[str, Any]:
        """
        Extract and type-cast AD fields from bronze JSONB to silver columns.

        Args:
            raw_data: Raw JSONB data from bronze.raw_entities
            raw_id: UUID of the bronze record

        Returns:
            Dictionary with all silver.ad_users columns
        """
        # Helper to safely convert to UUID or return None
        def to_uuid(val):
            if val is None:
                return None
            try:
                return str(uuid.UUID(str(val)))
            except (ValueError, AttributeError):
                return None

        # Helper for integer conversion
        def to_int(val):
            if val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
                
        # Helper for bigint conversion (some AD fields like uSNChanged can be large)
        def to_bigint(val):
            if val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        dn = raw_data.get("distinguishedName")
        ou_fields = self._parse_ou_hierarchy(dn or "")
        parent_ou_dn = self._parse_parent_ou_dn(dn)

        silver_record = {
            # Primary identifier
            "ad_user_guid": to_uuid(raw_data.get("objectGUID")),
            
            # Identity & Core
            "name": raw_data.get("name"),
            "cn": raw_data.get("cn"),
            "sam_account_name": raw_data.get("sAMAccountName"),
            "distinguished_name": dn,
            "user_principal_name": raw_data.get("userPrincipalName"),
            "display_name": raw_data.get("displayName"),
            "given_name": raw_data.get("givenName"),
            "sn": raw_data.get("sn"),
            "initials": raw_data.get("initials"),
            "title": raw_data.get("title"),
            "description": raw_data.get("description"),
            
            # OU Hierarchy
            "ou_root": ou_fields["ou_root"],
            "ou_organization_type": ou_fields["ou_organization_type"],
            "ou_organization": ou_fields["ou_organization"],
            "ou_category": ou_fields["ou_category"],
            "ou_status": ou_fields["ou_status"],
            "ou_division": ou_fields["ou_division"],
            "ou_department": ou_fields["ou_department"],
            "ou_subdepartment": ou_fields["ou_subdepartment"],
            "ou_immediate_parent": ou_fields["ou_immediate_parent"],
            "ou_full_path": ou_fields["ou_full_path"],
            "ou_depth": ou_fields["ou_depth"],
            "parent_ou_dn": parent_ou_dn,

            # Contact
            "mail": raw_data.get("mail"),
            "telephone_number": raw_data.get("telephoneNumber"),
            "mobile": raw_data.get("mobile"),
            "other_mobile": raw_data.get("otherMobile"),
            "facsimile_telephone_number": raw_data.get("facsimileTelephoneNumber"),
            "street_address": raw_data.get("streetAddress"),
            "proxy_addresses": self._normalize_list_field(raw_data.get("proxyAddresses")),
            
            # Organization
            "department": raw_data.get("department"),
            "umichad_ou": self._normalize_list_field(raw_data.get("umichadOU")),
            "umichad_role": self._normalize_list_field(raw_data.get("umichadRole")),
            
            # Account Status & Security
            "user_account_control": to_int(raw_data.get("userAccountControl")),
            "account_expires": self._parse_ad_timestamp(raw_data.get("accountExpires")),
            "pwd_last_set": self._parse_ad_timestamp(raw_data.get("pwdLastSet")),
            "last_logon": self._parse_ad_timestamp(raw_data.get("lastLogon")),
            "last_logon_timestamp": self._parse_ad_timestamp(raw_data.get("lastLogonTimestamp")),
            "last_logoff": self._parse_ad_timestamp(raw_data.get("lastLogoff")),
            "bad_pwd_count": to_int(raw_data.get("badPwdCount")),
            "bad_password_time": self._parse_ad_timestamp(raw_data.get("badPasswordTime")),
            "logon_count": to_int(raw_data.get("logonCount")),
            "lockout_time": self._parse_ad_timestamp(raw_data.get("lockoutTime")),
            "object_sid": raw_data.get("objectSid"),
            "sid_history": self._normalize_list_field(raw_data.get("sIDHistory")),
            
            # Metadata
            "when_created": self._parse_ad_timestamp(raw_data.get("whenCreated")),
            "when_changed": self._parse_ad_timestamp(raw_data.get("whenChanged")),
            "usn_created": to_bigint(raw_data.get("uSNCreated")),
            "usn_changed": to_bigint(raw_data.get("uSNChanged")),
            "object_class": self._normalize_list_field(raw_data.get("objectClass")),
            "object_category": raw_data.get("objectCategory"),
            "instance_type": to_int(raw_data.get("instanceType")),
            
            # Membership
            "member_of": self._normalize_list_field(raw_data.get("memberOf")),
            "primary_group_id": to_int(raw_data.get("primaryGroupID")),
            
            # Posix & Other
            "uid": raw_data.get("uid"),
            "uid_number": to_bigint(raw_data.get("uidNumber")),
            "gid_number": to_bigint(raw_data.get("gidNumber")),
            "home_directory": raw_data.get("homeDirectory"),
            "home_drive": raw_data.get("homeDrive"),
            "login_shell": raw_data.get("loginShell"),
            "employee_type": raw_data.get("employeeType"),
            
            # Traceability
            "raw_id": raw_id,
        }
        
        # Calculate hash after populating fields
        silver_record["entity_hash"] = self._calculate_content_hash(silver_record)

        return silver_record

    def _upsert_silver_record(
        self, silver_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> str:
        """
        Insert or update a silver.ad_users record.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
            dry_run: If True, log what would be done but don't commit

        Returns:
            Action taken: 'created', 'updated', or 'skipped'
        """
        ad_user_guid = silver_record["ad_user_guid"]

        if dry_run:
            logger.info(f"[DRY RUN] Would upsert user: GUID={ad_user_guid}, name={silver_record.get('name')}")
            return "dry_run"

        try:
            # Check if exists and compare hash
            check_query = """
            SELECT entity_hash 
            FROM silver.ad_users 
            WHERE ad_user_guid = :ad_user_guid
            """
            existing_df = self.db_adapter.query_to_dataframe(
                check_query, {"ad_user_guid": ad_user_guid}
            )

            is_new = existing_df.empty
            existing_hash = None if is_new else existing_df.iloc[0]["entity_hash"]

            # Skip if unchanged
            if not is_new and existing_hash == silver_record["entity_hash"]:
                return "skipped"

            with self.db_adapter.engine.connect() as conn:
                upsert_query = text("""
                    INSERT INTO silver.ad_users (
                        ad_user_guid, name, cn, sam_account_name, distinguished_name,
                        ou_root, ou_organization_type, ou_organization, ou_category, ou_status,
                        ou_division, ou_department, ou_subdepartment, ou_immediate_parent,
                        ou_full_path, ou_depth, parent_ou_dn,
                        user_principal_name, display_name, given_name, sn, initials, title, description,
                        mail, telephone_number, mobile, other_mobile, facsimile_telephone_number,
                        street_address, proxy_addresses,
                        department, umichad_ou, umichad_role,
                        user_account_control, account_expires, pwd_last_set, last_logon,
                        last_logon_timestamp, last_logoff, bad_pwd_count, bad_password_time,
                        logon_count, lockout_time, object_sid, sid_history,
                        when_created, when_changed, usn_created, usn_changed,
                        object_class, object_category, instance_type,
                        member_of, primary_group_id,
                        uid, uid_number, gid_number, home_directory, home_drive, login_shell, employee_type,
                        raw_id, entity_hash, ingestion_run_id, created_at, updated_at
                    ) VALUES (
                        :ad_user_guid, :name, :cn, :sam_account_name, :distinguished_name,
                        :ou_root, :ou_organization_type, :ou_organization, :ou_category, :ou_status,
                        :ou_division, :ou_department, :ou_subdepartment, :ou_immediate_parent,
                        CAST(:ou_full_path AS jsonb), :ou_depth, :parent_ou_dn,
                        :user_principal_name, :display_name, :given_name, :sn, :initials, :title, :description,
                        :mail, :telephone_number, :mobile, :other_mobile, :facsimile_telephone_number,
                        :street_address, CAST(:proxy_addresses AS jsonb),
                        :department, CAST(:umichad_ou AS jsonb), CAST(:umichad_role AS jsonb),
                        :user_account_control, :account_expires, :pwd_last_set, :last_logon,
                        :last_logon_timestamp, :last_logoff, :bad_pwd_count, :bad_password_time,
                        :logon_count, :lockout_time, :object_sid, CAST(:sid_history AS jsonb),
                        :when_created, :when_changed, :usn_created, :usn_changed,
                        CAST(:object_class AS jsonb), :object_category, :instance_type,
                        CAST(:member_of AS jsonb), :primary_group_id,
                        :uid, :uid_number, :gid_number, :home_directory, :home_drive, :login_shell, :employee_type,
                        :raw_id, :entity_hash, :ingestion_run_id, :created_at, :updated_at
                    )
                    ON CONFLICT (ad_user_guid) DO UPDATE SET
                        name = EXCLUDED.name,
                        cn = EXCLUDED.cn,
                        sam_account_name = EXCLUDED.sam_account_name,
                        distinguished_name = EXCLUDED.distinguished_name,
                        ou_root = EXCLUDED.ou_root,
                        ou_organization_type = EXCLUDED.ou_organization_type,
                        ou_organization = EXCLUDED.ou_organization,
                        ou_category = EXCLUDED.ou_category,
                        ou_status = EXCLUDED.ou_status,
                        ou_division = EXCLUDED.ou_division,
                        ou_department = EXCLUDED.ou_department,
                        ou_subdepartment = EXCLUDED.ou_subdepartment,
                        ou_immediate_parent = EXCLUDED.ou_immediate_parent,
                        ou_full_path = EXCLUDED.ou_full_path,
                        ou_depth = EXCLUDED.ou_depth,
                        parent_ou_dn = EXCLUDED.parent_ou_dn,
                        user_principal_name = EXCLUDED.user_principal_name,
                        display_name = EXCLUDED.display_name,
                        given_name = EXCLUDED.given_name,
                        sn = EXCLUDED.sn,
                        initials = EXCLUDED.initials,
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        mail = EXCLUDED.mail,
                        telephone_number = EXCLUDED.telephone_number,
                        mobile = EXCLUDED.mobile,
                        other_mobile = EXCLUDED.other_mobile,
                        facsimile_telephone_number = EXCLUDED.facsimile_telephone_number,
                        street_address = EXCLUDED.street_address,
                        proxy_addresses = EXCLUDED.proxy_addresses,
                        department = EXCLUDED.department,
                        umichad_ou = EXCLUDED.umichad_ou,
                        umichad_role = EXCLUDED.umichad_role,
                        user_account_control = EXCLUDED.user_account_control,
                        account_expires = EXCLUDED.account_expires,
                        pwd_last_set = EXCLUDED.pwd_last_set,
                        last_logon = EXCLUDED.last_logon,
                        last_logon_timestamp = EXCLUDED.last_logon_timestamp,
                        last_logoff = EXCLUDED.last_logoff,
                        bad_pwd_count = EXCLUDED.bad_pwd_count,
                        bad_password_time = EXCLUDED.bad_password_time,
                        logon_count = EXCLUDED.logon_count,
                        lockout_time = EXCLUDED.lockout_time,
                        object_sid = EXCLUDED.object_sid,
                        sid_history = EXCLUDED.sid_history,
                        when_created = EXCLUDED.when_created,
                        when_changed = EXCLUDED.when_changed,
                        usn_created = EXCLUDED.usn_created,
                        usn_changed = EXCLUDED.usn_changed,
                        object_class = EXCLUDED.object_class,
                        object_category = EXCLUDED.object_category,
                        instance_type = EXCLUDED.instance_type,
                        member_of = EXCLUDED.member_of,
                        primary_group_id = EXCLUDED.primary_group_id,
                        uid = EXCLUDED.uid,
                        uid_number = EXCLUDED.uid_number,
                        gid_number = EXCLUDED.gid_number,
                        home_directory = EXCLUDED.home_directory,
                        home_drive = EXCLUDED.home_drive,
                        login_shell = EXCLUDED.login_shell,
                        employee_type = EXCLUDED.employee_type,
                        raw_id = EXCLUDED.raw_id,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = EXCLUDED.updated_at
                    WHERE silver.ad_users.entity_hash != EXCLUDED.entity_hash
                """)

                conn.execute(
                    upsert_query,
                    {
                        **silver_record,
                        # JSON serialization for JSONB fields
                        "proxy_addresses": json.dumps(silver_record.get("proxy_addresses", [])),
                        "umichad_ou": json.dumps(silver_record.get("umichad_ou", [])),
                        "umichad_role": json.dumps(silver_record.get("umichad_role", [])),
                        "sid_history": json.dumps(silver_record.get("sid_history", [])),
                        "object_class": json.dumps(silver_record.get("object_class", [])),
                        "member_of": json.dumps(silver_record.get("member_of", [])),
                        "ou_full_path": json.dumps(silver_record.get("ou_full_path", [])),
                        "ingestion_run_id": run_id,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )

                conn.commit()

            action = "created" if is_new else "updated"
            # logger.debug(
            #     f"✅ {action.capitalize()} user: {ad_user_guid} (name: {silver_record.get('name')})"
            # )
            return action

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to upsert user {ad_user_guid}: {e}")
            raise

    def create_transformation_run(
        self, incremental_since: Optional[datetime] = None, full_sync: bool = False
    ) -> str:
        """
        Create a transformation run record for tracking.

        Args:
            incremental_since: Timestamp for incremental processing
            full_sync: Whether this is a full sync

        Returns:
            Run ID (UUID string)
        """
        try:
            run_id = str(uuid.uuid4())

            metadata = {
                "transformation_type": "bronze_to_silver_ad_users",
                "entity_type": "ad_user",
                "source_table": "bronze.raw_entities",
                "target_table": "silver.ad_users",
                "tier": "source_specific",
                "full_sync": full_sync,
                "incremental_since": incremental_since.isoformat()
                if incremental_since
                else None,
            }

            with self.db_adapter.engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status, metadata
                    ) VALUES (
                        :run_id, 'silver_transformation', 'ad_user', :started_at, 'running', :metadata
                    )
                """)

                conn.execute(
                    insert_query,
                    {
                        "run_id": run_id,
                        "started_at": datetime.now(timezone.utc),
                        "metadata": json.dumps(metadata),
                    },
                )

                conn.commit()

            mode = "FULL SYNC" if full_sync else "INCREMENTAL"
            logger.info(f"📝 Created transformation run {run_id} ({mode})")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create transformation run: {e}")
            raise

    def complete_transformation_run(
        self,
        run_id: str,
        records_processed: int,
        records_created: int,
        records_updated: int,
        records_skipped: int,
        error_message: Optional[str] = None,
    ):
        """
        Mark a transformation run as completed.

        Args:
            run_id: The run ID to complete
            records_processed: Total users processed
            records_created: New silver records created
            records_updated: Existing silver records updated
            records_skipped: Records skipped (unchanged)
            error_message: Error message if run failed
        """
        try:
            status = "failed" if error_message else "completed"

            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = :completed_at,
                        status = :status,
                        records_processed = :records_processed,
                        records_created = :records_created,
                        records_updated = :records_updated,
                        error_message = :error_message,
                        metadata = jsonb_set(
                            metadata,
                            '{records_skipped}',
                            to_jsonb(:records_skipped)
                        )
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
                        "records_updated": records_updated,
                        "records_skipped": records_skipped,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"✅ Completed transformation run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete transformation run: {e}")

    def transform_incremental(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Main entry point: Transform bronze AD users to silver.ad_users incrementally.

        Process flow:
        1. Determine last successful transformation timestamp (unless full_sync)
        2. Find AD users with bronze records newer than that timestamp
        3. For each user:
           a. Fetch latest bronze record
           b. Extract fields to silver columns
           c. Calculate entity hash
           d. Upsert to silver.ad_users
        4. Track statistics and return results

        Args:
            full_sync: If True, process all users regardless of timestamp
            dry_run: If True, preview changes without committing to database

        Returns:
            Dictionary with transformation statistics
        """
        # Get timestamp of last successful transformation
        last_transformation = (
            None if full_sync else self._get_last_transformation_timestamp()
        )

        # Create transformation run
        run_id = self.create_transformation_run(last_transformation, full_sync)

        stats = {
            "run_id": run_id,
            "incremental_since": last_transformation,
            "full_sync": full_sync,
            "dry_run": dry_run,
            "users_processed": 0,
            "records_created": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            if dry_run:
                logger.info("⚠️  DRY RUN MODE - No changes will be committed")

            if full_sync:
                logger.info("🔄 Full sync mode: Processing ALL AD users")
            elif last_transformation:
                logger.info(
                    f"⚡ Incremental mode: Processing users since {last_transformation}"
                )
            else:
                logger.info("🆕 First run: Processing ALL AD users")

            logger.info("🚀 Starting AD users silver transformation...")

            # Find users needing transformation
            user_guids = self._get_users_needing_transformation(
                last_transformation, full_sync
            )

            if not user_guids:
                logger.info("✨ All records up to date - no transformation needed")
                self.complete_transformation_run(run_id, 0, 0, 0, 0)
                return stats

            logger.info(f"📊 Processing {len(user_guids)} AD users")

            # Process each user
            for idx, ad_user_guid in enumerate(user_guids, 1):
                try:
                    # Fetch latest bronze record
                    bronze_result = self._fetch_latest_bronze_record(ad_user_guid)

                    if not bronze_result:
                        logger.warning(f"⚠️  No bronze data found for GUID {ad_user_guid}")
                        stats["errors"].append(f"No bronze data for {ad_user_guid}")
                        continue

                    raw_data, raw_id = bronze_result

                    # Extract AD fields to silver columns
                    silver_record = self._extract_ad_fields(raw_data, raw_id)

                    # Upsert to silver layer
                    action = self._upsert_silver_record(silver_record, run_id, dry_run)

                    if action == "created":
                        stats["records_created"] += 1
                    elif action == "updated":
                        stats["records_updated"] += 1
                    elif action == "skipped":
                        stats["records_skipped"] += 1

                    stats["users_processed"] += 1

                    # Log progress periodically
                    if idx % 100 == 0:
                        logger.info(
                            f"📈 Progress: {idx}/{len(user_guids)} users processed "
                            f"({stats['records_created']} created, {stats['records_updated']} updated, "
                            f"{stats['records_skipped']} skipped)"
                        )

                except Exception as record_error:
                    error_msg = f"Error processing user {ad_user_guid}: {str(record_error)}"
                    logger.error(f"❌ {error_msg}")
                    stats["errors"].append(error_msg)
                    # Continue processing other users

            # Calculate duration
            duration = (datetime.now(timezone.utc) - stats["started_at"]).total_seconds()
            
            logger.info(f"✅ Transformation finished in {duration:.2f} seconds")
            logger.info(f"   Total Processed: {stats['users_processed']}")
            logger.info(f"   Created: {stats['records_created']}")
            logger.info(f"   Updated: {stats['records_updated']}")
            logger.info(f"   Skipped: {stats['records_skipped']}")
            logger.info(f"   Errors: {len(stats['errors'])}")

            self.complete_transformation_run(
                run_id,
                stats["users_processed"],
                stats["records_created"],
                stats["records_updated"],
                stats["records_skipped"],
                f"{len(stats['errors'])} errors" if stats["errors"] else None,
            )

            return stats

        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}", exc_info=True)
            self.complete_transformation_run(
                run_id,
                stats["users_processed"],
                stats["records_created"],
                stats["records_updated"],
                stats["records_skipped"],
                str(e),
            )
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transform Bronze AD Users to Silver Layer"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all records regardless of last run time",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )
    args = parser.parse_args()

    # Load environment variables
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    try:
        service = ADUserTransformationService(database_url)
        service.transform_incremental(full_sync=args.full_sync, dry_run=args.dry_run)
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)
