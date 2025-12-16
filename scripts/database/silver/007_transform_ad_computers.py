#!/usr/bin/env python3
"""
Active Directory Computers Source-Specific Silver Layer Transformation Service

This service transforms bronze Active Directory computer records into the source-specific
silver.ad_computers table. This is TIER 1 of the two-tier silver architecture.

Key features:
- Extracts AD LDAP computer fields from JSONB to typed columns
- Universal OU extraction method for cross-entity matching (computers, groups, users, OUs)
- Normalizes memberOf field (handles both string and array formats)
- Content hash-based change detection
- Incremental processing (only transform computers with new bronze data)
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
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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
log_dir = "logs"
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


class ADComputerTransformationService:
    """
    Service for transforming bronze AD computer records into source-specific silver layer.

    This service creates silver.ad_computers records from bronze.raw_entities where:
    - entity_type = 'computer'
    - source_system = 'active_directory'

    Transformation Logic:
    - Extract AD LDAP fields from JSONB to typed columns
    - Universal OU parsing from distinguishedName (root→leaf extraction)
    - Normalize memberOf (string → array) and servicePrincipalName arrays
    - Calculate is_enabled from userAccountControl bit flags
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
        logger.info("🔌 AD computers silver transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful AD computers transformation.

        Returns:
            Timestamp of last completed run, or None if this is the first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'ad_computer'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "🆕 No previous transformation found - processing all computers"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last transformation timestamp: {e}")
            return None

    def _get_computers_needing_transformation(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Set[str]:
        """
        Find AD computer SAM account names that have new/updated bronze records.

        Args:
            since_timestamp: Only include computers with bronze records after this time
            full_sync: If True, return ALL AD computers regardless of timestamp

        Returns:
            Set of SAM account names that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                external_id as sam_account_name
            FROM bronze.raw_entities
            WHERE entity_type = 'computer'
              AND source_system = 'active_directory'
              {time_filter}
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            sam_names = set(result_df["sam_account_name"].tolist())

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"🔍 Found {len(sam_names)} AD computers needing transformation ({sync_mode} mode)"
            )
            return sam_names

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get computers needing transformation: {e}")
            raise

    def _fetch_latest_bronze_record(
        self, sam_account_name: str
    ) -> Optional[Tuple[Dict, str]]:
        """
        Fetch the latest bronze record for an AD computer.

        Args:
            sam_account_name: The sAMAccountName (e.g., "MCDB-5CG0183FB7$")

        Returns:
            Tuple of (raw_data dict, raw_id UUID) or None if not found
        """
        try:
            query = """
            SELECT raw_data, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'computer'
              AND source_system = 'active_directory'
              AND external_id = :sam_account_name
            ORDER BY ingested_at DESC
            LIMIT 1
            """

            result_df = self.db_adapter.query_to_dataframe(
                query, {"sam_account_name": sam_account_name}
            )

            if result_df.empty:
                return None

            return result_df.iloc[0]["raw_data"], result_df.iloc[0]["raw_id"]

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to fetch bronze record for {sam_account_name}: {e}"
            )
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
            "raw_id",
            "entity_hash",
            "ingestion_run_id",
            "created_at",
            "updated_at",
            "source_system",
        }

        content_to_hash = {
            k: v for k, v in silver_record.items() if k not in exclude_fields
        }

        normalized_json = json.dumps(
            content_to_hash, sort_keys=True, separators=(",", ":"), default=str
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _normalize_to_list(self, value: Any) -> List[str]:
        """
        Normalize a field that might be a string or a list into a list of strings.

        AD LDAP can return memberOf as either a single string (1 group) or an array
        (multiple groups). This normalizes to always return an array.

        Args:
            value: String, list, or None

        Returns:
            List of strings (empty list if None)
        """
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item) for item in value]
        return [str(value)]

    def _parse_ou_hierarchy(self, distinguished_name: str) -> Dict[str, Any]:
        """
        Parse OU hierarchy from distinguishedName using universal extraction method.

        Extracts OUs from both root (DC) → leaf (CN) and leaf → root directions to
        enable cross-entity matching with AD groups, users, and organizational units.

        Pattern (from root → leaf):
        DC=edu,DC=umich,DC=itcs,DC=adsroot,OU=UMICH,OU=Organizations,OU=LSA,OU=Workstations,OU=RSN,OU=Biology,OU=Standard,CN=<name>

        Extraction positions (from root):
        - ou_root = UMICH (array[length])
        - ou_organization_type = Organizations (array[length-1])
        - ou_organization = LSA (array[length-2])
        - ou_category = Workstations (array[length-3])
        - ou_division = RSN (array[length-4])
        - ou_department = Biology (array[length-5])
        - ou_subdepartment = (array[length-6])
        - ou_immediate_parent = Standard (array[1], first OU after CN)

        Args:
            distinguished_name: Full AD DN string

        Returns:
            Dictionary with parsed OU fields
        """
        import re

        try:
            # Remove CN and DC parts
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
            def get_ou_from_start(offset: int) -> Optional[str]:
                if 0 <= offset < ou_count:
                    return ou_parts[offset] if ou_parts[offset] else None
                return None

            return {
                "ou_root": get_ou_from_end(1),  # array[length]
                "ou_organization_type": get_ou_from_end(2),  # array[length-1]
                "ou_organization": get_ou_from_end(3),  # array[length-2]
                "ou_category": get_ou_from_end(4),  # array[length-3]
                "ou_division": get_ou_from_end(5),  # array[length-4]
                "ou_department": get_ou_from_end(6),  # array[length-5]
                "ou_subdepartment": get_ou_from_end(7),  # array[length-6]
                "ou_immediate_parent": get_ou_from_start(0),  # array[1], first OU
                "ou_full_path": ou_parts,  # Complete array for JSONB storage
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
                "ou_division": None,
                "ou_department": None,
                "ou_subdepartment": None,
                "ou_immediate_parent": None,
                "ou_full_path": [],
            }

    def _compute_is_enabled(self, user_account_control: Optional[int]) -> bool:
        """
        Compute is_enabled from userAccountControl bit flags.

        Bit 2 (value 2) = ACCOUNTDISABLE
        If bit is set, account is disabled.

        Args:
            user_account_control: UAC integer value

        Returns:
            True if account is enabled, False if disabled
        """
        if user_account_control is None:
            return False

        # Check if bit 2 (ACCOUNTDISABLE) is set
        return (user_account_control & 2) == 0

    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        """
        Parse AD timestamp to datetime with timezone.

        Args:
            value: Timestamp string or None

        Returns:
            Datetime object with UTC timezone or None
        """
        if value is None:
            return None

        try:
            if isinstance(value, datetime):
                return value
            dt = pd.to_datetime(value, errors="coerce")
            if pd.isna(dt):
                return None
            # Ensure timezone-aware
            if dt.tzinfo is None:
                dt = dt.tz_localize("UTC")
            return dt
        except Exception:
            return None

    def _to_int(self, value: Any) -> Optional[int]:
        """Safely convert to integer."""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _to_bigint(self, value: Any) -> Optional[int]:
        """Safely convert to bigint (for LAPS timestamps)."""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _extract_ad_computer_fields(
        self, raw_data: Dict[str, Any], raw_id: str
    ) -> Dict[str, Any]:
        """
        Extract and type-cast AD LDAP fields from bronze JSONB to silver columns.

        Args:
            raw_data: Raw JSONB data from bronze.raw_entities
            raw_id: UUID of the bronze record

        Returns:
            Dictionary with all silver.ad_computers columns
        """

        # Parse OU hierarchy
        dn = raw_data.get("distinguishedName") or raw_data.get("dn", "")
        ou_fields = self._parse_ou_hierarchy(dn)

        # Parse userAccountControl and compute is_enabled
        uac = self._to_int(raw_data.get("userAccountControl"))
        is_enabled = self._compute_is_enabled(uac)

        # Normalize memberOf (can be string or array)
        member_of_raw = raw_data.get("memberOf")
        member_of_groups = self._normalize_to_list(member_of_raw)

        # Normalize servicePrincipalName (already array in most cases)
        spn_raw = raw_data.get("servicePrincipalName")
        service_principal_names = self._normalize_to_list(spn_raw)

        # Normalize networkAddress (rare)
        network_addr_raw = raw_data.get("networkAddress")
        network_addresses = self._normalize_to_list(network_addr_raw)

        # dSCorePropagationData (array of timestamps)
        ds_core_raw = raw_data.get("dSCorePropagationData")
        ds_core_propagation_data = self._normalize_to_list(ds_core_raw)

        # Consolidate rare attributes into JSONB
        additional_attrs = {}
        rare_fields = [
            "pager",
            "networkAddress",
            "msDFSR-ComputerReferenceBL",
            "msDS-AllowedToActOnBehalfOfOtherIdentity",
            "msDS-GroupMSAMembership",
            "msDS-ManagedPasswordId",
            "msDS-ManagedPasswordInterval",
            "msDS-ManagedPasswordPreviousId",
            "msLAPS-EncryptedPassword",
            "msLAPS-EncryptedPasswordHistory",
            "netbootSCPBL",
            "mSMQDigests",
            "mSMQSignCertificates",
            "displayName",
            "lastLogoff",
            "badPasswordTime",
            "badPwdCount",
        ]

        for field in rare_fields:
            if field in raw_data and raw_data[field] is not None:
                additional_attrs[field] = raw_data[field]

        silver_record = {
            # Primary identifier
            "sam_account_name": raw_data.get("sAMAccountName"),
            # Core identity
            "computer_name": raw_data.get("cn") or raw_data.get("name"),
            "dns_hostname": raw_data.get("dNSHostName"),
            "distinguished_name": dn,
            "object_guid": raw_data.get("objectGUID"),
            "object_sid": raw_data.get("objectSid"),
            # OU hierarchy (from parsing function)
            "ou_root": ou_fields["ou_root"],
            "ou_organization_type": ou_fields["ou_organization_type"],
            "ou_organization": ou_fields["ou_organization"],
            "ou_category": ou_fields["ou_category"],
            "ou_division": ou_fields["ou_division"],
            "ou_department": ou_fields["ou_department"],
            "ou_subdepartment": ou_fields["ou_subdepartment"],
            "ou_immediate_parent": ou_fields["ou_immediate_parent"],
            "ou_full_path": ou_fields["ou_full_path"],
            # Operating system
            "operating_system": raw_data.get("operatingSystem"),
            "operating_system_version": raw_data.get("operatingSystemVersion"),
            "operating_system_service_pack": raw_data.get("operatingSystemServicePack"),
            # Account status
            "is_enabled": is_enabled,
            "user_account_control": uac,
            "is_critical_system_object": raw_data.get("isCriticalSystemObject")
            == "True"
            if "isCriticalSystemObject" in raw_data
            else None,
            # Network and services
            "service_principal_names": service_principal_names,
            "network_addresses": network_addresses,
            # Group membership
            "member_of_groups": member_of_groups,
            # Management
            "managed_by": raw_data.get("managedBy"),
            "description": raw_data.get("description"),
            "display_name": raw_data.get("displayName"),
            # Authentication and security
            "pwd_last_set": self._parse_timestamp(raw_data.get("pwdLastSet")),
            "account_expires": self._parse_timestamp(raw_data.get("accountExpires")),
            "bad_password_time": self._parse_timestamp(raw_data.get("badPasswordTime")),
            "bad_pwd_count": self._to_int(raw_data.get("badPwdCount")),
            "ms_laps_password_expiration_time": self._to_bigint(
                raw_data.get("msLAPS-PasswordExpirationTime")
            ),
            "ms_mcs_adm_pwd_expiration_time": self._to_bigint(
                raw_data.get("ms-Mcs-AdmPwdExpirationTime")
            ),
            "msds_supported_encryption_types": self._to_int(
                raw_data.get("msDS-SupportedEncryptionTypes")
            ),
            "msds_key_credential_link": raw_data.get("msDS-KeyCredentialLink"),
            "user_certificate": raw_data.get("userCertificate"),
            # Logon tracking
            "last_logon": self._parse_timestamp(raw_data.get("lastLogon")),
            "last_logon_timestamp": self._parse_timestamp(
                raw_data.get("lastLogonTimestamp")
            ),
            "last_logoff": self._parse_timestamp(raw_data.get("lastLogoff")),
            "logon_count": self._to_int(raw_data.get("logonCount")),
            # AD metadata
            "when_created": self._parse_timestamp(raw_data.get("whenCreated")),
            "when_changed": self._parse_timestamp(raw_data.get("whenChanged")),
            "usn_created": self._to_bigint(raw_data.get("uSNCreated")),
            "usn_changed": self._to_bigint(raw_data.get("uSNChanged")),
            "ds_core_propagation_data": ds_core_propagation_data,
            # Additional attributes (consolidated JSONB)
            "additional_attributes": additional_attrs if additional_attrs else None,
            # Traceability
            "raw_id": raw_id,
            # Metadata (will be set later)
            "source_system": "active_directory",
        }

        # Calculate entity hash
        silver_record["entity_hash"] = self._calculate_content_hash(silver_record)

        return silver_record

    def _upsert_silver_record(
        self,
        silver_record: Dict[str, Any],
        ingestion_run_id: uuid.UUID,
        dry_run: bool = False,
    ) -> str:
        """
        Insert or update a silver.ad_computers record.

        Args:
            silver_record: Dictionary with silver table columns
            ingestion_run_id: UUID of the transformation run
            dry_run: If True, log action but don't execute

        Returns:
            "inserted", "updated", or "unchanged"
        """
        sam_account_name = silver_record["sam_account_name"]

        try:
            # Check if record exists and compare hash
            check_query = """
            SELECT entity_hash
            FROM silver.ad_computers
            WHERE sam_account_name = :sam_account_name
            """

            existing_df = self.db_adapter.query_to_dataframe(
                check_query, {"sam_account_name": sam_account_name}
            )

            if existing_df.empty:
                # Insert new record
                if dry_run:
                    logger.info(
                        f"🔵 [DRY RUN] Would insert new computer: {sam_account_name}"
                    )
                    return "inserted"

                insert_query = """
                INSERT INTO silver.ad_computers (
                    sam_account_name, computer_name, dns_hostname, distinguished_name,
                    object_guid, object_sid,
                    ou_root, ou_organization_type, ou_organization, ou_category,
                    ou_division, ou_department, ou_subdepartment, ou_immediate_parent, ou_full_path,
                    operating_system, operating_system_version, operating_system_service_pack,
                    is_enabled, user_account_control, is_critical_system_object,
                    service_principal_names, network_addresses, member_of_groups,
                    managed_by, description, display_name,
                    pwd_last_set, account_expires, bad_password_time, bad_pwd_count,
                    ms_laps_password_expiration_time, ms_mcs_adm_pwd_expiration_time,
                    msds_supported_encryption_types, msds_key_credential_link, user_certificate,
                    last_logon, last_logon_timestamp, last_logoff, logon_count,
                    when_created, when_changed, usn_created, usn_changed,
                    ds_core_propagation_data, additional_attributes,
                    raw_id, source_system, entity_hash, ingestion_run_id,
                    created_at, updated_at
                ) VALUES (
                    :sam_account_name, :computer_name, :dns_hostname, :distinguished_name,
                    :object_guid, :object_sid,
                    :ou_root, :ou_organization_type, :ou_organization, :ou_category,
                    :ou_division, :ou_department, :ou_subdepartment, :ou_immediate_parent, :ou_full_path,
                    :operating_system, :operating_system_version, :operating_system_service_pack,
                    :is_enabled, :user_account_control, :is_critical_system_object,
                    :service_principal_names, :network_addresses, :member_of_groups,
                    :managed_by, :description, :display_name,
                    :pwd_last_set, :account_expires, :bad_password_time, :bad_pwd_count,
                    :ms_laps_password_expiration_time, :ms_mcs_adm_pwd_expiration_time,
                    :msds_supported_encryption_types, :msds_key_credential_link, :user_certificate,
                    :last_logon, :last_logon_timestamp, :last_logoff, :logon_count,
                    :when_created, :when_changed, :usn_created, :usn_changed,
                    :ds_core_propagation_data, :additional_attributes,
                    :raw_id, :source_system, :entity_hash, :ingestion_run_id,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                """

                params = {**silver_record, "ingestion_run_id": str(ingestion_run_id)}
                # Convert JSONB fields to JSON strings
                params["ou_full_path"] = json.dumps(params["ou_full_path"])
                params["service_principal_names"] = json.dumps(
                    params["service_principal_names"]
                )
                params["network_addresses"] = json.dumps(params["network_addresses"])
                params["member_of_groups"] = json.dumps(params["member_of_groups"])
                params["ds_core_propagation_data"] = json.dumps(
                    params["ds_core_propagation_data"]
                )
                params["additional_attributes"] = (
                    json.dumps(params["additional_attributes"])
                    if params["additional_attributes"]
                    else None
                )

                with self.db_adapter.engine.connect() as conn:
                    conn.execute(text(insert_query), params)
                    conn.commit()
                return "inserted"

            else:
                # Check if content has changed
                existing_hash = existing_df.iloc[0]["entity_hash"]
                if existing_hash == silver_record["entity_hash"]:
                    # No changes
                    return "unchanged"

                # Update existing record
                if dry_run:
                    logger.info(
                        f"🟡 [DRY RUN] Would update computer: {sam_account_name}"
                    )
                    return "updated"

                update_query = """
                UPDATE silver.ad_computers SET
                    computer_name = :computer_name,
                    dns_hostname = :dns_hostname,
                    distinguished_name = :distinguished_name,
                    object_guid = :object_guid,
                    object_sid = :object_sid,
                    ou_root = :ou_root,
                    ou_organization_type = :ou_organization_type,
                    ou_organization = :ou_organization,
                    ou_category = :ou_category,
                    ou_division = :ou_division,
                    ou_department = :ou_department,
                    ou_subdepartment = :ou_subdepartment,
                    ou_immediate_parent = :ou_immediate_parent,
                    ou_full_path = :ou_full_path,
                    operating_system = :operating_system,
                    operating_system_version = :operating_system_version,
                    operating_system_service_pack = :operating_system_service_pack,
                    is_enabled = :is_enabled,
                    user_account_control = :user_account_control,
                    is_critical_system_object = :is_critical_system_object,
                    service_principal_names = :service_principal_names,
                    network_addresses = :network_addresses,
                    member_of_groups = :member_of_groups,
                    managed_by = :managed_by,
                    description = :description,
                    display_name = :display_name,
                    pwd_last_set = :pwd_last_set,
                    account_expires = :account_expires,
                    bad_password_time = :bad_password_time,
                    bad_pwd_count = :bad_pwd_count,
                    ms_laps_password_expiration_time = :ms_laps_password_expiration_time,
                    ms_mcs_adm_pwd_expiration_time = :ms_mcs_adm_pwd_expiration_time,
                    msds_supported_encryption_types = :msds_supported_encryption_types,
                    msds_key_credential_link = :msds_key_credential_link,
                    user_certificate = :user_certificate,
                    last_logon = :last_logon,
                    last_logon_timestamp = :last_logon_timestamp,
                    last_logoff = :last_logoff,
                    logon_count = :logon_count,
                    when_created = :when_created,
                    when_changed = :when_changed,
                    usn_created = :usn_created,
                    usn_changed = :usn_changed,
                    ds_core_propagation_data = :ds_core_propagation_data,
                    additional_attributes = :additional_attributes,
                    raw_id = :raw_id,
                    entity_hash = :entity_hash,
                    ingestion_run_id = :ingestion_run_id,
                    updated_at = CURRENT_TIMESTAMP
                WHERE sam_account_name = :sam_account_name
                """

                params = {**silver_record, "ingestion_run_id": str(ingestion_run_id)}
                # Convert JSONB fields to JSON strings
                params["ou_full_path"] = json.dumps(params["ou_full_path"])
                params["service_principal_names"] = json.dumps(
                    params["service_principal_names"]
                )
                params["network_addresses"] = json.dumps(params["network_addresses"])
                params["member_of_groups"] = json.dumps(params["member_of_groups"])
                params["ds_core_propagation_data"] = json.dumps(
                    params["ds_core_propagation_data"]
                )
                params["additional_attributes"] = (
                    json.dumps(params["additional_attributes"])
                    if params["additional_attributes"]
                    else None
                )

                with self.db_adapter.engine.connect() as conn:
                    conn.execute(text(update_query), params)
                    conn.commit()
                return "updated"

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to upsert silver record for {sam_account_name}: {e}"
            )
            raise

    def transform_ad_computers(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, int]:
        """
        Main transformation function - processes AD computers from bronze → silver.

        Args:
            full_sync: If True, process all computers. If False, only process new/updated
            dry_run: If True, log actions but don't modify database

        Returns:
            Dictionary with transformation statistics
        """
        logger.info("🚀 Starting AD computers transformation to silver layer")
        logger.info(f"🔧 Mode: {'FULL SYNC' if full_sync else 'INCREMENTAL'}")
        logger.info(f"🔧 Dry run: {'YES' if dry_run else 'NO'}")

        stats = {
            "processed": 0,
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "errors": 0,
        }

        # Create transformation run record
        run_id = uuid.uuid4()
        if not dry_run:
            try:
                with self.db_adapter.engine.connect() as conn:
                    conn.execute(
                        text("""
                        INSERT INTO meta.ingestion_runs (
                            run_id, source_system, entity_type, started_at, status
                        ) VALUES (
                            :run_id, 'silver_transformation', 'ad_computer', CURRENT_TIMESTAMP, 'running'
                        )
                        """),
                        {"run_id": str(run_id)},
                    )
                    conn.commit()
            except SQLAlchemyError as e:
                logger.warning(f"⚠️  Could not create transformation run record: {e}")

        try:
            # Get computers needing transformation
            last_timestamp = (
                None if full_sync else self._get_last_transformation_timestamp()
            )
            computers_to_process = self._get_computers_needing_transformation(
                since_timestamp=last_timestamp, full_sync=full_sync
            )

            logger.info(f"📊 Processing {len(computers_to_process)} computers...")

            # Process each computer
            for idx, sam_account_name in enumerate(computers_to_process, 1):
                try:
                    # Fetch latest bronze record
                    bronze_result = self._fetch_latest_bronze_record(sam_account_name)
                    if not bronze_result:
                        logger.warning(
                            f"⚠️  No bronze record found for {sam_account_name}"
                        )
                        stats["errors"] += 1
                        continue

                    raw_data, raw_id = bronze_result

                    # Extract and transform fields
                    silver_record = self._extract_ad_computer_fields(raw_data, raw_id)

                    # Upsert to silver table
                    action = self._upsert_silver_record(
                        silver_record, run_id, dry_run=dry_run
                    )
                    stats[action] += 1
                    stats["processed"] += 1

                    # Progress logging
                    if idx % 100 == 0:
                        logger.info(
                            f"⏳ Progress: {idx}/{len(computers_to_process)} computers processed"
                        )

                except Exception as e:
                    logger.error(
                        f"❌ Error processing computer {sam_account_name}: {e}"
                    )
                    stats["errors"] += 1
                    continue

            # Complete transformation run
            if not dry_run:
                try:
                    with self.db_adapter.engine.connect() as conn:
                        conn.execute(
                            text("""
                            UPDATE meta.ingestion_runs SET
                                completed_at = CURRENT_TIMESTAMP,
                                status = 'completed',
                                records_processed = :processed,
                                records_created = :inserted,
                                records_updated = :updated
                            WHERE run_id = :run_id
                            """),
                            {
                                "run_id": str(run_id),
                                "processed": stats["processed"],
                                "inserted": stats["inserted"],
                                "updated": stats["updated"],
                            },
                        )
                        conn.commit()
                except SQLAlchemyError as e:
                    logger.warning(
                        f"⚠️  Could not update transformation run record: {e}"
                    )

            # Log final statistics
            logger.info("=" * 60)
            logger.info("✅ AD computers transformation completed successfully")
            logger.info(f"📊 Total processed: {stats['processed']}")
            logger.info(f"🆕 Inserted: {stats['inserted']}")
            logger.info(f"🔄 Updated: {stats['updated']}")
            logger.info(f"⏭️  Unchanged: {stats['unchanged']}")
            logger.info(f"❌ Errors: {stats['errors']}")
            logger.info("=" * 60)

            return stats

        except Exception as e:
            logger.error(f"❌ Fatal error during transformation: {e}")
            if not dry_run:
                try:
                    with self.db_adapter.engine.connect() as conn:
                        conn.execute(
                            text("""
                            UPDATE meta.ingestion_runs SET
                                completed_at = CURRENT_TIMESTAMP,
                                status = 'failed',
                                error_message = :error
                            WHERE run_id = :run_id
                            """),
                            {"run_id": str(run_id), "error": str(e)},
                        )
                        conn.commit()
                except SQLAlchemyError:
                    pass
            raise

    def close(self):
        """Close database connections."""
        self.db_adapter.close()
        logger.info("🔌 Database connections closed")


def main():
    """Main entry point for the AD computers transformation script."""
    parser = argparse.ArgumentParser(
        description="Transform bronze Active Directory computer records to silver layer"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all computers (default: incremental based on last run)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate transformation without writing to database",
    )

    args = parser.parse_args()

    # Load environment
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    # Initialize service
    service = ADComputerTransformationService(database_url=database_url)

    try:
        # Run transformation
        stats = service.transform_ad_computers(
            full_sync=args.full_sync, dry_run=args.dry_run
        )

        # Exit with error if there were failures
        if stats["errors"] > 0:
            logger.warning(f"⚠️  Transformation completed with {stats['errors']} errors")
            sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}")
        sys.exit(1)

    finally:
        service.close()


if __name__ == "__main__":
    main()
