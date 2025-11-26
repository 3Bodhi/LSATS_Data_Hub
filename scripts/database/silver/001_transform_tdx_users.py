#!/usr/bin/env python3
"""
TDX Users Source-Specific Silver Layer Transformation Service

This service transforms bronze TeamDynamix user records into the source-specific
silver.tdx_users table. This is TIER 1 of the two-tier silver architecture.

Key features:
- Extracts all TDX user fields from JSONB to typed columns
- Content hash-based change detection
- Incremental processing (only transform users with new bronze data)
- Comprehensive logging with emoji standards
- Dry-run mode for validation
- Standard service class pattern following medallion architecture

The existing transform_silver_users.py handles TIER 2 (consolidated merge from multiple sources).
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


class TDXUserTransformationService:
    """
    Service for transforming bronze TDX user records into source-specific silver layer.

    This service creates silver.tdx_users records from bronze.raw_entities where:
    - entity_type = 'user'
    - source_system = 'tdx'

    Transformation Logic:
    - Extract all TDX fields from JSONB to typed columns
    - Normalize uniqname to lowercase (from AlternateID field)
    - Keep complex fields (Attributes, Applications, GroupIDs) as JSONB
    - Calculate entity_hash for change detection
    - Track raw_id for traceability back to bronze

    This is TIER 1 (source-specific). The consolidated tier 2 merge happens in
    transform_silver_users.py which combines tdx + umapi + mcom + ad sources.
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
        logger.info("🔌 TDX users silver transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful TDX users transformation.

        Returns:
            Timestamp of last completed run, or None if this is the first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'tdx_user'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "🆕 No previous transformation found - processing all users"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last transformation timestamp: {e}")
            return None

    def _get_users_needing_transformation(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Set[str]:
        """
        Find TDX user UIDs that have new/updated bronze records.

        Args:
            since_timestamp: Only include users with bronze records after this time
            full_sync: If True, return ALL TDX users regardless of timestamp

        Returns:
            Set of TDX UIDs (UUID strings) that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                raw_data->>'UID' as tdx_user_uid
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
              AND source_system = 'tdx'
              {time_filter}
              AND raw_data->>'UID' IS NOT NULL
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            user_uids = set(result_df["tdx_user_uid"].tolist())

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"🔍 Found {len(user_uids)} TDX users needing transformation ({sync_mode} mode)"
            )
            return user_uids

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get users needing transformation: {e}")
            raise

    def _fetch_latest_bronze_record(
        self, tdx_user_uid: str
    ) -> Optional[Tuple[Dict, str]]:
        """
        Fetch the latest bronze record for a TDX user.

        Args:
            tdx_user_uid: The TDX UID (UUID string)

        Returns:
            Tuple of (raw_data dict, raw_id UUID) or None if not found
        """
        try:
            query = """
            SELECT raw_data, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
              AND source_system = 'tdx'
              AND raw_data->>'UID' = :tdx_user_uid
            ORDER BY ingested_at DESC
            LIMIT 1
            """

            result_df = self.db_adapter.query_to_dataframe(
                query, {"tdx_user_uid": tdx_user_uid}
            )

            if result_df.empty:
                return None

            return result_df.iloc[0]["raw_data"], result_df.iloc[0]["raw_id"]

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to fetch bronze record for UID {tdx_user_uid}: {e}"
            )
            raise

    def _fetch_latest_bronze_records_batch(
        self, tdx_user_uids: List[str], batch_size: int = 1000
    ) -> Dict[str, Tuple[Dict, str]]:
        """
        Fetch latest bronze records for multiple users in batches.

        This eliminates N individual queries to bronze layer.

        Args:
            tdx_user_uids: List of TDX user UIDs
            batch_size: Records to fetch per query (default: 1000)

        Returns:
            Dict mapping tdx_user_uid -> (raw_data, raw_id)
        """
        results = {}

        try:
            for i in range(0, len(tdx_user_uids), batch_size):
                batch_uids = tdx_user_uids[i : i + batch_size]

                query = """
                WITH ranked_records AS (
                    SELECT
                        raw_data->>'UID' as tdx_user_uid,
                        raw_data,
                        raw_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY raw_data->>'UID'
                            ORDER BY ingested_at DESC
                        ) as row_num
                    FROM bronze.raw_entities
                    WHERE entity_type = 'user'
                      AND source_system = 'tdx'
                      AND raw_data->>'UID' = ANY(:uids)
                )
                SELECT tdx_user_uid, raw_data, raw_id
                FROM ranked_records
                WHERE row_num = 1
                """

                result_df = self.db_adapter.query_to_dataframe(
                    query, {"uids": batch_uids}
                )

                for _, row in result_df.iterrows():
                    uid = row["tdx_user_uid"]
                    results[uid] = (row["raw_data"], row["raw_id"])

                logger.debug(
                    f"📦 Fetched batch {(i // batch_size) + 1} of bronze records ({len(batch_uids)} users)"
                )

            logger.info(
                f"📦 Fetched {len(results)} bronze records in {(len(tdx_user_uids) + batch_size - 1) // batch_size} batches"
            )
            return results

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to batch fetch bronze records: {e}")
            raise

    def _get_existing_user_hashes(self, tdx_user_uids: Set[str]) -> Dict[str, str]:
        """
        Fetch existing entity hashes for all users in one query.

        This eliminates N individual SELECT queries for hash checking.

        Args:
            tdx_user_uids: Set of TDX user UIDs to check

        Returns:
            Dict mapping tdx_user_uid -> entity_hash
        """
        if not tdx_user_uids:
            return {}

        try:
            # Convert string UUIDs to proper format for PostgreSQL
            uids_list = list(tdx_user_uids)

            query = """
            SELECT tdx_user_uid::text, entity_hash
            FROM silver.tdx_users
            WHERE tdx_user_uid::text = ANY(:uids)
            """

            result_df = self.db_adapter.query_to_dataframe(query, {"uids": uids_list})

            hashes = dict(zip(result_df["tdx_user_uid"], result_df["entity_hash"]))
            logger.info(
                f"📋 Fetched existing hashes for {len(hashes)} users from silver layer"
            )
            return hashes

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch existing user hashes: {e}")
            raise

    def _calculate_content_hash(self, raw_data: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 content hash for change detection.

        Only includes significant fields (not metadata like timestamps).

        Args:
            raw_data: Raw user data from bronze layer

        Returns:
            SHA-256 hash string
        """
        # Include only significant fields for change detection
        significant_fields = {
            "UID": raw_data.get("UID"),
            "UserName": raw_data.get("UserName"),
            "FirstName": raw_data.get("FirstName"),
            "LastName": raw_data.get("LastName"),
            "FullName": raw_data.get("FullName"),
            "PrimaryEmail": raw_data.get("PrimaryEmail"),
            "AlternateEmail": raw_data.get("AlternateEmail"),
            "AlternateID": raw_data.get("AlternateID"),
            "ExternalID": raw_data.get("ExternalID"),
            "IsActive": raw_data.get("IsActive"),
            "Title": raw_data.get("Title"),
            "DefaultAccountID": raw_data.get("DefaultAccountID"),
            "WorkPhone": raw_data.get("WorkPhone"),
            "WorkAddress": raw_data.get("WorkAddress"),
            "Attributes": raw_data.get("Attributes", []),
            "Applications": raw_data.get("Applications", []),
            "GroupIDs": raw_data.get("GroupIDs", []),
        }

        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _parse_tdx_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Parse TeamDynamix timestamp strings into Python datetime objects.

        Args:
            timestamp_str: ISO format timestamp (e.g., "2020-02-18T22:10:00Z")

        Returns:
            datetime object with timezone, or None if parsing fails
        """
        if not timestamp_str:
            return None

        try:
            parsed_dt = dateutil.parser.isoparse(timestamp_str)
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            return parsed_dt
        except (ValueError, TypeError) as e:
            logger.debug(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None

    def _extract_tdx_fields(
        self, raw_data: Dict[str, Any], raw_id: str
    ) -> Dict[str, Any]:
        """
        Extract and type-cast TDX fields from bronze JSONB to silver columns.

        Maps all 65+ fields from the TDX user API to typed columns.

        Args:
            raw_data: Raw JSONB data from bronze.raw_entities
            raw_id: UUID of the bronze record

        Returns:
            Dictionary with all silver.tdx_users columns
        """

        # Helper to safely convert to UUID or return None
        def to_uuid(val):
            if val is None:
                return None
            try:
                return str(uuid.UUID(str(val)))
            except (ValueError, AttributeError):
                return None

        silver_record = {
            # Primary identifier
            "tdx_user_uid": to_uuid(raw_data.get("UID")),
            # Business key (normalized to lowercase)
            "uniqname": raw_data.get("AlternateID", "").lower() or None,
            # Core identity
            "external_id": raw_data.get("ExternalID"),
            "username": raw_data.get("UserName"),
            "first_name": raw_data.get("FirstName"),
            "middle_name": raw_data.get("MiddleName"),
            "last_name": raw_data.get("LastName"),
            "full_name": raw_data.get("FullName"),
            "nickname": raw_data.get("Nickname"),
            # Contact information
            "primary_email": raw_data.get("PrimaryEmail"),
            "alternate_email": raw_data.get("AlternateEmail"),
            "alert_email": raw_data.get("AlertEmail"),
            "work_phone": raw_data.get("WorkPhone"),
            "mobile_phone": raw_data.get("MobilePhone"),
            "home_phone": raw_data.get("HomePhone"),
            "fax": raw_data.get("Fax"),
            "other_phone": raw_data.get("OtherPhone"),
            "pager": raw_data.get("Pager"),
            "im_provider": raw_data.get("IMProvider"),
            "im_handle": raw_data.get("IMHandle"),
            # Work location
            "work_address": raw_data.get("WorkAddress"),
            "work_city": raw_data.get("WorkCity"),
            "work_state": raw_data.get("WorkState"),
            "work_zip": raw_data.get("WorkZip"),
            "work_country": raw_data.get("WorkCountry"),
            # Employment and organizational
            "title": raw_data.get("Title"),
            "company": raw_data.get("Company"),
            "default_account_id": raw_data.get("DefaultAccountID"),
            "default_account_name": raw_data.get("DefaultAccountName"),
            "location_id": raw_data.get("LocationID"),
            "location_name": raw_data.get("LocationName"),
            "location_room_id": raw_data.get("LocationRoomID"),
            "location_room_name": raw_data.get("LocationRoomName"),
            "reports_to_uid": to_uuid(raw_data.get("ReportsToUID")),
            "reports_to_full_name": raw_data.get("ReportsToFullName"),
            # Status and authentication
            "is_active": raw_data.get("IsActive", True),
            "is_employee": raw_data.get("IsEmployee"),
            "is_confidential": raw_data.get("IsConfidential"),
            "authentication_provider_id": raw_data.get("AuthenticationProviderID"),
            "authentication_user_name": raw_data.get("AuthenticationUserName"),
            "security_role_id": to_uuid(raw_data.get("SecurityRoleID")),
            "security_role_name": raw_data.get("SecurityRoleName"),
            # TDX operational fields
            "beid": raw_data.get("BEID"),
            "beid_int": raw_data.get("BEIDInt"),
            "default_priority_id": raw_data.get("DefaultPriorityID"),
            "default_priority_name": raw_data.get("DefaultPriorityName"),
            "should_report_time": raw_data.get("ShouldReportTime"),
            "is_capacity_managed": raw_data.get("IsCapacityManaged"),
            "default_rate": raw_data.get("DefaultRate"),
            "cost_rate": raw_data.get("CostRate"),
            "primary_client_portal_application_id": raw_data.get(
                "PrimaryClientPortalApplicationID"
            ),
            # Signature and profile
            "technician_signature": raw_data.get("TechnicianSignature"),
            "profile_image_file_name": raw_data.get("ProfileImageFileName"),
            "apply_technician_signature_to_replies": raw_data.get(
                "ApplyTechnicianSignatureToReplies"
            ),
            "apply_technician_signature_to_updates_and_comments": raw_data.get(
                "ApplyTechnicianSignatureToUpdatesAndComments"
            ),
            # Dates
            "end_date": self._parse_tdx_timestamp(raw_data.get("EndDate")),
            "report_time_after_date": self._parse_tdx_timestamp(
                raw_data.get("ReportTimeAfterDate")
            ),
            # Complex fields (JSONB arrays/objects)
            "attributes": raw_data.get("Attributes", []),
            "applications": raw_data.get("Applications", []),
            "org_applications": raw_data.get("OrgApplications", []),
            "group_ids": raw_data.get("GroupIDs", []),
            "permissions": raw_data.get("Permissions", {}),
            # Traceability
            "raw_id": raw_id,
            "raw_data_snapshot": None,  # Optional: set to raw_data for full audit
            # Standard metadata
            "source_system": "tdx",
            "entity_hash": self._calculate_content_hash(raw_data),
        }

        return silver_record

    def _bulk_upsert_silver_records(
        self,
        silver_records: List[Dict[str, Any]],
        run_id: str,
        dry_run: bool = False,
        batch_size: int = 500,
    ) -> int:
        """
        Bulk upsert multiple silver records using batched operations.

        This is 10-100x faster than individual UPSERTs for large datasets.

        Args:
            silver_records: List of silver records to upsert
            run_id: Current transformation run ID
            dry_run: If True, log what would be done but don't commit
            batch_size: Records per batch (500-1000 recommended)

        Returns:
            Number of records processed
        """
        if not silver_records:
            return 0

        if dry_run:
            logger.info(
                f"[DRY RUN] Would bulk upsert {len(silver_records)} records in batches of {batch_size}"
            )
            return len(silver_records)

        total_processed = 0

        try:
            # Process in batches
            for i in range(0, len(silver_records), batch_size):
                batch = silver_records[i : i + batch_size]

                # Prepare batch data
                batch_params = []
                for record in batch:
                    batch_params.append(
                        {
                            **record,
                            # Convert JSONB fields to JSON strings
                            "attributes": json.dumps(record.get("attributes", [])),
                            "applications": json.dumps(record.get("applications", [])),
                            "org_applications": json.dumps(
                                record.get("org_applications", [])
                            ),
                            "group_ids": json.dumps(record.get("group_ids", [])),
                            "permissions": json.dumps(record.get("permissions", {})),
                            "raw_data_snapshot": json.dumps(
                                record.get("raw_data_snapshot")
                            )
                            if record.get("raw_data_snapshot")
                            else None,
                            "ingestion_run_id": run_id,
                            "created_at": datetime.now(timezone.utc),
                            "updated_at": datetime.now(timezone.utc),
                        }
                    )

                # Execute batch upsert
                with self.db_adapter.engine.connect() as conn:
                    upsert_query = text("""
                        INSERT INTO silver.tdx_users (
                            tdx_user_uid, uniqname, external_id, username,
                            first_name, middle_name, last_name, full_name, nickname,
                            primary_email, alternate_email, alert_email,
                            work_phone, mobile_phone, home_phone, fax, other_phone, pager,
                            im_provider, im_handle,
                            work_address, work_city, work_state, work_zip, work_country,
                            title, company, default_account_id, default_account_name,
                            location_id, location_name, location_room_id, location_room_name,
                            reports_to_uid, reports_to_full_name,
                            is_active, is_employee, is_confidential,
                            authentication_provider_id, authentication_user_name,
                            security_role_id, security_role_name,
                            beid, beid_int, default_priority_id, default_priority_name,
                            should_report_time, is_capacity_managed,
                            default_rate, cost_rate, primary_client_portal_application_id,
                            technician_signature, profile_image_file_name,
                            apply_technician_signature_to_replies,
                            apply_technician_signature_to_updates_and_comments,
                            end_date, report_time_after_date,
                            attributes, applications, org_applications, group_ids, permissions,
                            raw_id, raw_data_snapshot, source_system, entity_hash,
                            ingestion_run_id, created_at, updated_at
                        ) VALUES (
                            :tdx_user_uid, :uniqname, :external_id, :username,
                            :first_name, :middle_name, :last_name, :full_name, :nickname,
                            :primary_email, :alternate_email, :alert_email,
                            :work_phone, :mobile_phone, :home_phone, :fax, :other_phone, :pager,
                            :im_provider, :im_handle,
                            :work_address, :work_city, :work_state, :work_zip, :work_country,
                            :title, :company, :default_account_id, :default_account_name,
                            :location_id, :location_name, :location_room_id, :location_room_name,
                            :reports_to_uid, :reports_to_full_name,
                            :is_active, :is_employee, :is_confidential,
                            :authentication_provider_id, :authentication_user_name,
                            :security_role_id, :security_role_name,
                            :beid, :beid_int, :default_priority_id, :default_priority_name,
                            :should_report_time, :is_capacity_managed,
                            :default_rate, :cost_rate, :primary_client_portal_application_id,
                            :technician_signature, :profile_image_file_name,
                            :apply_technician_signature_to_replies,
                            :apply_technician_signature_to_updates_and_comments,
                            :end_date, :report_time_after_date,
                            CAST(:attributes AS jsonb), CAST(:applications AS jsonb),
                            CAST(:org_applications AS jsonb), CAST(:group_ids AS jsonb),
                            CAST(:permissions AS jsonb),
                            :raw_id, CAST(:raw_data_snapshot AS jsonb),
                            :source_system, :entity_hash,
                            :ingestion_run_id, :created_at, :updated_at
                        )
                        ON CONFLICT (tdx_user_uid) DO UPDATE SET
                            uniqname = EXCLUDED.uniqname,
                            external_id = EXCLUDED.external_id,
                            username = EXCLUDED.username,
                            first_name = EXCLUDED.first_name,
                            middle_name = EXCLUDED.middle_name,
                            last_name = EXCLUDED.last_name,
                            full_name = EXCLUDED.full_name,
                            nickname = EXCLUDED.nickname,
                            primary_email = EXCLUDED.primary_email,
                            alternate_email = EXCLUDED.alternate_email,
                            alert_email = EXCLUDED.alert_email,
                            work_phone = EXCLUDED.work_phone,
                            mobile_phone = EXCLUDED.mobile_phone,
                            home_phone = EXCLUDED.home_phone,
                            fax = EXCLUDED.fax,
                            other_phone = EXCLUDED.other_phone,
                            pager = EXCLUDED.pager,
                            im_provider = EXCLUDED.im_provider,
                            im_handle = EXCLUDED.im_handle,
                            work_address = EXCLUDED.work_address,
                            work_city = EXCLUDED.work_city,
                            work_state = EXCLUDED.work_state,
                            work_zip = EXCLUDED.work_zip,
                            work_country = EXCLUDED.work_country,
                            title = EXCLUDED.title,
                            company = EXCLUDED.company,
                            default_account_id = EXCLUDED.default_account_id,
                            default_account_name = EXCLUDED.default_account_name,
                            location_id = EXCLUDED.location_id,
                            location_name = EXCLUDED.location_name,
                            location_room_id = EXCLUDED.location_room_id,
                            location_room_name = EXCLUDED.location_room_name,
                            reports_to_uid = EXCLUDED.reports_to_uid,
                            reports_to_full_name = EXCLUDED.reports_to_full_name,
                            is_active = EXCLUDED.is_active,
                            is_employee = EXCLUDED.is_employee,
                            is_confidential = EXCLUDED.is_confidential,
                            authentication_provider_id = EXCLUDED.authentication_provider_id,
                            authentication_user_name = EXCLUDED.authentication_user_name,
                            security_role_id = EXCLUDED.security_role_id,
                            security_role_name = EXCLUDED.security_role_name,
                            beid = EXCLUDED.beid,
                            beid_int = EXCLUDED.beid_int,
                            default_priority_id = EXCLUDED.default_priority_id,
                            default_priority_name = EXCLUDED.default_priority_name,
                            should_report_time = EXCLUDED.should_report_time,
                            is_capacity_managed = EXCLUDED.is_capacity_managed,
                            default_rate = EXCLUDED.default_rate,
                            cost_rate = EXCLUDED.cost_rate,
                            primary_client_portal_application_id = EXCLUDED.primary_client_portal_application_id,
                            technician_signature = EXCLUDED.technician_signature,
                            profile_image_file_name = EXCLUDED.profile_image_file_name,
                            apply_technician_signature_to_replies = EXCLUDED.apply_technician_signature_to_replies,
                            apply_technician_signature_to_updates_and_comments = EXCLUDED.apply_technician_signature_to_updates_and_comments,
                            end_date = EXCLUDED.end_date,
                            report_time_after_date = EXCLUDED.report_time_after_date,
                            attributes = EXCLUDED.attributes,
                            applications = EXCLUDED.applications,
                            org_applications = EXCLUDED.org_applications,
                            group_ids = EXCLUDED.group_ids,
                            permissions = EXCLUDED.permissions,
                            raw_id = EXCLUDED.raw_id,
                            raw_data_snapshot = EXCLUDED.raw_data_snapshot,
                            entity_hash = EXCLUDED.entity_hash,
                            ingestion_run_id = EXCLUDED.ingestion_run_id,
                            updated_at = EXCLUDED.updated_at
                        WHERE silver.tdx_users.entity_hash != EXCLUDED.entity_hash
                    """)

                    # Execute batch in single transaction
                    conn.execute(upsert_query, batch_params)
                    conn.commit()

                total_processed += len(batch)

                batch_num = (i // batch_size) + 1
                total_batches = (len(silver_records) + batch_size - 1) // batch_size
                logger.debug(
                    f"💾 Batch {batch_num}/{total_batches} upserted ({len(batch)} records)"
                )

            logger.info(
                f"✅ Bulk upserted {total_processed} records in {(len(silver_records) + batch_size - 1) // batch_size} batches"
            )
            return total_processed

        except SQLAlchemyError as e:
            logger.error(f"❌ Bulk upsert failed: {e}")
            raise

    def _upsert_silver_record(
        self, silver_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> str:
        """
        Insert or update a silver.tdx_users record.

        Uses PostgreSQL UPSERT (INSERT ... ON CONFLICT) to handle both
        new users and updates to existing ones.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
            dry_run: If True, log what would be done but don't commit

        Returns:
            Action taken: 'created', 'updated', or 'skipped'
        """
        tdx_user_uid = silver_record["tdx_user_uid"]

        if dry_run:
            logger.info(
                f"[DRY RUN] Would upsert user: UID={tdx_user_uid}, uniqname={silver_record.get('uniqname')}"
            )
            return "dry_run"

        try:
            # Check if exists and compare hash
            check_query = """
            SELECT entity_hash
            FROM silver.tdx_users
            WHERE tdx_user_uid = :tdx_user_uid
            """
            existing_df = self.db_adapter.query_to_dataframe(
                check_query, {"tdx_user_uid": tdx_user_uid}
            )

            is_new = existing_df.empty
            existing_hash = None if is_new else existing_df.iloc[0]["entity_hash"]

            # Skip if unchanged
            if not is_new and existing_hash == silver_record["entity_hash"]:
                logger.debug(f"⏭️  User unchanged, skipping: {tdx_user_uid}")
                return "skipped"

            with self.db_adapter.engine.connect() as conn:
                upsert_query = text("""
                    INSERT INTO silver.tdx_users (
                        tdx_user_uid, uniqname, external_id, username,
                        first_name, middle_name, last_name, full_name, nickname,
                        primary_email, alternate_email, alert_email,
                        work_phone, mobile_phone, home_phone, fax, other_phone, pager,
                        im_provider, im_handle,
                        work_address, work_city, work_state, work_zip, work_country,
                        title, company, default_account_id, default_account_name,
                        location_id, location_name, location_room_id, location_room_name,
                        reports_to_uid, reports_to_full_name,
                        is_active, is_employee, is_confidential,
                        authentication_provider_id, authentication_user_name,
                        security_role_id, security_role_name,
                        beid, beid_int, default_priority_id, default_priority_name,
                        should_report_time, is_capacity_managed,
                        default_rate, cost_rate, primary_client_portal_application_id,
                        technician_signature, profile_image_file_name,
                        apply_technician_signature_to_replies,
                        apply_technician_signature_to_updates_and_comments,
                        end_date, report_time_after_date,
                        attributes, applications, org_applications, group_ids, permissions,
                        raw_id, raw_data_snapshot, source_system, entity_hash,
                        ingestion_run_id, created_at, updated_at
                    ) VALUES (
                        :tdx_user_uid, :uniqname, :external_id, :username,
                        :first_name, :middle_name, :last_name, :full_name, :nickname,
                        :primary_email, :alternate_email, :alert_email,
                        :work_phone, :mobile_phone, :home_phone, :fax, :other_phone, :pager,
                        :im_provider, :im_handle,
                        :work_address, :work_city, :work_state, :work_zip, :work_country,
                        :title, :company, :default_account_id, :default_account_name,
                        :location_id, :location_name, :location_room_id, :location_room_name,
                        :reports_to_uid, :reports_to_full_name,
                        :is_active, :is_employee, :is_confidential,
                        :authentication_provider_id, :authentication_user_name,
                        :security_role_id, :security_role_name,
                        :beid, :beid_int, :default_priority_id, :default_priority_name,
                        :should_report_time, :is_capacity_managed,
                        :default_rate, :cost_rate, :primary_client_portal_application_id,
                        :technician_signature, :profile_image_file_name,
                        :apply_technician_signature_to_replies,
                        :apply_technician_signature_to_updates_and_comments,
                        :end_date, :report_time_after_date,
                        CAST(:attributes AS jsonb), CAST(:applications AS jsonb),
                        CAST(:org_applications AS jsonb), CAST(:group_ids AS jsonb),
                        CAST(:permissions AS jsonb),
                        :raw_id, CAST(:raw_data_snapshot AS jsonb),
                        :source_system, :entity_hash,
                        :ingestion_run_id, :created_at, :updated_at
                    )
                    ON CONFLICT (tdx_user_uid) DO UPDATE SET
                        uniqname = EXCLUDED.uniqname,
                        external_id = EXCLUDED.external_id,
                        username = EXCLUDED.username,
                        first_name = EXCLUDED.first_name,
                        middle_name = EXCLUDED.middle_name,
                        last_name = EXCLUDED.last_name,
                        full_name = EXCLUDED.full_name,
                        nickname = EXCLUDED.nickname,
                        primary_email = EXCLUDED.primary_email,
                        alternate_email = EXCLUDED.alternate_email,
                        alert_email = EXCLUDED.alert_email,
                        work_phone = EXCLUDED.work_phone,
                        mobile_phone = EXCLUDED.mobile_phone,
                        home_phone = EXCLUDED.home_phone,
                        fax = EXCLUDED.fax,
                        other_phone = EXCLUDED.other_phone,
                        pager = EXCLUDED.pager,
                        im_provider = EXCLUDED.im_provider,
                        im_handle = EXCLUDED.im_handle,
                        work_address = EXCLUDED.work_address,
                        work_city = EXCLUDED.work_city,
                        work_state = EXCLUDED.work_state,
                        work_zip = EXCLUDED.work_zip,
                        work_country = EXCLUDED.work_country,
                        title = EXCLUDED.title,
                        company = EXCLUDED.company,
                        default_account_id = EXCLUDED.default_account_id,
                        default_account_name = EXCLUDED.default_account_name,
                        location_id = EXCLUDED.location_id,
                        location_name = EXCLUDED.location_name,
                        location_room_id = EXCLUDED.location_room_id,
                        location_room_name = EXCLUDED.location_room_name,
                        reports_to_uid = EXCLUDED.reports_to_uid,
                        reports_to_full_name = EXCLUDED.reports_to_full_name,
                        is_active = EXCLUDED.is_active,
                        is_employee = EXCLUDED.is_employee,
                        is_confidential = EXCLUDED.is_confidential,
                        authentication_provider_id = EXCLUDED.authentication_provider_id,
                        authentication_user_name = EXCLUDED.authentication_user_name,
                        security_role_id = EXCLUDED.security_role_id,
                        security_role_name = EXCLUDED.security_role_name,
                        beid = EXCLUDED.beid,
                        beid_int = EXCLUDED.beid_int,
                        default_priority_id = EXCLUDED.default_priority_id,
                        default_priority_name = EXCLUDED.default_priority_name,
                        should_report_time = EXCLUDED.should_report_time,
                        is_capacity_managed = EXCLUDED.is_capacity_managed,
                        default_rate = EXCLUDED.default_rate,
                        cost_rate = EXCLUDED.cost_rate,
                        primary_client_portal_application_id = EXCLUDED.primary_client_portal_application_id,
                        technician_signature = EXCLUDED.technician_signature,
                        profile_image_file_name = EXCLUDED.profile_image_file_name,
                        apply_technician_signature_to_replies = EXCLUDED.apply_technician_signature_to_replies,
                        apply_technician_signature_to_updates_and_comments = EXCLUDED.apply_technician_signature_to_updates_and_comments,
                        end_date = EXCLUDED.end_date,
                        report_time_after_date = EXCLUDED.report_time_after_date,
                        attributes = EXCLUDED.attributes,
                        applications = EXCLUDED.applications,
                        org_applications = EXCLUDED.org_applications,
                        group_ids = EXCLUDED.group_ids,
                        permissions = EXCLUDED.permissions,
                        raw_id = EXCLUDED.raw_id,
                        raw_data_snapshot = EXCLUDED.raw_data_snapshot,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = EXCLUDED.updated_at
                    WHERE silver.tdx_users.entity_hash != EXCLUDED.entity_hash
                """)

                conn.execute(
                    upsert_query,
                    {
                        **silver_record,
                        # Convert JSONB fields to JSON strings
                        "attributes": json.dumps(silver_record.get("attributes", [])),
                        "applications": json.dumps(
                            silver_record.get("applications", [])
                        ),
                        "org_applications": json.dumps(
                            silver_record.get("org_applications", [])
                        ),
                        "group_ids": json.dumps(silver_record.get("group_ids", [])),
                        "permissions": json.dumps(silver_record.get("permissions", {})),
                        "raw_data_snapshot": json.dumps(
                            silver_record.get("raw_data_snapshot")
                        )
                        if silver_record.get("raw_data_snapshot")
                        else None,
                        "ingestion_run_id": run_id,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )

                conn.commit()

            action = "created" if is_new else "updated"
            logger.debug(
                f"✅ {action.capitalize()} user: {tdx_user_uid} (uniqname: {silver_record.get('uniqname')})"
            )
            return action

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to upsert user {tdx_user_uid}: {e}")
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
                "transformation_type": "bronze_to_silver_tdx_users",
                "entity_type": "tdx_user",
                "source_table": "bronze.raw_entities",
                "target_table": "silver.tdx_users",
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
                        :run_id, 'silver_transformation', 'tdx_user', :started_at, 'running', :metadata
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
        Main entry point: Transform bronze TDX users to silver.tdx_users incrementally.

        Process flow:
        1. Determine last successful transformation timestamp (unless full_sync)
        2. Find TDX users with bronze records newer than that timestamp
        3. For each user:
           a. Fetch latest bronze record
           b. Extract fields to silver columns
           c. Calculate entity hash
           d. Upsert to silver.tdx_users
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
                logger.info("🔄 Full sync mode: Processing ALL TDX users")
            elif last_transformation:
                logger.info(
                    f"⚡ Incremental mode: Processing users since {last_transformation}"
                )
            else:
                logger.info("🆕 First run: Processing ALL TDX users")

            logger.info("🚀 Starting TDX users silver transformation...")

            # Find users needing transformation
            user_uids = self._get_users_needing_transformation(
                last_transformation, full_sync
            )

            if not user_uids:
                logger.info("✨ All records up to date - no transformation needed")
                self.complete_transformation_run(run_id, 0, 0, 0, 0)
                return stats

            user_uids_list = list(user_uids)
            total_users = len(user_uids_list)

            # Calculate dynamic progress interval (log every 2000 records max, min 100)
            progress_interval = min(2000, max(100, total_users // 50))
            batch_size = 500  # Records per upsert batch

            logger.info(f"📊 Processing {total_users:,} TDX users")
            logger.info(f"📈 Progress updates: Every {progress_interval:,} records")
            logger.info(f"💾 Batch size: {batch_size} records per upsert")
            logger.info("")

            # PHASE 1: Batch fetch all bronze records (replaces N queries with ~100 queries)
            logger.info("📦 Phase 1: Fetching bronze records in batches...")
            bronze_records = self._fetch_latest_bronze_records_batch(user_uids_list)
            logger.info("")

            # PHASE 2: Batch fetch existing hashes (replaces N queries with 1 query)
            logger.info("📋 Phase 2: Fetching existing silver hashes...")
            existing_hashes = self._get_existing_user_hashes(user_uids)
            logger.info("")

            # PHASE 3: Process users and accumulate changes
            logger.info("⚙️  Phase 3: Processing users and accumulating changes...")
            upsert_batch = []
            pending_creates = 0
            pending_updates = 0

            for idx, tdx_user_uid in enumerate(user_uids_list, 1):
                try:
                    # Get bronze record from pre-fetched batch
                    bronze_result = bronze_records.get(tdx_user_uid)

                    if not bronze_result:
                        logger.warning(
                            f"⚠️  No bronze data found for UID {tdx_user_uid}"
                        )
                        stats["errors"].append(f"No bronze data for {tdx_user_uid}")
                        continue

                    raw_data, raw_id = bronze_result

                    # Extract TDX fields to silver columns
                    silver_record = self._extract_tdx_fields(raw_data, raw_id)

                    # Check hash against pre-fetched existing hashes (in-memory, no query)
                    existing_hash = existing_hashes.get(tdx_user_uid)
                    new_hash = silver_record["entity_hash"]

                    if existing_hash == new_hash:
                        # Unchanged - skip
                        stats["records_skipped"] += 1
                        logger.debug(f"⏭️  User unchanged, skipping: {tdx_user_uid}")
                        continue

                    # Determine if create or update
                    is_new = tdx_user_uid not in existing_hashes
                    if is_new:
                        pending_creates += 1
                    else:
                        pending_updates += 1

                    # Add to batch
                    upsert_batch.append(silver_record)

                    # Flush batch when full
                    if len(upsert_batch) >= batch_size:
                        self._bulk_upsert_silver_records(
                            upsert_batch, run_id, dry_run, batch_size
                        )

                        stats["records_created"] += pending_creates
                        stats["records_updated"] += pending_updates
                        stats["users_processed"] += len(upsert_batch)

                        # Reset batch
                        upsert_batch = []
                        pending_creates = 0
                        pending_updates = 0

                    # Log progress periodically
                    if idx % progress_interval == 0 and idx > 0:
                        elapsed = (
                            datetime.now(timezone.utc) - stats["started_at"]
                        ).total_seconds()
                        rate = idx / elapsed if elapsed > 0 else 0
                        remaining_records = total_users - idx
                        eta_seconds = remaining_records / rate if rate > 0 else 0

                        logger.info(
                            f"📈 Progress: {idx:,}/{total_users:,} ({idx / total_users * 100:.1f}%) | "
                            f"✅ Created: {stats['records_created']:,} | "
                            f"📝 Updated: {stats['records_updated']:,} | "
                            f"⏭️  Skipped: {stats['records_skipped']:,} | "
                            f"⚡ Rate: {rate:.1f} rec/s | "
                            f"⏱️  ETA: {eta_seconds / 60:.1f} min"
                        )

                except Exception as record_error:
                    error_msg = (
                        f"Error processing user {tdx_user_uid}: {str(record_error)}"
                    )
                    logger.error(f"❌ {error_msg}")
                    stats["errors"].append(error_msg)
                    # Continue processing other users

            # Flush remaining records in batch
            if upsert_batch:
                logger.info(
                    f"💾 Flushing final batch of {len(upsert_batch)} records..."
                )
                self._bulk_upsert_silver_records(
                    upsert_batch, run_id, dry_run, batch_size
                )

                stats["records_created"] += pending_creates
                stats["records_updated"] += pending_updates
                stats["users_processed"] += len(upsert_batch)

            logger.info("")

            # Calculate duration
            stats["completed_at"] = datetime.now(timezone.utc)
            stats["duration_seconds"] = (
                stats["completed_at"] - stats["started_at"]
            ).total_seconds()

            # Complete the run
            if not dry_run:
                self.complete_transformation_run(
                    run_id,
                    stats["users_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                )

            # Log summary
            logger.info("=" * 80)
            logger.info("🎉 TRANSFORMATION COMPLETE")
            logger.info("=" * 80)
            logger.info(f"📊 Users processed: {stats['users_processed']:,}")
            logger.info(f"🆕 Records created: {stats['records_created']:,}")
            logger.info(f"📝 Records updated: {stats['records_updated']:,}")
            logger.info(f"⏭️  Records skipped (unchanged): {stats['records_skipped']:,}")
            if stats["errors"]:
                logger.warning(f"⚠️  Errors encountered: {len(stats['errors']):,}")
                if len(stats["errors"]) <= 5:
                    logger.warning("Errors:")
                    for error in stats["errors"]:
                        logger.warning(f"  - {error}")
                else:
                    logger.warning("First 5 errors:")
                    for error in stats["errors"][:5]:
                        logger.warning(f"  - {error}")
            logger.info(f"⏱️  Duration: {stats['duration_seconds']:.2f} seconds")
            if stats["users_processed"] > 0:
                rate = stats["users_processed"] / stats["duration_seconds"]
                logger.info(f"⚡ Average rate: {rate:.1f} records/second")
            logger.info("=" * 80)

            return stats

        except Exception as e:
            error_msg = f"Fatal error during transformation: {str(e)}"
            logger.error(f"❌ {error_msg}")
            stats["errors"].append(error_msg)

            if not dry_run:
                self.complete_transformation_run(
                    run_id,
                    stats["users_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                    error_msg,
                )

            raise

    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()


def main():
    """Command-line entry point with argument parsing."""
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Transform TDX users from bronze to source-specific silver layer"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all records (ignore last transformation timestamp)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )
    args = parser.parse_args()

    # Get database URL from environment
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    # Initialize service
    service = TDXUserTransformationService(database_url)

    try:
        # Run transformation
        stats = service.transform_incremental(
            full_sync=args.full_sync, dry_run=args.dry_run
        )

        # Exit with appropriate code
        if stats["errors"]:
            sys.exit(1)
        else:
            sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}")
        sys.exit(1)
    finally:
        service.close()


if __name__ == "__main__":
    main()
