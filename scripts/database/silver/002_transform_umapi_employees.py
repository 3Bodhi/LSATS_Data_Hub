#!/usr/bin/env python3
"""
UMAPI Employees Source-Specific Silver Layer Transformation Service

This service transforms bronze UMich API employee records into the source-specific
silver.umapi_employees table. This is TIER 1 of the two-tier silver architecture.

Key features:
- Extracts all UMAPI employee fields from JSONB to typed columns
- Handles multi-department employees (stores each employment record separately)
- Content hash-based change detection
- Incremental processing (only transform employees with new bronze data)
- Handles duplicates by selecting latest bronze record
- Comprehensive logging with emoji standards
- Dry-run mode for validation
- Standard service class pattern following medallion architecture

The consolidated silver.users transformation (TIER 2) will aggregate employment
records from umapi_employees into arrays (department_ids, job_codes, supervisor_ids).
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


class UMAPIEmployeeTransformationService:
    """
    Service for transforming bronze UMAPI employee records into source-specific silver layer.

    This service creates silver.umapi_employees records from bronze.raw_entities where:
    - entity_type = 'user'
    - source_system = 'umich_api'

    Transformation Logic:
    - Extract all UMAPI fields from JSONB to typed columns
    - Normalize uniqname to uppercase (UMAPI uses uppercase, unlike TDX)
    - Store each employment record separately (EmplId+EmplRcd composite key)
    - Consolidate work location fields into JSONB
    - Calculate entity_hash for change detection
    - Track raw_id for traceability back to bronze
    - Handle duplicates by selecting latest bronze record (14,633 duplicates exist)

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
        logger.info("🔌 UMAPI employees silver transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful UMAPI employees transformation.

        Returns:
            Timestamp of last completed run, or None if this is the first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'umapi_employee'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "🆕 No previous transformation found - processing all employees"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last transformation timestamp: {e}")
            return None

    def _get_employment_records_needing_transformation(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Set[Tuple[str, int]]:
        """
        Find UMAPI employment records (EmplId, EmplRcd) that have new/updated bronze records.

        Args:
            since_timestamp: Only include records with bronze data after this time
            full_sync: If True, return ALL employment records regardless of timestamp

        Returns:
            Set of (empl_id, empl_rcd) tuples that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                raw_data->>'EmplId' as empl_id,
                (raw_data->>'EmplRcd')::int as empl_rcd
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
              AND source_system = 'umich_api'
              {time_filter}
              AND raw_data->>'EmplId' IS NOT NULL
              AND raw_data->>'EmplRcd' IS NOT NULL
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            employment_records = set(
                (row["empl_id"], row["empl_rcd"]) for _, row in result_df.iterrows()
            )

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"🔍 Found {len(employment_records)} UMAPI employment records needing transformation ({sync_mode} mode)"
            )
            return employment_records

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to get employment records needing transformation: {e}"
            )
            raise

    def _fetch_latest_bronze_record(
        self, empl_id: str, empl_rcd: int
    ) -> Optional[Tuple[Dict, str]]:
        """
        Fetch the latest bronze record for a UMAPI employment record.

        Handles duplicates by selecting the most recent bronze record based on ingested_at.
        (14,633 duplicate EmplId+EmplRcd combinations exist from historical re-ingestions)

        Args:
            empl_id: The UMAPI EmplId
            empl_rcd: The employment record number

        Returns:
            Tuple of (raw_data dict, raw_id UUID) or None if not found
        """
        try:
            query = """
            SELECT raw_data, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
              AND source_system = 'umich_api'
              AND raw_data->>'EmplId' = :empl_id
              AND (raw_data->>'EmplRcd')::int = :empl_rcd
            ORDER BY ingested_at DESC
            LIMIT 1
            """

            result_df = self.db_adapter.query_to_dataframe(
                query, {"empl_id": empl_id, "empl_rcd": empl_rcd}
            )

            if result_df.empty:
                return None

            return result_df.iloc[0]["raw_data"], result_df.iloc[0]["raw_id"]

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to fetch bronze record for EmplId={empl_id}, EmplRcd={empl_rcd}: {e}"
            )
            raise

    def _calculate_content_hash(self, raw_data: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 content hash for change detection.

        Only includes significant fields (not metadata like timestamps or content_hash).

        Args:
            raw_data: Raw employee data from bronze layer

        Returns:
            SHA-256 hash string
        """
        # Include only significant fields for change detection
        significant_fields = {
            "EmplId": raw_data.get("EmplId"),
            "EmplRcd": raw_data.get("EmplRcd"),
            "UniqName": raw_data.get("UniqName"),
            "FirstName": raw_data.get("FirstName"),
            "LastName": raw_data.get("LastName"),
            "Name": raw_data.get("Name"),
            "DepartmentId": raw_data.get("DepartmentId"),
            "Dept_Description": raw_data.get("Dept_Description"),
            "SupervisorId": raw_data.get("SupervisorId"),
            "Jobcode": raw_data.get("Jobcode"),
            "DepartmentJobTitle": raw_data.get("DepartmentJobTitle"),
            "UniversityJobTitle": raw_data.get("UniversityJobTitle"),
            "Work_Address1": raw_data.get("Work_Address1"),
            "Work_Address2": raw_data.get("Work_Address2"),
            "Work_Address3": raw_data.get("Work_Address3"),
            "Work_City": raw_data.get("Work_City"),
            "Work_State": raw_data.get("Work_State"),
            "Work_Postal": raw_data.get("Work_Postal"),
            "Work_Country": raw_data.get("Work_Country"),
            "Work_Phone": raw_data.get("Work_Phone"),
            "Work_Phone_Extension": raw_data.get("Work_Phone_Extension"),
        }

        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _build_work_location_jsonb(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Consolidate work location fields into JSONB object.

        Follows pattern from silver.tdx_users for consistency.

        Args:
            raw_data: Raw employee data from bronze layer

        Returns:
            Dictionary with work location fields
        """

        # Helper to clean empty strings and whitespace-only values
        def clean_value(val):
            if val is None:
                return None
            cleaned = str(val).strip()
            return cleaned if cleaned else None

        return {
            "address1": clean_value(raw_data.get("Work_Address1")),
            "address2": clean_value(raw_data.get("Work_Address2")),
            "address3": clean_value(raw_data.get("Work_Address3")),
            "city": clean_value(raw_data.get("Work_City")),
            "state": clean_value(raw_data.get("Work_State")),
            "postal": clean_value(raw_data.get("Work_Postal")),
            "country": clean_value(raw_data.get("Work_Country")),
            "phone": clean_value(raw_data.get("Work_Phone")),
            "phone_extension": clean_value(raw_data.get("Work_Phone_Extension")),
        }

    def _calculate_data_quality_score(
        self, raw_data: Dict[str, Any], work_location: Dict[str, Any]
    ) -> Tuple[float, List[str]]:
        """
        Calculate data quality score (0.00-1.00) and quality flags.

        Scoring criteria:
        - Has uniqname: +0.30 (critical for joining to silver.users)
        - Has supervisor_id: +0.15
        - Has work phone: +0.15
        - Has complete work location (address1, city, state, postal): +0.20
        - Has jobcode: +0.10
        - Has university_job_title: +0.10

        Args:
            raw_data: Raw employee data from bronze layer
            work_location: Processed work_location JSONB object

        Returns:
            Tuple of (score, flags_list)
        """
        score = 0.0
        flags = []

        # Helper to safely get and strip string values
        def safe_strip(val):
            if val is None:
                return ""
            return str(val).strip()

        # Core identity (30%) - critical for joining
        uniqname = safe_strip(raw_data.get("UniqName"))
        if uniqname:
            score += 0.30
        else:
            flags.append("missing_uniqname")

        # Organizational hierarchy (15%)
        supervisor_id = safe_strip(raw_data.get("SupervisorId"))
        if supervisor_id:
            score += 0.15
        else:
            flags.append("missing_supervisor")

        # Contact info (15%)
        if work_location.get("phone"):
            score += 0.15
        else:
            flags.append("missing_work_phone")

        # Work location completeness (20%)
        location_fields = ["address1", "city", "state", "postal"]
        complete_location = all(work_location.get(f) for f in location_fields)
        if complete_location:
            score += 0.20
        else:
            flags.append("incomplete_work_location")

        # Job classification (10%)
        jobcode = safe_strip(raw_data.get("Jobcode"))
        if jobcode:
            score += 0.10
        else:
            flags.append("missing_jobcode")

        # Job title (10%)
        university_job_title = safe_strip(raw_data.get("UniversityJobTitle"))
        if university_job_title:
            score += 0.10
        else:
            flags.append("missing_university_job_title")

        return round(score, 2), flags

    def _extract_umapi_fields(
        self, raw_data: Dict[str, Any], raw_id: str
    ) -> Dict[str, Any]:
        """
        Extract and type-cast UMAPI fields from bronze JSONB to silver columns.

        Args:
            raw_data: Raw JSONB data from bronze.raw_entities
            raw_id: UUID of the bronze record

        Returns:
            Dictionary with all silver.umapi_employees columns
        """

        # Helper to clean and normalize strings
        def clean_str(val, max_length=None):
            if val is None:
                return None
            cleaned = str(val).strip()
            if not cleaned:
                return None
            if max_length and len(cleaned) > max_length:
                logger.warning(
                    f"⚠️  Value truncated from {len(cleaned)} to {max_length} chars: {cleaned[:50]}..."
                )
                cleaned = cleaned[:max_length]
            return cleaned

        # Build work location JSONB
        work_location = self._build_work_location_jsonb(raw_data)

        # Calculate data quality
        quality_score, quality_flags = self._calculate_data_quality_score(
            raw_data, work_location
        )

        # Extract EmplRcd as integer
        try:
            empl_rcd = int(raw_data.get("EmplRcd", 0))
        except (ValueError, TypeError):
            logger.warning(
                f"⚠️  Invalid EmplRcd value: {raw_data.get('EmplRcd')}, defaulting to 0"
            )
            empl_rcd = 0

        silver_record = {
            # Primary composite key
            "empl_id": clean_str(raw_data.get("EmplId"), 10),
            "empl_rcd": empl_rcd,
            # Business key (normalize to lowercase for consistency across all silver tables)
            "uniqname": clean_str(raw_data.get("UniqName"), 10).lower()
            if clean_str(raw_data.get("UniqName"), 10)
            else None,
            # Core identity
            "first_name": clean_str(raw_data.get("FirstName"), 30),
            "last_name": clean_str(raw_data.get("LastName"), 35),
            "full_name": clean_str(raw_data.get("Name"), 60),
            # Employment and organizational
            "department_id": clean_str(raw_data.get("DepartmentId"), 10),
            "dept_description": clean_str(raw_data.get("Dept_Description"), 50),
            "supervisor_id": clean_str(raw_data.get("SupervisorId"), 10),
            # Job title fields
            "jobcode": clean_str(raw_data.get("Jobcode"), 10),
            "department_job_title": clean_str(raw_data.get("DepartmentJobTitle"), 50),
            "university_job_title": clean_str(
                raw_data.get("UniversityJobTitle")
            ),  # TEXT, no length limit
            # Work location (JSONB)
            "work_location": work_location,
            # Data quality
            "data_quality_score": quality_score,
            "quality_flags": quality_flags,
            # Traceability
            "raw_id": raw_id,
            "entity_hash": self._calculate_content_hash(raw_data),
            # Standard metadata
            "source_system": "umich_api",
        }

        return silver_record

    def _upsert_silver_record(
        self, silver_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> str:
        """
        Insert or update a silver.umapi_employees record.

        Uses PostgreSQL UPSERT (INSERT ... ON CONFLICT) to handle both
        new records and updates to existing ones.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
            dry_run: If True, log what would be done but don't commit

        Returns:
            Action taken: 'created', 'updated', or 'skipped'
        """
        empl_id = silver_record["empl_id"]
        empl_rcd = silver_record["empl_rcd"]

        if dry_run:
            logger.info(
                f"[DRY RUN] Would upsert employee: EmplId={empl_id}, EmplRcd={empl_rcd}, "
                f"uniqname={silver_record.get('uniqname')}"
            )
            return "dry_run"

        try:
            # Check if exists and compare hash
            check_query = """
            SELECT entity_hash
            FROM silver.umapi_employees
            WHERE empl_id = :empl_id AND empl_rcd = :empl_rcd
            """
            existing_df = self.db_adapter.query_to_dataframe(
                check_query, {"empl_id": empl_id, "empl_rcd": empl_rcd}
            )

            is_new = existing_df.empty
            existing_hash = None if is_new else existing_df.iloc[0]["entity_hash"]

            # Skip if unchanged
            if not is_new and existing_hash == silver_record["entity_hash"]:
                logger.debug(
                    f"⏭️  Employment record unchanged, skipping: {empl_id}:{empl_rcd}"
                )
                return "skipped"

            with self.db_adapter.engine.connect() as conn:
                upsert_query = text("""
                    INSERT INTO silver.umapi_employees (
                        empl_id, empl_rcd, uniqname,
                        first_name, last_name, full_name,
                        department_id, dept_description, supervisor_id,
                        jobcode, department_job_title, university_job_title,
                        work_location, data_quality_score, quality_flags,
                        raw_id, entity_hash, source_system,
                        ingestion_run_id, created_at, updated_at
                    ) VALUES (
                        :empl_id, :empl_rcd, :uniqname,
                        :first_name, :last_name, :full_name,
                        :department_id, :dept_description, :supervisor_id,
                        :jobcode, :department_job_title, :university_job_title,
                        CAST(:work_location AS jsonb), :data_quality_score, CAST(:quality_flags AS jsonb),
                        :raw_id, :entity_hash, :source_system,
                        :ingestion_run_id, :created_at, :updated_at
                    )
                    ON CONFLICT (empl_id, empl_rcd) DO UPDATE SET
                        uniqname = EXCLUDED.uniqname,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        full_name = EXCLUDED.full_name,
                        department_id = EXCLUDED.department_id,
                        dept_description = EXCLUDED.dept_description,
                        supervisor_id = EXCLUDED.supervisor_id,
                        jobcode = EXCLUDED.jobcode,
                        department_job_title = EXCLUDED.department_job_title,
                        university_job_title = EXCLUDED.university_job_title,
                        work_location = EXCLUDED.work_location,
                        data_quality_score = EXCLUDED.data_quality_score,
                        quality_flags = EXCLUDED.quality_flags,
                        raw_id = EXCLUDED.raw_id,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = EXCLUDED.updated_at
                    WHERE silver.umapi_employees.entity_hash != EXCLUDED.entity_hash
                """)

                conn.execute(
                    upsert_query,
                    {
                        **silver_record,
                        # Convert JSONB fields to JSON strings
                        "work_location": json.dumps(
                            silver_record.get("work_location", {})
                        ),
                        "quality_flags": json.dumps(
                            silver_record.get("quality_flags", [])
                        ),
                        "ingestion_run_id": run_id,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )

                conn.commit()

            action = "created" if is_new else "updated"
            logger.debug(
                f"✅ {action.capitalize()} employment record: {empl_id}:{empl_rcd} "
                f"(uniqname: {silver_record.get('uniqname') or 'N/A'})"
            )
            return action

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to upsert employment record {empl_id}:{empl_rcd}: {e}"
            )
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
                "transformation_type": "bronze_to_silver_umapi_employees",
                "entity_type": "umapi_employee",
                "source_table": "bronze.raw_entities",
                "target_table": "silver.umapi_employees",
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
                        :run_id, 'silver_transformation', 'umapi_employee', :started_at, 'running', :metadata
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
            records_processed: Total employment records processed
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
                            to_jsonb(:records_skipped::int)
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
        Main entry point: Transform bronze UMAPI employees to silver.umapi_employees incrementally.

        Process flow:
        1. Determine last successful transformation timestamp (unless full_sync)
        2. Find employment records with bronze data newer than that timestamp
        3. For each employment record (EmplId, EmplRcd):
           a. Fetch latest bronze record (handles duplicates)
           b. Extract fields to silver columns
           c. Build work_location JSONB
           d. Calculate data quality score and flags
           e. Calculate entity hash
           f. Upsert to silver.umapi_employees
        4. Track statistics and return results

        Args:
            full_sync: If True, process all records regardless of timestamp
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
            "records_processed": 0,
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
                logger.info(
                    "🔄 Full sync mode: Processing ALL UMAPI employment records"
                )
            elif last_transformation:
                logger.info(
                    f"⚡ Incremental mode: Processing records since {last_transformation}"
                )
            else:
                logger.info("🆕 First run: Processing ALL UMAPI employment records")

            logger.info("🚀 Starting UMAPI employees silver transformation...")

            # Find employment records needing transformation
            employment_records = self._get_employment_records_needing_transformation(
                last_transformation, full_sync
            )

            if not employment_records:
                logger.info("✨ All records up to date - no transformation needed")
                self.complete_transformation_run(run_id, 0, 0, 0, 0)
                return stats

            logger.info(
                f"📊 Processing {len(employment_records)} UMAPI employment records"
            )

            # Process each employment record
            for idx, (empl_id, empl_rcd) in enumerate(employment_records, 1):
                try:
                    # Fetch latest bronze record (handles duplicates)
                    bronze_result = self._fetch_latest_bronze_record(empl_id, empl_rcd)

                    if not bronze_result:
                        logger.warning(
                            f"⚠️  No bronze data found for EmplId={empl_id}, EmplRcd={empl_rcd}"
                        )
                        stats["errors"].append(
                            f"No bronze data for {empl_id}:{empl_rcd}"
                        )
                        continue

                    raw_data, raw_id = bronze_result

                    # Extract UMAPI fields to silver columns
                    silver_record = self._extract_umapi_fields(raw_data, raw_id)

                    # Validate required fields
                    if not silver_record.get("empl_id"):
                        logger.warning(f"⚠️  Missing EmplId in bronze record, skipping")
                        stats["errors"].append(f"Missing EmplId in record {raw_id}")
                        continue

                    if not silver_record.get("first_name"):
                        logger.warning(
                            f"⚠️  Missing FirstName for EmplId={empl_id}, skipping"
                        )
                        stats["errors"].append(
                            f"Missing FirstName for {empl_id}:{empl_rcd}"
                        )
                        continue

                    if not silver_record.get("department_id"):
                        logger.warning(
                            f"⚠️  Missing DepartmentId for EmplId={empl_id}, skipping"
                        )
                        stats["errors"].append(
                            f"Missing DepartmentId for {empl_id}:{empl_rcd}"
                        )
                        continue

                    # Upsert to silver layer
                    action = self._upsert_silver_record(silver_record, run_id, dry_run)

                    if action == "created":
                        stats["records_created"] += 1
                    elif action == "updated":
                        stats["records_updated"] += 1
                    elif action == "skipped":
                        stats["records_skipped"] += 1

                    stats["records_processed"] += 1

                    # Log progress periodically
                    if idx % 1000 == 0:
                        logger.info(
                            f"📈 Progress: {idx}/{len(employment_records)} records processed "
                            f"({stats['records_created']} created, {stats['records_updated']} updated, "
                            f"{stats['records_skipped']} skipped)"
                        )

                except Exception as record_error:
                    error_msg = (
                        f"Error processing employment record {empl_id}:{empl_rcd}: "
                        f"{str(record_error)}"
                    )
                    logger.error(f"❌ {error_msg}")
                    stats["errors"].append(error_msg)
                    # Continue processing other records

            # Calculate duration
            stats["completed_at"] = datetime.now(timezone.utc)
            stats["duration_seconds"] = (
                stats["completed_at"] - stats["started_at"]
            ).total_seconds()

            # Complete the run
            if not dry_run:
                self.complete_transformation_run(
                    run_id,
                    stats["records_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                )

            # Log summary
            logger.info("=" * 80)
            logger.info("🎉 Transformation Complete!")
            logger.info(
                f"📊 Employment records processed: {stats['records_processed']}"
            )
            logger.info(f"🆕 Records created: {stats['records_created']}")
            logger.info(f"📝 Records updated: {stats['records_updated']}")
            logger.info(f"⏭️  Records skipped (unchanged): {stats['records_skipped']}")
            if stats["errors"]:
                logger.warning(f"⚠️  Errors encountered: {len(stats['errors'])}")
            logger.info(f"⏱️  Duration: {stats['duration_seconds']:.2f} seconds")
            logger.info("=" * 80)

            return stats

        except Exception as e:
            error_msg = f"Fatal error during transformation: {str(e)}"
            logger.error(f"❌ {error_msg}")
            stats["errors"].append(error_msg)

            if not dry_run:
                self.complete_transformation_run(
                    run_id,
                    stats["records_processed"],
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
        description="Transform UMAPI employees from bronze to source-specific silver layer"
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
    service = UMAPIEmployeeTransformationService(database_url)

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
