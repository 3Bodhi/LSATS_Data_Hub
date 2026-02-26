#!/usr/bin/env python3
"""
Lab Awards Source-Specific Silver Layer Transformation Service

This service transforms bronze lab_award records into the source-specific
silver.lab_awards table. This is TIER 1 of the two-tier silver architecture.

Key features:
- Extracts all 34 lab award fields from JSONB to typed columns
- Content hash-based change detection
- Incremental processing (only transform awards with new bronze data)
- Date parsing from "M/D/YYYY" format
- Dollar amount parsing from "$X,XXX" format
- Comprehensive logging with emoji standards
- Dry-run mode for validation
- Standard service class pattern following medallion architecture

The extracted typed columns enable efficient aggregation and joins during
future consolidated silver lab table transformations.
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

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


class LabAwardTransformationService:
    """
    Service for transforming bronze lab_award records into source-specific silver layer.

    This service creates silver.lab_awards records from bronze.raw_entities where:
    - entity_type = 'lab_award'
    - source_system = 'lab_awards'

    Transformation Logic:
    - Extract all lab award fields from JSONB to typed columns
    - Parse date strings ("9/22/2006") to DATE
    - Parse dollar strings ("$2,958,610") to NUMERIC(15,2)
    - Calculate entity_hash for change detection
    - Track raw_id for traceability back to bronze

    This is TIER 1 (source-specific). Future consolidated tier 2 will aggregate
    awards by lab/PI for new consolidated lab tables.
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
        logger.info("🔌 Lab awards silver transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful lab awards transformation.

        Returns:
            Timestamp of last completed run, or None if this is the first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'lab_award'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "🆕 No previous transformation found - processing all awards"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last transformation timestamp: {e}")
            return None

    def _get_awards_needing_transformation(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Set[str]:
        """
        Find bronze lab_award raw_ids that have new/updated records.

        Args:
            since_timestamp: Only include awards with bronze records after this time
            full_sync: If True, return ALL lab awards regardless of timestamp

        Returns:
            Set of raw_id UUIDs (strings) that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                raw_id::text as raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'lab_award'
              AND source_system = 'lab_awards'
              {time_filter}
            ORDER BY raw_id::text
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            raw_ids = set(result_df["raw_id"].tolist())

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"🔍 Found {len(raw_ids)} lab award records needing transformation ({sync_mode} mode)"
            )
            return raw_ids

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get awards needing transformation: {e}")
            raise

    def _fetch_bronze_record(self, raw_id: str) -> Optional[Dict]:
        """
        Fetch a bronze record by raw_id.

        Args:
            raw_id: The raw_id UUID (as string)

        Returns:
            Dict with raw_data or None if not found
        """
        try:
            query = """
            SELECT raw_data
            FROM bronze.raw_entities
            WHERE raw_id = :raw_id
            """

            result_df = self.db_adapter.query_to_dataframe(
                query, {"raw_id": raw_id}
            )

            if result_df.empty:
                return None

            return result_df.iloc[0]["raw_data"]

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch bronze record {raw_id}: {e}")
            raise

    def _calculate_content_hash(self, raw_data: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 content hash for change detection.

        Only includes significant fields (excludes metadata like _content_hash, _source_file).

        Args:
            raw_data: Raw award data from bronze layer

        Returns:
            SHA-256 hash string
        """
        # Include only significant fields for change detection
        # Exclude metadata fields starting with '_'
        significant_fields = {
            "Award Id": raw_data.get("Award Id"),
            "Award Title": raw_data.get("Award Title"),
            "Award Class": raw_data.get("Award Class"),
            "Project/Grant": raw_data.get("Project/Grant"),
            "Award Total Dollars": raw_data.get("Award Total Dollars"),
            "Award Direct Dollars": raw_data.get("Award Direct Dollars"),
            "Award Indirect Dollars": raw_data.get("Award Indirect Dollars"),
            "Facilities & Admin Rate (%)": raw_data.get("Facilities & Admin Rate (%)"),
            "Award Project Start Date": raw_data.get("Award Project Start Date"),
            "Award Project End Date": raw_data.get("Award Project End Date"),
            "Pre NCE Project End Date": raw_data.get("Pre NCE Project End Date"),
            "Award Publish Date": raw_data.get("Award Publish Date"),
            "Direct Sponsor Name": raw_data.get("Direct Sponsor Name"),
            "Direct Sponsor Category": raw_data.get("Direct Sponsor Category"),
            "Direct Sponsor Subcategory": raw_data.get("Direct Sponsor Subcategory"),
            "Direct Sponsor Award Reference Number\n(Current Budget Period)": raw_data.get(
                "Direct Sponsor Award Reference Number\n(Current Budget Period)"
            ),
            "Prime Sponsor Name": raw_data.get("Prime Sponsor Name"),
            "Prime Sponsor Category": raw_data.get("Prime Sponsor Category"),
            "Prime Sponsor Subcategory": raw_data.get("Prime Sponsor Subcategory"),
            "Prime Sponsor Award Reference Number": raw_data.get(
                "Prime Sponsor Award Reference Number"
            ),
            "Award Admin Department": raw_data.get("Award Admin Department"),
            "Award Admin School/College": raw_data.get("Award Admin School/College"),
            "Person Uniqname": raw_data.get("Person Uniqname"),
            "Person Role": raw_data.get("Person Role"),
            "Person First Name": raw_data.get("Person First Name"),
            "Person Last Name": raw_data.get("Person Last Name"),
            "Person Appt Department": raw_data.get("Person Appt Department"),
            "Person Appt Department Id": raw_data.get("Person Appt Department Id"),
            "Person Appt School/College": raw_data.get("Person Appt School/College"),
        }

        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _parse_dollar_amount(self, dollar_str: Optional[str]) -> Optional[Decimal]:
        """
        Parse dollar amount from string like "$2,958,610" to Decimal.

        Args:
            dollar_str: Dollar string from bronze

        Returns:
            Decimal amount or None if parsing fails
        """
        if not dollar_str:
            return None

        try:
            # Remove $, commas, and whitespace
            cleaned = str(dollar_str).replace("$", "").replace(",", "").strip()
            if not cleaned:
                return None
            return Decimal(cleaned)
        except (ValueError, InvalidOperation):
            logger.debug(f"Failed to parse dollar amount: '{dollar_str}'")
            return None

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """
        Parse date from strings like "9/22/2006" or "10/1/2006".

        Args:
            date_str: Date string from bronze

        Returns:
            date object or None if parsing fails
        """
        if not date_str:
            return None

        # Try multiple formats
        for fmt in ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"]:
            try:
                return datetime.strptime(str(date_str).strip(), fmt).date()
            except ValueError:
                continue

        logger.debug(f"Failed to parse date: '{date_str}'")
        return None

    def _parse_decimal(self, decimal_str: Optional[str]) -> Optional[Decimal]:
        """
        Parse decimal value (for facilities admin rate).

        Args:
            decimal_str: Decimal string (may have % sign)

        Returns:
            Decimal or None if parsing fails
        """
        if not decimal_str:
            return None

        try:
            cleaned = str(decimal_str).replace("%", "").strip()
            if not cleaned:
                return None
            return Decimal(cleaned)
        except (ValueError, InvalidOperation):
            logger.debug(f"Failed to parse decimal: '{decimal_str}'")
            return None

    def _extract_lab_award_fields(
        self, raw_data: Dict[str, Any], raw_id: str
    ) -> Dict[str, Any]:
        """
        Extract and type-cast lab award fields from bronze JSONB to silver columns.

        Args:
            raw_data: Raw JSONB data from bronze.raw_entities
            raw_id: UUID of the bronze record

        Returns:
            Dictionary with all silver.lab_awards columns
        """

        # Helper to normalize strings
        def normalize_str(val):
            if val is None or (isinstance(val, str) and val.strip() == ""):
                return None
            return str(val).strip()

        # Parse department ID (stored as float in bronze, we want string)
        def parse_dept_id(dept_id_raw):
            if not dept_id_raw:
                return None
            try:
                # Convert to int then string to remove decimals (175000.0 → "175000")
                return str(int(float(dept_id_raw)))
            except (ValueError, TypeError):
                return None

        silver_record = {
            # Business keys
            "award_id": normalize_str(raw_data.get("Award Id")),
            "person_uniqname": normalize_str(raw_data.get("Person Uniqname", "")).lower(),
            "person_role": normalize_str(raw_data.get("Person Role")),
            # Award identification
            "project_grant_id": normalize_str(raw_data.get("Project/Grant")),
            "award_title": normalize_str(raw_data.get("Award Title")),
            "award_class": normalize_str(raw_data.get("Award Class")),
            # Financial
            "award_total_dollars": self._parse_dollar_amount(
                raw_data.get("Award Total Dollars")
            ),
            "award_direct_dollars": self._parse_dollar_amount(
                raw_data.get("Award Direct Dollars")
            ),
            "award_indirect_dollars": self._parse_dollar_amount(
                raw_data.get("Award Indirect Dollars")
            ),
            "facilities_admin_rate": self._parse_decimal(
                raw_data.get("Facilities & Admin Rate (%)")
            ),
            # Timeline
            "award_start_date": self._parse_date(
                raw_data.get("Award Project Start Date")
            ),
            "award_end_date": self._parse_date(raw_data.get("Award Project End Date")),
            "pre_nce_end_date": self._parse_date(
                raw_data.get("Pre NCE Project End Date")
            ),
            "award_publish_date": self._parse_date(raw_data.get("Award Publish Date")),
            # Sponsors
            "direct_sponsor_name": normalize_str(raw_data.get("Direct Sponsor Name")),
            "direct_sponsor_category": normalize_str(
                raw_data.get("Direct Sponsor Category")
            ),
            "direct_sponsor_subcategory": normalize_str(
                raw_data.get("Direct Sponsor Subcategory")
            ),
            "direct_sponsor_reference": normalize_str(
                raw_data.get("Direct Sponsor Award Reference Number\n(Current Budget Period)")
            ),
            "prime_sponsor_name": normalize_str(raw_data.get("Prime Sponsor Name")),
            "prime_sponsor_category": normalize_str(
                raw_data.get("Prime Sponsor Category")
            ),
            "prime_sponsor_subcategory": normalize_str(
                raw_data.get("Prime Sponsor Subcategory")
            ),
            "prime_sponsor_reference": normalize_str(
                raw_data.get("Prime Sponsor Award Reference Number")
            ),
            # Administrative
            "award_admin_department": normalize_str(
                raw_data.get("Award Admin Department")
            ),
            "award_admin_school_college": normalize_str(
                raw_data.get("Award Admin School/College")
            ),
            # Person information
            "person_first_name": normalize_str(raw_data.get("Person First Name")),
            "person_last_name": normalize_str(raw_data.get("Person Last Name")),
            "person_appt_department": normalize_str(
                raw_data.get("Person Appt Department")
            ),
            "person_appt_department_id": parse_dept_id(
                raw_data.get("Person Appt Department Id")
            ),
            "person_appt_school_college": normalize_str(
                raw_data.get("Person Appt School/College")
            ),
            # Traceability
            "raw_id": raw_id,
            "raw_data_snapshot": None,  # Optional: set to raw_data for full audit
            # Standard metadata
            "source_system": "lab_awards",
            "entity_hash": self._calculate_content_hash(raw_data),
        }

        return silver_record

    def _upsert_silver_record(
        self, silver_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> str:
        """
        Insert or update a silver.lab_awards record.

        Uses PostgreSQL UPSERT (INSERT ... ON CONFLICT) to handle both
        new awards and updates to existing ones.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
            dry_run: If True, log what would be done but don't commit

        Returns:
            Action taken: 'created', 'updated', or 'skipped'
        """
        award_id = silver_record["award_id"]
        person_uniqname = silver_record["person_uniqname"]
        person_role = silver_record["person_role"]

        if dry_run:
            logger.info(
                f"[DRY RUN] Would upsert award: {award_id}, person={person_uniqname}, role={person_role}"
            )
            return "dry_run"

        try:
            # Check if exists and compare hash
            check_query = """
            SELECT entity_hash
            FROM silver.lab_awards
            WHERE award_id = :award_id
              AND person_uniqname = :person_uniqname
              AND person_role = :person_role
            """
            existing_df = self.db_adapter.query_to_dataframe(
                check_query,
                {
                    "award_id": award_id,
                    "person_uniqname": person_uniqname,
                    "person_role": person_role,
                },
            )

            is_new = existing_df.empty
            existing_hash = None if is_new else existing_df.iloc[0]["entity_hash"]

            # Skip if unchanged
            if not is_new and existing_hash == silver_record["entity_hash"]:
                logger.debug(
                    f"⏭️  Award unchanged, skipping: {award_id} ({person_uniqname})"
                )
                return "skipped"

            with self.db_adapter.engine.connect() as conn:
                upsert_query = text("""
                    INSERT INTO silver.lab_awards (
                        award_id, person_uniqname, person_role,
                        project_grant_id, award_title, award_class,
                        award_total_dollars, award_direct_dollars, award_indirect_dollars,
                        facilities_admin_rate,
                        award_start_date, award_end_date, pre_nce_end_date, award_publish_date,
                        direct_sponsor_name, direct_sponsor_category, direct_sponsor_subcategory,
                        direct_sponsor_reference,
                        prime_sponsor_name, prime_sponsor_category, prime_sponsor_subcategory,
                        prime_sponsor_reference,
                        award_admin_department, award_admin_school_college,
                        person_first_name, person_last_name,
                        person_appt_department, person_appt_department_id, person_appt_school_college,
                        raw_id, raw_data_snapshot, source_system, entity_hash,
                        ingestion_run_id, created_at, updated_at
                    ) VALUES (
                        :award_id, :person_uniqname, :person_role,
                        :project_grant_id, :award_title, :award_class,
                        :award_total_dollars, :award_direct_dollars, :award_indirect_dollars,
                        :facilities_admin_rate,
                        :award_start_date, :award_end_date, :pre_nce_end_date, :award_publish_date,
                        :direct_sponsor_name, :direct_sponsor_category, :direct_sponsor_subcategory,
                        :direct_sponsor_reference,
                        :prime_sponsor_name, :prime_sponsor_category, :prime_sponsor_subcategory,
                        :prime_sponsor_reference,
                        :award_admin_department, :award_admin_school_college,
                        :person_first_name, :person_last_name,
                        :person_appt_department, :person_appt_department_id, :person_appt_school_college,
                        :raw_id, CAST(:raw_data_snapshot AS jsonb), :source_system, :entity_hash,
                        :ingestion_run_id, :created_at, :updated_at
                    )
                    ON CONFLICT (award_id, person_uniqname, person_role) DO UPDATE SET
                        project_grant_id = EXCLUDED.project_grant_id,
                        award_title = EXCLUDED.award_title,
                        award_class = EXCLUDED.award_class,
                        award_total_dollars = EXCLUDED.award_total_dollars,
                        award_direct_dollars = EXCLUDED.award_direct_dollars,
                        award_indirect_dollars = EXCLUDED.award_indirect_dollars,
                        facilities_admin_rate = EXCLUDED.facilities_admin_rate,
                        award_start_date = EXCLUDED.award_start_date,
                        award_end_date = EXCLUDED.award_end_date,
                        pre_nce_end_date = EXCLUDED.pre_nce_end_date,
                        award_publish_date = EXCLUDED.award_publish_date,
                        direct_sponsor_name = EXCLUDED.direct_sponsor_name,
                        direct_sponsor_category = EXCLUDED.direct_sponsor_category,
                        direct_sponsor_subcategory = EXCLUDED.direct_sponsor_subcategory,
                        direct_sponsor_reference = EXCLUDED.direct_sponsor_reference,
                        prime_sponsor_name = EXCLUDED.prime_sponsor_name,
                        prime_sponsor_category = EXCLUDED.prime_sponsor_category,
                        prime_sponsor_subcategory = EXCLUDED.prime_sponsor_subcategory,
                        prime_sponsor_reference = EXCLUDED.prime_sponsor_reference,
                        award_admin_department = EXCLUDED.award_admin_department,
                        award_admin_school_college = EXCLUDED.award_admin_school_college,
                        person_first_name = EXCLUDED.person_first_name,
                        person_last_name = EXCLUDED.person_last_name,
                        person_appt_department = EXCLUDED.person_appt_department,
                        person_appt_department_id = EXCLUDED.person_appt_department_id,
                        person_appt_school_college = EXCLUDED.person_appt_school_college,
                        raw_id = EXCLUDED.raw_id,
                        raw_data_snapshot = EXCLUDED.raw_data_snapshot,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = EXCLUDED.updated_at
                    WHERE silver.lab_awards.entity_hash != EXCLUDED.entity_hash
                """)

                conn.execute(
                    upsert_query,
                    {
                        **silver_record,
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
                f"✅ {action.capitalize()} award: {award_id} "
                f"(person: {person_uniqname}, role: {person_role[:30]}...)"
            )
            return action

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to upsert award {award_id}: {e}")
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
                "transformation_type": "bronze_to_silver_lab_awards",
                "entity_type": "lab_award",
                "source_table": "bronze.raw_entities",
                "target_table": "silver.lab_awards",
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
                        :run_id, 'silver_transformation', 'lab_award', :started_at, 'running', :metadata
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

            logger.info(f"📝 Created transformation run {run_id}")
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
        """Mark a transformation run as completed with comprehensive statistics."""
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
                        "records_updated": records_updated,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"✅ Completed transformation run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete transformation run: {e}")

    def transform_lab_awards(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Transform lab awards from bronze to silver layer.

        This method:
        1. Determines which awards need transformation (incremental or full)
        2. Fetches bronze records
        3. Extracts fields and calculates content hashes
        4. Upserts into silver.lab_awards
        5. Provides detailed statistics

        Args:
            full_sync: If True, process all awards regardless of timestamps
            dry_run: If True, preview without committing

        Returns:
            Dictionary with comprehensive transformation statistics
        """
        stats = {
            "run_id": None,
            "records_processed": 0,
            "records_created": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info(
                "🚀 Starting lab awards silver transformation"
                + (" (FULL SYNC)" if full_sync else " (incremental)")
                + (" [DRY RUN]" if dry_run else "")
            )

            # Step 1: Determine incremental timestamp
            incremental_since = None
            if not full_sync:
                incremental_since = self._get_last_transformation_timestamp()

            # Create transformation run
            run_id = self.create_transformation_run(incremental_since, full_sync)
            stats["run_id"] = run_id

            # Step 2: Get awards needing transformation
            raw_ids_to_process = self._get_awards_needing_transformation(
                incremental_since, full_sync
            )

            if not raw_ids_to_process:
                logger.info("✨ No awards need transformation - all up to date!")
                self.complete_transformation_run(run_id, 0, 0, 0, 0, None)
                return stats

            # Step 3: Process each award
            for raw_id in raw_ids_to_process:
                try:
                    # Fetch bronze record
                    raw_data = self._fetch_bronze_record(raw_id)
                    if not raw_data:
                        logger.warning(f"⚠️  No bronze record found for raw_id {raw_id}")
                        continue

                    # Extract fields to silver schema
                    silver_record = self._extract_lab_award_fields(raw_data, raw_id)

                    # Upsert to silver
                    action = self._upsert_silver_record(silver_record, run_id, dry_run)

                    if action == "created":
                        stats["records_created"] += 1
                    elif action == "updated":
                        stats["records_updated"] += 1
                    elif action == "skipped":
                        stats["records_skipped"] += 1

                    stats["records_processed"] += 1

                    # Progress logging
                    if stats["records_processed"] % 100 == 0:
                        logger.info(
                            f"📊 Progress: {stats['records_processed']}/{len(raw_ids_to_process)} awards processed "
                            f"({stats['records_created']} created, {stats['records_updated']} updated, "
                            f"{stats['records_skipped']} skipped)"
                        )

                except Exception as record_error:
                    error_msg = f"Failed to process award raw_id {raw_id}: {record_error}"
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)

            # Complete the run
            error_summary = None
            if stats["errors"]:
                error_summary = f"{len(stats['errors'])} individual record errors occurred"

            if not dry_run:
                self.complete_transformation_run(
                    run_id,
                    stats["records_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                    error_summary,
                )

            stats["completed_at"] = datetime.now(timezone.utc)
            duration = (stats["completed_at"] - stats["started_at"]).total_seconds()

            # Log comprehensive results
            logger.info(
                f"🎉 Lab awards transformation completed in {duration:.2f} seconds"
            )
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Total Processed: {stats['records_processed']}")
            logger.info(f"   Created: {stats['records_created']}")
            logger.info(f"   Updated: {stats['records_updated']}")
            logger.info(f"   Skipped (unchanged): {stats['records_skipped']}")
            logger.info(f"   Errors: {len(stats['errors'])}")

            return stats

        except Exception as e:
            error_msg = f"Lab awards transformation failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            if stats["run_id"]:
                self.complete_transformation_run(
                    stats["run_id"],
                    stats["records_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                    error_msg,
                )

            raise


def main():
    """Main entry point for lab awards silver transformation."""
    parser = argparse.ArgumentParser(
        description="Transform lab awards from bronze to silver layer"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all awards (ignore incremental timestamp)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview transformation without committing to database",
    )

    args = parser.parse_args()

    # Load environment
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    # Run transformation
    try:
        service = LabAwardTransformationService(database_url)
        stats = service.transform_lab_awards(
            full_sync=args.full_sync, dry_run=args.dry_run
        )

        if stats["errors"]:
            logger.warning(
                f"⚠️  Transformation completed with {len(stats['errors'])} errors"
            )
            sys.exit(1)
        else:
            logger.info("✅ Transformation completed successfully")
            sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
