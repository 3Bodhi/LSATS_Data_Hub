#!/usr/bin/env python3
"""
University of Michigan Employee Ingestion with Content Hashing

This service ingests employee data from the UMich Department API using content hashing
for change detection since the umich API doesn't provide modification timestamps.
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
from umich.api.department_api import DepartmentAPI
from umich.api.um_api import create_headers  # For um ich API authentication

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


class UMichEmployeeIngestionService:
    """
    Employee ingestion service for University of Michigan employee data.

    Uses content hashing for change detection since umich API doesn't provide
    modification timestamps. This approach:

    1. Fetches current employee data from umich API
    2. Calculates content hashes for each employee
    3. Compares against stored hashes from previous ingestions
    4. Only creates new bronze records when employee content has actually changed
    5. Preserves complete change history for employee analysis

    Key Features:
    - Efficient change detection without requiring timestamps
    - Employee-department relationship tracking
    - Comprehensive audit trail for employee changes
    - Detailed ingestion statistics and monitoring
    - Dry run support for testing
    - Full sync option to bypass change detection
    """

    def __init__(
        self,
        database_url: str,
        um_base_url: str,
        um_category_id: str,
        um_client_key: str,
        um_client_secret: str,
        scope: str = "department",
        force_full_sync: bool = False,
        dry_run: bool = False,
    ):
        """
        Initialize the UMich employee ingestion service.

        Args:
            database_url: PostgreSQL connection string
            um_base_url: University of Michigan API base URL
            um_category_id: UMich API category ID
            um_client_key: UMich API client key
            um_client_secret: UMich API client secret
            scope: API scope (default: "department")
            force_full_sync: If True, bypass change detection and process all records
            dry_run: If True, preview changes without committing to database
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        # Initialize UMich Department API with proper authentication
        self.um_headers = create_headers(um_client_key, um_client_secret, scope)
        self.um_dept_api = DepartmentAPI(um_base_url, um_category_id, self.um_headers)

        # Store flags
        self.force_full_sync = force_full_sync
        self.dry_run = dry_run

        logger.info(
            f"✅ UMich employee ingestion service initialized with content hashing "
            f"(force_full_sync={'enabled' if force_full_sync else 'disabled'}, "
            f"dry_run={'enabled' if dry_run else 'disabled'})"
        )

    def _calculate_employee_content_hash(self, emp_data: Dict[str, Any]) -> str:
        """
        Calculate a content hash for umich employee data to detect meaningful changes.

        This hash represents the "content fingerprint" of the employee record.
        We include all fields that would represent meaningful employee changes.

        Args:
            emp_data: Raw employee data from umich API

        Returns:
            SHA-256 hash of the normalized employee content
        """
        # Extract significant fields for change detection
        # Based on the actual umich employee API schema
        significant_fields = {
            "EmplId": str(emp_data.get("EmplId", "")).strip(),
            "UniqName": str(emp_data.get("UniqName", "")).strip(),
            "Name": str(emp_data.get("Name", "")).strip(),
            "FirstName": str(emp_data.get("FirstName", "")).strip(),
            "LastName": str(emp_data.get("LastName", "")).strip(),
            "DepartmentId": str(emp_data.get("DepartmentId", "")).strip(),
            "Dept_Description": str(emp_data.get("Dept_Description", "")).strip(),
            "UniversityJobTitle": str(emp_data.get("UniversityJobTitle", "")).strip(),
            "DepartmentJobTitle": str(emp_data.get("DepartmentJobTitle", "")).strip(),
            "Jobcode": str(emp_data.get("Jobcode", "")).strip(),
            "SupervisorId": str(emp_data.get("SupervisorId", "")).strip(),
            "EmplRcd": emp_data.get("EmplRcd", 0),
            "Work_Phone": str(emp_data.get("Work_Phone", "")).strip(),
            "Work_Address1": str(emp_data.get("Work_Address1", "")).strip(),
            "Work_City": str(emp_data.get("Work_City", "")).strip(),
            "Work_State": str(emp_data.get("Work_State", "")).strip(),
            "Work_Postal": str(emp_data.get("Work_Postal", "")).strip(),
        }

        # Create normalized JSON for consistent hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )

        # Generate SHA-256 hash
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        empl_id = emp_data.get("EmplId", "unknown")
        uniqname = emp_data.get("UniqName", "Unknown Employee")
        logger.debug(
            f"Content hash for employee {empl_id} ({uniqname}): {content_hash}"
        )

        return content_hash

    def _get_existing_employee_hashes(self) -> Dict[str, str]:
        """
        Retrieve the latest content hash for each umich employee from the bronze layer.

        This uses a window function to get only the most recent record for each
        employee, allowing efficient comparison with new data.

        Returns:
            Dictionary mapping EmplId -> latest_content_hash
        """
        try:
            # Query to get the most recent record for each employee
            query = """
            WITH latest_employees AS (
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
                AND source_system = 'umich_api'
            )
            SELECT
                external_id,
                raw_data
            FROM latest_employees
            WHERE row_num = 1
            """

            results_df = self.db_adapter.query_to_dataframe(query)

            # Calculate content hashes for existing records
            existing_hashes = {}
            for _, row in results_df.iterrows():
                empl_id = row["external_id"]
                raw_data = row["raw_data"]  # JSONB comes back as dict

                # Use stored hash if available, otherwise calculate it
                content_hash = raw_data.get("_content_hash")
                if not content_hash:
                    content_hash = self._calculate_employee_content_hash(raw_data)

                existing_hashes[empl_id] = content_hash

            logger.info(
                f"📚 Retrieved content hashes for {len(existing_hashes)} existing umich employees"
            )
            return existing_hashes

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to retrieve existing employee hashes: {e}")
            raise

    def create_ingestion_run(self, source_system: str, entity_type: str) -> str:
        """Create a new ingestion run record for tracking purposes."""
        if self.dry_run:
            run_id = f"dry-run-{uuid.uuid4()}"
            logger.info(f"[DRY RUN] Would create ingestion run {run_id}")
            return run_id

        try:
            run_id = str(uuid.uuid4())

            # Metadata specific to umich content hashing approach
            metadata = {
                "ingestion_type": "content_hash_based",
                "source_api": "umich_department_employee_api",
                "change_detection_method": "sha256_content_hash",
                "includes_department_relationships": True,
                "full_sync": self.force_full_sync,
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
                f"🚀 Created umich ingestion run {run_id} for {source_system}/{entity_type}"
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

            logger.info(f"🏁 Completed umich ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete ingestion run: {e}")

    def ingest_umich_employees_with_change_detection(self) -> Dict[str, Any]:
        """
        Ingest University of Michigan employees using intelligent content hashing.

        This method:
        1. Fetches all employee data from the umich API
        2. Calculates content hashes for each employee
        3. Compares against existing bronze records
        4. Only creates new records when content has actually changed
        5. Provides detailed statistics about employee changes detected

        Returns:
            Dictionary with comprehensive ingestion statistics
        """
        # Create ingestion run for tracking
        run_id = self.create_ingestion_run("umich_api", "user")

        ingestion_stats = {
            "run_id": run_id,
            "records_processed": 0,
            "records_created": 0,
            "records_skipped_unchanged": 0,
            "new_employees": 0,
            "changed_employees": 0,
            "unique_departments": set(),
            "job_families": set(),
            "employment_statuses": set(),
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info(
                "🚀 Starting UMich employee ingestion with content hash change detection..."
            )

            # Step 1: Get existing employee content hashes from bronze layer
            if self.force_full_sync:
                logger.info("🔄 Full sync mode: Processing ALL records")
                existing_hashes = {}
            else:
                logger.info("⚡ Incremental mode: Processing only new/changed records")
                existing_hashes = self._get_existing_employee_hashes()

            # Step 2: Fetch current data from UMich Department API
            logger.info("🔬 Fetching employee data from University of Michigan API...")
            raw_employees = self.um_dept_api.get_all_department_employees()
            logger.info(f"📥 Retrieved {len(raw_employees)} employees from UMich API")

            # Step 3: Process each employee with content hash change detection
            for emp_data in raw_employees:
                try:
                    # Extract employee identifier
                    empl_id = emp_data.get("EmplId", "unknown")
                    uniqname = emp_data.get("UniqName", "Unknown Employee")

                    # Track analytics for reporting (using actual API fields)
                    ingestion_stats["unique_departments"].add(
                        emp_data.get("DepartmentId", "Unknown")
                    )
                    # Track job codes since API doesn't have Job_Family
                    ingestion_stats["job_families"].add(
                        emp_data.get("Jobcode", "Unknown")
                    )
                    # Track departments as "employment status" proxy since no actual status field
                    ingestion_stats["employment_statuses"].add(
                        "Active"  # All returned employees are active
                    )

                    # Calculate content hash for this employee
                    current_hash = self._calculate_employee_content_hash(emp_data)

                    # Check if this employee is new or has changed
                    existing_hash = existing_hashes.get(empl_id)

                    if existing_hash is None:
                        # This is a completely new employee
                        logger.info(
                            f"🆕 New employee detected: {uniqname} (ID: {empl_id})"
                        )
                        should_insert = True
                        ingestion_stats["new_employees"] += 1

                    elif existing_hash != current_hash:
                        # This employee exists but has changed
                        logger.info(f"📝 Employee changed: {uniqname} (ID: {empl_id})")
                        logger.debug(f"   Old hash: {existing_hash}")
                        logger.debug(f"   New hash: {current_hash}")
                        should_insert = True
                        ingestion_stats["changed_employees"] += 1

                    else:
                        # This employee exists and hasn't changed - skip it
                        logger.debug(
                            f"⏭️  Employee unchanged, skipping: {uniqname} (ID: {empl_id})"
                        )
                        should_insert = False
                        ingestion_stats["records_skipped_unchanged"] += 1

                    # Only insert if the employee is new or changed
                    if should_insert:
                        if self.dry_run:
                            logger.info(
                                f"[DRY RUN] Would insert employee {empl_id} ({uniqname})"
                            )
                        else:
                            # Enhance raw data with metadata for future reference
                            enhanced_raw_data = emp_data.copy()
                            enhanced_raw_data["_content_hash"] = current_hash
                            enhanced_raw_data["_change_detection"] = (
                                "content_hash_based"
                            )
                            enhanced_raw_data["_department_name"] = emp_data.get(
                                "Dept_Description", "Unknown Department"
                            )
                            enhanced_raw_data["_full_job_title"] = (
                                f"{emp_data.get('Job_Family', '')} - "
                                f"{emp_data.get('Job_Title', '')}"
                            ).strip(" - ")

                            # Insert into bronze layer
                            entity_id = self.db_adapter.insert_raw_entity(
                                entity_type="user",
                                source_system="umich_api",
                                external_id=empl_id,
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
                            f"📈 Progress: {ingestion_stats['records_processed']} employees processed "
                            f"({ingestion_stats['records_created']} new/changed, "
                            f"{ingestion_stats['records_skipped_unchanged']} unchanged)"
                        )

                except Exception as record_error:
                    error_msg = f"Failed to process employee {empl_id}: {record_error}"
                    logger.error(f"❌ {error_msg}")
                    ingestion_stats["errors"].append(error_msg)

                ingestion_stats["records_processed"] += 1

            # Convert sets to counts for final reporting
            analytics_counts = {
                "departments": len(ingestion_stats["unique_departments"]),
                "job_families": len(ingestion_stats["job_families"]),
                "employment_statuses": len(ingestion_stats["employment_statuses"]),
            }
            ingestion_stats["analytics_summary"] = analytics_counts

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
                f"🎉 UMich employee ingestion completed in {duration:.2f} seconds"
            )
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Total Processed: {ingestion_stats['records_processed']}")
            logger.info(f"   New Records Created: {ingestion_stats['records_created']}")
            logger.info(f"   ├─ New Employees: {ingestion_stats['new_employees']}")
            logger.info(
                f"   └─ Changed Employees: {ingestion_stats['changed_employees']}"
            )
            logger.info(
                f"   Skipped (Unchanged): {ingestion_stats['records_skipped_unchanged']}"
            )
            logger.info(f"   Employee Analytics:")
            logger.info(f"   ├─ Unique Departments: {analytics_counts['departments']}")
            logger.info(f"   ├─ Job Families: {analytics_counts['job_families']}")
            logger.info(
                f"   └─ Employment Statuses: {analytics_counts['employment_statuses']}"
            )
            logger.info(f"   Errors: {len(ingestion_stats['errors'])}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"UMich employee ingestion failed: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats["records_processed"],
                records_created=ingestion_stats["records_created"],
                records_skipped=ingestion_stats["records_skipped_unchanged"],
                error_message=error_msg,
            )

            raise

    def get_employee_analytics(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze employee data from bronze layer.

        This provides insights into the employee structure and can help
        identify patterns or anomalies in the employee data.

        Returns:
            Dictionary containing DataFrames for different employee analyses
        """
        try:
            # Query for employee analytics using actual UMich API fields
            analytics_query = """
            WITH latest_employees AS (
                SELECT
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'user'
                AND source_system = 'umich_api'
            )
            SELECT
                raw_data->>'EmplId' as empl_id,
                raw_data->>'UniqName' as uniqname,
                raw_data->>'Name' as full_name,
                raw_data->>'FirstName' as first_name,
                raw_data->>'LastName' as last_name,
                raw_data->>'DepartmentId' as department_id,
                raw_data->>'Dept_Description' as department_name,
                raw_data->>'UniversityJobTitle' as university_job_title,
                raw_data->>'DepartmentJobTitle' as department_job_title,
                raw_data->>'Jobcode' as job_code,
                raw_data->>'SupervisorId' as supervisor_id,
                (raw_data->>'EmplRcd')::int as empl_rcd,
                raw_data->>'Work_Phone' as work_phone,
                raw_data->>'Work_City' as work_city,
                raw_data->>'Work_State' as work_state
            FROM latest_employees
            WHERE row_num = 1
            ORDER BY department_name, uniqname
            """

            analytics_df = self.db_adapter.query_to_dataframe(analytics_query)

            # Create summary analyses
            analyses = {}

            # Department summary
            dept_summary = (
                analytics_df.groupby(["department_id", "department_name"])
                .size()
                .reset_index(name="employee_count")
            )
            analyses["department_summary"] = dept_summary.sort_values(
                "employee_count", ascending=False
            )

            # Job code summary (actual field from API)
            job_code_summary = (
                analytics_df.groupby("job_code")
                .size()
                .reset_index(name="employee_count")
            )
            analyses["job_code_summary"] = job_code_summary.sort_values(
                "employee_count", ascending=False
            )

            # University job title summary
            univ_job_summary = (
                analytics_df.groupby("university_job_title")
                .size()
                .reset_index(name="employee_count")
            )
            analyses["university_job_title_summary"] = univ_job_summary.sort_values(
                "employee_count", ascending=False
            )

            # Department job title summary
            dept_job_summary = (
                analytics_df.groupby("department_job_title")
                .size()
                .reset_index(name="employee_count")
            )
            analyses["department_job_title_summary"] = dept_job_summary.sort_values(
                "employee_count", ascending=False
            )

            # Geographic distribution (Work City)
            city_summary = (
                analytics_df.groupby("work_city")
                .size()
                .reset_index(name="employee_count")
            )
            analyses["city_summary"] = city_summary.sort_values(
                "employee_count", ascending=False
            )

            # Full employee list
            analyses["full_employee_list"] = analytics_df

            logger.info(
                f"Generated employee analytics with {len(analytics_df)} employees"
            )
            return analyses

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to generate employee analytics: {e}")
            raise

    def log_employee_analytics(
        self, log_file_path: str = "logs/umich_employee_analytics.log"
    ) -> None:
        """
        Generate and log complete employee analytics to a dedicated log file.

        This creates a comprehensive employee analysis and appends
        it to a log file with timestamps for historical tracking.

        Args:
            log_file_path: Path to the employee analytics log file
        """
        try:
            # Ensure log directory exists
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

            # Get the analytics data
            employee_analyses = self.get_employee_analytics()

            # Open log file for appending
            with open(log_file_path, "a", encoding="utf-8") as log_file:
                # Write header with timestamp
                timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                log_file.write(f"\n{'=' * 80}\n")
                log_file.write(f"UNIVERSITY OF MICHIGAN EMPLOYEE ANALYTICS\n")
                log_file.write(f"Generated: {timestamp}\n")
                log_file.write(f"{'=' * 80}\n\n")

                # Department Analysis
                dept_summary = employee_analyses["department_summary"]
                log_file.write(
                    f"🏛️  DEPARTMENT DISTRIBUTION ({len(dept_summary)} departments)\n"
                )
                log_file.write(f"{'-' * 70}\n")
                for _, row in dept_summary.head(20).iterrows():
                    dept_name = (
                        row["department_name"][:50] + "..."
                        if len(str(row["department_name"])) > 50
                        else str(row["department_name"])
                    )
                    log_file.write(
                        f"   {dept_name:<53} {row['employee_count']:>6} employees\n"
                    )

                if len(dept_summary) > 20:
                    remaining = len(dept_summary) - 20
                    log_file.write(f"   ... and {remaining} more departments\n")
                log_file.write(f"\n")

                # Job Code Analysis
                job_code_summary = employee_analyses["job_code_summary"]
                log_file.write(
                    f"💼 JOB CODE DISTRIBUTION ({len(job_code_summary)} job codes)\n"
                )
                log_file.write(f"{'-' * 70}\n")
                for _, row in job_code_summary.head(20).iterrows():
                    job_code = str(row["job_code"]) if row["job_code"] else "Unknown"
                    log_file.write(
                        f"   {job_code:<50} {row['employee_count']:>6} employees\n"
                    )
                if len(job_code_summary) > 20:
                    remaining = len(job_code_summary) - 20
                    log_file.write(f"   ... and {remaining} more job codes\n")
                log_file.write(f"\n")

                # University Job Title Analysis
                univ_job_summary = employee_analyses["university_job_title_summary"]
                log_file.write(
                    f"👥 UNIVERSITY JOB TITLE DISTRIBUTION ({len(univ_job_summary)} titles)\n"
                )
                log_file.write(f"{'-' * 80}\n")
                for _, row in univ_job_summary.head(15).iterrows():
                    job_title = (
                        str(row["university_job_title"])[:60] + "..."
                        if row["university_job_title"]
                        and len(str(row["university_job_title"])) > 60
                        else (
                            str(row["university_job_title"])
                            if row["university_job_title"]
                            else "Unknown"
                        )
                    )
                    log_file.write(
                        f"   {job_title:<64} {row['employee_count']:>6} emps\n"
                    )

                if len(univ_job_summary) > 15:
                    remaining = len(univ_job_summary) - 15
                    log_file.write(
                        f"   ... and {remaining} more university job titles\n"
                    )
                log_file.write(f"\n")

                # Department Job Title Analysis
                dept_job_summary = employee_analyses["department_job_title_summary"]
                log_file.write(
                    f"🏢 DEPARTMENT JOB TITLE DISTRIBUTION ({len(dept_job_summary)} titles)\n"
                )
                log_file.write(f"{'-' * 80}\n")
                for _, row in dept_job_summary.head(15).iterrows():
                    job_title = (
                        str(row["department_job_title"])[:60] + "..."
                        if row["department_job_title"]
                        and len(str(row["department_job_title"])) > 60
                        else (
                            str(row["department_job_title"])
                            if row["department_job_title"]
                            else "Unknown"
                        )
                    )
                    log_file.write(
                        f"   {job_title:<64} {row['employee_count']:>6} emps\n"
                    )

                if len(dept_job_summary) > 15:
                    remaining = len(dept_job_summary) - 15
                    log_file.write(
                        f"   ... and {remaining} more department job titles\n"
                    )
                log_file.write(f"\n")

                # Geographic Distribution Analysis
                city_summary = employee_analyses["city_summary"]
                log_file.write(
                    f"📍 GEOGRAPHIC DISTRIBUTION ({len(city_summary)} cities)\n"
                )
                log_file.write(f"{'-' * 70}\n")
                for _, row in city_summary.head(15).iterrows():
                    city = str(row["work_city"]) if row["work_city"] else "Unknown"
                    log_file.write(
                        f"   {city:<50} {row['employee_count']:>6} employees\n"
                    )
                if len(city_summary) > 15:
                    remaining = len(city_summary) - 15
                    log_file.write(f"   ... and {remaining} more cities\n")
                log_file.write(f"\n")

                # Summary statistics
                full_list = employee_analyses["full_employee_list"]
                log_file.write(f"\n{'=' * 50}\n")
                log_file.write(f"SUMMARY STATISTICS\n")
                log_file.write(f"{'=' * 50}\n")
                log_file.write(f"Total Employees:           {len(full_list):>6}\n")
                log_file.write(f"Total Departments:         {len(dept_summary):>6}\n")
                log_file.write(
                    f"Total Job Codes:           {len(job_code_summary):>6}\n"
                )
                log_file.write(
                    f"Total Univ. Job Titles:    {len(univ_job_summary):>6}\n"
                )
                log_file.write(
                    f"Total Dept. Job Titles:    {len(dept_job_summary):>6}\n"
                )
                log_file.write(f"Total Cities:              {len(city_summary):>6}\n")
                log_file.write(f"\n")

            logger.info(f"Complete employee analytics written to: {log_file_path}")

        except Exception as e:
            logger.error(f"❌ Failed to write employee analytics to log file: {e}")
            raise

    def get_employee_change_history(self, empl_id: str) -> pd.DataFrame:
        """
        Get the complete change history for a specific UMich employee.

        Args:
            empl_id: The UMich Employee ID

        Returns:
            DataFrame with all historical versions of the employee
        """
        try:
            query = """
            SELECT
                raw_id,
                raw_data->>'UniqName' as uniqname,
                raw_data->>'Name' as full_name,
                raw_data->>'DepartmentId' as department_id,
                raw_data->>'Dept_Description' as department_name,
                raw_data->>'UniversityJobTitle' as university_job_title,
                raw_data->>'DepartmentJobTitle' as department_job_title,
                raw_data->>'Jobcode' as job_code,
                raw_data->>'SupervisorId' as supervisor_id,
                raw_data->>'Work_City' as work_city,
                raw_data->>'_content_hash' as content_hash,
                ingested_at,
                ingestion_run_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
            AND source_system = 'umich_api'
            AND external_id = :empl_id
            ORDER BY ingested_at DESC
            """

            history_df = self.db_adapter.query_to_dataframe(query, {"empl_id": empl_id})

            logger.info(
                f"Retrieved {len(history_df)} historical records for UMich employee {empl_id}"
            )
            return history_df

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to retrieve employee history: {e}")
            raise

    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()
        logger.info("UMich employee ingestion service closed")


def main():
    """
    Main function to run UMich employee ingestion from command line.
    """
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(
            description="Ingest UMich API employees into bronze layer"
        )
        parser.add_argument(
            "--full-sync",
            action="store_true",
            help="Force full sync (bypass change detection)",
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
        um_base_url = os.getenv("UM_BASE_URL")
        um_category_id = os.getenv("UM_CATEGORY_ID")
        um_client_key = os.getenv("UM_CLIENT_KEY")
        um_client_secret = os.getenv("UM_CLIENT_SECRET")

        # Validate configuration
        required_vars = {
            "DATABASE_URL": database_url,
            "UM_BASE_URL": um_base_url,
            "UM_CATEGORY_ID": um_category_id,
            "UM_CLIENT_KEY": um_client_key,
            "UM_CLIENT_SECRET": um_client_secret,
        }

        missing_vars = [name for name, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

        # Create and run UMich ingestion service
        ingestion_service = UMichEmployeeIngestionService(
            database_url=database_url,
            um_base_url=um_base_url,
            um_category_id=um_category_id,
            um_client_key=um_client_key,
            um_client_secret=um_client_secret,
            force_full_sync=args.full_sync,
            dry_run=args.dry_run,
        )

        # Run the content hash-based ingestion process
        print(
            "👥 Starting University of Michigan employee ingestion with content hashing..."
        )
        if args.dry_run:
            print("🧪 DRY RUN MODE - No changes will be committed to database")
        if args.full_sync:
            print("🔄 FULL SYNC MODE - Processing all records")

        results = ingestion_service.ingest_umich_employees_with_change_detection()

        # Display comprehensive summary
        print(f"\n📊 UMich Employee Ingestion Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Total Employees Processed: {results['records_processed']}")
        print(f"   New Records Created: {results['records_created']}")
        print(f"     ├─ Brand New Employees: {results['new_employees']}")
        print(f"     └─ Employees with Changes: {results['changed_employees']}")
        print(f"   Skipped (No Changes): {results['records_skipped_unchanged']}")
        print(f"   Employee Analytics:")
        print(
            f"     ├─ Unique Departments: {results['analytics_summary']['departments']}"
        )
        print(f"     ├─ Job Families: {results['analytics_summary']['job_families']}")
        print(
            f"     └─ Employment Statuses: {results['analytics_summary']['employment_statuses']}"
        )
        print(f"   Errors: {len(results['errors'])}")

        if (
            results["records_skipped_unchanged"] > 0
            and results["records_processed"] > 0
        ):
            efficiency_percentage = (
                results["records_skipped_unchanged"] / results["records_processed"]
            ) * 100
            print(
                f"\n⚡ Efficiency: {efficiency_percentage:.1f}% of employees were unchanged and skipped"
            )

        if not args.dry_run:
            # Show employee analytics
            print("\n🏗️  Analyzing employee data...")
            employee_analyses = ingestion_service.get_employee_analytics()

            print("📝 Writing complete employee analytics to log file...")
            ingestion_service.log_employee_analytics()

            # Department distribution
            print("\n📋 Top 10 Departments by Employee Count:")
            dept_summary = employee_analyses["department_summary"]
            for _, row in dept_summary.head(10).iterrows():
                print(
                    f"   - {row['department_name']}: {row['employee_count']} employees"
                )

            if len(dept_summary) > 10:
                remaining_dept_count = len(dept_summary) - 10
                remaining_emp_count = dept_summary.iloc[10:]["employee_count"].sum()
                print(
                    f"   - ... and {remaining_dept_count} more departments with {remaining_emp_count} additional employees"
                )

            # Job code distribution (top 10)
            print("\n💼 Top 10 Job Codes:")
            job_code_summary = employee_analyses["job_code_summary"]
            for _, row in job_code_summary.head(10).iterrows():
                job_code = str(row["job_code"]) if row["job_code"] else "Unknown"
                print(f"   - {job_code}: {row['employee_count']} employees")

            if len(job_code_summary) > 10:
                remaining_job_count = len(job_code_summary) - 10
                remaining_emp_count = job_code_summary.iloc[10:]["employee_count"].sum()
                print(
                    f"   - ... and {remaining_job_count} more job codes with {remaining_emp_count} additional employees"
                )

            # University job title distribution (top 10)
            print("\n👥 Top 10 University Job Titles:")
            univ_job_summary = employee_analyses["university_job_title_summary"]
            for _, row in univ_job_summary.head(10).iterrows():
                job_title = (
                    str(row["university_job_title"])
                    if row["university_job_title"]
                    else "Unknown"
                )
                print(f"   - {job_title}: {row['employee_count']} employees")

            if len(univ_job_summary) > 10:
                remaining_title_count = len(univ_job_summary) - 10
                remaining_emp_count = univ_job_summary.iloc[10:]["employee_count"].sum()
                print(
                    f"   - ... and {remaining_title_count} more job titles with {remaining_emp_count} additional employees"
                )

            # Geographic distribution (top 5 cities)
            print("\n📍 Top 5 Cities by Employee Count:")
            city_summary = employee_analyses["city_summary"]
            for _, row in city_summary.head(5).iterrows():
                city = str(row["work_city"]) if row["work_city"] else "Unknown"
                print(f"   - {city}: {row['employee_count']} employees")

            # Overall statistics
            total_stats = {
                "Total Employees": len(employee_analyses["full_employee_list"]),
                "Unique Departments": len(dept_summary),
                "Unique Job Codes": len(job_code_summary),
                "Unique University Job Titles": len(univ_job_summary),
                "Unique Cities": len(city_summary),
            }

            print(f"\n📈 Overall Employee Statistics:")
            for stat, count in total_stats.items():
                print(f"   - {stat}: {count}")

        # Clean up
        ingestion_service.close()

        if args.dry_run:
            print("\n✅ Dry run completed successfully - no database changes made!")
        else:
            print(
                "\n✅ University of Michigan employee ingestion completed successfully!"
            )

    except Exception as e:
        logger.error(f"❌ UMich employee ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
