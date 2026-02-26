#!/usr/bin/env python3
"""
UMAPI Department Silver Layer Transformation Service

Transforms bronze UMich API department data into source-specific silver.umapi_departments table.
This is a source-specific table that preserves organizational hierarchy from the UMich API.

Key features:
- Single-source transformation (UMAPI only, no merging)
- Preserves organizational hierarchy (campus, VP area, college)
- No custom attributes (UMAPI has standardized fields)
- Incremental processing using content hash change detection
- Comprehensive data quality scoring based on hierarchy completeness
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add LSATS project to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

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


class UmapiDepartmentTransformationService:
    """
    Service for transforming bronze UMAPI department records into source-specific silver layer.

    This service creates silver.umapi_departments records with organizational hierarchy,
    separate from the merged silver.departments table.
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
        logger.info("✨ UMAPI department transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful transformation run.

        Enables incremental processing by tracking when we last transformed.

        Returns:
            datetime of last successful transformation, or None for first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
            AND entity_type = 'umapi_department'
            AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"⏰ Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "🆕 No previous transformation found - processing all UMAPI departments"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️ Could not determine last transformation timestamp: {e}")
            return None

    def _fetch_bronze_records(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Fetch UMAPI department bronze records for transformation.

        Args:
            since_timestamp: Only fetch records ingested after this time
            full_sync: Ignore timestamp and fetch all records

        Returns:
            List of dictionaries with raw_id, raw_data, ingested_at
        """
        try:
            params = {}
            time_filter = ""

            if since_timestamp and not full_sync:
                # Incremental: only fetch records ingested after last run
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp
                logger.info(
                    f"📊 Fetching UMAPI departments ingested after {since_timestamp}"
                )
            else:
                logger.info("📊 Fetching all UMAPI departments (full transformation)")

            query = f"""
            SELECT
                raw_id,
                raw_data,
                ingested_at,
                external_id
            FROM bronze.raw_entities
            WHERE source_system = 'umich_api'
            AND entity_type = 'department'
            {time_filter}
            ORDER BY ingested_at
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)

            if result_df.empty:
                logger.info("✅ No UMAPI departments to transform")
                return []

            records = result_df.to_dict("records")
            logger.info(
                f"📦 Found {len(records)} UMAPI department records to transform"
            )
            return records

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch bronze records: {e}")
            raise

    def _clean_field(self, value: Optional[str]) -> Optional[str]:
        """
        Clean a string field by stripping whitespace and treating empty/space-only as NULL.

        UMAPI sometimes returns single space " " to represent empty/null values.

        Args:
            value: Raw field value

        Returns:
            Cleaned string or None
        """
        if not value or not isinstance(value, str):
            return None

        cleaned = value.strip()
        return cleaned if cleaned else None

    def _transform_bronze_to_silver(
        self, bronze_record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Transform a single bronze UMAPI department record to silver format.

        Args:
            bronze_record: Dictionary with raw_id, raw_data, ingested_at

        Returns:
            Dictionary representing silver.umapi_departments record
        """
        raw_data = bronze_record["raw_data"]
        raw_id = bronze_record["raw_id"]

        # Extract and clean core UMAPI fields
        silver_record = {
            "dept_id": self._clean_field(raw_data.get("DeptId")),
            "dept_description": self._clean_field(raw_data.get("DeptDescription")),
            # Organizational hierarchy codes (machine-readable)
            "dept_group": self._clean_field(raw_data.get("DeptGroup")),
            "dept_group_campus": self._clean_field(raw_data.get("DeptGroupCampus")),
            "dept_group_vp_area": self._clean_field(raw_data.get("DeptGroupVPArea")),
            # Organizational hierarchy descriptions (human-readable)
            "college_name": self._clean_field(raw_data.get("DeptGroupDescription")),
            "campus_name": self._clean_field(raw_data.get("DeptGroupCampusDescr")),
            "vp_area_name": self._clean_field(raw_data.get("DeptGroupVPAreaDescr")),
            # Computed hierarchical path
            "hierarchical_path": self._clean_field(raw_data.get("_hierarchical_path")),
            "source_bronze_id": raw_id,
        }

        return silver_record

    def _calculate_content_hash(self, silver_record: Dict[str, Any]) -> str:
        """
        Calculate content hash for the silver record.

        Hash represents the "fingerprint" of the UMAPI data for change detection.

        Args:
            silver_record: The silver record dictionary

        Returns:
            SHA-256 hash string
        """
        # Include all significant fields in hash
        significant_fields = {
            "dept_id": silver_record.get("dept_id"),
            "dept_description": silver_record.get("dept_description"),
            "dept_group": silver_record.get("dept_group"),
            "dept_group_campus": silver_record.get("dept_group_campus"),
            "dept_group_vp_area": silver_record.get("dept_group_vp_area"),
            "college_name": silver_record.get("college_name"),
            "campus_name": silver_record.get("campus_name"),
            "vp_area_name": silver_record.get("vp_area_name"),
            "hierarchical_path": silver_record.get("hierarchical_path"),
        }

        # Create normalized JSON for hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":"), default=str
        )
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        return content_hash

    def _calculate_data_quality(
        self, silver_record: Dict[str, Any]
    ) -> Tuple[Decimal, List[str]]:
        """
        Calculate data quality score and identify quality flags.

        Scoring criteria (start at 1.00):
        - Missing dept_id: -0.50 (critical primary key)
        - Missing dept_description: -0.50 (critical name)
        - Missing campus_name: -0.10
        - Missing college_name: -0.10
        - Missing vp_area_name: -0.10
        - Missing dept_group (code): flag only
        - Inconsistent hierarchy: -0.10 (path doesn't contain expected parts)

        Args:
            silver_record: The transformed silver record

        Returns:
            Tuple of (quality_score, quality_flags_list)
        """
        score = Decimal("1.00")
        flags = []

        # Critical fields missing
        if not silver_record.get("dept_id"):
            score -= Decimal("0.50")
            flags.append("missing_dept_id")

        if not silver_record.get("dept_description"):
            score -= Decimal("0.50")
            flags.append("missing_dept_description")

        # Important hierarchy fields (-0.10 each if missing)
        if not silver_record.get("campus_name"):
            score -= Decimal("0.10")
            flags.append("missing_campus")

        if not silver_record.get("college_name"):
            score -= Decimal("0.10")
            flags.append("missing_college")

        if not silver_record.get("vp_area_name"):
            score -= Decimal("0.10")
            flags.append("missing_vp_area")

        # Machine-readable codes (flag only, no penalty)
        if not silver_record.get("dept_group"):
            flags.append("missing_dept_group_code")

        if not silver_record.get("dept_group_campus"):
            flags.append("missing_campus_code")

        if not silver_record.get("dept_group_vp_area"):
            flags.append("missing_vp_area_code")

        # Path consistency check
        hierarchical_path = silver_record.get("hierarchical_path")
        if hierarchical_path:
            # Check if path contains all non-null hierarchy parts
            expected_parts = [
                silver_record.get("campus_name"),
                silver_record.get("vp_area_name"),
                silver_record.get("college_name"),
                silver_record.get("dept_description"),
            ]

            missing_from_path = [
                part
                for part in expected_parts
                if part and part.strip() and part not in hierarchical_path
            ]

            if missing_from_path:
                score -= Decimal("0.10")
                flags.append("inconsistent_hierarchy")
        else:
            # No hierarchical path at all
            if any(
                [
                    silver_record.get("campus_name"),
                    silver_record.get("vp_area_name"),
                    silver_record.get("college_name"),
                ]
            ):
                flags.append("missing_hierarchical_path")

        # Ensure score doesn't go below 0
        score = max(Decimal("0.00"), score)

        return score, flags

    def _upsert_silver_record(
        self, silver_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> bool:
        """
        Insert or update a silver.umapi_departments record.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
            dry_run: If True, only log what would be done

        Returns:
            True if record was inserted/updated, False if skipped
        """
        if dry_run:
            logger.info(
                f"🔍 [DRY RUN] Would upsert UMAPI department: "
                f"dept_id={silver_record['dept_id']}, "
                f"description={silver_record['dept_description']}, "
                f"quality={silver_record['data_quality_score']}"
            )
            return True

        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    upsert_query = text("""
                        INSERT INTO silver.umapi_departments (
                            dept_id, dept_description,
                            dept_group, dept_group_campus, dept_group_vp_area,
                            college_name, campus_name, vp_area_name,
                            hierarchical_path,
                            data_quality_score, quality_flags,
                            entity_hash, source_bronze_id,
                            ingestion_run_id, updated_at
                        ) VALUES (
                            :dept_id, :dept_description,
                            :dept_group, :dept_group_campus, :dept_group_vp_area,
                            :college_name, :campus_name, :vp_area_name,
                            :hierarchical_path,
                            :data_quality_score, :quality_flags,
                            :entity_hash, :source_bronze_id,
                            :ingestion_run_id, CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (dept_id) DO UPDATE SET
                            dept_description = EXCLUDED.dept_description,
                            dept_group = EXCLUDED.dept_group,
                            dept_group_campus = EXCLUDED.dept_group_campus,
                            dept_group_vp_area = EXCLUDED.dept_group_vp_area,
                            college_name = EXCLUDED.college_name,
                            campus_name = EXCLUDED.campus_name,
                            vp_area_name = EXCLUDED.vp_area_name,
                            hierarchical_path = EXCLUDED.hierarchical_path,
                            data_quality_score = EXCLUDED.data_quality_score,
                            quality_flags = EXCLUDED.quality_flags,
                            entity_hash = EXCLUDED.entity_hash,
                            source_bronze_id = EXCLUDED.source_bronze_id,
                            ingestion_run_id = EXCLUDED.ingestion_run_id,
                            updated_at = CURRENT_TIMESTAMP
                    """)

                    conn.execute(
                        upsert_query,
                        {
                            "dept_id": silver_record["dept_id"],
                            "dept_description": silver_record["dept_description"],
                            "dept_group": silver_record["dept_group"],
                            "dept_group_campus": silver_record["dept_group_campus"],
                            "dept_group_vp_area": silver_record["dept_group_vp_area"],
                            "college_name": silver_record["college_name"],
                            "campus_name": silver_record["campus_name"],
                            "vp_area_name": silver_record["vp_area_name"],
                            "hierarchical_path": silver_record["hierarchical_path"],
                            "data_quality_score": silver_record["data_quality_score"],
                            "quality_flags": json.dumps(silver_record["quality_flags"]),
                            "entity_hash": silver_record["entity_hash"],
                            "source_bronze_id": silver_record["source_bronze_id"],
                            "ingestion_run_id": run_id,
                        },
                    )

            return True

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to upsert silver record for dept_id {silver_record['dept_id']}: {e}"
            )
            raise

    def create_transformation_run(self) -> str:
        """
        Create a new ingestion run record in meta.ingestion_runs.

        Returns:
            UUID string of the new run
        """
        run_id = str(uuid.uuid4())

        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    insert_query = text("""
                        INSERT INTO meta.ingestion_runs (
                            run_id, source_system, entity_type, started_at, status
                        ) VALUES (
                            :run_id, :source_system, :entity_type, :started_at, :status
                        )
                    """)

                    conn.execute(
                        insert_query,
                        {
                            "run_id": run_id,
                            "source_system": "silver_transformation",
                            "entity_type": "umapi_department",
                            "started_at": datetime.now(timezone.utc),
                            "status": "running",
                        },
                    )

            logger.info(f"🚀 Created transformation run: {run_id}")
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
        error_message: Optional[str] = None,
    ):
        """
        Mark a transformation run as completed or failed.

        Args:
            run_id: The run ID to update
            records_processed: Total records processed
            records_created: Number of new records created
            records_updated: Number of existing records updated
            error_message: Error message if run failed
        """
        status = "failed" if error_message else "completed"

        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
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

            if error_message:
                logger.error(f"❌ Transformation run {run_id} failed: {error_message}")
            else:
                logger.info(f"✅ Transformation run {run_id} completed successfully")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete transformation run: {e}")

    def transform_incremental(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Perform incremental transformation of UMAPI departments.

        Args:
            full_sync: Force full transformation instead of incremental
            dry_run: Preview changes without committing

        Returns:
            Dictionary with transformation statistics
        """
        run_id = self.create_transformation_run() if not dry_run else "dry-run-id"

        try:
            # Get last transformation timestamp
            last_timestamp = (
                None if full_sync else self._get_last_transformation_timestamp()
            )

            # Fetch bronze records
            bronze_records = self._fetch_bronze_records(
                since_timestamp=last_timestamp, full_sync=full_sync
            )

            if not bronze_records:
                self.complete_transformation_run(
                    run_id, 0, 0, 0
                ) if not dry_run else None
                return {
                    "records_processed": 0,
                    "records_created": 0,
                    "records_updated": 0,
                    "run_id": run_id,
                }

            # Transform each bronze record
            records_processed = 0
            records_created = 0
            records_updated = 0

            # Quality tracking
            high_quality_count = 0  # score >= 0.90
            medium_quality_count = 0  # 0.70 <= score < 0.90
            low_quality_count = 0  # score < 0.70

            for bronze_record in bronze_records:
                try:
                    # Transform bronze to silver
                    silver_record = self._transform_bronze_to_silver(bronze_record)

                    # Calculate quality
                    quality_score, quality_flags = self._calculate_data_quality(
                        silver_record
                    )
                    silver_record["data_quality_score"] = quality_score
                    silver_record["quality_flags"] = quality_flags

                    # Calculate hash
                    silver_record["entity_hash"] = self._calculate_content_hash(
                        silver_record
                    )

                    # Track quality distribution
                    if quality_score >= Decimal("0.90"):
                        high_quality_count += 1
                    elif quality_score >= Decimal("0.70"):
                        medium_quality_count += 1
                    else:
                        low_quality_count += 1

                    # Upsert record
                    self._upsert_silver_record(silver_record, run_id, dry_run)

                    records_processed += 1

                    # Log progress every 500 records
                    if records_processed % 500 == 0:
                        logger.info(
                            f"📊 Processed {records_processed}/{len(bronze_records)} UMAPI departments..."
                        )

                except Exception as e:
                    logger.error(
                        f"❌ Failed to transform bronze record {bronze_record.get('raw_id')}: {e}"
                    )
                    continue

            # Complete run
            if not dry_run:
                self.complete_transformation_run(
                    run_id, records_processed, records_created, records_updated
                )

            # Log summary
            logger.info("=" * 80)
            logger.info("🎉 UMAPI DEPARTMENT TRANSFORMATION COMPLETE")
            logger.info("=" * 80)
            logger.info(f"📊 Records processed: {records_processed}")
            logger.info(f"✅ High quality (≥0.90): {high_quality_count}")
            logger.info(f"⚠️  Medium quality (0.70-0.89): {medium_quality_count}")
            logger.info(f"❌ Low quality (<0.70): {low_quality_count}")
            logger.info(f"🔑 Run ID: {run_id}")
            logger.info("=" * 80)

            return {
                "records_processed": records_processed,
                "records_created": records_created,
                "records_updated": records_updated,
                "high_quality_count": high_quality_count,
                "medium_quality_count": medium_quality_count,
                "low_quality_count": low_quality_count,
                "run_id": run_id,
            }

        except Exception as e:
            error_msg = f"Transformation failed: {str(e)}"
            if not dry_run:
                self.complete_transformation_run(run_id, 0, 0, 0, error_msg)
            raise

    def close(self):
        """Close database connection."""
        self.db_adapter.close()
        logger.info("🔌 Database connection closed")


def main():
    """Main entry point for UMAPI department transformation."""
    parser = argparse.ArgumentParser(
        description="Transform bronze UMAPI departments to silver.umapi_departments table"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Force full transformation instead of incremental",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )
    parser.add_argument(
        "--log",
        type=str,
        default="logs/005_transform_umapi_departments.log",
        help="Log file path",
    )

    args = parser.parse_args()

    # Reconfigure logging with custom log file
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(args.log),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Load environment
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    # Create service and run transformation
    service = UmapiDepartmentTransformationService(database_url)

    try:
        if args.dry_run:
            logger.info("🔍 DRY RUN MODE - No changes will be committed")

        result = service.transform_incremental(
            full_sync=args.full_sync, dry_run=args.dry_run
        )

        logger.info("✨ Transformation completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}", exc_info=True)
        sys.exit(1)

    finally:
        service.close()


if __name__ == "__main__":
    main()
