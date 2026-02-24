#!/usr/bin/env python3
"""
Consolidated Departments Silver Layer Transformation Service

Transforms source-specific silver department records (tdx_departments + umapi_departments)
into consolidated silver.departments table.

Key features:
- Merges data from TDX and UMAPI sources
- UMAPI takes precedence for organizational hierarchy
- TDX provides operational data (manager, location)
- Content hash-based change detection
- Incremental processing with --full-sync override
- Comprehensive logging with emoji standards
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
from typing import Any, Dict, List, Optional, Set, Tuple

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


class DepartmentConsolidationService:
    """
    Service for consolidating department records from TDX and UMAPI into silver.departments.
    
    Merge Strategy:
    - dept_id: Primary key (6-digit code)
    - dept_name: UMAPI dept_description > TDX dept_name
    - Hierarchy: UMAPI (college, campus, VP area)
    - Operational: TDX (manager_uid, location_info, tdx_id)
    - is_active: TDX status (UMAPI assumed active)
    """

    def __init__(self, database_url: str):
        """
        Initialize the consolidation service.

        Args:
            database_url: PostgreSQL connection string
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        logger.info("✨ Department consolidation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful consolidation run.

        Returns:
            Timestamp of last completed run, or None if first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'departments_consolidated'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"⏰ Last successful consolidation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info("🆕 No previous consolidation found - processing all departments")
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️ Could not determine last consolidation timestamp: {e}")
            return None

    def _fetch_source_records(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Fetch department records from both source-specific tables.

        Args:
            since_timestamp: Only fetch records updated after this time
            full_sync: Ignore timestamp and fetch all records

        Returns:
            Tuple of (tdx_records, umapi_records)
        """
        try:
            time_filter = ""
            params = {}

            if since_timestamp and not full_sync:
                time_filter = "WHERE updated_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp
                logger.info(f"📊 Fetching records updated after {since_timestamp}")
            else:
                logger.info("📊 Fetching all department records (full sync)")

            # Fetch TDX departments
            tdx_query = f"""
            SELECT *
            FROM silver.tdx_departments
            {time_filter}
            ORDER BY updated_at
            """
            tdx_df = self.db_adapter.query_to_dataframe(tdx_query, params)
            tdx_records = tdx_df.to_dict("records") if not tdx_df.empty else []

            # Fetch UMAPI departments
            umapi_query = f"""
            SELECT *
            FROM silver.umapi_departments
            {time_filter}
            ORDER BY updated_at
            """
            umapi_df = self.db_adapter.query_to_dataframe(umapi_query, params)
            umapi_records = umapi_df.to_dict("records") if not umapi_df.empty else []

            logger.info(f"📦 Found {len(tdx_records)} TDX + {len(umapi_records)} UMAPI departments")

            return tdx_records, umapi_records

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch source records: {e}")
            raise

    def _merge_department_records(
        self,
        tdx_record: Optional[Dict[str, Any]],
        umapi_record: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Merge TDX and UMAPI department records into consolidated format.

        Merge Priority:
        - dept_id: Required (from either tdx_record.dept_code or umapi_record.dept_id)
        - dept_name: UMAPI > TDX
        - Hierarchy fields: UMAPI
        - Operational fields: TDX
        - is_active: TDX (UMAPI assumed active)

        Args:
            tdx_record: Record from silver.tdx_departments (or None)
            umapi_record: Record from silver.umapi_departments (or None)

        Returns:
            Merged department record
        """
        sources = []
        
        # Determine dept_id
        dept_id = None
        if umapi_record:
            dept_id = umapi_record.get("dept_id")
            sources.append("umapi")
        if tdx_record:
            if not dept_id:
                dept_id = tdx_record.get("dept_code")
            sources.append("tdx")

        if not dept_id:
            raise ValueError("Cannot merge records without dept_id")

        # Merge fields with priority rules
        merged = {
            # Primary key
            "dept_id": dept_id,
            
            # Core identity (UMAPI > TDX)
            "dept_name": (
                umapi_record.get("dept_description") if umapi_record 
                else tdx_record.get("dept_name") if tdx_record
                else dept_id
            ),
            
            # Status (TDX only)
            "is_active": tdx_record.get("is_active") if tdx_record else True,
            
            # Hierarchy (UMAPI only, but can try to derive from TDX parent)
            "parent_dept_id": tdx_record.get("parent_id") if tdx_record else None,
            "college_name": umapi_record.get("college_name") if umapi_record else None,
            "campus_name": umapi_record.get("campus_name") if umapi_record else None,
            "vp_area_name": umapi_record.get("vp_area_name") if umapi_record else None,
            "dept_group": umapi_record.get("dept_group") if umapi_record else None,
            "dept_group_campus": umapi_record.get("dept_group_campus") if umapi_record else None,
            "dept_group_vp_area": umapi_record.get("dept_group_vp_area") if umapi_record else None,
            
            # Operational (TDX only)
            "tdx_id": tdx_record.get("tdx_id") if tdx_record else None,
            "tdx_manager_uid": tdx_record.get("manager_uid") if tdx_record else None,
            "location_info": tdx_record.get("location_info") if tdx_record else {},
            
            # Metadata
            "sources": sources,
        }

        # Update legacy columns for backward compatibility
        merged["department_name"] = merged["dept_name"]
        merged["department_code"] = dept_id
        merged["vp_area"] = merged["vp_area_name"]
        merged["college_group"] = merged["college_name"]
        merged["source_system"] = "+".join(sources) if sources else "unknown"
        merged["source_entity_id"] = dept_id


        return merged

    def _calculate_content_hash(self, merged_record: Dict[str, Any]) -> str:
        """
        Calculate content hash for change detection.

        Args:
            merged_record: The merged department record

        Returns:
            SHA-256 hash string
        """
        # Include significant fields in hash
        significant_fields = {
            "dept_id": merged_record.get("dept_id"),
            "dept_name": merged_record.get("dept_name"),
            "is_active": merged_record.get("is_active"),
            "parent_dept_id": merged_record.get("parent_dept_id"),
            "college_name": merged_record.get("college_name"),
            "campus_name": merged_record.get("campus_name"),
            "vp_area_name": merged_record.get("vp_area_name"),
            "dept_group": merged_record.get("dept_group"),
            "dept_group_campus": merged_record.get("dept_group_campus"),
            "dept_group_vp_area": merged_record.get("dept_group_vp_area"),
            "tdx_id": merged_record.get("tdx_id"),
            "tdx_manager_uid": str(merged_record.get("tdx_manager_uid")) if merged_record.get("tdx_manager_uid") else None,
            "location_info": merged_record.get("location_info"),
        }

        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":"), default=str
        )
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        return content_hash

    def _calculate_data_quality(
        self, merged_record: Dict[str, Any]
    ) -> Tuple[Decimal, List[str]]:
        """
        Calculate data quality score and identify quality flags.

        Scoring criteria (start at 1.00):
        - Missing dept_name: -0.50
        - Missing campus_name: -0.10
        - Missing college_name: -0.10
        - Missing vp_area_name: -0.10
        - TDX-only (no UMAPI data): -0.10
        - UMAPI-only (no TDX operational data): -0.05

        Args:
            merged_record: The merged department record

        Returns:
            Tuple of (quality_score, quality_flags_list)
        """
        score = Decimal("1.00")
        flags = []

        # Critical fields
        if not merged_record.get("dept_name"):
            score -= Decimal("0.50")
            flags.append("missing_dept_name")

        # Hierarchy completeness
        if not merged_record.get("campus_name"):
            score -= Decimal("0.10")
            flags.append("missing_campus")

        if not merged_record.get("college_name"):
            score -= Decimal("0.10")
            flags.append("missing_college")

        if not merged_record.get("vp_area_name"):
            score -= Decimal("0.10")
            flags.append("missing_vp_area")

        # Source completeness
        sources = merged_record.get("sources", [])
        if "tdx" not in sources:
            score -= Decimal("0.10")
            flags.append("no_tdx_data")

        if "umapi" not in sources:
            score -= Decimal("0.05")
            flags.append("no_umapi_data")

        # Ensure score doesn't go below 0
        score = max(Decimal("0.00"), score)

        return score, flags

    def _upsert_consolidated_record(
        self, merged_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> str:
        """
        Insert or update a silver.departments record.

        Args:
            merged_record: The merged record to upsert
            run_id: The current transformation run ID
            dry_run: If True, only log what would be done

        Returns:
            Action taken: 'created', 'updated', or 'skipped'
        """
        dept_id = merged_record["dept_id"]

        if dry_run:
            logger.info(
                f"🔍 [DRY RUN] Would upsert department: "
                f"dept_id={dept_id}, "
                f"name={merged_record['dept_name']}, "
                f"sources={merged_record['sources']}, "
                f"quality={merged_record['data_quality_score']}"
            )
            return "dry_run"

        try:
            # Check if exists and compare hash
            check_query = """
            SELECT entity_hash 
            FROM silver.departments 
            WHERE dept_id = :dept_id
            """
            existing_df = self.db_adapter.query_to_dataframe(
                check_query, {"dept_id": dept_id}
            )

            is_new = existing_df.empty
            existing_hash = None if is_new else existing_df.iloc[0]["entity_hash"]

            # Skip if unchanged
            if not is_new and existing_hash == merged_record["entity_hash"]:
                logger.debug(f"⏭️ Department unchanged, skipping: {dept_id}")
                return "skipped"

            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    upsert_query = text("""
                        INSERT INTO silver.departments (
                            dept_id, dept_name, is_active, parent_dept_id,
                            college_name, campus_name, vp_area_name,
                            dept_group, dept_group_campus, dept_group_vp_area,
                            tdx_id, tdx_manager_uid, location_info,
                            sources, entity_hash,
                            data_quality_score, quality_flags,
                            ingestion_run_id, updated_at,
                            department_name, department_code, vp_area, college_group,
                            source_system, source_entity_id
                        ) VALUES (
                            :dept_id, :dept_name, :is_active, :parent_dept_id,
                            :college_name, :campus_name, :vp_area_name,
                            :dept_group, :dept_group_campus, :dept_group_vp_area,
                            :tdx_id, :tdx_manager_uid, CAST(:location_info AS jsonb),
                            CAST(:sources AS jsonb), :entity_hash,
                            :data_quality_score, CAST(:quality_flags AS jsonb),
                            :ingestion_run_id, CURRENT_TIMESTAMP,
                            :department_name, :department_code, :vp_area, :college_group,
                            :source_system, :source_entity_id
                        )
                        ON CONFLICT (dept_id) DO UPDATE SET
                            dept_name = EXCLUDED.dept_name,
                            is_active = EXCLUDED.is_active,
                            parent_dept_id = EXCLUDED.parent_dept_id,
                            college_name = EXCLUDED.college_name,
                            campus_name = EXCLUDED.campus_name,
                            vp_area_name = EXCLUDED.vp_area_name,
                            dept_group = EXCLUDED.dept_group,
                            dept_group_campus = EXCLUDED.dept_group_campus,
                            dept_group_vp_area = EXCLUDED.dept_group_vp_area,
                            tdx_id = EXCLUDED.tdx_id,
                            tdx_manager_uid = EXCLUDED.tdx_manager_uid,
                            location_info = EXCLUDED.location_info,
                            sources = EXCLUDED.sources,
                            entity_hash = EXCLUDED.entity_hash,
                            data_quality_score = EXCLUDED.data_quality_score,
                            quality_flags = EXCLUDED.quality_flags,
                            ingestion_run_id = EXCLUDED.ingestion_run_id,
                            updated_at = CURRENT_TIMESTAMP,
                            department_name = EXCLUDED.department_name,
                            department_code = EXCLUDED.department_code,
                            vp_area = EXCLUDED.vp_area,
                            college_group = EXCLUDED.college_group,
                            source_system = EXCLUDED.source_system,
                            source_entity_id = EXCLUDED.source_entity_id
                        WHERE silver.departments.entity_hash != EXCLUDED.entity_hash
                    """)

                    conn.execute(
                        upsert_query,
                        {
                            "dept_id": merged_record["dept_id"],
                            "dept_name": merged_record["dept_name"],
                            "is_active": merged_record["is_active"],
                            "parent_dept_id": merged_record["parent_dept_id"],
                            "college_name": merged_record["college_name"],
                            "campus_name": merged_record["campus_name"],
                            "vp_area_name": merged_record["vp_area_name"],
                            "dept_group": merged_record["dept_group"],
                            "dept_group_campus": merged_record["dept_group_campus"],
                            "dept_group_vp_area": merged_record["dept_group_vp_area"],
                            "tdx_id": merged_record["tdx_id"],
                            "tdx_manager_uid": merged_record["tdx_manager_uid"],
                            "location_info": json.dumps(merged_record["location_info"]),
                            "sources": json.dumps(merged_record["sources"]),
                            "entity_hash": merged_record["entity_hash"],
                            "data_quality_score": merged_record["data_quality_score"],
                            "quality_flags": json.dumps(merged_record["quality_flags"]),
                            "ingestion_run_id": run_id,
                            "department_name": merged_record["department_name"],
                            "department_code": merged_record["department_code"],
                            "vp_area": merged_record["vp_area"],
                            "college_group": merged_record["college_group"],
                            "source_system": merged_record["source_system"],
                            "source_entity_id": merged_record["source_entity_id"],
                        },
                    )

            action = "created" if is_new else "updated"
            logger.debug(
                f"✅ {action.capitalize()} department: {dept_id} ({merged_record['dept_name']})"
            )
            return action

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to upsert department {dept_id}: {e}")
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
                "transformation_type": "consolidate_departments",
                "entity_type": "departments_consolidated",
                "source_tables": ["silver.tdx_departments", "silver.umapi_departments"],
                "target_table": "silver.departments",
                "tier": "consolidated",
                "full_sync": full_sync,
                "incremental_since": incremental_since.isoformat()
                if incremental_since
                else None,
            }

            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    insert_query = text("""
                        INSERT INTO meta.ingestion_runs (
                            run_id, source_system, entity_type, started_at, status, metadata
                        ) VALUES (
                            :run_id, 'silver_transformation', 'departments_consolidated', :started_at, 'running', :metadata
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
            records_processed: Total departments processed
            records_created: New records created
            records_updated: Existing records updated
            records_skipped: Records skipped (unchanged)
            error_message: Error message if run failed
        """
        try:
            status = "failed" if error_message else "completed"

            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
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
                                to_jsonb(CAST(:records_skipped AS int))
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

            logger.info(f"✅ Completed transformation run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete transformation run: {e}")

    def consolidate_departments(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Main entry point: Consolidate TDX and UMAPI departments into silver.departments.

        Process flow:
        1. Determine last successful consolidation timestamp (unless full_sync)
        2. Fetch records from silver.tdx_departments and silver.umapi_departments
        3. For each unique dept_id:
           a. Merge TDX and UMAPI records
           b. Calculate quality score
           c. Calculate entity hash
           d. Upsert to silver.departments
        4. Track statistics and return results

        Args:
            full_sync: If True, process all departments regardless of timestamp
            dry_run: If True, preview changes without committing to database

        Returns:
            Dictionary with transformation statistics
        """
        # Get timestamp of last successful consolidation
        last_consolidation = (
            None if full_sync else self._get_last_transformation_timestamp()
        )

        # Create transformation run
        run_id = self.create_transformation_run(last_consolidation, full_sync)

        stats = {
            "run_id": run_id,
            "incremental_since": last_consolidation,
            "full_sync": full_sync,
            "dry_run": dry_run,
            "departments_processed": 0,
            "records_created": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "tdx_only": 0,
            "umapi_only": 0,
            "both_sources": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            if dry_run:
                logger.info("⚠️ DRY RUN MODE - No changes will be committed")

            if full_sync:
                logger.info("🔄 Full sync mode: Processing ALL departments")
            elif last_consolidation:
                logger.info(
                    f"⚡ Incremental mode: Processing departments since {last_consolidation}"
                )
            else:
                logger.info("🆕 First run: Processing ALL departments")

            logger.info("🚀 Starting department consolidation...")

            # Fetch source records
            tdx_records, umapi_records = self._fetch_source_records(
                last_consolidation, full_sync
            )

            # Create lookup dictionaries
            tdx_by_code = {r["dept_code"]: r for r in tdx_records}
            umapi_by_id = {r["dept_id"]: r for r in umapi_records}

            # Get all unique dept_ids
            all_dept_ids = set(tdx_by_code.keys()) | set(umapi_by_id.keys())

            if not all_dept_ids:
                logger.info("✨ All records up to date - no consolidation needed")
                if not dry_run:
                    self.complete_transformation_run(run_id, 0, 0, 0, 0)
                return stats

            logger.info(f"📊 Processing {len(all_dept_ids)} unique departments")

            # Process each unique department
            for idx, dept_id in enumerate(sorted(all_dept_ids), 1):
                try:
                    tdx_record = tdx_by_code.get(dept_id)
                    umapi_record = umapi_by_id.get(dept_id)

                    # Track source distribution
                    if tdx_record and umapi_record:
                        stats["both_sources"] += 1
                    elif tdx_record:
                        stats["tdx_only"] += 1
                    elif umapi_record:
                        stats["umapi_only"] += 1

                    # Merge records
                    merged_record = self._merge_department_records(
                        tdx_record, umapi_record
                    )

                    # Calculate quality
                    quality_score, quality_flags = self._calculate_data_quality(
                        merged_record
                    )
                    merged_record["data_quality_score"] = quality_score
                    merged_record["quality_flags"] = quality_flags

                    # Calculate hash
                    merged_record["entity_hash"] = self._calculate_content_hash(
                        merged_record
                    )

                    # Upsert record
                    action = self._upsert_consolidated_record(
                        merged_record, run_id, dry_run
                    )

                    if action == "created":
                        stats["records_created"] += 1
                    elif action == "updated":
                        stats["records_updated"] += 1
                    elif action == "skipped":
                        stats["records_skipped"] += 1

                    stats["departments_processed"] += 1

                    # Log progress periodically
                    if idx % 500 == 0:
                        logger.info(
                            f"📈 Progress: {idx}/{len(all_dept_ids)} departments processed "
                            f"({stats['records_created']} created, {stats['records_updated']} updated, "
                            f"{stats['records_skipped']} skipped)"
                        )

                except Exception as record_error:
                    error_msg = (
                        f"Error processing department {dept_id}: {str(record_error)}"
                    )
                    logger.error(f"❌ {error_msg}")
                    stats["errors"].append(error_msg)
                    # Continue processing other departments

            # Calculate duration
            stats["duration_seconds"] = (
                datetime.now(timezone.utc) - stats["started_at"]
            ).total_seconds()

            # Complete run
            if not dry_run:
                self.complete_transformation_run(
                    run_id,
                    stats["departments_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                )

            # Log summary
            logger.info("=" * 80)
            logger.info("🎉 DEPARTMENT CONSOLIDATION COMPLETE")
            logger.info("=" * 80)
            logger.info(f"📊 Departments processed: {stats['departments_processed']}")
            logger.info(f"✅ Created: {stats['records_created']}")
            logger.info(f"📝 Updated: {stats['records_updated']}")
            logger.info(f"⏭️  Skipped (unchanged): {stats['records_skipped']}")
            logger.info(f"   - TDX only: {stats['tdx_only']}")
            logger.info(f"   - UMAPI only: {stats['umapi_only']}")
            logger.info(f"   - Both sources: {stats['both_sources']}")
            logger.info(f"⏱️  Duration: {stats['duration_seconds']:.2f}s")
            logger.info(f"🔑 Run ID: {run_id}")
            if stats["errors"]:
                logger.warning(f"⚠️  Errors: {len(stats['errors'])}")
            logger.info("=" * 80)

            return stats

        except Exception as e:
            error_msg = f"Consolidation failed: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)
            if not dry_run:
                self.complete_transformation_run(run_id, 0, 0, 0, 0, error_msg)
            raise

    def close(self):
        """Close database connection."""
        self.db_adapter.close()
        logger.info("🔌 Database connection closed")


def main():
    """Main entry point for department consolidation."""
    parser = argparse.ArgumentParser(
        description="Consolidate TDX and UMAPI departments to silver.departments"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Force full consolidation instead of incremental",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )
    parser.add_argument(
        "--log",
        type=str,
        default="logs/010_transform_departments.log",
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

    # Create service and run consolidation
    service = DepartmentConsolidationService(database_url)

    try:
        if args.dry_run:
            logger.info("🔍 DRY RUN MODE - No changes will be committed")

        result = service.consolidate_departments(
            full_sync=args.full_sync, dry_run=args.dry_run
        )

        logger.info("✨ Consolidation completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Consolidation failed: {e}", exc_info=True)
        sys.exit(1)

    finally:
        service.close()


if __name__ == "__main__":
    main()
