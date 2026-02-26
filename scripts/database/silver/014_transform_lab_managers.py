#!/usr/bin/env python3
"""
Lab Managers Transformation Script

Populates silver.lab_managers table using Python-based scoring logic.
Replaces database function populate_lab_managers().

Key features:
- Uses LabManagerIdentificationService for scoring logic
- Processes all legitimate labs or specific lab_id
- Full refresh strategy (TRUNCATE + INSERT)
- Incremental mode available (single lab update)
- Tracks processing statistics in meta.ingestion_runs

Usage:
    python scripts/database/silver/014_transform_lab_managers.py
    python scripts/database/silver/014_transform_lab_managers.py --lab-id csmonk
    python scripts/database/silver/014_transform_lab_managers.py --dry-run
"""

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add LSATS project to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

# LSATS imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter
from services.lab_manager_identification_service import LabManagerIdentificationService

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


class LabManagersTransformationService:
    """
    Service for transforming lab manager identifications into silver layer.

    Replaces populate_lab_managers() database function with Python implementation.
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
        self.manager_service = LabManagerIdentificationService(database_url)
        
        # Caches for department enrichment
        self.lab_department_cache = {}  # {lab_id: {dept_id, dept_name}}
        self.member_department_cache = {}  # {(lab_id, uniqname): {dept_id, dept_name}}
        
        logger.info("✨ Lab managers transformation service initialized")

    def _create_ingestion_run(self, lab_id: Optional[str] = None) -> str:
        """
        Create a new ingestion run record.

        Args:
            lab_id: Optional specific lab being processed

        Returns:
            run_id UUID string
        """
        run_id = str(uuid.uuid4())
        metadata = {}
        if lab_id:
            metadata["lab_id"] = lab_id
            metadata["mode"] = "incremental"
        else:
            metadata["mode"] = "full_refresh"

        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(
                            """
                            INSERT INTO meta.ingestion_runs (
                                run_id, source_system, entity_type,
                                started_at, status, metadata
                            )
                            VALUES (
                                :run_id, 'silver_transformation', 'lab_manager',
                                :started_at, 'running', CAST(:metadata AS jsonb)
                            )
                            """
                        ),
                        {
                            "run_id": run_id,
                            "started_at": datetime.now(timezone.utc),
                            "metadata": json.dumps(metadata),
                        },
                    )

            logger.info(f"📝 Created ingestion run: {run_id}")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create ingestion run: {e}")
            raise

    def _update_ingestion_run(
        self,
        run_id: str,
        status: str,
        records_processed: int = 0,
        records_created: int = 0,
        records_updated: int = 0,
        error_message: Optional[str] = None,
    ):
        """
        Update ingestion run with final statistics.

        Args:
            run_id: The run UUID
            status: 'completed' or 'failed'
            records_processed: Number of labs processed
            records_created: Number of managers inserted
            records_updated: Not used (always 0 for full refresh)
            error_message: Error details if failed
        """
        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(
                            """
                            UPDATE meta.ingestion_runs
                            SET completed_at = :completed_at,
                                status = :status,
                                records_processed = :records_processed,
                                records_created = :records_created,
                                records_updated = :records_updated,
                                error_message = :error_message
                            WHERE run_id = :run_id
                            """
                        ),
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

            emoji = "✅" if status == "completed" else "❌"
            logger.info(f"{emoji} Ingestion run {status}: {run_id}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to update ingestion run: {e}")
            # Don't raise - this is just logging

    def _load_department_caches(self):
        """
        Load department data caches for enrichment.
        
        Loads:
        1. Lab department data (from silver.labs -> silver.departments)
        2. Member department data (from silver.lab_members)
        """
        logger.info("📚 Loading department caches...")
        
        # Load lab department data
        lab_dept_query = """
            SELECT 
                l.lab_id,
                l.primary_department_id as dept_id,
                d.dept_name
            FROM silver.labs l
            LEFT JOIN silver.departments d ON l.primary_department_id = d.dept_id
        """
        
        lab_dept_df = self.db_adapter.query_to_dataframe(lab_dept_query)
        for _, row in lab_dept_df.iterrows():
            self.lab_department_cache[row['lab_id']] = {
                'dept_id': row['dept_id'],
                'dept_name': row['dept_name']
            }
        
        logger.info(f"   Loaded {len(self.lab_department_cache)} lab department entries")
        
        # Load member department data
        member_dept_query = """
            SELECT 
                lab_id,
                member_uniqname,
                member_department_id as dept_id,
                member_department_name as dept_name
            FROM silver.lab_members
        """
        
        member_dept_df = self.db_adapter.query_to_dataframe(member_dept_query)
        for _, row in member_dept_df.iterrows():
            key = (row['lab_id'], row['member_uniqname'])
            self.member_department_cache[key] = {
                'dept_id': row['dept_id'],
                'dept_name': row['dept_name']
            }
        
        logger.info(f"   Loaded {len(self.member_department_cache)} member department entries")

    def _enrich_with_department_data(self, managers: List[Dict]) -> List[Dict]:
        """
        Enrich manager records with department information.
        
        Args:
            managers: List of manager dicts from LabManagerIdentificationService
            
        Returns:
            Enriched manager dicts with department columns added
        """
        enriched = []
        
        for manager in managers:
            lab_id = manager['lab_id']
            uniqname = manager['manager_uniqname']
            
            # Get lab department data
            lab_dept = self.lab_department_cache.get(lab_id, {})
            manager['lab_department_id'] = lab_dept.get('dept_id')
            manager['lab_department_name'] = lab_dept.get('dept_name')
            
            # Get manager department data
            member_key = (lab_id, uniqname)
            member_dept = self.member_department_cache.get(member_key, {})
            manager['manager_department_id'] = member_dept.get('dept_id')
            manager['manager_department_name'] = member_dept.get('dept_name')
            
            enriched.append(manager)
        
        return enriched

    def _get_legitimate_labs(self, lab_id: Optional[str] = None) -> List[str]:
        """
        Get list of legitimate lab IDs to process.

        Args:
            lab_id: Optional specific lab to process

        Returns:
            List of lab_id strings
        """
        query = """
            SELECT lab_id
            FROM silver.v_legitimate_labs
        """
        params = {}

        if lab_id:
            query += " WHERE lab_id = :lab_id"
            params["lab_id"] = lab_id

        query += " ORDER BY lab_id"

        df = self.db_adapter.query_to_dataframe(query, params)
        return df["lab_id"].tolist()

    def _delete_existing_managers(self, lab_id: Optional[str] = None):
        """
        Delete existing lab managers.

        Args:
            lab_id: Optional specific lab to delete (None = delete all)
        """
        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    if lab_id:
                        result = conn.execute(
                            text(
                                "DELETE FROM silver.lab_managers WHERE lab_id = :lab_id"
                            ),
                            {"lab_id": lab_id},
                        )
                        logger.info(
                            f"🗑️  Deleted {result.rowcount} existing managers for lab '{lab_id}'"
                        )
                    else:
                        result = conn.execute(
                            text("TRUNCATE TABLE silver.lab_managers")
                        )
                        logger.info("🗑️  Truncated silver.lab_managers table")

        except SQLAlchemyError as e:
            logger.error(f"Failed to delete existing managers: {e}")
            raise

    def _insert_managers_batch(self, managers: List[Dict]):
        """
        Insert a batch of manager records.

        Args:
            managers: List of manager dicts
        """
        if not managers:
            return

        # Convert job_codes to JSON strings for JSONB columns
        prepared_managers = []
        for manager in managers:
            prepared = manager.copy()
            # Serialize job_codes to JSON string if it's a list/dict
            if prepared.get("manager_job_codes") is not None:
                if isinstance(prepared["manager_job_codes"], (list, dict)):
                    prepared["manager_job_codes"] = json.dumps(
                        prepared["manager_job_codes"]
                    )
            prepared_managers.append(prepared)

        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(
                            """
                            INSERT INTO silver.lab_managers (
                                lab_id,
                                manager_uniqname,
                                manager_tdx_uid,
                                manager_role,
                                manager_job_codes,
                                manager_confidence_score,
                                manager_rank,
                                detection_reason,
                                lab_department_id,
                                lab_department_name,
                                manager_department_id,
                                manager_department_name
                            )
                            VALUES (
                                :lab_id,
                                :manager_uniqname,
                                :manager_tdx_uid,
                                :manager_role,
                                CAST(:manager_job_codes AS jsonb),
                                :manager_confidence_score,
                                :manager_rank,
                                :detection_reason,
                                :lab_department_id,
                                :lab_department_name,
                                :manager_department_id,
                                :manager_department_name
                            )
                            """
                        ),
                        prepared_managers,
                    )

            logger.debug(f"   Inserted {len(managers)} managers")

        except SQLAlchemyError as e:
            logger.error(f"Failed to insert manager batch: {e}")
            raise

    def transform(
        self, lab_id: Optional[str] = None, dry_run: bool = False
    ) -> Dict[str, int]:
        """
        Transform lab managers for all labs or specific lab.

        Port of populate_lab_managers() database function.

        Args:
            lab_id: Optional specific lab to process (None = all labs)
            dry_run: If True, don't write to database

        Returns:
            Dict with processing statistics
        """
        run_id = self._create_ingestion_run(lab_id)

        stats = {
            "labs_processed": 0,
            "managers_identified": 0,
            "managers_added": 0,
            "managers_removed": 0,
            "labs_with_managers": 0,
            "labs_without_managers": 0,
        }

        try:
            # Step 1: Get labs to process
            lab_ids = self._get_legitimate_labs(lab_id)

            if not lab_ids:
                logger.warning("No legitimate labs found to process")
                self._update_ingestion_run(run_id, "completed", 0, 0, 0)
                return stats

            logger.info(f"📊 Processing {len(lab_ids)} legitimate labs...")

            # Step 2: Load department caches for enrichment
            self._load_department_caches()

            # Step 3: Delete existing managers (unless dry run)
            if not dry_run:
                self._delete_existing_managers(lab_id)

            # Step 4: Process each lab
            all_managers = []

            for current_lab_id in lab_ids:
                stats["labs_processed"] += 1

                # Identify managers for this lab
                managers = self.manager_service.identify_managers_for_lab(
                    current_lab_id
                )

                if managers:
                    # Enrich with department data
                    managers = self._enrich_with_department_data(managers)
                    
                    stats["managers_identified"] += len(managers)
                    stats["labs_with_managers"] += 1
                    all_managers.extend(managers)

                    logger.info(
                        f"✓ Lab '{current_lab_id}': {len(managers)} managers "
                        f"(scores: {', '.join(str(m['manager_confidence_score']) for m in managers)})"
                    )
                else:
                    stats["labs_without_managers"] += 1
                    logger.info(f"○ Lab '{current_lab_id}': No managers identified")

            # Step 5: Insert all managers (unless dry run)
            if not dry_run:
                if all_managers:
                    # Insert in batches of 100
                    batch_size = 100
                    for i in range(0, len(all_managers), batch_size):
                        batch = all_managers[i : i + batch_size]
                        self._insert_managers_batch(batch)
                        stats["managers_added"] += len(batch)

                    logger.info(f"✅ Inserted {stats['managers_added']} managers total")
                else:
                    logger.warning("⚠️  No managers to insert")
            else:
                logger.info("🔍 DRY RUN - No database changes made")

            # Step 6: Update run statistics
            self._update_ingestion_run(
                run_id,
                "completed",
                records_processed=stats["labs_processed"],
                records_created=stats["managers_added"],
                records_updated=0,
            )

            # Step 7: Print summary
            logger.info("")
            logger.info("=" * 60)
            logger.info("LAB MANAGERS TRANSFORMATION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Labs processed:        {stats['labs_processed']}")
            logger.info(
                f"Labs with managers:    {stats['labs_with_managers']} ({stats['labs_with_managers'] / stats['labs_processed'] * 100:.1f}%)"
            )
            logger.info(
                f"Labs without managers: {stats['labs_without_managers']} ({stats['labs_without_managers'] / stats['labs_processed'] * 100:.1f}%)"
            )
            logger.info(f"Total managers:        {stats['managers_identified']}")
            logger.info(
                f"Average per lab:       {stats['managers_identified'] / stats['labs_with_managers']:.1f}"
                if stats["labs_with_managers"] > 0
                else "N/A"
            )
            logger.info(
                f"Managers inserted:     {stats['managers_added']}"
                if not dry_run
                else "Managers (dry run):    {stats['managers_identified']}"
            )
            logger.info("=" * 60)

            return stats

        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}")
            self._update_ingestion_run(
                run_id,
                "failed",
                records_processed=stats["labs_processed"],
                error_message=str(e),
            )
            raise

    def close(self):
        """Close database connections."""
        if self.manager_service:
            self.manager_service.close()
        if self.db_adapter:
            self.db_adapter.close()
        logger.info("Database connections closed")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Transform lab managers into silver layer using Python scoring logic"
    )
    parser.add_argument(
        "--lab-id",
        type=str,
        help="Process specific lab only (default: all legitimate labs)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to database",
    )

    args = parser.parse_args()

    # Load environment
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)

    # Run transformation
    service = None
    try:
        logger.info("🚀 Starting lab managers transformation...")
        logger.info(
            f"   Mode: {'Incremental (lab: ' + args.lab_id + ')' if args.lab_id else 'Full refresh'}"
        )
        logger.info(f"   Dry run: {args.dry_run}")
        logger.info("")

        service = LabManagersTransformationService(database_url)
        stats = service.transform(lab_id=args.lab_id, dry_run=args.dry_run)

        logger.info("")
        logger.info("✅ Transformation completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)

    finally:
        if service:
            service.close()


if __name__ == "__main__":
    main()
