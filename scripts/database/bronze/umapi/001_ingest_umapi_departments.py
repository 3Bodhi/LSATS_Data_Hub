#!/usr/bin/env python3
"""
University of Michigan Department Ingestion Service

This service ingests department data from the University of Michigan API into the bronze layer
for cross-referencing and analysis.

UMich API provides hierarchical organizational information including:
- Department identifiers (DeptId, DeptDescription)
- Hierarchical structure (Campus -> VP Area -> Group -> Department)
- Organizational metadata

Uses content hashing for change detection since the UMich API doesn't provide
modification timestamps. Only inserts bronze records when department content has
actually changed.
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
from typing import Any, Dict, List, Optional

# Core Python imports for PostgreSQL operations
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add your LSATS project to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter
from umich.api.department_api import DepartmentAPI
from umich.api.um_api import create_headers

# Detect layer from script path for log organization
script_path = os.path.abspath(__file__)
script_name = os.path.basename(__file__).replace(".py", "")

if "/bronze/" in script_path:
    log_dir = "logs/bronze"
elif "/silver/" in script_path:
    log_dir = "logs/silver"
elif "/gold/" in script_path:
    log_dir = "logs/gold"
else:
    log_dir = "logs"

# Create log directory if it doesn't exist
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


class UMichDepartmentIngestionService:
    """
    Department ingestion service for University of Michigan department data.

    Uses content hashing for change detection since umich API doesn't provide
    modification timestamps. This approach:

    1. Fetches current department data from umich API
    2. Calculates content hashes for each department
    3. Compares against stored hashes from previous ingestions
    4. Only creates new bronze records when department content has actually changed
    5. Preserves complete change history for organizational analysis

    Key Features:
    - Efficient change detection without requiring timestamps
    - Hierarchical department structure support (Campus -> VP Area -> Group -> Department)
    - Comprehensive audit trail for organizational changes
    - Detailed ingestion statistics and monitoring
    - Dry-run mode for previewing changes without committing
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
        Initialize the UMich department ingestion service.

        Args:
            database_url: PostgreSQL connection string
            um_base_url: University of Michigan API base URL
            um_category_id: UMich API category ID
            um_client_key: UMich API client key
            um_client_secret: UMich API client secret
            scope: API scope (default: "department")
            force_full_sync: If True, bypass change detection (Note: UMich API always returns all departments)
            dry_run: If True, preview changes without committing to database
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        # Initialize UMich Department API with proper authentication
        self.um_headers = create_headers(um_client_key, um_client_secret, scope)
        self.um_dept_api = DepartmentAPI(um_base_url, um_category_id, self.um_headers)

        # Store full sync and dry run flags
        self.force_full_sync = force_full_sync
        self.dry_run = dry_run

        logger.info(
            f"UMich department ingestion service initialized with content hashing "
            f"(force_full_sync={'enabled' if force_full_sync else 'disabled'}, "
            f"dry_run={'enabled' if dry_run else 'disabled'})"
        )

    def _calculate_department_content_hash(self, dept_data: Dict[str, Any]) -> str:
        """
        Calculate a content hash for umich department data to detect meaningful changes.

        This hash represents the "content fingerprint" of the department record.
        We include all fields that would represent meaningful organizational changes.

        IMPORTANT: Metadata fields (starting with '_') are explicitly EXCLUDED from
        the hash calculation to ensure that changes in ingestion timestamps or other
        metadata don't affect change detection.

        Args:
            dept_data: Raw department data from umich API

        Returns:
            SHA-256 hash of the normalized department content
        """
        # Extract significant fields for change detection
        # Based on the umich department structure
        significant_fields = {
            "DeptId": dept_data.get("DeptId", "").strip(),
            "DeptDescription": dept_data.get("DeptDescription", "").strip(),
            "DeptGroup": dept_data.get("DeptGroup", "").strip(),
            "DeptGroupDescription": dept_data.get("DeptGroupDescription", "").strip(),
            "DeptGroupVPArea": dept_data.get("DeptGroupVPArea", "").strip(),
            "DeptGroupVPAreaDescr": dept_data.get("DeptGroupVPAreaDescr", "").strip(),
            "DeptGroupCampus": dept_data.get("DeptGroupCampus", "").strip(),
            "DeptGroupCampusDescr": dept_data.get("DeptGroupCampusDescr", "").strip(),
        }

        # Create normalized JSON for consistent hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )

        # Generate SHA-256 hash
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        dept_id = dept_data.get("DeptId", "unknown")
        dept_name = dept_data.get("DeptDescription", "Unknown Department")
        logger.debug(
            f"Content hash for department {dept_id} ({dept_name}): {content_hash}"
        )

        return content_hash

    def _get_existing_department_hashes(self) -> Dict[str, str]:
        """
        Retrieve the latest content hash for each umich department from the bronze layer.

        This uses a window function to get only the most recent record for each
        department, allowing efficient comparison with new data.

        Returns:
            Dictionary mapping DeptId -> latest_content_hash
        """
        try:
            # Query to get the most recent record for each department
            query = """
            WITH latest_departments AS (
                SELECT
                    external_id,
                    raw_data,
                    ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'department'
                AND source_system = 'umich_api'
            )
            SELECT
                external_id,
                raw_data
            FROM latest_departments
            WHERE row_num = 1
            """

            results_df = self.db_adapter.query_to_dataframe(query)

            # Retrieve stored content hashes from existing records
            existing_hashes = {}
            recalculated_count = 0
            for _, row in results_df.iterrows():
                dept_id = row["external_id"]
                raw_data = row["raw_data"]  # JSONB comes back as dict

                # Use the stored hash that was calculated at ingestion time
                content_hash = raw_data.get("_content_hash")

                if not content_hash:
                    # Fallback for old records without stored hash
                    content_hash = self._calculate_department_content_hash(raw_data)
                    recalculated_count += 1
                    logger.debug(
                        f"Department {dept_id} missing stored _content_hash, recalculating"
                    )

                existing_hashes[dept_id] = content_hash

            if recalculated_count > 0:
                logger.warning(
                    f"Recalculated hashes for {recalculated_count} departments missing stored _content_hash"
                )

            logger.info(
                f"🔬 Retrieved content hashes for {len(existing_hashes)} existing umich departments"
            )
            return existing_hashes

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to retrieve existing department hashes: {e}")
            raise

    def create_ingestion_run(self, source_system: str, entity_type: str) -> str:
        """
        Create a new ingestion run record for tracking purposes.

        Args:
            source_system: Source system identifier
            entity_type: Entity type being ingested

        Returns:
            The ingestion run ID (UUID string)
        """
        if self.dry_run:
            run_id = f"dry-run-{uuid.uuid4()}"
            logger.info(f"[DRY RUN] Would create ingestion run {run_id}")
            return run_id

        try:
            run_id = str(uuid.uuid4())

            # Metadata specific to umich content hashing approach
            metadata = {
                "ingestion_type": "content_hash_based",
                "source_api": "umich_department_api",
                "change_detection_method": "sha256_content_hash",
                "hierarchical_structure": True,
                "force_full_sync": self.force_full_sync,
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
                f"Created umich ingestion run {run_id} for {source_system}/{entity_type}"
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
        """
        Mark an ingestion run as completed with comprehensive statistics.

        Args:
            run_id: The ingestion run ID
            records_processed: Total number of records processed
            records_created: Number of new records created
            records_skipped: Number of records skipped (unchanged)
            error_message: Error message if the run failed
        """
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

            logger.info(f"Completed umich ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete ingestion run: {e}")

    def ingest_umich_departments_with_change_detection(self) -> Dict[str, Any]:
        """
        Ingest University of Michigan departments using intelligent content hashing.

        This method:
        1. Fetches all department data from the umich API
        2. Calculates content hashes for each department
        3. Compares against existing bronze records
        4. Only creates new records when content has actually changed
        5. Provides detailed statistics about organizational changes detected

        Returns:
            Dictionary with comprehensive ingestion statistics
        """
        # Create ingestion run for tracking
        run_id = self.create_ingestion_run("umich_api", "department")

        ingestion_stats = {
            "run_id": run_id,
            "records_processed": 0,
            "records_created": 0,
            "records_skipped_unchanged": 0,
            "new_departments": 0,
            "changed_departments": 0,
            "hierarchical_levels": {
                "campuses": set(),
                "vp_areas": set(),
                "groups": set(),
                "departments": set(),
            },
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info("=" * 80)
            logger.info("🚀 STARTING UMICH DEPARTMENT INGESTION")
            logger.info("=" * 80)
            if self.force_full_sync:
                logger.info(
                    "🔄 Full sync mode: Processing ALL departments (Note: UMich API always returns all)"
                )
            else:
                logger.info(
                    "⚡ Standard mode: Using content hashing for change detection"
                )

            if self.dry_run:
                logger.info("⚠️  DRY RUN MODE - No changes will be committed")

            # Step 1: Get existing department content hashes from bronze layer
            existing_hashes = self._get_existing_department_hashes()

            # Step 2: Fetch current data from UMich Department API
            logger.info(
                "🔬 Fetching department data from University of Michigan API..."
            )
            raw_departments = self.um_dept_api.get_all_departments()
            logger.info(
                f"Retrieved {len(raw_departments):,} departments from UMich API"
            )

            # Step 3: Process each department with content hash change detection
            for i, dept_data in enumerate(raw_departments, 1):
                try:
                    # Extract department identifier
                    dept_id = dept_data.get("DeptId", "unknown")
                    dept_name = dept_data.get("DeptDescription", "Unknown Department")

                    # Track hierarchical structure for analysis
                    ingestion_stats["hierarchical_levels"]["campuses"].add(
                        dept_data.get("DeptGroupCampus", "Unknown")
                    )
                    ingestion_stats["hierarchical_levels"]["vp_areas"].add(
                        dept_data.get("DeptGroupVPArea", "Unknown")
                    )
                    ingestion_stats["hierarchical_levels"]["groups"].add(
                        dept_data.get("DeptGroup", "Unknown")
                    )
                    ingestion_stats["hierarchical_levels"]["departments"].add(dept_name)

                    # Calculate content hash for this department
                    current_hash = self._calculate_department_content_hash(dept_data)

                    # Check if this department is new or has changed
                    existing_hash = existing_hashes.get(dept_id)

                    if existing_hash is None:
                        # This is a completely new department
                        logger.info(
                            f"🆕 New department detected: {dept_name} (ID: {dept_id})"
                        )
                        should_insert = True
                        ingestion_stats["new_departments"] += 1

                    elif existing_hash != current_hash:
                        # This department exists but has changed
                        logger.info(
                            f"📝 Department changed: {dept_name} (ID: {dept_id})"
                        )
                        logger.debug(f"   Old hash: {existing_hash}")
                        logger.debug(f"   New hash: {current_hash}")
                        should_insert = True
                        ingestion_stats["changed_departments"] += 1

                    else:
                        # This department exists and hasn't changed - skip it
                        logger.debug(
                            f"⏭️  Department unchanged, skipping: {dept_name} (ID: {dept_id})"
                        )
                        should_insert = False
                        ingestion_stats["records_skipped_unchanged"] += 1

                    # Only insert if the department is new or changed
                    if should_insert:
                        if self.dry_run:
                            logger.info(
                                f"[DRY RUN] Would insert department: {dept_name} (ID: {dept_id})"
                            )
                            logger.debug(f"[DRY RUN] Content hash: {current_hash}")
                        else:
                            # Enhance raw data with metadata for future reference
                            enhanced_raw_data = dept_data.copy()
                            enhanced_raw_data["_content_hash"] = current_hash
                            enhanced_raw_data["_change_detection"] = (
                                "content_hash_based"
                            )
                            enhanced_raw_data["_hierarchical_path"] = (
                                f"{dept_data.get('DeptGroupCampusDescr', 'Unknown Campus')} -> "
                                f"{dept_data.get('DeptGroupVPAreaDescr', 'Unknown VP Area')} -> "
                                f"{dept_data.get('DeptGroupDescription', 'Unknown Group')} -> "
                                f"{dept_name}"
                            )
                            enhanced_raw_data["_ingestion_timestamp"] = datetime.now(
                                timezone.utc
                            ).isoformat()

                            # Insert into bronze layer
                            entity_id = self.db_adapter.insert_raw_entity(
                                entity_type="department",
                                source_system="umich_api",
                                external_id=dept_id,
                                raw_data=enhanced_raw_data,
                                ingestion_run_id=run_id,
                            )

                        ingestion_stats["records_created"] += 1

                    # Log progress periodically
                    if i % 100 == 0:
                        elapsed = (
                            datetime.now(timezone.utc) - ingestion_stats["started_at"]
                        ).total_seconds()
                        rate = i / elapsed if elapsed > 0 else 0

                        logger.info(
                            f"📈 Progress: {i:,}/{len(raw_departments):,} departments processed "
                            f"({ingestion_stats['records_created']:,} new/changed, "
                            f"{ingestion_stats['records_skipped_unchanged']:,} unchanged) | "
                            f"Rate: {rate:.1f} records/sec"
                        )

                except Exception as record_error:
                    error_msg = (
                        f"Failed to process department {dept_id}: {record_error}"
                    )
                    logger.error(f"❌ {error_msg}")
                    ingestion_stats["errors"].append(error_msg)

                ingestion_stats["records_processed"] += 1

            # Convert sets to counts for final reporting
            hierarchy_counts = {
                level: len(items)
                for level, items in ingestion_stats["hierarchical_levels"].items()
            }
            ingestion_stats["hierarchy_summary"] = hierarchy_counts

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
            logger.info("=" * 80)
            logger.info("🎉 INGESTION COMPLETED")
            logger.info("=" * 80)
            logger.info(f"📊 Results Summary:")
            logger.info(
                f"   Total Processed:      {ingestion_stats['records_processed']:>6,}"
            )
            logger.info(
                f"   ├─ New Created:       {ingestion_stats['records_created']:>6,}"
            )
            logger.info(
                f"   │  ├─ New Depts:      {ingestion_stats['new_departments']:>6,}"
            )
            logger.info(
                f"   │  └─ Changed Depts:  {ingestion_stats['changed_departments']:>6,}"
            )
            logger.info(
                f"   └─ Skipped:           {ingestion_stats['records_skipped_unchanged']:>6,}"
            )
            logger.info(f"")
            logger.info(f"   Organizational Structure:")
            logger.info(f"   ├─ Campuses:          {hierarchy_counts['campuses']:>6,}")
            logger.info(f"   ├─ VP Areas:          {hierarchy_counts['vp_areas']:>6,}")
            logger.info(f"   ├─ Groups:            {hierarchy_counts['groups']:>6,}")
            logger.info(
                f"   └─ Departments:       {hierarchy_counts['departments']:>6,}"
            )
            logger.info(f"")
            logger.info(
                f"   Errors:               {len(ingestion_stats['errors']):>6,}"
            )
            logger.info(f"   Duration:             {duration:.2f}s")
            logger.info("=" * 80)

            if self.dry_run:
                logger.info("⚠️  DRY RUN MODE - No changes were committed to database")

            return ingestion_stats

        except Exception as e:
            error_msg = f"UMich department ingestion failed: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats["records_processed"],
                records_created=ingestion_stats["records_created"],
                records_skipped=ingestion_stats["records_skipped_unchanged"],
                error_message=error_msg,
            )

            raise

    def get_department_hierarchy_analysis(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze the hierarchical structure of UMich departments from bronze data.

        This provides insights into the organizational structure and can help
        identify patterns or anomalies in the department hierarchy.

        Returns:
            Dictionary containing DataFrames for different hierarchical analyses
        """
        try:
            # Query for hierarchical analysis
            hierarchy_query = """
            WITH latest_departments AS (
                SELECT
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'department'
                AND source_system = 'umich_api'
            )
            SELECT
                raw_data->>'DeptId' as dept_id,
                raw_data->>'DeptDescription' as department_name,
                raw_data->>'DeptGroup' as dept_group,
                raw_data->>'DeptGroupDescription' as group_description,
                raw_data->>'DeptGroupVPArea' as vp_area,
                raw_data->>'DeptGroupVPAreaDescr' as vp_area_description,
                raw_data->>'DeptGroupCampus' as campus,
                raw_data->>'DeptGroupCampusDescr' as campus_description
            FROM latest_departments
            WHERE row_num = 1
            ORDER BY campus_description, vp_area_description, group_description, department_name
            """

            hierarchy_df = self.db_adapter.query_to_dataframe(hierarchy_query)

            # Create summary analyses
            analyses = {}

            # Campus-level summary
            campus_summary = (
                hierarchy_df.groupby(["campus", "campus_description"])
                .size()
                .reset_index(name="department_count")
            )
            analyses["campus_summary"] = campus_summary

            # VP Area summary
            vp_summary = (
                hierarchy_df.groupby(
                    ["vp_area", "vp_area_description", "campus_description"]
                )
                .size()
                .reset_index(name="department_count")
            )
            analyses["vp_area_summary"] = vp_summary

            # Group summary
            group_summary = (
                hierarchy_df.groupby(
                    ["dept_group", "group_description", "vp_area_description"]
                )
                .size()
                .reset_index(name="department_count")
            )
            analyses["group_summary"] = group_summary

            # Full hierarchy
            analyses["full_hierarchy"] = hierarchy_df

            logger.info(
                f"Generated hierarchical analysis with {len(hierarchy_df)} departments"
            )
            return analyses

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to generate hierarchy analysis: {e}")
            raise

    def get_department_change_history(self, dept_id: str) -> pd.DataFrame:
        """
        Get the complete change history for a specific UMich department.

        Args:
            dept_id: The UMich Department ID

        Returns:
            DataFrame with all historical versions of the department
        """
        try:
            query = """
            SELECT
                raw_id,
                raw_data->>'DeptDescription' as department_name,
                raw_data->>'DeptGroup' as dept_group,
                raw_data->>'DeptGroupDescription' as group_description,
                raw_data->>'DeptGroupVPAreaDescr' as vp_area_description,
                raw_data->>'DeptGroupCampusDescr' as campus_description,
                raw_data->>'_content_hash' as content_hash,
                raw_data->>'_hierarchical_path' as hierarchical_path,
                ingested_at,
                ingestion_run_id
            FROM bronze.raw_entities
            WHERE entity_type = 'department'
            AND source_system = 'umich_api'
            AND external_id = :dept_id
            ORDER BY ingested_at DESC
            """

            history_df = self.db_adapter.query_to_dataframe(query, {"dept_id": dept_id})

            logger.info(
                f"Retrieved {len(history_df)} historical records for UMich department {dept_id}"
            )
            return history_df

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to retrieve department history: {e}")
            raise

    def close(self) -> None:
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()
        logger.info("🧹 UMich department ingestion service closed")


def main():
    """
    Main function to run UMich department ingestion from command line.
    """
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(
            description="Ingest UMich departments to bronze layer"
        )
        parser.add_argument(
            "--full-sync",
            action="store_true",
            help="Process all records (Note: UMich API always returns all departments)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without committing to database",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=500,
            help="Number of records to process per batch (for future use)",
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
        ingestion_service = UMichDepartmentIngestionService(
            database_url=database_url,
            um_base_url=um_base_url,
            um_category_id=um_category_id,
            um_client_key=um_client_key,
            um_client_secret=um_client_secret,
            force_full_sync=args.full_sync,
            dry_run=args.dry_run,
        )

        # Run the content hash-based ingestion process
        print("=" * 80)
        print("🚀 Starting University of Michigan department ingestion")
        print("=" * 80)
        print(f"   Mode: {'FULL SYNC' if args.full_sync else 'STANDARD'}")
        print(f"   Dry Run: {args.dry_run}")
        print("=" * 80)

        results = ingestion_service.ingest_umich_departments_with_change_detection()

        # Display comprehensive summary
        print("\n" + "=" * 80)
        print("📊 UMICH DEPARTMENT INGESTION SUMMARY")
        print("=" * 80)
        print(f"Run ID:              {results['run_id']}")
        print(f"Total Processed:     {results['records_processed']:,}")
        print(f"New Records Created: {results['records_created']:,}")
        print(f"  ├─ New Depts:      {results['new_departments']:,}")
        print(f"  └─ Changed Depts:  {results['changed_departments']:,}")
        print(f"Skipped (No Change): {results['records_skipped_unchanged']:,}")
        print(f"")
        print(f"Organizational Structure:")
        print(f"  ├─ Campuses:       {results['hierarchy_summary']['campuses']:,}")
        print(f"  ├─ VP Areas:       {results['hierarchy_summary']['vp_areas']:,}")
        print(f"  ├─ Groups:         {results['hierarchy_summary']['groups']:,}")
        print(f"  └─ Departments:    {results['hierarchy_summary']['departments']:,}")
        print(f"")
        print(f"Errors:              {len(results['errors']):,}")
        duration = (results["completed_at"] - results["started_at"]).total_seconds()
        print(f"Duration:            {duration:.2f}s")
        print("=" * 80)

        if (
            results["records_skipped_unchanged"] > 0
            and results["records_processed"] > 0
        ):
            efficiency_percentage = (
                results["records_skipped_unchanged"] / results["records_processed"]
            ) * 100
            print(
                f"\n⚡ Efficiency: {efficiency_percentage:.1f}% of departments were unchanged and skipped"
            )

        if args.dry_run:
            print("\n⚠️  DRY RUN MODE - No changes committed to database")
        else:
            print("\n✅ UMich department ingestion completed successfully!")

        # Clean up
        ingestion_service.close()

    except Exception as e:
        logger.error(f"❌ UMich department ingestion failed: {e}", exc_info=True)
        print(f"\n❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
