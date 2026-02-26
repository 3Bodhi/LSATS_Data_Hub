#!/usr/bin/env python3
"""
University of Michigan Department Ingestion with Content Hashing
This version uses content hashing to detect changes in umich department data
since the umich API doesn't provide modification timestamps
"""

import os
import sys
import logging
import uuid
import hashlib
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timezone

# Core Python imports for PostgreSQL operations
import pandas as pd
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from sqlalchemy.pool import QueuePool
import psycopg2
from psycopg2.extras import RealDictCursor
import json

# Add your LSATS project to Python path (adjust path as needed)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# LSATS Data Hub imports
from database.adapters.postgres_adapter import PostgresAdapter, create_postgres_adapter
from umich.api.um_api import create_headers  # For umich API authentication
from umich.api.department_api import DepartmentAPI
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/umich_department_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
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
    """

    def __init__(self, database_url: str, um_base_url: str, um_category_id: str,
                 um_client_key: str, um_client_secret: str, scope: str = "department"):
        """
        Initialize the UMich department ingestion service.

        Args:
            database_url: PostgreSQL connection string
            um_base_url: University of Michigan API base URL
            um_category_id: UMich API category ID
            um_client_key: UMich API client key
            um_client_secret: UMich API client secret
            scope: API scope (default: "department")
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url,
            pool_size=5,
            max_overflow=10
        )

        # Initialize UMich Department API with proper authentication
        self.um_headers = create_headers(um_client_key, um_client_secret, scope)
        self.um_dept_api = DepartmentAPI(um_base_url, um_category_id, self.um_headers)

        logger.info("UMich department ingestion service initialized with content hashing")

    def _calculate_department_content_hash(self, dept_data: Dict[str, Any]) -> str:
        """
        Calculate a content hash for umich department data to detect meaningful changes.

        This hash represents the "content fingerprint" of the department record.
        We include all fields that would represent meaningful organizational changes.

        Args:
            dept_data: Raw department data from umich API

        Returns:
            SHA-256 hash of the normalized department content
        """
        # Extract significant fields for change detection
        # Based on the umich department structure you provided
        significant_fields = {
            'DeptId': dept_data.get('DeptId', '').strip(),
            'DeptDescription': dept_data.get('DeptDescription', '').strip(),
            'DeptGroup': dept_data.get('DeptGroup', '').strip(),
            'DeptGroupDescription': dept_data.get('DeptGroupDescription', '').strip(),
            'DeptGroupVPArea': dept_data.get('DeptGroupVPArea', '').strip(),
            'DeptGroupVPAreaDescr': dept_data.get('DeptGroupVPAreaDescr', '').strip(),
            'DeptGroupCampus': dept_data.get('DeptGroupCampus', '').strip(),
            'DeptGroupCampusDescr': dept_data.get('DeptGroupCampusDescr', '').strip()
        }

        # Create normalized JSON for consistent hashing
        normalized_json = json.dumps(significant_fields, sort_keys=True, separators=(',', ':'))

        # Generate SHA-256 hash
        content_hash = hashlib.sha256(normalized_json.encode('utf-8')).hexdigest()

        dept_id = dept_data.get('DeptId', 'unknown')
        dept_name = dept_data.get('DeptDescription', 'Unknown Department')
        logger.debug(f"Content hash for department {dept_id} ({dept_name}): {content_hash}")

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

            # Calculate content hashes for existing records
            existing_hashes = {}
            for _, row in results_df.iterrows():
                dept_id = row['external_id']
                raw_data = row['raw_data']  # JSONB comes back as dict
                content_hash = self._calculate_department_content_hash(raw_data)
                existing_hashes[dept_id] = content_hash

            logger.info(f"Retrieved content hashes for {len(existing_hashes)} existing umich departments")
            return existing_hashes

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve existing department hashes: {e}")
            raise

    def create_ingestion_run(self, source_system: str, entity_type: str) -> str:
        """Create a new ingestion run record for tracking purposes."""
        try:
            run_id = str(uuid.uuid4())

            # Metadata specific to umich content hashing approach
            metadata = {
                'ingestion_type': 'content_hash_based',
                'source_api': 'umich_department_api',
                'change_detection_method': 'sha256_content_hash',
                'hierarchical_structure': True
            }

            with self.db_adapter.engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status, metadata
                    ) VALUES (
                        :run_id, :source_system, :entity_type, :started_at, 'running', :metadata
                    )
                """)

                conn.execute(insert_query, {
                    'run_id': run_id,
                    'source_system': source_system,
                    'entity_type': entity_type,
                    'started_at': datetime.now(timezone.utc),
                    'metadata': json.dumps(metadata)
                })

                conn.commit()

            logger.info(f"Created umich ingestion run {run_id} for {source_system}/{entity_type}")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create ingestion run: {e}")
            raise

    def complete_ingestion_run(self, run_id: str, records_processed: int,
                             records_created: int, records_skipped: int = 0,
                             error_message: Optional[str] = None):
        """Mark an ingestion run as completed with comprehensive statistics."""
        try:
            status = 'failed' if error_message else 'completed'

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

                conn.execute(update_query, {
                    'run_id': run_id,
                    'completed_at': datetime.now(timezone.utc),
                    'status': status,
                    'records_processed': records_processed,
                    'records_created': records_created,
                    'records_skipped': records_skipped,
                    'error_message': error_message
                })

                conn.commit()

            logger.info(f"Completed umich ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete ingestion run: {e}")

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
        run_id = self.create_ingestion_run('umich_api', 'department')

        ingestion_stats = {
            'run_id': run_id,
            'records_processed': 0,
            'records_created': 0,
            'records_skipped_unchanged': 0,
            'new_departments': 0,
            'changed_departments': 0,
            'hierarchical_levels': {
                'campuses': set(),
                'vp_areas': set(),
                'groups': set(),
                'departments': set()
            },
            'errors': [],
            'started_at': datetime.now(timezone.utc)
        }

        try:
            logger.info("Starting UMich department ingestion with content hash change detection...")

            # Step 1: Get existing department content hashes from bronze layer
            existing_hashes = self._get_existing_department_hashes()

            # Step 2: Fetch current data from UMich Department API
            logger.info("Fetching department data from University of Michigan API...")
            raw_departments = self.um_dept_api.get_all_departments()
            logger.info(f"Retrieved {len(raw_departments)} departments from UMich API")

            # Step 3: Process each department with content hash change detection
            for dept_data in raw_departments:
                try:
                    # Extract department identifier
                    dept_id = dept_data.get('DeptId', 'unknown')
                    dept_name = dept_data.get('DeptDescription', 'Unknown Department')

                    # Track hierarchical structure for analysis
                    ingestion_stats['hierarchical_levels']['campuses'].add(
                        dept_data.get('DeptGroupCampus', 'Unknown')
                    )
                    ingestion_stats['hierarchical_levels']['vp_areas'].add(
                        dept_data.get('DeptGroupVPArea', 'Unknown')
                    )
                    ingestion_stats['hierarchical_levels']['groups'].add(
                        dept_data.get('DeptGroup', 'Unknown')
                    )
                    ingestion_stats['hierarchical_levels']['departments'].add(dept_name)

                    # Calculate content hash for this department
                    current_hash = self._calculate_department_content_hash(dept_data)

                    # Check if this department is new or has changed
                    existing_hash = existing_hashes.get(dept_id)

                    if existing_hash is None:
                        # This is a completely new department
                        logger.info(f"🆕 New department detected: {dept_name} (ID: {dept_id})")
                        should_insert = True
                        ingestion_stats['new_departments'] += 1

                    elif existing_hash != current_hash:
                        # This department exists but has changed
                        logger.info(f"📝 Department changed: {dept_name} (ID: {dept_id})")
                        logger.debug(f"   Old hash: {existing_hash}")
                        logger.debug(f"   New hash: {current_hash}")
                        should_insert = True
                        ingestion_stats['changed_departments'] += 1

                    else:
                        # This department exists and hasn't changed - skip it
                        logger.debug(f"⏭️  Department unchanged, skipping: {dept_name} (ID: {dept_id})")
                        should_insert = False
                        ingestion_stats['records_skipped_unchanged'] += 1

                    # Only insert if the department is new or changed
                    if should_insert:
                        # Enhance raw data with metadata for future reference
                        enhanced_raw_data = dept_data.copy()
                        enhanced_raw_data['_content_hash'] = current_hash
                        enhanced_raw_data['_change_detection'] = 'content_hash_based'
                        enhanced_raw_data['_hierarchical_path'] = (
                            f"{dept_data.get('DeptGroupCampusDescr', 'Unknown Campus')} -> "
                            f"{dept_data.get('DeptGroupVPAreaDescr', 'Unknown VP Area')} -> "
                            f"{dept_data.get('DeptGroupDescription', 'Unknown Group')} -> "
                            f"{dept_name}"
                        )

                        # Insert into bronze layer
                        entity_id = self.db_adapter.insert_raw_entity(
                            entity_type='department',
                            source_system='umich_api',
                            external_id=dept_id,
                            raw_data=enhanced_raw_data,
                            ingestion_run_id=run_id
                        )

                        ingestion_stats['records_created'] += 1

                    # Log progress periodically
                    if ingestion_stats['records_processed'] % 50 == 0 and ingestion_stats['records_processed'] > 0:
                        logger.info(f"Progress: {ingestion_stats['records_processed']} departments processed "
                                  f"({ingestion_stats['records_created']} new/changed, "
                                  f"{ingestion_stats['records_skipped_unchanged']} unchanged)")

                except Exception as record_error:
                    error_msg = f"Failed to process department {dept_id}: {record_error}"
                    logger.error(error_msg)
                    ingestion_stats['errors'].append(error_msg)

                ingestion_stats['records_processed'] += 1

            # Convert sets to counts for final reporting
            hierarchy_counts = {
                level: len(items) for level, items in ingestion_stats['hierarchical_levels'].items()
            }
            ingestion_stats['hierarchy_summary'] = hierarchy_counts

            # Complete the ingestion run
            error_summary = None
            if ingestion_stats['errors']:
                error_summary = f"{len(ingestion_stats['errors'])} individual record errors occurred"

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats['records_processed'],
                records_created=ingestion_stats['records_created'],
                records_skipped=ingestion_stats['records_skipped_unchanged'],
                error_message=error_summary
            )

            ingestion_stats['completed_at'] = datetime.now(timezone.utc)
            duration = (ingestion_stats['completed_at'] - ingestion_stats['started_at']).total_seconds()

            # Log comprehensive results
            logger.info(f"🎉 UMich department ingestion completed in {duration:.2f} seconds")
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Total Processed: {ingestion_stats['records_processed']}")
            logger.info(f"   New Records Created: {ingestion_stats['records_created']}")
            logger.info(f"   ├─ New Departments: {ingestion_stats['new_departments']}")
            logger.info(f"   └─ Changed Departments: {ingestion_stats['changed_departments']}")
            logger.info(f"   Skipped (Unchanged): {ingestion_stats['records_skipped_unchanged']}")
            logger.info(f"   Organizational Structure:")
            logger.info(f"   ├─ Campuses: {hierarchy_counts['campuses']}")
            logger.info(f"   ├─ VP Areas: {hierarchy_counts['vp_areas']}")
            logger.info(f"   ├─ Groups: {hierarchy_counts['groups']}")
            logger.info(f"   └─ Departments: {hierarchy_counts['departments']}")
            logger.info(f"   Errors: {len(ingestion_stats['errors'])}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"UMich department ingestion failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats['records_processed'],
                records_created=ingestion_stats['records_created'],
                records_skipped=ingestion_stats['records_skipped_unchanged'],
                error_message=error_msg
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
            campus_summary = hierarchy_df.groupby(['campus', 'campus_description']).size().reset_index(name='department_count')
            analyses['campus_summary'] = campus_summary

            # VP Area summary
            vp_summary = hierarchy_df.groupby(['vp_area', 'vp_area_description', 'campus_description']).size().reset_index(name='department_count')
            analyses['vp_area_summary'] = vp_summary

            # Group summary
            group_summary = hierarchy_df.groupby(['dept_group', 'group_description', 'vp_area_description']).size().reset_index(name='department_count')
            analyses['group_summary'] = group_summary

            # Full hierarchy
            analyses['full_hierarchy'] = hierarchy_df

            logger.info(f"Generated hierarchical analysis with {len(hierarchy_df)} departments")
            return analyses

        except SQLAlchemyError as e:
            logger.error(f"Failed to generate hierarchy analysis: {e}")
            raise
    def log_complete_hierarchy_analysis(self, log_file_path: str = 'logs/umich_hierarchy_analysis.log') -> None:
            """
            Generate and log a complete hierarchical analysis to a dedicated log file.

            This creates a comprehensive organizational structure analysis and appends
            it to a log file with timestamps for historical tracking.

            Args:
                log_file_path: Path to the hierarchy analysis log file
            """
            try:
                # Ensure log directory exists
                os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
                # Get the hierarchy data
                hierarchy_analyses = self.get_department_hierarchy_analysis()

                # Open log file for appending
                with open(log_file_path, 'a', encoding='utf-8') as log_file:
                    # Write header with timestamp
                    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                    log_file.write(f"\n{'='*80}\n")
                    log_file.write(f"UNIVERSITY OF MICHIGAN ORGANIZATIONAL HIERARCHY ANALYSIS\n")
                    log_file.write(f"Generated: {timestamp}\n")
                    log_file.write(f"{'='*80}\n\n")

                    # Campus Analysis
                    campus_summary = hierarchy_analyses['campus_summary']
                    log_file.write(f"🏛️  CAMPUS DISTRIBUTION ({len(campus_summary)} campuses)\n")
                    log_file.write(f"{'-'*50}\n")
                    for _, row in campus_summary.iterrows():
                        log_file.write(f"   {row['campus_description']:<40} {row['department_count']:>6} departments\n")
                    log_file.write(f"\n")

                    # VP Area Analysis
                    vp_summary = hierarchy_analyses['vp_area_summary']
                    log_file.write(f"🏢 VP AREA DISTRIBUTION ({len(vp_summary)} VP areas)\n")
                    log_file.write(f"{'-'*70}\n")
                    for _, row in vp_summary.iterrows():
                        vp_area = row['vp_area_description'][:45] + "..." if len(row['vp_area_description']) > 45 else row['vp_area_description']
                        campus = row['campus_description'][:20] + "..." if len(row['campus_description']) > 20 else row['campus_description']
                        log_file.write(f"   {vp_area:<48} ({campus:<23}) {row['department_count']:>4} depts\n")
                    log_file.write(f"\n")

                    # Department Group Analysis
                    group_summary = hierarchy_analyses['group_summary']
                    log_file.write(f"🏬 DEPARTMENT GROUP DISTRIBUTION ({len(group_summary)} groups)\n")
                    log_file.write(f"{'-'*80}\n")
                    for _, row in group_summary.iterrows():
                        group_name = row['group_description'][:35] + "..." if len(row['group_description']) > 35 else row['group_description']
                        vp_area = row['vp_area_description'][:25] + "..." if len(row['vp_area_description']) > 25 else row['vp_area_description']
                        log_file.write(f"   {group_name:<38} under {vp_area:<28} {row['department_count']:>4} depts\n")
                    log_file.write(f"\n")

                    # Complete Department Listing
                    full_hierarchy = hierarchy_analyses['full_hierarchy']
                    log_file.write(f"📋 COMPLETE DEPARTMENT LISTING ({len(full_hierarchy)} departments)\n")
                    log_file.write(f"{'-'*80}\n")

                    # Group by campus for organized listing
                    for campus in full_hierarchy['campus_description'].unique():
                        campus_depts = full_hierarchy[full_hierarchy['campus_description'] == campus]
                        log_file.write(f"\n📍 {campus.upper()} ({len(campus_depts)} departments)\n")

                        # Group by VP area within campus
                        for vp_area in campus_depts['vp_area_description'].unique():
                            vp_depts = campus_depts[campus_depts['vp_area_description'] == vp_area]
                            log_file.write(f"\n   🏢 {vp_area} ({len(vp_depts)} departments)\n")

                            # Group by department group within VP area
                            for dept_group in vp_depts['group_description'].unique():
                                group_depts = vp_depts[vp_depts['group_description'] == dept_group]
                                if dept_group.strip():  # Only show if group description exists
                                    log_file.write(f"\n      🏬 {dept_group} ({len(group_depts)} departments)\n")

                                    # List individual departments
                                    for _, dept in group_depts.iterrows():
                                        dept_name = dept['department_name'][:50] + "..." if len(dept['department_name']) > 50 else dept['department_name']
                                        log_file.write(f"         • [{dept['dept_id']}] {dept_name}\n")
                                else:
                                    # Departments with no group - list directly under VP area
                                    for _, dept in group_depts.iterrows():
                                        dept_name = dept['department_name'][:50] + "..." if len(dept['department_name']) > 50 else dept['department_name']
                                        log_file.write(f"      • [{dept['dept_id']}] {dept_name}\n")

                    # Summary statistics
                    log_file.write(f"\n{'='*50}\n")
                    log_file.write(f"SUMMARY STATISTICS\n")
                    log_file.write(f"{'='*50}\n")
                    log_file.write(f"Total Campuses:        {len(campus_summary):>6}\n")
                    log_file.write(f"Total VP Areas:        {len(vp_summary):>6}\n")
                    log_file.write(f"Total Department Groups: {len(group_summary):>4}\n")
                    log_file.write(f"Total Departments:     {len(full_hierarchy):>6}\n")
                    log_file.write(f"\n")

                logger.info(f"Complete hierarchy analysis written to: {log_file_path}")

            except Exception as e:
                logger.error(f"Failed to write hierarchy analysis to log file: {e}")
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

            history_df = self.db_adapter.query_to_dataframe(query, {'dept_id': dept_id})

            logger.info(f"Retrieved {len(history_df)} historical records for UMich department {dept_id}")
            return history_df

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve department history: {e}")
            raise

    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()
        logger.info("UMich department ingestion service closed")


def main():
    """
    Main function to run UMich department ingestion from command line.
    """
    try:
        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv('DATABASE_URL')
        um_base_url = os.getenv('UM_BASE_URL')
        um_category_id = os.getenv('UM_CATEGORY_ID')
        um_client_key = os.getenv('UM_CLIENT_KEY')
        um_client_secret = os.getenv('UM_CLIENT_SECRET')

        # Validate configuration
        required_vars = {
            'DATABASE_URL': database_url,
            'UM_BASE_URL': um_base_url,
            'UM_CATEGORY_ID': um_category_id,
            'UM_CLIENT_KEY': um_client_key,
            'UM_CLIENT_SECRET': um_client_secret
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
            um_client_secret=um_client_secret
        )

        # Run the content hash-based ingestion process
        print("🏛️  Starting University of Michigan department ingestion with content hashing...")
        results = ingestion_service.ingest_umich_departments_with_change_detection()

        # Display comprehensive summary
        print(f"\n📊 UMich Department Ingestion Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Total Departments Processed: {results['records_processed']}")
        print(f"   New Records Created: {results['records_created']}")
        print(f"     ├─ Brand New Departments: {results['new_departments']}")
        print(f"     └─ Departments with Changes: {results['changed_departments']}")
        print(f"   Skipped (No Changes): {results['records_skipped_unchanged']}")
        print(f"   Organizational Structure Detected:")
        print(f"     ├─ Campuses: {results['hierarchy_summary']['campuses']}")
        print(f"     ├─ VP Areas: {results['hierarchy_summary']['vp_areas']}")
        print(f"     ├─ Department Groups: {results['hierarchy_summary']['groups']}")
        print(f"     └─ Individual Departments: {results['hierarchy_summary']['departments']}")
        print(f"   Errors: {len(results['errors'])}")

        if results['records_skipped_unchanged'] > 0:
            efficiency_percentage = (results['records_skipped_unchanged'] / results['records_processed']) * 100
            print(f"\n⚡ Efficiency: {efficiency_percentage:.1f}% of departments were unchanged and skipped")

        # Show organizational hierarchy analysis
        print("\n🏗️  Analyzing organizational hierarchy...")
        hierarchy_analyses = ingestion_service.get_department_hierarchy_analysis()

        print("📝 Writing complete hierarchy analysis to log file...")
        ingestion_service.log_complete_hierarchy_analysis()

        # Campus-level summary
        print("\n📋 Campus Distribution:")
        campus_summary = hierarchy_analyses['campus_summary']
        for _, row in campus_summary.iterrows():
            print(f"   - {row['campus_description']}: {row['department_count']} departments")

        # VP Area summary
        print("\n🏢 VP Area Distribution:")
        vp_summary = hierarchy_analyses['vp_area_summary']
        for _, row in vp_summary.head(10).iterrows():  # Show top 10 VP areas
            print(f"   - {row['vp_area_description']} ({row['campus_description']}): {row['department_count']} departments")

        if len(vp_summary) > 10:
            remaining_vp_count = len(vp_summary) - 10
            remaining_dept_count = vp_summary.iloc[10:]['department_count'].sum()
            print(f"   - ... and {remaining_vp_count} more VP areas with {remaining_dept_count} additional departments")

        # Department Group summary
        print("\n🏬 Department Group Distribution (Top 15):")
        group_summary = hierarchy_analyses['group_summary']
        for _, row in group_summary.head(15).iterrows():
            print(f"   - {row['group_description']} ({row['vp_area_description']}): {row['department_count']} departments")

        if len(group_summary) > 15:
            remaining_group_count = len(group_summary) - 15
            remaining_dept_count = group_summary.iloc[15:]['department_count'].sum()
            print(f"   - ... and {remaining_group_count} more groups with {remaining_dept_count} additional departments")

        # Overall statistics
        total_hierarchy_levels = {
            'Unique Campuses': len(campus_summary),
            'Unique VP Areas': len(vp_summary),
            'Unique Department Groups': len(group_summary),
            'Total Departments': len(hierarchy_analyses['full_hierarchy'])
        }

        print(f"\n📈 Organizational Structure Summary:")
        for level, count in total_hierarchy_levels.items():
            print(f"   - {level}: {count}")

        # Clean up
        ingestion_service.close()

        print("✅ University of Michigan department ingestion completed successfully!")

    except Exception as e:
        logger.error(f"UMich department ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
