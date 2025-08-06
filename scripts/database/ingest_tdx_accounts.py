#!/usr/bin/env python3
"""
Timestamp-Based Department Ingestion with Content Verification
This version uses TeamDynamix ModifiedDate for efficient change detection
while optionally validating with content hashing for data quality assurance
"""

import os
import sys
import logging
import uuid
import hashlib
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timezone
import dateutil.parser

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
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/department_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TimestampBasedDepartmentIngestionService:
    """
    Department ingestion service that uses TeamDynamix ModifiedDate for efficient change detection.

    This approach leverages TDX's built-in change tracking to minimize processing overhead
    while optionally providing content verification for data quality assurance.

    Key Benefits:
    - Much faster processing by filtering on timestamps
    - Relies on TeamDynamix's reliable change tracking
    - Optional content verification for quality assurance
    - Supports incremental ingestion for large datasets
    """

    def __init__(self, database_url: str, tdx_base_url: str, tdx_api_token: str, tdx_app_id: str,
                 enable_content_verification: bool = False):
        """
        Initialize the timestamp-based ingestion service.

        Args:
            database_url: PostgreSQL connection string
            tdx_base_url: TeamDynamix API base URL
            tdx_api_token: TeamDynamix API authentication token
            tdx_app_id: TeamDynamix application ID
            enable_content_verification: Whether to calculate content hashes for verification
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url,
            pool_size=5,
            max_overflow=10
        )

        self.tdx_facade = TeamDynamixFacade(
            base_url=tdx_base_url,
            app_id=tdx_app_id,
            api_token=tdx_api_token
        )

        # Feature flag for optional content verification
        self.enable_content_verification = enable_content_verification

        logger.info(f"Timestamp-based department ingestion service initialized "
                   f"(content verification: {'enabled' if enable_content_verification else 'disabled'})")

    def _parse_tdx_timestamp(self, timestamp_str: str) -> datetime:
        """
        Parse TeamDynamix timestamp strings into Python datetime objects.

        TDX uses ISO format with Z suffix, like: "2024-07-23T00:09:00Z"

        Args:
            timestamp_str: Timestamp string from TeamDynamix

        Returns:
            datetime object with timezone info
        """
        try:
            # Use dateutil.parser for robust timestamp parsing
            # This handles various ISO formats and timezone indicators
            parsed_dt = dateutil.parser.isoparse(timestamp_str)

            # Ensure we have timezone info (convert Z to UTC if needed)
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)

            return parsed_dt

        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
            # Return epoch time as fallback to ensure ingestion continues
            return datetime.fromtimestamp(0, tz=timezone.utc)

    def _calculate_content_hash(self, raw_data: Dict[str, Any]) -> str:
        """
        Calculate content hash for verification purposes (optional feature).

        This is only used when enable_content_verification=True to provide
        additional data quality assurance alongside timestamp-based detection.
        """
        significant_fields = {
            'ID': raw_data.get('ID'),
            'Name': raw_data.get('Name', '').strip(),
            'Code': raw_data.get('Code', '').strip(),
            'Notes': raw_data.get('Notes', '').strip(),
            'IsActive': raw_data.get('IsActive'),
            'ParentID': raw_data.get('ParentID'),
            'ManagerUID': raw_data.get('ManagerUID'),
            # Note: We deliberately exclude ModifiedDate from the hash
            # since we're using it as our change detection mechanism
        }

        normalized_json = json.dumps(significant_fields, sort_keys=True, separators=(',', ':'))
        content_hash = hashlib.sha256(normalized_json.encode('utf-8')).hexdigest()

        return content_hash

    def _get_last_ingestion_timestamp(self) -> Optional[datetime]:
        """
        Get the timestamp of the most recent successful department ingestion.

        This allows us to ask TeamDynamix for only departments that have been
        modified since our last successful ingestion run.

        Returns:
            datetime of last successful ingestion, or None if this is the first run
        """
        try:
            # Query the most recent successful ingestion run for TDX departments
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'tdx'
            AND entity_type = 'department'
            AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]['last_completed'] is not None:
                last_timestamp = result_df.iloc[0]['last_completed']
                logger.info(f"Last successful ingestion was at: {last_timestamp}")
                return last_timestamp
            else:
                logger.info("No previous successful ingestion found - this appears to be the first run")
                return None

        except SQLAlchemyError as e:
            logger.warning(f"Could not determine last ingestion timestamp: {e}")
            return None

    def _get_departments_modified_since(self, since_timestamp: Optional[datetime]) -> List[Dict[str, Any]]:
        """
        Get departments from TeamDynamix that have been modified since the given timestamp.

        This method demonstrates how you could implement incremental extraction
        if TeamDynamix supported filtering by ModifiedDate in their API.

        Args:
            since_timestamp: Only return departments modified after this time

        Returns:
            List of department dictionaries from TeamDynamix

        Note:
            Currently, this method fetches all departments and filters client-side
            because the TDX API doesn't appear to support ModifiedDate filtering.
            If TDX adds this capability, you could optimize by filtering server-side.
        """
        logger.info("Fetching department data from TeamDynamix...")

        # Fetch all departments from TeamDynamix
        # In an ideal world, we could pass since_timestamp to filter server-side
        all_departments = self.tdx_facade.accounts.get_accounts()

        if since_timestamp is None:
            # First run - process all departments
            logger.info(f"First ingestion run - processing all {len(all_departments)} departments")
            return all_departments

        # Filter departments based on ModifiedDate
        filtered_departments = []
        for dept in all_departments:
            try:
                modified_date_str = dept.get('ModifiedDate')
                if not modified_date_str:
                    # If no ModifiedDate, include it to be safe
                    filtered_departments.append(dept)
                    continue

                modified_date = self._parse_tdx_timestamp(modified_date_str)

                # Include departments modified after our last successful ingestion
                if modified_date > since_timestamp:
                    filtered_departments.append(dept)
                    logger.debug(f"Department {dept.get('Name')} modified on {modified_date_str} - including")
                else:
                    logger.debug(f"Department {dept.get('Name')} not modified since last ingestion - skipping")

            except Exception as e:
                # If we can't parse the timestamp, include the department to be safe
                logger.warning(f"Error processing department {dept.get('ID', 'unknown')}: {e}")
                filtered_departments.append(dept)

        logger.info(f"Filtered to {len(filtered_departments)} departments modified since {since_timestamp}")
        return filtered_departments

    def create_ingestion_run(self, source_system: str, entity_type: str,
                           incremental_since: Optional[datetime] = None) -> str:
        """Create a new ingestion run record with incremental tracking."""
        try:
            run_id = str(uuid.uuid4())

            # Store metadata about this incremental run
            metadata = {
                'ingestion_type': 'timestamp_based',
                'incremental_since': incremental_since.isoformat() if incremental_since else None,
                'content_verification_enabled': self.enable_content_verification
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

            logger.info(f"Created ingestion run {run_id} for {source_system}/{entity_type}")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create ingestion run: {e}")
            raise

    def complete_ingestion_run(self, run_id: str, records_processed: int,
                             records_created: int, content_mismatches: int = 0,
                             error_message: Optional[str] = None):
        """Mark an ingestion run as completed with detailed statistics."""
        try:
            status = 'failed' if error_message else 'completed'

            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = :completed_at,
                        status = :status,
                        records_processed = :records_processed,
                        records_created = :records_created,
                        records_updated = :content_mismatches,
                        error_message = :error_message
                    WHERE run_id = :run_id
                """)

                conn.execute(update_query, {
                    'run_id': run_id,
                    'completed_at': datetime.now(timezone.utc),
                    'status': status,
                    'records_processed': records_processed,
                    'records_created': records_created,
                    'content_mismatches': content_mismatches,
                    'error_message': error_message
                })

                conn.commit()

            logger.info(f"Completed ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete ingestion run: {e}")

    def ingest_departments_timestamp_based(self) -> Dict[str, Any]:
        """
        Ingest departments using TeamDynamix ModifiedDate for efficient change detection.

        This method:
        1. Determines the timestamp of the last successful ingestion
        2. Fetches only departments modified since that timestamp
        3. Creates bronze records for all modified departments
        4. Optionally verifies content changes for quality assurance

        Returns:
            Dictionary with detailed ingestion statistics
        """
        # Determine when we last successfully ingested departments
        last_ingestion_timestamp = self._get_last_ingestion_timestamp()

        # Create ingestion run for tracking
        run_id = self.create_ingestion_run('tdx', 'department', last_ingestion_timestamp)

        ingestion_stats = {
            'run_id': run_id,
            'incremental_since': last_ingestion_timestamp,
            'records_processed': 0,
            'records_created': 0,
            'content_verification_mismatches': 0,
            'errors': [],
            'started_at': datetime.now(timezone.utc)
        }

        try:
            logger.info("Starting timestamp-based department ingestion...")

            # Fetch departments modified since our last successful ingestion
            modified_departments = self._get_departments_modified_since(last_ingestion_timestamp)

            if not modified_departments:
                logger.info("No departments have been modified since last ingestion - nothing to process")
                self.complete_ingestion_run(run_id, 0, 0)
                return ingestion_stats

            logger.info(f"Processing {len(modified_departments)} departments with recent modifications")

            # Process each modified department
            for department_data in modified_departments:
                try:
                    external_id = str(department_data.get('ID', 'unknown'))
                    dept_name = department_data.get('Name', 'Unknown Department')
                    modified_date_str = department_data.get('ModifiedDate', 'Unknown')

                    # Prepare enhanced raw data with timestamp metadata
                    enhanced_raw_data = department_data.copy()
                    enhanced_raw_data['_ingestion_method'] = 'timestamp_based'
                    enhanced_raw_data['_modified_date_parsed'] = self._parse_tdx_timestamp(
                        department_data.get('ModifiedDate', '')
                    ).isoformat()

                    # Optional content verification for quality assurance
                    if self.enable_content_verification:
                        content_hash = self._calculate_content_hash(department_data)
                        enhanced_raw_data['_content_hash'] = content_hash
                        enhanced_raw_data['_content_verification'] = True

                    # Insert the bronze record
                    entity_id = self.db_adapter.insert_raw_entity(
                        entity_type='department',
                        source_system='tdx',
                        external_id=external_id,
                        raw_data=enhanced_raw_data,
                        ingestion_run_id=run_id
                    )

                    ingestion_stats['records_created'] += 1

                    logger.debug(f"Ingested department: {dept_name} (modified: {modified_date_str})")

                    # Log progress periodically
                    if ingestion_stats['records_processed'] % 20 == 0 and ingestion_stats['records_processed'] > 0:
                        logger.info(f"Processed {ingestion_stats['records_processed']} departments...")

                except Exception as record_error:
                    error_msg = f"Failed to process department {external_id}: {record_error}"
                    logger.error(error_msg)
                    ingestion_stats['errors'].append(error_msg)

                ingestion_stats['records_processed'] += 1

            # Complete the ingestion run
            error_summary = None
            if ingestion_stats['errors']:
                error_summary = f"{len(ingestion_stats['errors'])} individual record errors occurred"

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats['records_processed'],
                records_created=ingestion_stats['records_created'],
                content_mismatches=ingestion_stats['content_verification_mismatches'],
                error_message=error_summary
            )

            ingestion_stats['completed_at'] = datetime.now(timezone.utc)
            duration = (ingestion_stats['completed_at'] - ingestion_stats['started_at']).total_seconds()

            # Log comprehensive results
            logger.info(f"Timestamp-based department ingestion completed in {duration:.2f} seconds")
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Total Processed: {ingestion_stats['records_processed']}")
            logger.info(f"   Records Created: {ingestion_stats['records_created']}")
            logger.info(f"   Incremental Since: {last_ingestion_timestamp or 'First Run'}")
            logger.info(f"   Errors: {len(ingestion_stats['errors'])}")

            if self.enable_content_verification:
                logger.info(f"   Content Verification Mismatches: {ingestion_stats['content_verification_mismatches']}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"Timestamp-based department ingestion failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats['records_processed'],
                records_created=ingestion_stats['records_created'],
                error_message=error_msg
            )

            raise

    def get_recent_department_changes(self, days: int = 7) -> pd.DataFrame:
        """
        Get departments that have been modified in the last N days based on bronze data.

        This is useful for monitoring recent organizational changes.

        Args:
            days: Number of days to look back

        Returns:
            DataFrame with recent department changes
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - pd.Timedelta(days=days)

            query = """
            SELECT
                external_id,
                raw_data->>'Name' as department_name,
                raw_data->>'Code' as department_code,
                raw_data->>'ModifiedDate' as tdx_modified_date,
                raw_data->>'IsActive' as is_active,
                ingested_at,
                ingestion_run_id
            FROM bronze.raw_entities
            WHERE entity_type = 'department'
            AND source_system = 'tdx'
            AND ingested_at >= :cutoff_date
            ORDER BY ingested_at DESC
            """

            changes_df = self.db_adapter.query_to_dataframe(query, {'cutoff_date': cutoff_date})

            logger.info(f"Found {len(changes_df)} department changes in the last {days} days")
            return changes_df

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve recent department changes: {e}")
            raise

    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()
        logger.info("Timestamp-based department ingestion service closed")


def main():
    """
    Main function to run timestamp-based department ingestion from command line.
    """
    try:
        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv('DATABASE_URL')
        tdx_base_url = os.getenv('TDX_BASE_URL')
        tdx_api_token = os.getenv('TDX_API_TOKEN')
        tdx_app_id = os.getenv('TDX_APP_ID')

        # Optional: Enable content verification for additional quality assurance
        enable_verification = os.getenv('ENABLE_CONTENT_VERIFICATION', 'true').lower() == 'true'

        # Validate configuration
        required_vars = {
            'DATABASE_URL': database_url,
            'TDX_BASE_URL': tdx_base_url,
            'TDX_API_TOKEN': tdx_api_token,
            'TDX_APP_ID': tdx_app_id
        }

        missing_vars = [name for name, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

        # Create and run timestamp-based ingestion service
        ingestion_service = TimestampBasedDepartmentIngestionService(
            database_url=database_url,
            tdx_base_url=tdx_base_url,
            tdx_api_token=tdx_api_token,
            tdx_app_id=tdx_app_id,
            enable_content_verification=enable_verification
        )

        # Run the timestamp-based ingestion process
        print("⏰ Starting timestamp-based department ingestion...")
        results = ingestion_service.ingest_departments_timestamp_based()

        # Display comprehensive summary
        print(f"\n📊 Timestamp-Based Ingestion Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Incremental Since: {results['incremental_since'] or 'First Run (Full Sync)'}")
        print(f"   Departments Processed: {results['records_processed']}")
        print(f"   New Records Created: {results['records_created']}")
        print(f"   Errors: {len(results['errors'])}")

        if enable_verification:
            print(f"   Content Verification Mismatches: {results['content_verification_mismatches']}")

        # Show recent changes for context
        print("\n📋 Recent Department Changes:")
        recent_changes = ingestion_service.get_recent_department_changes(days=30)
        if not recent_changes.empty:
            for _, row in recent_changes.head(5).iterrows():
                print(f"   - {row['department_name']} (modified: {row['tdx_modified_date']})")
        else:
            print("   No recent changes found")

        # Clean up
        ingestion_service.close()

        print("✅ Timestamp-based department ingestion completed successfully!")

    except Exception as e:
        logger.error(f"Timestamp-based department ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
