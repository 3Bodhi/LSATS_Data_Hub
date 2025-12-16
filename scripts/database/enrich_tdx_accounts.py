#!/usr/bin/env python3
"""
Progressive Bronze Layer Enrichment Service

This service implements a two-stage bronze ingestion pattern:
1. Rapid ingestion using get_accounts() for change detection
2. Detailed enrichment using get_account(ID) for complete data

This approach optimizes API usage while ensuring comprehensive data capture.
"""

import os
import sys
import logging
import uuid
import time
from typing import Dict, List, Any, Optional, Set, Tuple
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
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/progressive_bronze_enrichment.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ProgressiveBronzeEnrichmentService:
    """
    Service that implements progressive bronze layer enrichment for TeamDynamix departments.

    This service handles both the rapid initial ingestion using get_accounts() and the
    detailed enrichment using individual get_account(ID) calls to complete the data.
    """

    def __init__(self, database_url: str, tdx_base_url: str, tdx_api_token: str, tdx_app_id: str,
                 api_rate_limit_delay: float = 1.0):
        """
        Initialize the progressive enrichment service.

        Args:
            database_url: PostgreSQL connection string
            tdx_base_url: TeamDynamix API base URL
            tdx_api_token: TeamDynamix API authentication token
            tdx_app_id: TeamDynamix application ID
            api_rate_limit_delay: Delay between individual API calls (seconds)
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

        # Rate limiting for individual get_account() calls
        self.api_rate_limit_delay = api_rate_limit_delay

        logger.info(f"Progressive bronze enrichment service initialized "
                   f"(API delay: {api_rate_limit_delay}s between calls)")

    def get_departments_needing_enrichment(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Find bronze layer records that need detailed enrichment.

        This queries for records where _ingestion_method is 'timestamp_based'
        and returns the information needed to enrich them.

        Args:
            limit: Maximum number of records to return for processing

        Returns:
            DataFrame with department records needing enrichment
        """
        try:
            # Query bronze layer for records needing enrichment
            query = """
            SELECT
                raw_id,
                external_id,
                raw_data,
                ingested_at,
                ingestion_run_id,
                raw_data->>'Name' as department_name,
                raw_data->>'ID' as tdx_id,
                raw_data->>'_ingestion_method' as current_method
            FROM bronze.raw_entities
            WHERE entity_type = 'department'
            AND source_system = 'tdx'
            AND raw_data->>'_ingestion_method' = 'timestamp_based'
            ORDER BY ingested_at ASC
            """

            # Add limit if specified
            if limit:
                query += f" LIMIT {limit}"

            results_df = self.db_adapter.query_to_dataframe(query)

            logger.info(f"Found {len(results_df)} departments needing enrichment")

            # Log some sample data for visibility
            if not results_df.empty:
                logger.info("Sample departments to enrich:")
                for _, row in results_df.head(3).iterrows():
                    logger.info(f"  - {row['department_name']} (TDX ID: {row['tdx_id']})")

            return results_df

        except SQLAlchemyError as e:
            logger.error(f"Failed to query departments needing enrichment: {e}")
            raise

    def enrich_department_record(self, raw_id: str, external_id: str,
                                original_raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single department record by calling get_account(ID) for complete data.

        Args:
            raw_id: The raw_id of the bronze record to enrich
            external_id: The TeamDynamix department ID
            original_raw_data: The current raw_data from the bronze record

        Returns:
            Dictionary with enrichment results and statistics
        """
        enrichment_result = {
            'raw_id': raw_id,
            'external_id': external_id,
            'success': False,
            'error_message': None,
            'fields_added': [],
            'attributes_count': 0
        }

        try:
            # Call TeamDynamix get_account(ID) for complete department data
            logger.debug(f"Calling get_account({external_id}) for complete data...")

            # Add rate limiting to respect TeamDynamix API limits
            time.sleep(self.api_rate_limit_delay)

            complete_data = self.tdx_facade.accounts.get_account(int(external_id))

            if not complete_data:
                raise ValueError(f"get_account({external_id}) returned empty response")

            # Analyze what new fields we're getting
            original_fields = set(original_raw_data.keys())
            complete_fields = set(complete_data.keys())
            new_fields = complete_fields - original_fields

            # Special attention to Attributes field since that's what we're primarily after
            attributes = complete_data.get('Attributes', [])
            attributes_count = len(attributes) if attributes else 0

            enrichment_result['fields_added'] = list(new_fields)
            enrichment_result['attributes_count'] = attributes_count

            # Prepare the enriched raw data
            enriched_raw_data = complete_data.copy()
            enriched_raw_data['_ingestion_method'] = 'timestamp_based_complete'
            enriched_raw_data['_enrichment_timestamp'] = datetime.now(timezone.utc).isoformat()
            enriched_raw_data['_original_ingestion_method'] = original_raw_data.get('_ingestion_method', 'unknown')

            # Update the bronze record with the enriched data
            with self.db_adapter.engine.connect() as conn:
                # Update the raw_data field and ingestion_metadata
                update_query = text("""
                    UPDATE bronze.raw_entities
                    SET raw_data = :enriched_raw_data,
                        ingestion_metadata = jsonb_set(
                            COALESCE(ingestion_metadata, '{}'::jsonb),
                            '{full_data}',
                            'true'::jsonb
                        )
                    WHERE raw_id = :raw_id
                """)

                conn.execute(update_query, {
                    'raw_id': raw_id,
                    'enriched_raw_data': json.dumps(enriched_raw_data)
                })

                conn.commit()

            enrichment_result['success'] = True

            logger.debug(f"Successfully enriched department {external_id} - added {len(new_fields)} fields, "
                        f"{attributes_count} attributes")

            return enrichment_result

        except Exception as e:
            error_msg = f"Failed to enrich department {external_id}: {str(e)}"
            logger.warning(error_msg)

            enrichment_result['error_message'] = error_msg
            return enrichment_result

    def update_original_records_metadata(self, processed_raw_ids: List[str]) -> int:
        """
        Update the ingestion_metadata for original records to mark them as having basic data only.

        This retroactively adds full_data=false to records that were processed with basic ingestion.

        Args:
            processed_raw_ids: List of raw_id values that were processed for enrichment

        Returns:
            Number of records updated
        """
        try:
            if not processed_raw_ids:
                return 0

            # Create a query to update ingestion_metadata for all processed records
            # We'll update any records that don't already have full_data metadata
            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE bronze.raw_entities
                    SET ingestion_metadata = jsonb_set(
                        COALESCE(ingestion_metadata, '{}'::jsonb),
                        '{full_data}',
                        CASE
                            WHEN raw_data->>'_ingestion_method' = 'timestamp_based_complete' THEN 'true'::jsonb
                            ELSE 'false'::jsonb
                        END
                    )
                    WHERE entity_type = 'department'
                    AND source_system = 'tdx'
                    AND (ingestion_metadata->>'full_data' IS NULL)
                """)

                result = conn.execute(update_query)
                updated_count = result.rowcount
                conn.commit()

            logger.info(f"Updated ingestion_metadata for {updated_count} existing records")
            return updated_count

        except SQLAlchemyError as e:
            logger.error(f"Failed to update original records metadata: {e}")
            raise

    def create_enrichment_run(self, departments_to_process: int) -> str:
        """Create an ingestion run record specifically for the enrichment process."""
        try:
            run_id = str(uuid.uuid4())

            metadata = {
                'ingestion_type': 'progressive_enrichment',
                'stage': 'detail_enrichment',
                'departments_to_process': departments_to_process,
                'api_rate_limit_delay': self.api_rate_limit_delay
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
                    'source_system': 'tdx',
                    'entity_type': 'department',
                    'started_at': datetime.now(timezone.utc),
                    'metadata': json.dumps(metadata)
                })

                conn.commit()

            logger.info(f"Created enrichment run {run_id} for {departments_to_process} departments")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create enrichment run: {e}")
            raise

    def complete_enrichment_run(self, run_id: str, departments_processed: int,
                               departments_enriched: int, departments_failed: int,
                               error_message: Optional[str] = None):
        """Mark an enrichment run as completed with detailed statistics."""
        try:
            status = 'failed' if error_message else 'completed'

            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = :completed_at,
                        status = :status,
                        records_processed = :records_processed,
                        records_created = :records_enriched,
                        records_updated = :records_failed,
                        error_message = :error_message
                    WHERE run_id = :run_id
                """)

                conn.execute(update_query, {
                    'run_id': run_id,
                    'completed_at': datetime.now(timezone.utc),
                    'status': status,
                    'records_processed': departments_processed,
                    'records_enriched': departments_enriched,
                    'records_failed': departments_failed,
                    'error_message': error_message
                })

                conn.commit()

            logger.info(f"Completed enrichment run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete enrichment run: {e}")

    def run_progressive_enrichment(self, max_departments: Optional[int] = None,
                                 stop_on_errors: bool = False) -> Dict[str, Any]:
        """
        Run the progressive enrichment process to complete department data.

        This method:
        1. Finds bronze records with _ingestion_method = 'timestamp_based'
        2. Calls get_account(ID) for each to get complete data including Attributes
        3. Updates raw_data with complete information
        4. Changes _ingestion_method to 'timestamp_based_complete'
        5. Updates ingestion_metadata with full_data flags

        Args:
            max_departments: Maximum number of departments to process in this run
            stop_on_errors: Whether to stop processing if individual enrichments fail

        Returns:
            Dictionary with detailed enrichment statistics
        """
        # Find departments needing enrichment
        departments_to_enrich = self.get_departments_needing_enrichment(limit=max_departments)

        if departments_to_enrich.empty:
            logger.info("No departments found needing enrichment")
            return {
                'departments_processed': 0,
                'departments_enriched': 0,
                'departments_failed': 0,
                'total_attributes_added': 0,
                'errors': []
            }

        # Create enrichment run for tracking
        run_id = self.create_enrichment_run(len(departments_to_enrich))

        enrichment_stats = {
            'run_id': run_id,
            'departments_processed': 0,
            'departments_enriched': 0,
            'departments_failed': 0,
            'total_attributes_added': 0,
            'total_new_fields': 0,
            'errors': [],
            'started_at': datetime.now(timezone.utc)
        }

        try:
            logger.info(f"Starting progressive enrichment for {len(departments_to_enrich)} departments...")

            # Process each department for enrichment
            for index, row in departments_to_enrich.iterrows():
                try:
                    raw_id = row['raw_id']
                    external_id = row['external_id']
                    department_name = row['department_name']
                    original_raw_data = row['raw_data']  # This comes as dict from JSONB

                    logger.info(f"Enriching department {enrichment_stats['departments_processed'] + 1}/{len(departments_to_enrich)}: "
                               f"{department_name} (ID: {external_id})")

                    # Enrich this specific department
                    enrichment_result = self.enrich_department_record(
                        raw_id=raw_id,
                        external_id=external_id,
                        original_raw_data=original_raw_data
                    )

                    if enrichment_result['success']:
                        enrichment_stats['departments_enriched'] += 1
                        enrichment_stats['total_attributes_added'] += enrichment_result['attributes_count']
                        enrichment_stats['total_new_fields'] += len(enrichment_result['fields_added'])

                        logger.info(f"✅ Enriched {department_name}: +{len(enrichment_result['fields_added'])} fields, "
                                   f"+{enrichment_result['attributes_count']} attributes")
                    else:
                        enrichment_stats['departments_failed'] += 1
                        enrichment_stats['errors'].append(enrichment_result['error_message'])

                        logger.warning(f"❌ Failed to enrich {department_name}: {enrichment_result['error_message']}")

                        if stop_on_errors:
                            logger.error("Stopping enrichment due to error and stop_on_errors=True")
                            break

                    enrichment_stats['departments_processed'] += 1

                    # Progress logging
                    if enrichment_stats['departments_processed'] % 10 == 0:
                        logger.info(f"Progress: {enrichment_stats['departments_processed']}/{len(departments_to_enrich)} departments processed")

                except Exception as dept_error:
                    error_msg = f"Unexpected error processing department {row.get('external_id', 'unknown')}: {dept_error}"
                    logger.error(error_msg)
                    enrichment_stats['errors'].append(error_msg)
                    enrichment_stats['departments_failed'] += 1
                    enrichment_stats['departments_processed'] += 1

                    if stop_on_errors:
                        logger.error("Stopping enrichment due to unexpected error and stop_on_errors=True")
                        break


            # Complete the enrichment run
            error_summary = None
            if enrichment_stats['errors']:
                error_summary = f"{len(enrichment_stats['errors'])} enrichment errors occurred"

            self.complete_enrichment_run(
                run_id=run_id,
                departments_processed=enrichment_stats['departments_processed'],
                departments_enriched=enrichment_stats['departments_enriched'],
                departments_failed=enrichment_stats['departments_failed'],
                error_message=error_summary
            )

            enrichment_stats['completed_at'] = datetime.now(timezone.utc)
            duration = (enrichment_stats['completed_at'] - enrichment_stats['started_at']).total_seconds()

            # Log comprehensive results
            logger.info(f"Progressive enrichment completed in {duration:.2f} seconds")
            logger.info(f"📊 Enrichment Results Summary:")
            logger.info(f"   Departments Processed: {enrichment_stats['departments_processed']}")
            logger.info(f"   ✅ Successfully Enriched: {enrichment_stats['departments_enriched']}")
            logger.info(f"   ❌ Failed: {enrichment_stats['departments_failed']}")
            logger.info(f"   📄 Total New Fields Added: {enrichment_stats['total_new_fields']}")
            logger.info(f"   🏷️  Total Attributes Added: {enrichment_stats['total_attributes_added']}")
            logger.info(f"   Errors: {len(enrichment_stats['errors'])}")

            return enrichment_stats

        except Exception as e:
            error_msg = f"Progressive enrichment failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            self.complete_enrichment_run(
                run_id=run_id,
                departments_processed=enrichment_stats['departments_processed'],
                departments_enriched=enrichment_stats['departments_enriched'],
                departments_failed=enrichment_stats['departments_failed'],
                error_message=error_msg
            )

            raise

    def get_enrichment_status_summary(self) -> pd.DataFrame:
        """
        Get a summary of enrichment status across all department records.

        Returns:
            DataFrame with counts of different enrichment states
        """
        try:
            query = """
            SELECT
                raw_data->>'_ingestion_method' as ingestion_method,
                ingestion_metadata->>'full_data' as full_data_flag,
                COUNT(*) as record_count,
                MIN(ingested_at) as earliest_record,
                MAX(ingested_at) as latest_record
            FROM bronze.raw_entities
            WHERE entity_type = 'department'
            AND source_system = 'tdx'
            GROUP BY raw_data->>'_ingestion_method', ingestion_metadata->>'full_data'
            ORDER BY ingestion_method, full_data_flag
            """

            status_df = self.db_adapter.query_to_dataframe(query)

            logger.info("Current enrichment status summary:")
            for _, row in status_df.iterrows():
                logger.info(f"  {row['ingestion_method']} (full_data={row['full_data_flag']}): "
                           f"{row['record_count']} records")

            return status_df

        except SQLAlchemyError as e:
            logger.error(f"Failed to get enrichment status summary: {e}")
            raise

    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()
        logger.info("Progressive bronze enrichment service closed")


def main():
    """
    Main function to run progressive bronze enrichment from command line.
    """
    try:
        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv('DATABASE_URL')
        tdx_base_url = os.getenv('TDX_BASE_URL')
        tdx_api_token = os.getenv('TDX_API_TOKEN')
        tdx_app_id = os.getenv('TDX_APP_ID')

        # Optional configuration
        max_departments = int(os.getenv('MAX_ENRICHMENT_DEPARTMENTS', '5000'))  # Process in batches
        api_delay = float(os.getenv('API_RATE_LIMIT_DELAY', '1.0'))  # Respect rate limits

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

        # Create and run progressive enrichment service
        enrichment_service = ProgressiveBronzeEnrichmentService(
            database_url=database_url,
            tdx_base_url=tdx_base_url,
            tdx_api_token=tdx_api_token,
            tdx_app_id=tdx_app_id,
            api_rate_limit_delay=api_delay
        )

        # Show current status before processing
        print("📊 Current enrichment status:")
        enrichment_service.get_enrichment_status_summary()

        # Run the progressive enrichment process
        print(f"\n🔄 Starting progressive enrichment (max {max_departments} departments)...")
        results = enrichment_service.run_progressive_enrichment(
            max_departments=max_departments,
            stop_on_errors=False  # Continue processing even if some departments fail
        )

        # Display comprehensive summary
        print(f"\n📊 Progressive Enrichment Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Departments Processed: {results['departments_processed']}")
        print(f"   ✅ Successfully Enriched: {results['departments_enriched']}")
        print(f"   ❌ Failed Enrichments: {results['departments_failed']}")
        print(f"   📄 Total New Fields Added: {results['total_new_fields']}")
        print(f"   🏷️  Total Attributes Added: {results['total_attributes_added']}")
        print(f"   Errors: {len(results['errors'])}")

        # Show updated status after processing
        print(f"\n📊 Updated enrichment status:")
        enrichment_service.get_enrichment_status_summary()

        # Clean up
        enrichment_service.close()

        print("✅ Progressive bronze enrichment completed successfully!")

    except Exception as e:
        logger.error(f"Progressive enrichment failed: {e}", exc_info=True)
        print(f"❌ Enrichment failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
