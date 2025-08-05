#!/usr/bin/env python3
"""
Complete department ingestion script for LSATS Data Hub
Demonstrates Bronze layer ingestion with full PostgreSQL imports and error handling
"""

import os
import sys
import logging
import uuid
from typing import Dict, List, Any, Optional
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

# Set up logging to track the ingestion process
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/department_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DepartmentIngestionService:
    """
    Service class for ingesting department data from TeamDynamix into PostgreSQL
    following the Bronze-Silver-Gold architecture pattern.
    """

    def __init__(self, database_url: str, tdx_base_url: str, tdx_api_token: str, tdx_app_id: str):
        """
        Initialize the ingestion service with database and TeamDynamix connections.

        Args:
            database_url: PostgreSQL connection string
            tdx_base_url: TeamDynamix API base URL
            tdx_api_token: TeamDynamix API authentication token
            tdx_app_id: TeamDynamix application ID
        """
        # Initialize database adapter with connection pooling
        self.db_adapter = PostgresAdapter(
            database_url=database_url,
            pool_size=5,  # Moderate pool size for ingestion workload
            max_overflow=10  # Allow burst capacity during heavy operations
        )

        # Initialize TeamDynamix facade using existing LSATS architecture
        self.tdx_facade = TeamDynamixFacade(
            base_url=tdx_base_url,
            app_id=tdx_app_id,
            api_token=tdx_api_token
        )

        logger.info("Department ingestion service initialized successfully")

    def create_ingestion_run(self, source_system: str, entity_type: str) -> str:
        """
        Create a new ingestion run record for tracking purposes.
        This helps with monitoring and debugging ingestion processes.

        Args:
            source_system: Identifier for the source (e.g., 'tdx')
            entity_type: Type of entity being ingested (e.g., 'department')

        Returns:
            UUID of the created ingestion run
        """
        try:
            run_id = str(uuid.uuid4())

            with self.db_adapter.engine.connect() as conn:
                # Insert new ingestion run record
                insert_query = text("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status
                    ) VALUES (
                        :run_id, :source_system, :entity_type, :started_at, 'running'
                    )
                """)

                conn.execute(insert_query, {
                    'run_id': run_id,
                    'source_system': source_system,
                    'entity_type': entity_type,
                    'started_at': datetime.now(timezone.utc)
                })

                # Commit the transaction
                conn.commit()

            logger.info(f"Created ingestion run {run_id} for {source_system}/{entity_type}")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create ingestion run: {e}")
            raise

    def complete_ingestion_run(self, run_id: str, records_processed: int,
                             records_created: int, error_message: Optional[str] = None):
        """
        Mark an ingestion run as completed and update statistics.

        Args:
            run_id: UUID of the ingestion run
            records_processed: Total number of records processed
            records_created: Number of new records created
            error_message: Error message if the run failed
        """
        try:
            status = 'failed' if error_message else 'completed'

            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = :completed_at,
                        status = :status,
                        records_processed = :records_processed,
                        records_created = :records_created,
                        error_message = :error_message
                    WHERE run_id = :run_id
                """)

                conn.execute(update_query, {
                    'run_id': run_id,
                    'completed_at': datetime.now(timezone.utc),
                    'status': status,
                    'records_processed': records_processed,
                    'records_created': records_created,
                    'error_message': error_message
                })

                conn.commit()

            logger.info(f"Completed ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete ingestion run: {e}")

    def ingest_departments_bronze_layer(self) -> Dict[str, Any]:
        """
        Ingest department data from TeamDynamix into the Bronze layer.
        Each department is stored as an individual raw entity record.

        Returns:
            Dictionary with ingestion statistics and run information
        """
        # Create ingestion run for tracking
        run_id = self.create_ingestion_run('tdx', 'department')

        ingestion_stats = {
            'run_id': run_id,
            'records_processed': 0,
            'records_created': 0,
            'errors': [],
            'started_at': datetime.now(timezone.utc)
        }

        try:
            logger.info("Starting TeamDynamix department ingestion...")

            # Use existing TeamDynamix facade to get raw account data
            # This leverages your existing API integration and authentication
            raw_accounts = self.tdx_facade.accounts.get_accounts()

            logger.info(f"Retrieved {len(raw_accounts)} departments from TeamDynamix")

            # Process each department as an individual bronze record
            for department_data in raw_accounts:
                try:
                    # Extract the TeamDynamix department ID
                    external_id = str(department_data.get('ID', 'unknown'))

                    # Insert raw entity using the postgres adapter
                    # This stores the complete JSON object exactly as received
                    entity_id = self.db_adapter.insert_raw_entity(
                        entity_type='department',
                        source_system='tdx',
                        external_id=external_id,
                        raw_data=department_data,  # Complete department JSON
                        ingestion_run_id=run_id
                    )

                    ingestion_stats['records_created'] += 1

                    # Log progress every 10 departments
                    if ingestion_stats['records_created'] % 10 == 0:
                        logger.info(f"Processed {ingestion_stats['records_created']} departments...")

                except Exception as record_error:
                    error_msg = f"Failed to process department {external_id}: {record_error}"
                    logger.error(error_msg)
                    ingestion_stats['errors'].append(error_msg)

                ingestion_stats['records_processed'] += 1

            # Complete the ingestion run successfully
            error_summary = None
            if ingestion_stats['errors']:
                error_summary = f"{len(ingestion_stats['errors'])} individual record errors occurred"

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats['records_processed'],
                records_created=ingestion_stats['records_created'],
                error_message=error_summary
            )

            ingestion_stats['completed_at'] = datetime.now(timezone.utc)
            duration = (ingestion_stats['completed_at'] - ingestion_stats['started_at']).total_seconds()

            logger.info(f"Department ingestion completed in {duration:.2f} seconds")
            logger.info(f"Processed: {ingestion_stats['records_processed']}, Created: {ingestion_stats['records_created']}")

            return ingestion_stats

        except Exception as e:
            # Handle any major failures during ingestion
            error_msg = f"Department ingestion failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats['records_processed'],
                records_created=ingestion_stats['records_created'],
                error_message=error_msg
            )

            raise

    def verify_ingestion_results(self, run_id: str) -> pd.DataFrame:
        """
        Verify the ingestion results by querying the bronze layer.
        This helps confirm that data was stored correctly.

        Args:
            run_id: UUID of the ingestion run to verify

        Returns:
            DataFrame with sample of ingested department data
        """
        try:
            # Query the bronze layer for our ingested departments
            verification_query = """
                SELECT
                    raw_id,
                    entity_type,
                    source_system,
                    external_id,
                    raw_data->>'Name' as department_name,
                    raw_data->>'ID' as tdx_id,
                    raw_data->>'IsActive' as is_active,
                    ingested_at,
                    ingestion_run_id
                FROM bronze.raw_entities
                WHERE ingestion_run_id = :run_id
                ORDER BY raw_data->>'Name'
                LIMIT 20
            """

            results_df = self.db_adapter.query_to_dataframe(
                verification_query,
                {'run_id': run_id}
            )

            logger.info(f"Verification query returned {len(results_df)} sample records")

            return results_df

        except SQLAlchemyError as e:
            logger.error(f"Verification query failed: {e}")
            raise

    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()
        logger.info("Department ingestion service closed")


def main():
    """
    Main function to run department ingestion from command line.
    Loads environment variables and executes the ingestion process.
    """
    try:
        # Load environment variables from .env file
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv('DATABASE_URL')
        tdx_base_url = os.getenv('TDX_BASE_URL')
        tdx_api_token = os.getenv('TDX_API_TOKEN')
        tdx_app_id = os.getenv('TDX_APP_ID')

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

        # Create and run ingestion service
        ingestion_service = DepartmentIngestionService(
            database_url=database_url,
            tdx_base_url=tdx_base_url,
            tdx_api_token=tdx_api_token,
            tdx_app_id=tdx_app_id
        )

        # Run the ingestion process
        print("🚀 Starting department ingestion from TeamDynamix...")
        results = ingestion_service.ingest_departments_bronze_layer()

        # Verify results
        print("✅ Verifying ingestion results...")
        sample_data = ingestion_service.verify_ingestion_results(results['run_id'])

        # Display summary
        print(f"\n📊 Ingestion Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Records Processed: {results['records_processed']}")
        print(f"   Records Created: {results['records_created']}")
        print(f"   Errors: {len(results['errors'])}")

        if not sample_data.empty:
            print(f"\n📋 Sample ingested departments:")
            for _, row in sample_data.head(5).iterrows():
                print(f"   - {row['department_name']} (TDX ID: {row['tdx_id']})")

        # Clean up
        ingestion_service.close()

        print("✅ Department ingestion completed successfully!")

    except Exception as e:
        logger.error(f"Department ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
