#!/usr/bin/env python3
"""
Async User Ingestion Service for TeamDynamix

This service implements async user ingestion with:
1. Batched API calls to search_users with department ID chunks
2. Concurrent processing of API responses
3. Async ingestion of individual user records
4. Proper rate limiting and error handling

The approach optimizes for maximum throughput while respecting API constraints.
"""

import os
import sys
import logging
import uuid
import asyncio
import time
from typing import Dict, List, Any, Optional, Set, Tuple
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import json

# Core Python imports for PostgreSQL operations
import pandas as pd
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from sqlalchemy.pool import QueuePool
import psycopg2
from psycopg2.extras import RealDictCursor

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
        logging.FileHandler('logs/async_user_ingestion.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class AsyncUserIngestionService:
    """
    Async service for ingesting TeamDynamix users with batched API calls and concurrent processing.

    This service:
    1. Queries bronze layer for all department IDs
    2. Batches department IDs into chunks of 200 (API limitation)
    3. Makes concurrent API calls to search_user for each batch
    4. Processes user records concurrently
    5. Ingests individual user records asynchronously
    """

    def __init__(self, database_url: str, tdx_base_url: str, tdx_api_token: str, tdx_app_id: str,
                 max_concurrent_batches: int = 5, max_concurrent_ingestions: int = 20,
                 api_rate_limit_delay: float = 1.0, batch_size: int = 200):
        """
        Initialize the async user ingestion service.

        Args:
            database_url: PostgreSQL connection string
            tdx_base_url: TeamDynamix API base URL
            tdx_api_token: TeamDynamix API authentication token
            tdx_app_id: TeamDynamix application ID
            max_concurrent_batches: Maximum number of API batches to process concurrently
            max_concurrent_ingestions: Maximum number of user records to ingest concurrently
            api_rate_limit_delay: Delay between API calls (seconds)
            batch_size: Number of department IDs per API call (max 200)
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url,
            pool_size=10,  # Higher pool size for concurrent operations
            max_overflow=20
        )

        self.tdx_facade = TeamDynamixFacade(
            base_url=tdx_base_url,
            app_id=tdx_app_id,
            api_token=tdx_api_token
        )

        # Async processing configuration
        self.max_concurrent_batches = max_concurrent_batches
        self.max_concurrent_ingestions = max_concurrent_ingestions
        self.api_rate_limit_delay = api_rate_limit_delay
        self.batch_size = min(batch_size, 200)  # Enforce API limit

        # Create semaphores for controlling concurrency
        self.batch_semaphore = asyncio.Semaphore(max_concurrent_batches)
        self.ingestion_semaphore = asyncio.Semaphore(max_concurrent_ingestions)

        # Thread pool for running synchronous operations in async context
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_batches + max_concurrent_ingestions)

        logger.info(f"Async user ingestion service initialized:")
        logger.info(f"  Max concurrent batches: {max_concurrent_batches}")
        logger.info(f"  Max concurrent ingestions: {max_concurrent_ingestions}")
        logger.info(f"  Batch size: {batch_size} departments")
        logger.info(f"  API rate limit delay: {api_rate_limit_delay}s")

    def get_department_ids_from_bronze(self) -> List[int]:
        """
        Query the bronze layer to get all department IDs for user searching.

        Returns:
            List of department IDs as integers
        """
        try:
            # Query bronze layer for all ingested departments
            query = """
            SELECT DISTINCT
                CAST(raw_data->>'ID' AS INTEGER) as department_id,
                raw_data->>'Name' as department_name,
                raw_data->>'IsActive' as is_active
            FROM bronze.raw_entities
            WHERE entity_type = 'department'
            AND source_system = 'tdx'
            AND raw_data->>'ID' IS NOT NULL
            AND raw_data->>'IsActive' = 'true'
            ORDER BY department_id
            """

            results_df = self.db_adapter.query_to_dataframe(query)

            if results_df.empty:
                logger.warning("No departments found in bronze layer - you may need to ingest departments first")
                return []

            department_ids = results_df['department_id'].tolist()

            logger.info(f"Retrieved {len(department_ids)} active department IDs from bronze layer")
            logger.info(f"Department ID range: {min(department_ids)} to {max(department_ids)}")

            # Log sample departments for visibility
            sample_depts = results_df.head(5)
            logger.info("Sample departments:")
            for _, dept in sample_depts.iterrows():
                logger.info(f"  - {dept['department_name']} (ID: {dept['department_id']})")

            return department_ids

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve department IDs from bronze layer: {e}")
            raise

    def create_department_batches(self, department_ids: List[int]) -> List[List[int]]:
        """
        Split department IDs into batches for API calls.

        Args:
            department_ids: List of all department IDs

        Returns:
            List of department ID batches, each containing max 200 IDs
        """
        batches = []

        for i in range(0, len(department_ids), self.batch_size):
            batch = department_ids[i:i + self.batch_size]
            batches.append(batch)

        logger.info(f"Created {len(batches)} department batches (max {self.batch_size} departments each)")

        # Log batch size distribution
        batch_sizes = [len(batch) for batch in batches]
        logger.info(f"Batch sizes: min={min(batch_sizes)}, max={max(batch_sizes)}, avg={sum(batch_sizes)/len(batch_sizes):.1f}")

        return batches

    async def fetch_users_for_department_batch(self, batch_index: int, department_ids: List[int],
                                             loop: asyncio.AbstractEventLoop) -> Dict[str, Any]:
        """
        Fetch users for a specific batch of department IDs using async execution.

        Args:
            batch_index: Index of this batch for tracking
            department_ids: List of department IDs for this batch
            loop: Event loop for async execution

        Returns:
            Dictionary with batch results and metadata
        """
        async with self.batch_semaphore:  # Limit concurrent API calls
            batch_result = {
                'batch_index': batch_index,
                'department_ids': department_ids,
                'department_count': len(department_ids),
                'users_found': [],
                'success': False,
                'error_message': None,
                'api_call_duration': 0,
                'started_at': datetime.now(timezone.utc)
            }

            try:
                logger.info(f"Batch {batch_index}: Fetching users for {len(department_ids)} departments...")

                # Prepare search criteria for TeamDynamix API
                search_data = {
                    "AccountIDs": department_ids,
                    "IsActive": True  # Only get active users
                }

                # Execute the synchronous API call in a thread pool
                start_time = time.time()

                def make_api_call():
                    time.sleep(self.api_rate_limit_delay)  # Rate limiting
                    return self.tdx_facade.users.search_user(search_data)

                users_data = await loop.run_in_executor(self.executor, make_api_call)

                batch_result['api_call_duration'] = time.time() - start_time

                if users_data is None:
                    users_data = []
                elif not isinstance(users_data, list):
                    users_data = [users_data]  # Convert single result to list

                batch_result['users_found'] = users_data
                batch_result['user_count'] = len(users_data)
                batch_result['success'] = True

                logger.info(f"Batch {batch_index}: Found {len(users_data)} users "
                           f"(API call took {batch_result['api_call_duration']:.2f}s)")

                return batch_result

            except Exception as e:
                error_msg = f"Batch {batch_index} API call failed: {str(e)}"
                logger.error(error_msg)

                batch_result['error_message'] = error_msg
                batch_result['completed_at'] = datetime.now(timezone.utc)

                return batch_result

    async def ingest_user_record(self, user_data: Dict[str, Any], batch_index: int,
                                ingestion_run_id: str, loop: asyncio.AbstractEventLoop) -> Dict[str, Any]:
        """
        Ingest a single user record into the bronze layer asynchronously.

        Args:
            user_data: User data from TeamDynamix API
            batch_index: Which batch this user came from
            ingestion_run_id: UUID of the current ingestion run
            loop: Event loop for async execution

        Returns:
            Dictionary with ingestion result
        """
        async with self.ingestion_semaphore:  # Limit concurrent ingestions
            ingestion_result = {
                'user_uid': user_data.get('UID', 'unknown'),
                'user_name': user_data.get('FullName', 'Unknown User'),
                'batch_index': batch_index,
                'success': False,
                'error_message': None,
                'started_at': datetime.now(timezone.utc)
            }

            try:
                # Prepare enhanced user data with ingestion metadata
                enhanced_user_data = user_data.copy()
                enhanced_user_data['_ingestion_method'] = 'async_batch_search'
                enhanced_user_data['_batch_index'] = batch_index
                enhanced_user_data['_ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()

                # Use UID as external_id for user records
                external_id = user_data.get('UID', str(uuid.uuid4()))

                # Execute the synchronous database insert in a thread pool
                def perform_ingestion():
                    return self.db_adapter.insert_raw_entity(
                        entity_type='user',
                        source_system='tdx',
                        external_id=external_id,
                        raw_data=enhanced_user_data,
                        ingestion_run_id=ingestion_run_id
                    )

                raw_id = await loop.run_in_executor(self.executor, perform_ingestion)

                ingestion_result['raw_id'] = raw_id
                ingestion_result['success'] = True

                logger.debug(f"Ingested user: {ingestion_result['user_name']} (UID: {ingestion_result['user_uid']})")

                return ingestion_result

            except Exception as e:
                error_msg = f"Failed to ingest user {ingestion_result['user_uid']}: {str(e)}"
                logger.error(error_msg)

                ingestion_result['error_message'] = error_msg
                return ingestion_result

    async def process_batch_users_concurrently(self, batch_result: Dict[str, Any],
                                             ingestion_run_id: str,
                                             loop: asyncio.AbstractEventLoop) -> List[Dict[str, Any]]:
        """
        Process all users from a batch concurrently.

        Args:
            batch_result: Result from fetch_users_for_department_batch
            ingestion_run_id: UUID of the current ingestion run
            loop: Event loop for async execution

        Returns:
            List of ingestion results for all users in the batch
        """
        if not batch_result['success'] or not batch_result['users_found']:
            logger.warning(f"Batch {batch_result['batch_index']}: No users to process")
            return []

        users = batch_result['users_found']
        batch_index = batch_result['batch_index']

        logger.info(f"Batch {batch_index}: Starting concurrent ingestion of {len(users)} users...")

        # Create ingestion tasks for all users in this batch
        ingestion_tasks = [
            self.ingest_user_record(user_data, batch_index, ingestion_run_id, loop)
            for user_data in users
        ]

        # Execute all ingestion tasks concurrently
        ingestion_results = await asyncio.gather(*ingestion_tasks, return_exceptions=True)

        # Handle any exceptions that occurred during ingestion
        processed_results = []
        for result in ingestion_results:
            if isinstance(result, Exception):
                error_result = {
                    'user_uid': 'unknown',
                    'user_name': 'Unknown User',
                    'batch_index': batch_index,
                    'success': False,
                    'error_message': f"Async ingestion exception: {str(result)}"
                }
                processed_results.append(error_result)
                logger.error(f"Async ingestion exception in batch {batch_index}: {result}")
            else:
                processed_results.append(result)

        successful_ingestions = sum(1 for r in processed_results if r['success'])

        logger.info(f"Batch {batch_index}: Completed ingestion - "
                   f"{successful_ingestions}/{len(processed_results)} users ingested successfully")

        return processed_results

    def create_ingestion_run(self, total_departments: int, total_batches: int) -> str:
        """Create an ingestion run record for the async user ingestion."""
        try:
            run_id = str(uuid.uuid4())

            metadata = {
                'ingestion_type': 'async_user_batch_ingestion',
                'total_departments': total_departments,
                'total_batches': total_batches,
                'batch_size': self.batch_size,
                'max_concurrent_batches': self.max_concurrent_batches,
                'max_concurrent_ingestions': self.max_concurrent_ingestions,
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
                    'entity_type': 'user',
                    'started_at': datetime.now(timezone.utc),
                    'metadata': json.dumps(metadata)
                })

                conn.commit()

            logger.info(f"Created async ingestion run {run_id} for {total_departments} departments in {total_batches} batches")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create ingestion run: {e}")
            raise

    def complete_ingestion_run(self, run_id: str, total_users_processed: int,
                             total_users_ingested: int, total_batches_processed: int,
                             error_message: Optional[str] = None):
        """Mark the async ingestion run as completed with detailed statistics."""
        try:
            status = 'failed' if error_message else 'completed'

            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = :completed_at,
                        status = :status,
                        records_processed = :records_processed,
                        records_created = :records_created,
                        records_updated = :batches_processed,
                        error_message = :error_message
                    WHERE run_id = :run_id
                """)

                conn.execute(update_query, {
                    'run_id': run_id,
                    'completed_at': datetime.now(timezone.utc),
                    'status': status,
                    'records_processed': total_users_processed,
                    'records_created': total_users_ingested,
                    'batches_processed': total_batches_processed,
                    'error_message': error_message
                })

                conn.commit()

            logger.info(f"Completed async ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete ingestion run: {e}")

    async def run_async_user_ingestion(self) -> Dict[str, Any]:
        """
        Run the complete async user ingestion process.

        This method orchestrates:
        1. Querying department IDs from bronze layer
        2. Creating department batches for API calls
        3. Concurrent API calls to fetch users by department batch
        4. Concurrent ingestion of user records
        5. Progress tracking and error handling

        Returns:
            Dictionary with comprehensive ingestion statistics
        """
        ingestion_stats = {
            'started_at': datetime.now(timezone.utc),
            'total_departments': 0,
            'total_batches': 0,
            'batches_processed': 0,
            'batches_successful': 0,
            'batches_failed': 0,
            'total_users_found': 0,
            'total_users_ingested': 0,
            'total_users_failed': 0,
            'api_call_duration_total': 0,
            'ingestion_duration_total': 0,
            'errors': []
        }

        try:
            logger.info("🚀 Starting async user ingestion process...")

            # Step 1: Get department IDs from bronze layer
            department_ids = self.get_department_ids_from_bronze()

            if not department_ids:
                logger.warning("No department IDs found - cannot proceed with user ingestion")
                return ingestion_stats

            ingestion_stats['total_departments'] = len(department_ids)

            # Step 2: Create department batches
            department_batches = self.create_department_batches(department_ids)
            ingestion_stats['total_batches'] = len(department_batches)

            # Step 3: Create ingestion run for tracking
            run_id = self.create_ingestion_run(len(department_ids), len(department_batches))
            ingestion_stats['run_id'] = run_id

            # Step 4: Execute concurrent API calls for all batches
            logger.info(f"⚡ Starting {len(department_batches)} concurrent API batches...")

            loop = asyncio.get_event_loop()

            # Create API call tasks for all batches
            api_tasks = [
                self.fetch_users_for_department_batch(batch_index, batch_ids, loop)
                for batch_index, batch_ids in enumerate(department_batches)
            ]

            # Execute all API calls concurrently
            batch_results = await asyncio.gather(*api_tasks, return_exceptions=True)

            # Process batch results and handle exceptions
            processed_batch_results = []
            for result in batch_results:
                if isinstance(result, Exception):
                    error_msg = f"Async API call exception: {str(result)}"
                    logger.error(error_msg)
                    ingestion_stats['errors'].append(error_msg)
                    ingestion_stats['batches_failed'] += 1
                else:
                    processed_batch_results.append(result)

                    if result['success']:
                        ingestion_stats['batches_successful'] += 1
                        ingestion_stats['total_users_found'] += result.get('user_count', 0)
                        ingestion_stats['api_call_duration_total'] += result.get('api_call_duration', 0)
                    else:
                        ingestion_stats['batches_failed'] += 1
                        if result.get('error_message'):
                            ingestion_stats['errors'].append(result['error_message'])

                ingestion_stats['batches_processed'] += 1

            logger.info(f"📊 API Phase Complete: {ingestion_stats['batches_successful']}/{ingestion_stats['total_batches']} batches successful, "
                       f"{ingestion_stats['total_users_found']} total users found")

            # Step 5: Process all users concurrently across all successful batches
            if processed_batch_results:
                logger.info(f"🔄 Starting concurrent user ingestion for {ingestion_stats['total_users_found']} users...")

                # Create user processing tasks for all successful batches
                user_processing_tasks = [
                    self.process_batch_users_concurrently(batch_result, run_id, loop)
                    for batch_result in processed_batch_results
                    if batch_result['success']
                ]

                # Execute all user processing tasks concurrently
                if user_processing_tasks:
                    all_ingestion_results = await asyncio.gather(*user_processing_tasks, return_exceptions=True)

                    # Flatten and count ingestion results
                    for batch_ingestion_results in all_ingestion_results:
                        if isinstance(batch_ingestion_results, Exception):
                            error_msg = f"Async user processing exception: {str(batch_ingestion_results)}"
                            logger.error(error_msg)
                            ingestion_stats['errors'].append(error_msg)
                        else:
                            for user_result in batch_ingestion_results:
                                if user_result['success']:
                                    ingestion_stats['total_users_ingested'] += 1
                                else:
                                    ingestion_stats['total_users_failed'] += 1
                                    if user_result.get('error_message'):
                                        ingestion_stats['errors'].append(user_result['error_message'])

            # Step 6: Complete the ingestion run
            error_summary = None
            if ingestion_stats['errors']:
                error_summary = f"{len(ingestion_stats['errors'])} errors occurred during async ingestion"

            self.complete_ingestion_run(
                run_id=run_id,
                total_users_processed=ingestion_stats['total_users_found'],
                total_users_ingested=ingestion_stats['total_users_ingested'],
                total_batches_processed=ingestion_stats['batches_processed'],
                error_message=error_summary
            )

            ingestion_stats['completed_at'] = datetime.now(timezone.utc)
            total_duration = (ingestion_stats['completed_at'] - ingestion_stats['started_at']).total_seconds()

            # Calculate performance metrics
            avg_api_call_time = (ingestion_stats['api_call_duration_total'] /
                               ingestion_stats['batches_successful'] if ingestion_stats['batches_successful'] > 0 else 0)

            users_per_second = (ingestion_stats['total_users_ingested'] /
                              total_duration if total_duration > 0 else 0)

            # Log comprehensive results
            logger.info(f"✅ Async user ingestion completed in {total_duration:.2f} seconds")
            logger.info(f"📊 Final Results Summary:")
            logger.info(f"   Total Departments: {ingestion_stats['total_departments']}")
            logger.info(f"   Total Batches: {ingestion_stats['total_batches']}")
            logger.info(f"   ✅ Successful Batches: {ingestion_stats['batches_successful']}")
            logger.info(f"   ❌ Failed Batches: {ingestion_stats['batches_failed']}")
            logger.info(f"   👥 Users Found: {ingestion_stats['total_users_found']}")
            logger.info(f"   ✅ Users Ingested: {ingestion_stats['total_users_ingested']}")
            logger.info(f"   ❌ Users Failed: {ingestion_stats['total_users_failed']}")
            logger.info(f"   ⚡ Performance: {users_per_second:.1f} users/second")
            logger.info(f"   🕐 Avg API Call Time: {avg_api_call_time:.2f}s")
            logger.info(f"   ❗ Total Errors: {len(ingestion_stats['errors'])}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"Async user ingestion failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            if 'run_id' in ingestion_stats:
                self.complete_ingestion_run(
                    run_id=ingestion_stats['run_id'],
                    total_users_processed=ingestion_stats['total_users_found'],
                    total_users_ingested=ingestion_stats['total_users_ingested'],
                    total_batches_processed=ingestion_stats['batches_processed'],
                    error_message=error_msg
                )

            ingestion_stats['errors'].append(error_msg)
            raise

    def close(self):
        """Clean up database connections and thread pool."""
        if self.db_adapter:
            self.db_adapter.close()
        if self.executor:
            self.executor.shutdown(wait=True)
        logger.info("Async user ingestion service closed")


async def main():
    """
    Main async function to run user ingestion from command line.
    """
    try:
        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv('DATABASE_URL')
        tdx_base_url = os.getenv('TDX_BASE_URL')
        tdx_api_token = os.getenv('TDX_API_TOKEN')
        tdx_app_id = os.getenv('TDX_APP_ID')

        # Optional async configuration
        max_concurrent_batches = int(os.getenv('MAX_CONCURRENT_BATCHES', '5'))
        max_concurrent_ingestions = int(os.getenv('MAX_CONCURRENT_INGESTIONS', '20'))
        api_delay = float(os.getenv('API_RATE_LIMIT_DELAY', '1.0'))
        batch_size = int(os.getenv('DEPARTMENT_BATCH_SIZE', '200'))

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

        # Create and run async user ingestion service
        ingestion_service = AsyncUserIngestionService(
            database_url=database_url,
            tdx_base_url=tdx_base_url,
            tdx_api_token=tdx_api_token,
            tdx_app_id=tdx_app_id,
            max_concurrent_batches=max_concurrent_batches,
            max_concurrent_ingestions=max_concurrent_ingestions,
            api_rate_limit_delay=api_delay,
            batch_size=batch_size
        )

        # Run the async ingestion process
        print("🚀 Starting async user ingestion with concurrent processing...")
        results = await ingestion_service.run_async_user_ingestion()

        # Display comprehensive summary
        total_duration = (results['completed_at'] - results['started_at']).total_seconds()
        users_per_second = results['total_users_ingested'] / total_duration if total_duration > 0 else 0

        print(f"\n📊 Async User Ingestion Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Total Duration: {total_duration:.2f} seconds")
        print(f"   Departments Processed: {results['total_departments']}")
        print(f"   API Batches: {results['batches_successful']}/{results['total_batches']} successful")
        print(f"   👥 Users Found: {results['total_users_found']}")
        print(f"   ✅ Users Ingested: {results['total_users_ingested']}")
        print(f"   ❌ Users Failed: {results['total_users_failed']}")
        print(f"   ⚡ Performance: {users_per_second:.1f} users/second")
        print(f"   ❗ Total Errors: {len(results['errors'])}")

        # Show sample errors if any occurred
        if results['errors']:
            print(f"\n❗ Sample Errors (first 3):")
            for error in results['errors'][:3]:
                print(f"   - {error}")

        # Clean up
        ingestion_service.close()

        print("✅ Async user ingestion completed successfully!")

    except Exception as e:
        logger.error(f"Async user ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    # Run the async main function
    asyncio.run(main())
