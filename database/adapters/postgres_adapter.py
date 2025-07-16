"""
PostgreSQL adapter for LSATS Database.

This adapter provides a clean interface to PostgreSQL operations,
following the LSATS adapter pattern established in the project.
"""

import os
import logging
import pandas as pd
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool
import psycopg2
from psycopg2.extras import RealDictCursor
import json

logger = logging.getLogger(__name__)


class PostgresAdapter:
    """
    PostgreSQL adapter for LSATS Database operations.

    This adapter handles all direct database interactions and provides
    methods for the Bronze-Silver-Gold data pipeline operations.
    """

    def __init__(self, database_url: str, pool_size: int = 5, max_overflow: int = 10):
        """
        Initialize PostgreSQL connection with connection pooling.

        Args:
            database_url (str): PostgreSQL connection string
            pool_size (int): Number of connections to maintain in the pool
            max_overflow (int): Maximum overflow connections beyond pool_size
        """
        self.database_url = database_url
        self.engine = self._create_engine(database_url, pool_size, max_overflow)

        # Test the connection immediately to catch configuration errors early
        self._test_connection()

        logger.info(f"PostgreSQL adapter initialized with pool size {pool_size}")

    def _create_engine(self, database_url: str, pool_size: int, max_overflow: int) -> Engine:
        """
        Create SQLAlchemy engine with appropriate settings for LSATS workload.

        The connection pool helps manage database connections efficiently,
        especially important when running multiple ingestion processes.
        """
        return create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,  # Validates connections before use
            pool_recycle=3600,   # Recycle connections every hour
            echo=os.getenv('ENABLE_SQL_LOGGING', 'false').lower() == 'true'
        )

    def _test_connection(self) -> None:
        """
        Test database connection and verify schema structure.

        This is called during initialization to catch configuration
        problems early rather than waiting for the first operation.
        """
        try:
            with self.engine.connect() as conn:
                # Test basic connectivity
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"Connected to PostgreSQL: {version}")

                # Verify our schemas exist
                schema_check = text("""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name IN ('bronze', 'silver', 'gold', 'meta')
                    ORDER BY schema_name
                """)
                schemas = [row[0] for row in conn.execute(schema_check)]

                expected_schemas = ['bronze', 'gold', 'meta', 'silver']
                if schemas == expected_schemas:
                    logger.info("All required schemas found: bronze, silver, gold, meta")
                else:
                    missing = set(expected_schemas) - set(schemas)
                    logger.warning(f"Missing schemas: {missing}")

        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            raise ConnectionError(f"Cannot connect to PostgreSQL: {e}")

    # =========================================================================
    # BRONZE LAYER OPERATIONS (Raw Data Storage)
    # =========================================================================

    def insert_raw_entity(self, entity_type: str, source_system: str,
                         external_id: str, raw_data: Dict[str, Any],
                         ingestion_run_id: Optional[str] = None) -> str:
        """
        Insert raw entity data into the bronze layer.

        This is the entry point for all data into the LSATS Database.
        Data is stored exactly as received from the source system.

        Args:
            entity_type (str): Type of entity ('department', 'user', 'lab', etc.)
            source_system (str): Source system identifier ('tdx', 'lab_csv', etc.)
            external_id (str): Identifier from the source system
            raw_data (Dict): Complete raw data from source
            ingestion_run_id (str, optional): UUID of the ingestion run

        Returns:
            str: UUID of the created raw entity record
        """
        try:
            with self.engine.connect() as conn:
                # Use PostgreSQL's RETURNING clause to get the generated UUID
                insert_query = text("""
                    INSERT INTO bronze.raw_entities
                    (entity_type, source_system, external_id, raw_data, ingestion_run_id)
                    VALUES (:entity_type, :source_system, :external_id, :raw_data, :ingestion_run_id)
                    RETURNING raw_id
                """)

                result = conn.execute(insert_query, {
                    'entity_type': entity_type,
                    'source_system': source_system,
                    'external_id': external_id,
                    'raw_data': json.dumps(raw_data),  # Convert dict to JSON string
                    'ingestion_run_id': ingestion_run_id
                })

                raw_id = result.fetchone()[0]
                conn.commit()

                logger.debug(f"Inserted raw {entity_type} from {source_system}: {external_id}")
                return str(raw_id)

        except SQLAlchemyError as e:
            logger.error(f"Failed to insert raw entity: {e}")
            raise

    def bulk_insert_raw_entities(self, entities: List[Dict[str, Any]],
                                batch_size: int = 1000) -> int:
        """
        Efficiently insert multiple raw entities in batches.

        This method is optimized for ingesting large datasets like
        all departments from TeamDynamix or thousands of users from CSV files.

        Args:
            entities (List[Dict]): List of entity dictionaries with required fields
            batch_size (int): Number of records to insert in each batch

        Returns:
            int: Number of entities successfully inserted
        """
        total_inserted = 0

        try:
            with self.engine.connect() as conn:
                # Process entities in batches for better memory management
                for i in range(0, len(entities), batch_size):
                    batch = entities[i:i + batch_size]

                    # Prepare batch data for insertion
                    batch_data = []
                    for entity in batch:
                        batch_data.append({
                            'entity_type': entity['entity_type'],
                            'source_system': entity['source_system'],
                            'external_id': entity['external_id'],
                            'raw_data': json.dumps(entity['raw_data']),
                            'ingestion_run_id': entity.get('ingestion_run_id')
                        })

                    # Execute batch insert
                    insert_query = text("""
                        INSERT INTO bronze.raw_entities
                        (entity_type, source_system, external_id, raw_data, ingestion_run_id)
                        VALUES (:entity_type, :source_system, :external_id, :raw_data, :ingestion_run_id)
                    """)

                    conn.execute(insert_query, batch_data)
                    total_inserted += len(batch)

                    logger.debug(f"Inserted batch of {len(batch)} entities (total: {total_inserted})")

                conn.commit()
                logger.info(f"Successfully inserted {total_inserted} raw entities")

        except SQLAlchemyError as e:
            logger.error(f"Bulk insert failed: {e}")
            raise

        return total_inserted

    # =========================================================================
    # SILVER LAYER OPERATIONS (Cleaned Data)
    # =========================================================================

    def upsert_silver_departments(self, df_departments: pd.DataFrame,
                                 ingestion_run_id: Optional[str] = None) -> int:
        """
        Insert or update cleaned department data in the silver layer.

        This method uses pandas DataFrame for efficient bulk operations.
        It's designed to work with the output of your pandas cleaning pipeline.

        Args:
            df_departments (pd.DataFrame): Cleaned department data
            ingestion_run_id (str, optional): UUID of the ingestion run

        Returns:
            int: Number of departments processed
        """
        if df_departments.empty:
            logger.warning("No departments to upsert")
            return 0

        try:
            # Add ingestion run ID to all records if provided
            if ingestion_run_id:
                df_departments = df_departments.copy()
                df_departments['ingestion_run_id'] = ingestion_run_id

            # Use pandas to_sql for efficient bulk upsert
            # PostgreSQL ON CONFLICT handles updates for existing records
            df_departments.to_sql(
                name='departments',
                schema='silver',
                con=self.engine,
                if_exists='append',
                index=False,
                method='multi'  # Use multi-row insert for better performance
            )

            logger.info(f"Upserted {len(df_departments)} silver departments")
            return len(df_departments)

        except SQLAlchemyError as e:
            logger.error(f"Failed to upsert silver departments: {e}")
            raise

    # =========================================================================
    # GOLD LAYER OPERATIONS (Master Records)
    # =========================================================================

    def create_department_master(self, canonical_name: str, canonical_code: str,
                               primary_source: str, confidence_score: float,
                               **kwargs) -> str:
        """
        Create a new master department record in the gold layer.

        Master records represent the authoritative truth about entities,
        potentially combining information from multiple sources.

        Args:
            canonical_name (str): Authoritative department name
            canonical_code (str): Authoritative department code
            primary_source (str): Most trusted source system
            confidence_score (float): Confidence in this master record (0.0-1.0)
            **kwargs: Additional fields for the master record

        Returns:
            str: UUID of the created master record
        """
        try:
            with self.engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO gold.department_masters
                    (canonical_name, canonical_code, primary_source, confidence_score,
                     canonical_description, is_active, region)
                    VALUES (:canonical_name, :canonical_code, :primary_source, :confidence_score,
                            :canonical_description, :is_active, :region)
                    RETURNING master_id
                """)

                result = conn.execute(insert_query, {
                    'canonical_name': canonical_name,
                    'canonical_code': canonical_code,
                    'primary_source': primary_source,
                    'confidence_score': confidence_score,
                    'canonical_description': kwargs.get('description'),
                    'is_active': kwargs.get('is_active', True),
                    'region': kwargs.get('region')
                })

                master_id = result.fetchone()[0]
                conn.commit()

                logger.info(f"Created department master: {canonical_name} ({master_id})")
                return str(master_id)

        except SQLAlchemyError as e:
            logger.error(f"Failed to create department master: {e}")
            raise

    # =========================================================================
    # QUERY OPERATIONS (Reading Data)
    # =========================================================================

    def query_to_dataframe(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a pandas DataFrame.

        This method is perfect for analytical queries and integrates
        seamlessly with your existing pandas-based workflows.

        Args:
            query (str): SQL query to execute
            params (Dict, optional): Query parameters for safe parameter binding

        Returns:
            pd.DataFrame: Query results
        """
        try:
            df = pd.read_sql_query(
                sql=text(query),
                con=self.engine,
                params=params or {}
            )
            logger.debug(f"Query returned {len(df)} rows")
            return df

        except SQLAlchemyError as e:
            logger.error(f"Query failed: {e}")
            raise

    def get_latest_ingestion_run(self, source_system: str, entity_type: str) -> Optional[Dict]:
        """
        Get information about the most recent ingestion run.

        This is useful for monitoring data freshness and debugging
        ingestion issues.
        """
        try:
            query = """
                SELECT run_id, started_at, completed_at, status, records_processed
                FROM meta.ingestion_runs
                WHERE source_system = :source_system AND entity_type = :entity_type
                ORDER BY started_at DESC
                LIMIT 1
            """

            df = self.query_to_dataframe(query, {
                'source_system': source_system,
                'entity_type': entity_type
            })

            if not df.empty:
                return df.iloc[0].to_dict()
            return None

        except SQLAlchemyError as e:
            logger.error(f"Failed to get latest ingestion run: {e}")
            return None

    def close(self) -> None:
        """
        Close the database connection pool.

        This should be called when shutting down the application
        to ensure clean resource cleanup.
        """
        if self.engine:
            self.engine.dispose()
            logger.info("PostgreSQL adapter closed")


# Convenience function for easy initialization from environment
def create_postgres_adapter() -> PostgresAdapter:
    """
    Create PostgreSQL adapter using environment configuration.

    This function reads your .env file settings and creates a properly
    configured adapter instance.
    """
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    pool_size = int(os.getenv('DB_POOL_SIZE', '5'))
    max_overflow = int(os.getenv('DB_MAX_OVERFLOW', '10'))

    return PostgresAdapter(
        database_url=database_url,
        pool_size=pool_size,
        max_overflow=max_overflow
    )
