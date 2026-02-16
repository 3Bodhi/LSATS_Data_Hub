"""
Test script for LSATS Database connection and basic operations.

This script validates that the Docker PostgreSQL setup is working correctly
and that the PostgresAdapter can perform basic operations.
"""

import os
import sys
import logging
from typing import Dict, Any
from datetime import datetime

# Add the parent directory to Python path so we can import LSATS modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from dotenv import load_dotenv
from database.adapters.postgres_adapter import create_postgres_adapter, PostgresAdapter

# Set up logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_basic_connection() -> bool:
    """
    Test basic database connectivity and schema validation.

    Returns:
        bool: True if connection test passes
    """
    print("🔧 Testing basic database connection...")

    try:
        # Load environment variables from .env file
        load_dotenv()

        # Create adapter using environment configuration
        adapter = create_postgres_adapter()

        # Test a simple query to verify everything works
        result_df = adapter.query_to_dataframe("SELECT current_timestamp as test_time")
        test_time = result_df.iloc[0]['test_time']

        print(f"✅ Database connection successful! Server time: {test_time}")

        # Clean up
        adapter.close()
        return True

    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_bronze_layer_operations() -> bool:
    """
    Test Bronze layer raw data insertion operations.

    This simulates inserting data as it would come from TeamDynamix.
    """
    print("\n🥉 Testing Bronze layer operations...")

    try:
        adapter = create_postgres_adapter()

        # Create sample raw data like what might come from TeamDynamix
        sample_department = {
            'ID': 12345,
            'Name': 'LSA Technology Services',
            'Code': 'LSATS',
            'Description': 'Technology support for LSA',
            'IsActive': True,
            'CreatedDate': '2023-01-15T10:30:00Z',
            'custom_fields': {
                'region': 'Central Campus',
                'building': 'Modern Languages Building'
            }
        }

        # Insert raw entity into Bronze layer
        raw_id = adapter.insert_raw_entity(
            entity_type='department',
            source_system='tdx',
            external_id='12345',
            raw_data=sample_department
        )

        print(f"✅ Inserted raw department with ID: {raw_id}")

        # Test bulk insertion with multiple entities
        bulk_entities = []
        for i in range(3):
            bulk_entities.append({
                'entity_type': 'user',
                'source_system': 'test_csv',
                'external_id': f'test_user_{i}',
                'raw_data': {
                    'uniqname': f'testuser{i}',
                    'name': f'Test User {i}',
                    'email': f'testuser{i}@umich.edu',
                    'department': 'LSATS'
                }
            })

        inserted_count = adapter.bulk_insert_raw_entities(bulk_entities)
        print(f"✅ Bulk inserted {inserted_count} test users")

        adapter.close()
        return True

    except Exception as e:
        print(f"❌ Bronze layer test failed: {e}")
        return False

def test_query_operations() -> bool:
    """
    Test query operations and data retrieval.

    This validates that we can read back the data we inserted.
    """
    print("\n📊 Testing query operations...")

    try:
        adapter = create_postgres_adapter()

        # Query all raw entities we just inserted
        query = """
            SELECT entity_type, source_system, external_id,
                   raw_data->>'Name' as department_name,
                   ingested_at
            FROM bronze.raw_entities
            WHERE source_system IN ('tdx', 'test_csv')
            ORDER BY ingested_at DESC
            LIMIT 10
        """

        results_df = adapter.query_to_dataframe(query)

        print(f"✅ Query returned {len(results_df)} records")

        if not results_df.empty:
            print("\n📋 Sample results:")
            for _, row in results_df.head(3).iterrows():
                print(f"   - {row['entity_type']} from {row['source_system']}: {row['external_id']}")

        # Test the metadata views
        status_df = adapter.query_to_dataframe("SELECT * FROM meta.current_ingestion_status LIMIT 5")
        print(f"✅ Ingestion status view returned {len(status_df)} records")

        adapter.close()
        return True

    except Exception as e:
        print(f"❌ Query operations test failed: {e}")
        return False

def test_environment_configuration() -> bool:
    """
    Validate that environment configuration is set up correctly.
    """
    print("\n⚙️  Testing environment configuration...")

    try:
        load_dotenv()

        required_vars = ['DATABASE_URL', 'TDX_BASE_URL', 'TDX_API_TOKEN']
        missing_vars = []

        for var in required_vars:
            value = os.getenv(var)
            if not value:
                missing_vars.append(var)
            else:
                print(f"✅ {var} is configured")

        if missing_vars:
            print(f"❌ Missing environment variables: {missing_vars}")
            print("💡 Please check your .env file configuration")
            return False

        # Test optional configuration
        data_path = os.getenv('DATA_PATH', './data')
        print(f"✅ Data path configured: {data_path}")

        return True

    except Exception as e:
        print(f"❌ Environment configuration test failed: {e}")
        return False

def cleanup_test_data() -> bool:
    """
    Clean up any test data we created during testing.

    This corrected version properly uses SQLAlchemy's text() function
    to ensure the SQL query is executed correctly.
    """
    print("\n🧹 Cleaning up test data...")

    try:
        adapter = create_postgres_adapter()

        # Import text from SQLAlchemy for proper SQL execution
        from sqlalchemy import text

        # Remove test data (be careful with this in production!)
        # The key fix: wrap the SQL string in text() for SQLAlchemy
        cleanup_query = text("""
            DELETE FROM bronze.raw_entities
            WHERE source_system IN ('test_csv', 'tdx')
            AND (external_id LIKE 'test_%' OR external_id = '12345')
        """)

        with adapter.engine.connect() as conn:
            result = conn.execute(cleanup_query)
            deleted_count = result.rowcount
            conn.commit()

        print(f"✅ Cleaned up {deleted_count} test records")

        adapter.close()
        return True

    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False

def main():
    """
    Run all database tests to validate the LSATS Database setup.
    """
    print("🧪 LSATS Database Connection Tests")
    print("=" * 50)

    tests = [
        ("Environment Configuration", test_environment_configuration),
        ("Basic Connection", test_basic_connection),
        ("Bronze Layer Operations", test_bronze_layer_operations),
        ("Query Operations", test_query_operations),
        ("Cleanup", cleanup_test_data)
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"⚠️  {test_name} test had issues")
        except Exception as e:
            print(f"❌ {test_name} test failed with exception: {e}")

    print("\n" + "=" * 50)
    print(f"🎯 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Your LSATS Database setup is working correctly.")
        print("\n🚀 Next steps:")
        print("   1. Create your first ingestion pipeline")
        print("   2. Import data from TeamDynamix")
        print("   3. Set up CSV data processing")
    else:
        print("⚠️  Some tests failed. Please check the error messages above.")
        print("💡 Common issues:")
        print("   - Docker container not running (docker-compose up -d)")
        print("   - Missing environment variables in .env file")
        print("   - PostgreSQL connection issues")

if __name__ == '__main__':
    main()
