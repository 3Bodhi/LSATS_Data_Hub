
import os
import sys
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

# Add LSATS project to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

load_dotenv()
database_url = os.getenv("DATABASE_URL")

if not database_url:
    print("DATABASE_URL not found")
    sys.exit(1)

engine = create_engine(database_url)
inspector = inspect(engine)

tables = ["group_members", "group_owners"]
schema = "silver"

for table in tables:
    print(f"\nTable: {schema}.{table}")
    try:
        columns = inspector.get_columns(table, schema=schema)
        if not columns:
            print("  Table not found or no columns")
            continue
        for col in columns:
            print(f"  - {col['name']} ({col['type']})")
    except Exception as e:
        print(f"  Error: {e}")
