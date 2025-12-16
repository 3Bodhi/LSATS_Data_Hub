#!/usr/bin/env python3
"""
Silver Layer Aggregation: Award Labs (Pipeline Helper)

Aggregates lab award data per PI to create intermediate lab records.
This is a pipeline helper table, not a source-specific table.

Logic:
1. Find all users with is_pi = true
2. Query silver.lab_awards for each PI
3. Aggregate financial data and departments
4. Create award_labs record
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add LSATS project to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from database.adapters.postgres_adapter import PostgresAdapter

# ============================================================================
# LOGGING SETUP
# ============================================================================

script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "logs"
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


class AwardLabAggregationService:
    """Service for aggregating Award lab data."""

    def __init__(self, database_url: str):
        self.db_adapter = PostgresAdapter(database_url=database_url)
        logger.info("✨ Award Lab Aggregation Service initialized")

    def _get_pis(self) -> List[Dict[str, Any]]:
        """Fetch all users flagged as PIs."""
        query = """
        SELECT uniqname, full_name
        FROM silver.users
        WHERE is_pi = true
        """
        return self.db_adapter.query_to_dataframe(query).to_dict("records")

    def _get_awards(self, pi_uniqname: str) -> List[Dict[str, Any]]:
        """Fetch awards for a PI."""
        query = """
        SELECT *
        FROM silver.lab_awards
        WHERE person_uniqname = :pi
        """
        return self.db_adapter.query_to_dataframe(query, {"pi": pi_uniqname}).to_dict("records")

    def _calculate_aggregates(self, awards: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate financial and other aggregates from awards."""
        if not awards:
            return {}

        total_dollars = Decimal(0)
        total_direct = Decimal(0)
        total_indirect = Decimal(0)
        active_count = 0
        earliest_start = None
        latest_end = None
        dept_ids = set()
        sponsors = set()
        award_titles = set()
        primary_dept_name = None
        
        # Convert to DataFrame for easier handling if needed, but list loop is fine for small N
        for award in awards:
            # Financials
            total_dollars += Decimal(award.get("award_total_dollars") or 0)
            total_direct += Decimal(award.get("award_direct_dollars") or 0)
            total_indirect += Decimal(award.get("award_indirect_dollars") or 0)
            
            # Dates
            start = award.get("award_start_date")
            end = award.get("award_end_date")
            
            if start:
                if earliest_start is None or start < earliest_start:
                    earliest_start = start
            
            if end:
                if latest_end is None or end > latest_end:
                    latest_end = end
                
                # Active check (simple date check)
                if end >= datetime.now().date():
                    active_count += 1
            
            # Departments
            if award.get("person_appt_department_id"):
                dept_ids.add(award["person_appt_department_id"])
                # Capture department name if available (assuming it's in the award record)
                if not primary_dept_name and award.get("person_appt_department"):
                    primary_dept_name = award["person_appt_department"]

            # Sponsors
            if award.get("direct_sponsor_name"):
                sponsors.add(award["direct_sponsor_name"])
            if award.get("prime_sponsor_name"):
                sponsors.add(award["prime_sponsor_name"])

            # Titles
            if award.get("award_title"):
                award_titles.add(award["award_title"])

        # Determine primary department (most frequent or first)
        primary_dept = None
        if dept_ids:
            # Simple logic: take the first one found. 
            # Ideally we'd count frequency but let's keep it simple for now.
            primary_dept = list(dept_ids)[0]

        return {
            "total_award_dollars": total_dollars,
            "total_direct_dollars": total_direct,
            "total_indirect_dollars": total_indirect,
            "award_count": len(awards),
            "active_award_count": active_count,
            "earliest_award_start": earliest_start,
            "latest_award_end": latest_end,
            "primary_department_id": primary_dept,
            "department_ids": list(dept_ids),
            "sponsors": list(sponsors),
            "award_titles": list(award_titles),
            "primary_department_name": primary_dept_name
        }

    def _calculate_quality_score(self, award_count: int) -> float:
        """Calculate data quality score."""
        if award_count > 0:
            return 1.0
        return 0.0

    def _generate_hash(self, record: Dict[str, Any]) -> str:
        """Generate SHA-256 hash of the record content."""
        # Use a subset of fields for hash to detect meaningful changes
        content = f"{record['pi_uniqname']}|{record['total_award_dollars']}|{record['award_count']}|{record.get('primary_department_name')}"
        return hashlib.sha256(content.encode()).hexdigest()

    def _create_ingestion_run(self, source_system: str, entity_type: str) -> str:
        """Create a new ingestion run record."""
        run_id = str(uuid.uuid4())
        query = """
        INSERT INTO meta.ingestion_runs (run_id, source_system, entity_type, status)
        VALUES (:run_id, :source_system, :entity_type, 'running')
        """
        try:
            with self.db_adapter.engine.connect() as conn:
                conn.execute(text(query), {
                    "run_id": run_id,
                    "source_system": source_system,
                    "entity_type": entity_type
                })
                conn.commit()
            logger.info(f"🚀 Created ingestion run {run_id}")
            return run_id
        except SQLAlchemyError as e:
            logger.error(f"Failed to create ingestion run: {e}")
            raise

    def aggregate_labs(self, dry_run: bool = False, full_sync: bool = False):
        """Run the aggregation process."""
        logger.info("🔄 Starting Award Lab aggregation...")
        
        if dry_run:
            run_id = "dry_run"
        else:
            run_id = self._create_ingestion_run("lab_award", "lab_aggregation")
        
        pis = self._get_pis()
        logger.info(f"👥 Found {len(pis)} PIs to process")

        stats = {"processed": 0, "created": 0, "updated": 0, "skipped": 0}

        for pi in pis:
            uniqname = pi["uniqname"]
            full_name = pi["full_name"]
            
            awards = self._get_awards(uniqname)
            
            if awards:
                aggs = self._calculate_aggregates(awards)
                lab_name = f"{full_name} Lab (Awards)"
                
                silver_record = {
                    "award_lab_id": uniqname,
                    "pi_uniqname": uniqname,
                    "lab_name": lab_name,
                    "total_award_dollars": aggs["total_award_dollars"],
                    "total_direct_dollars": aggs["total_direct_dollars"],
                    "total_indirect_dollars": aggs["total_indirect_dollars"],
                    "award_count": aggs["award_count"],
                    "active_award_count": aggs["active_award_count"],
                    "earliest_award_start": aggs["earliest_award_start"],
                    "latest_award_end": aggs["latest_award_end"],
                    "primary_department_id": aggs["primary_department_id"],
                    "department_ids": json.dumps(aggs["department_ids"]),
                    "sponsors": json.dumps(aggs["sponsors"]),
                    "award_titles": json.dumps(aggs["award_titles"]),
                    "primary_department_name": aggs["primary_department_name"],
                    "data_quality_score": self._calculate_quality_score(aggs["award_count"]),
                    "quality_flags": json.dumps([]),
                    "source_system": "lab_award",
                    "ingestion_run_id": run_id
                }
                
                entity_hash = self._generate_hash(silver_record)
                silver_record["entity_hash"] = entity_hash
                
                if dry_run:
                    logger.info(f"🧪 DRY RUN: Would upsert Award Lab for {uniqname} ({aggs['award_count']} awards)")
                    stats["processed"] += 1
                    continue

                try:
                    existing_query = "SELECT entity_hash FROM silver.award_labs WHERE award_lab_id = :id"
                    existing = self.db_adapter.query_to_dataframe(existing_query, {"id": uniqname})
                    
                    if not existing.empty and existing.iloc[0]["entity_hash"] == entity_hash and not full_sync:
                        stats["skipped"] += 1
                        continue
                        
                    upsert_sql = """
                    INSERT INTO silver.award_labs (
                        award_lab_id, pi_uniqname, lab_name, 
                        total_award_dollars, total_direct_dollars, total_indirect_dollars,
                        award_count, active_award_count, earliest_award_start, latest_award_end,
                        primary_department_id, department_ids,
                        sponsors, award_titles, primary_department_name,
                        data_quality_score, quality_flags, source_system, entity_hash, ingestion_run_id, updated_at
                    ) VALUES (
                        :award_lab_id, :pi_uniqname, :lab_name,
                        :total_award_dollars, :total_direct_dollars, :total_indirect_dollars,
                        :award_count, :active_award_count, :earliest_award_start, :latest_award_end,
                        :primary_department_id, :department_ids,
                        :sponsors, :award_titles, :primary_department_name,
                        :data_quality_score, :quality_flags, :source_system, :entity_hash, :ingestion_run_id, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (award_lab_id) DO UPDATE SET
                        lab_name = EXCLUDED.lab_name,
                        total_award_dollars = EXCLUDED.total_award_dollars,
                        total_direct_dollars = EXCLUDED.total_direct_dollars,
                        total_indirect_dollars = EXCLUDED.total_indirect_dollars,
                        award_count = EXCLUDED.award_count,
                        active_award_count = EXCLUDED.active_award_count,
                        earliest_award_start = EXCLUDED.earliest_award_start,
                        latest_award_end = EXCLUDED.latest_award_end,
                        primary_department_id = EXCLUDED.primary_department_id,
                        department_ids = EXCLUDED.department_ids,
                        sponsors = EXCLUDED.sponsors,
                        award_titles = EXCLUDED.award_titles,
                        primary_department_name = EXCLUDED.primary_department_name,
                        data_quality_score = EXCLUDED.data_quality_score,
                        quality_flags = EXCLUDED.quality_flags,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    
                    with self.db_adapter.engine.connect() as conn:
                        conn.execute(text(upsert_sql), silver_record)
                        conn.commit()
                    
                    if existing.empty:
                        stats["created"] += 1
                        logger.info(f"🆕 Created Award Lab for {uniqname}")
                    else:
                        stats["updated"] += 1
                        logger.info(f"📝 Updated Award Lab for {uniqname}")
                        
                except SQLAlchemyError as e:
                    logger.error(f"❌ Failed to upsert Award Lab for {uniqname}: {e}")

            stats["processed"] += 1

        logger.info("📊 Aggregation Summary:")
        logger.info(f"   ├─ Processed PIs: {stats['processed']}")
        logger.info(f"   ├─ Created: {stats['created']}")
        logger.info(f"   ├─ Updated: {stats['updated']}")
        logger.info(f"   └─ Skipped: {stats['skipped']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate Award Labs")
    parser.add_argument("--dry-run", action="store_true", help="Run without making changes")
    parser.add_argument("--full-sync", action="store_true", help="Force update all records")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("❌ DATABASE_URL not found in environment")
        sys.exit(1)

    try:
        service = AwardLabAggregationService(database_url)
        service.aggregate_labs(dry_run=args.dry_run, full_sync=args.full_sync)
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)
