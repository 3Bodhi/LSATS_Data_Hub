#!/usr/bin/env python3
"""
Silver Layer Aggregation: TDX Labs (Pipeline Helper)

Aggregates TDX computer ownership data per PI to create intermediate lab records.
This is a pipeline helper table, not a source-specific table.

Logic:
1. Find all users with is_pi = true (or potential PIs)
2. Count computers where owner_uniqname = pi OR financial_owner_uniqname = pi
3. If count > 0, create tdx_labs record
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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


class TDXLabAggregationService:
    """Service for aggregating TDX lab data."""

    def __init__(self, database_url: str):
        self.db_adapter = PostgresAdapter(database_url=database_url)
        self.dept_cache = {}  # Cache of dept_id -> dept_name for matching
        logger.info("✨ TDX Lab Aggregation Service initialized")

    def _get_pis(self) -> List[Dict[str, Any]]:
        """Fetch all users flagged as PIs, joined with TDX user details."""
        query = """
        SELECT 
            u.uniqname, 
            u.full_name,
            t.tdx_user_uid,
            t.primary_email,
            t.work_phone,
            t.title,
            t.default_account_name as department_name,
            t.company,
            t.is_active
        FROM silver.users u
        LEFT JOIN silver.tdx_users t ON u.uniqname = t.uniqname
        WHERE u.is_pi = true
        """
        return self.db_adapter.query_to_dataframe(query).to_dict("records")

    def _count_computers(self, pi_uniqname: str) -> int:
        """Count computers owned or financially owned by the PI."""
        query = """
        SELECT COUNT(*) as count
        FROM silver.computers
        WHERE (owner_uniqname = :pi OR financial_owner_uniqname = :pi)
          AND source_system LIKE '%tdx%'
        """
        result = self.db_adapter.query_to_dataframe(query, {"pi": pi_uniqname})
        return int(result.iloc[0]["count"])

    def _calculate_quality_score(self, computer_count: int) -> float:
        """Calculate data quality score."""
        # Simple scoring: 1.0 if computers > 0, else 0.5 (shouldn't happen if we filter)
        if computer_count > 0:
            return 1.0
        return 0.0

    def _generate_hash(self, record: Dict[str, Any]) -> str:
        """Generate SHA-256 hash of the record content."""
        # Include new department fields in hash to detect changes
        content = f"{record['pi_uniqname']}|{record['computer_count']}|{record.get('title')}|{record.get('department_name')}|{record.get('department_id')}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _load_department_cache(self):
        """Load all departments into cache for matching."""
        logger.info("📚 Loading department cache...")
        query = "SELECT dept_id, dept_name FROM silver.departments"
        depts = self.db_adapter.query_to_dataframe(query).to_dict("records")
        for dept in depts:
            self.dept_cache[dept["dept_id"]] = dept["dept_name"]
        logger.info(f"📚 Loaded {len(self.dept_cache)} departments into cache")
    
    def _match_department(self, department_name: str, pi_uniqname: str) -> Dict[str, Any]:
        """Match department name to dept_id using multiple strategies.
        
        Returns dict with keys: department_id, match_method, confidence
        """
        if not department_name:
            return {"department_id": None, "match_method": None, "confidence": None}
        
        # Strategy 1: Extract department code (6-digit number)
        code_match = re.search(r'\b(\d{6})\b', department_name)
        if code_match:
            dept_code = code_match.group(1)
            if dept_code in self.dept_cache:
                logger.debug(f"✅ Exact code match for {pi_uniqname}: {dept_code}")
                return {
                    "department_id": dept_code,
                    "match_method": "exact_code",
                    "confidence": 1.0
                }
        
        # Strategy 2: Fuzzy match using PostgreSQL similarity
        query = """
        SELECT 
            dept_id,
            dept_name,
            SIMILARITY(:name, dept_name) as score
        FROM silver.departments
        WHERE SIMILARITY(:name, dept_name) > 0.65
        ORDER BY score DESC
        LIMIT 1
        """
        result = self.db_adapter.query_to_dataframe(query, {"name": department_name})
        if not result.empty:
            match = result.iloc[0]
            logger.debug(f"🔍 Fuzzy match for {pi_uniqname}: {match['dept_id']} (score: {match['score']:.2f})")
            return {
                "department_id": match["dept_id"],
                "match_method": "fuzzy_match",
                "confidence": round(float(match["score"]), 2)
            }
        
        # Strategy 3: Inherit from PI's department
        pi_dept = self._get_pi_department(pi_uniqname)
        if pi_dept:
            logger.debug(f"👤 Inherited PI department for {pi_uniqname}: {pi_dept}")
            return {
                "department_id": pi_dept,
                "match_method": "pi_inherit",
                "confidence": 0.75
            }
        
        # No match found
        logger.debug(f"❌ No department match for {pi_uniqname}")
        return {"department_id": None, "match_method": None, "confidence": None}
    
    def _get_pi_department(self, pi_uniqname: str) -> Optional[str]:
        """Get PI's department from silver.users."""
        query = """
        SELECT department_id 
        FROM silver.users 
        WHERE uniqname = :pi
        """
        result = self.db_adapter.query_to_dataframe(query, {"pi": pi_uniqname})
        if not result.empty and result.iloc[0]["department_id"]:
            return result.iloc[0]["department_id"]
        return None

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
        logger.info("🔄 Starting TDX Lab aggregation...")
        
        # Load department cache for matching
        self._load_department_cache()
        
        if dry_run:
            run_id = "dry_run"
        else:
            run_id = self._create_ingestion_run("tdx", "lab_aggregation")

        pis = self._get_pis()
        logger.info(f"👥 Found {len(pis)} PIs to process")

        stats = {"processed": 0, "created": 0, "updated": 0, "skipped": 0, "dept_exact": 0, "dept_fuzzy": 0, "dept_pi": 0, "dept_none": 0}

        for pi in pis:
            uniqname = pi["uniqname"]
            full_name = pi["full_name"]
            
            # Count computers
            computer_count = self._count_computers(uniqname)
            
            if computer_count > 0:
                lab_name = f"{full_name} Lab (TDX)"
                
                # Match department
                dept_match = self._match_department(pi.get("department_name"), uniqname)
                
                # Track statistics
                if dept_match["match_method"] == "exact_code":
                    stats["dept_exact"] += 1
                elif dept_match["match_method"] == "fuzzy_match":
                    stats["dept_fuzzy"] += 1
                elif dept_match["match_method"] == "pi_inherit":
                    stats["dept_pi"] += 1
                else:
                    stats["dept_none"] += 1
                
                silver_record = {
                    "tdx_lab_id": uniqname,
                    "pi_uniqname": uniqname,
                    "lab_name": lab_name,
                    "computer_count": computer_count,
                    "has_tdx_presence": True,
                    "tdx_user_uid": pi.get("tdx_user_uid"),
                    "primary_email": pi.get("primary_email"),
                    "work_phone": pi.get("work_phone"),
                    "title": pi.get("title"),
                    "department_name": pi.get("department_name"),
                    "company": pi.get("company"),
                    "is_active": pi.get("is_active"),
                    "department_id": dept_match["department_id"],
                    "department_match_method": dept_match["match_method"],
                    "department_match_confidence": dept_match["confidence"],
                    "data_quality_score": self._calculate_quality_score(computer_count),
                    "quality_flags": json.dumps([]),
                    "source_system": "tdx",
                    "ingestion_run_id": run_id
                }
                
                # Calculate hash
                entity_hash = self._generate_hash(silver_record)
                silver_record["entity_hash"] = entity_hash
                
                if dry_run:
                    logger.info(f"🧪 DRY RUN: Would upsert TDX Lab for {uniqname} ({computer_count} computers)")
                    stats["processed"] += 1
                    continue

                # Upsert logic
                try:
                    # Check existing hash
                    existing_query = "SELECT entity_hash FROM silver.tdx_labs WHERE tdx_lab_id = :id"
                    existing = self.db_adapter.query_to_dataframe(existing_query, {"id": uniqname})
                    
                    if not existing.empty and existing.iloc[0]["entity_hash"] == entity_hash and not full_sync:
                        stats["skipped"] += 1
                        continue
                        
                    # Upsert
                    upsert_sql = """
                    INSERT INTO silver.tdx_labs (
                        tdx_lab_id, pi_uniqname, lab_name, computer_count, has_tdx_presence,
                        tdx_user_uid, primary_email, work_phone, title, department_name, company, is_active,
                        department_id, department_match_method, department_match_confidence,
                        data_quality_score, quality_flags, source_system, entity_hash, ingestion_run_id, updated_at
                    ) VALUES (
                        :tdx_lab_id, :pi_uniqname, :lab_name, :computer_count, :has_tdx_presence,
                        :tdx_user_uid, :primary_email, :work_phone, :title, :department_name, :company, :is_active,
                        :department_id, :department_match_method, :department_match_confidence,
                        :data_quality_score, :quality_flags, :source_system, :entity_hash, :ingestion_run_id, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (tdx_lab_id) DO UPDATE SET
                        lab_name = EXCLUDED.lab_name,
                        computer_count = EXCLUDED.computer_count,
                        has_tdx_presence = EXCLUDED.has_tdx_presence,
                        tdx_user_uid = EXCLUDED.tdx_user_uid,
                        primary_email = EXCLUDED.primary_email,
                        work_phone = EXCLUDED.work_phone,
                        title = EXCLUDED.title,
                        department_name = EXCLUDED.department_name,
                        company = EXCLUDED.company,
                        is_active = EXCLUDED.is_active,
                        department_id = EXCLUDED.department_id,
                        department_match_method = EXCLUDED.department_match_method,
                        department_match_confidence = EXCLUDED.department_match_confidence,
                        data_quality_score = EXCLUDED.data_quality_score,
                        quality_flags = EXCLUDED.quality_flags,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    
                    with self.db_adapter.engine.connect() as conn:
                        conn.execute(text(upsert_sql), parameters=silver_record)
                        conn.commit()
                    
                    if existing.empty:
                        stats["created"] += 1
                        logger.info(f"🆕 Created TDX Lab for {uniqname}")
                    else:
                        stats["updated"] += 1
                        logger.info(f"📝 Updated TDX Lab for {uniqname}")
                        
                except SQLAlchemyError as e:
                    logger.error(f"❌ Failed to upsert TDX Lab for {uniqname}: {e}")

            else:
                # If computer count is 0, we might want to delete existing record or mark inactive?
                # For now, we just don't create/update it.
                pass
                
            stats["processed"] += 1

        logger.info("📊 Aggregation Summary:")
        logger.info(f"   ├─ Processed PIs: {stats['processed']}")
        logger.info(f"   ├─ Created: {stats['created']}")
        logger.info(f"   ├─ Updated: {stats['updated']}")
        logger.info(f"   └─ Skipped: {stats['skipped']}")
        logger.info("📊 Department Matching Summary:")
        logger.info(f"   ├─ Exact code matches: {stats['dept_exact']}")
        logger.info(f"   ├─ Fuzzy matches: {stats['dept_fuzzy']}")
        logger.info(f"   ├─ PI inherited: {stats['dept_pi']}")
        logger.info(f"   └─ No match: {stats['dept_none']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate TDX Labs")
    parser.add_argument("--dry-run", action="store_true", help="Run without making changes")
    parser.add_argument("--full-sync", action="store_true", help="Force update all records")
    args = parser.parse_args()

    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("❌ DATABASE_URL not found in environment")
        sys.exit(1)

    try:
        service = TDXLabAggregationService(database_url)
        service.aggregate_labs(dry_run=args.dry_run, full_sync=args.full_sync)
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)
