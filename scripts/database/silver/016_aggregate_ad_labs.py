#!/usr/bin/env python3
"""
Silver Layer Aggregation: AD Labs (Pipeline Helper)

Aggregates AD organizational unit data per PI to create intermediate lab records.
This is a pipeline helper table, not a source-specific table.

Logic:
1. Find all users with is_pi = true
2. Find AD OUs matching OU=<pi_uniqname> or OU name contains pi_uniqname
3. Extract OU hierarchy and metadata
4. Create ad_labs record
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


class ADLabAggregationService:
    """Service for aggregating AD lab data."""

    def __init__(self, database_url: str):
        self.db_adapter = PostgresAdapter(database_url=database_url)
        self.dept_cache = {}  # Cache of dept_id -> dept_name for matching
        logger.info("✨ AD Lab Aggregation Service initialized")

    def _get_pis(self) -> List[Dict[str, Any]]:
        """Fetch all users flagged as PIs."""
        query = """
        SELECT uniqname, full_name
        FROM silver.users
        WHERE is_pi = true
        """
        return self.db_adapter.query_to_dataframe(query).to_dict("records")

    def _find_ou(self, pi_uniqname: str) -> Optional[Dict[str, Any]]:
        """Find AD OU for a PI."""
        # Look for OU where name matches uniqname exactly or contains it
        # Prioritize exact match
        query = """
        SELECT *
        FROM silver.ad_organizational_units
        WHERE name = :uniqname OR name ILIKE :pattern
        ORDER BY CASE WHEN name = :uniqname THEN 0 ELSE 1 END, created_at DESC
        LIMIT 1
        """
        result = self.db_adapter.query_to_dataframe(query, {
            "uniqname": pi_uniqname,
            "pattern": f"%{pi_uniqname}%"
        })
        
        if not result.empty:
            return result.iloc[0].to_dict()
        return None

    def _calculate_quality_score(self, has_ou: bool) -> float:
        """Calculate data quality score."""
        if has_ou:
            return 1.0
        return 0.0

    def _generate_hash(self, record: Dict[str, Any]) -> str:
        """Generate SHA-256 hash of the record content."""
        content = f"{record['pi_uniqname']}|{record['ad_ou_dn']}|{record.get('description')}|{record.get('managed_by')}|{record.get('department_id')}"
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _load_department_cache(self):
        """Load all departments into cache for matching."""
        logger.info("📚 Loading department cache...")
        query = "SELECT dept_id, dept_name FROM silver.departments"
        depts = self.db_adapter.query_to_dataframe(query).to_dict("records")
        for dept in depts:
            self.dept_cache[dept["dept_id"]] = dept["dept_name"]
        logger.info(f"📚 Loaded {len(self.dept_cache)} departments into cache")
    
    def _extract_department_from_ou(self, ad_ou_dn: str) -> Optional[str]:
        """Extract department name from AD OU DN (typically 2nd level OU).
        
        Example: OU=zamanlh-lab,OU=Ecological and Evolutionary Biology,OU=RSN,...
        Returns: "Ecological and Evolutionary Biology"
        """
        if not ad_ou_dn:
            return None
        
        # Split by commas and find the second OU component
        parts = ad_ou_dn.split(',')
        if len(parts) >= 2:
            # Get second part
            second_ou = parts[1].strip()
            # Extract OU name (remove "OU=" prefix)
            if second_ou.startswith("OU="):
                dept_name = second_ou[3:]  # Remove "OU="
                logger.debug(f"Extracted department name from OU: {dept_name}")
                return dept_name
        
        return None
    
    def _match_department(self, department_name: str) -> Dict[str, Any]:
        """Match department name to dept_id using fuzzy similarity.
        
        Returns dict with keys: department_id, match_method, confidence
        """
        if not department_name:
            return {"department_id": None, "match_method": None, "confidence": None}
        
        # Strategy: Fuzzy match using PostgreSQL similarity
        query = """
        SELECT 
            dept_id,
            dept_name,
            SIMILARITY(:name, dept_name) as score
        FROM silver.departments
        WHERE SIMILARITY(:name, dept_name) > 0.50
        ORDER BY score DESC
        LIMIT 1
        """
        result = self.db_adapter.query_to_dataframe(query, {"name": department_name})
        if not result.empty:
            match = result.iloc[0]
            logger.debug(f"🔍 Fuzzy match: '{department_name}' -> {match['dept_id']} (score: {match['score']:.2f})")
            return {
                "department_id": match["dept_id"],
                "match_method": "fuzzy_match",
                "confidence": round(float(match["score"]), 2)
            }
        
        # No match found
        logger.debug(f"❌ No department match for: {department_name}")
        return {"department_id": None, "match_method": None, "confidence": None}

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
        logger.info("🔄 Starting AD Lab aggregation...")
        
        # Load department cache for matching
        self._load_department_cache()
        
        if dry_run:
            run_id = "dry_run"
        else:
            run_id = self._create_ingestion_run("active_directory", "lab_aggregation")
        
        pis = self._get_pis()
        logger.info(f"👥 Found {len(pis)} PIs to process")

        stats = {"processed": 0, "created": 0, "updated": 0, "skipped": 0, "dept_matched": 0, "dept_none": 0}

        for pi in pis:
            uniqname = pi["uniqname"]
            full_name = pi["full_name"]
            
            ou = self._find_ou(uniqname)
            
            if ou:
                lab_name = f"{full_name} Lab (AD)"
                
                # Extract and match department
                ad_ou_dn = ou.get("distinguished_name")
                dept_name = self._extract_department_from_ou(ad_ou_dn)
                dept_match = self._match_department(dept_name)
                
                # Track statistics
                if dept_match["department_id"]:
                    stats["dept_matched"] += 1
                else:
                    stats["dept_none"] += 1
                
                silver_record = {
                    "ad_lab_id": uniqname,
                    "pi_uniqname": uniqname,
                    "lab_name": lab_name,
                    "has_ad_ou": True,
                    "ad_ou_dn": ad_ou_dn,
                    "ad_ou_hierarchy": json.dumps(ou.get("ou_hierarchy", [])), # Assuming jsonb comes as list/dict
                    "ad_parent_ou": ou.get("parent_ou_dn"),
                    "ad_ou_depth": ou.get("ou_depth"),
                    "ad_ou_created": ou.get("when_created"),
                    "ad_ou_modified": ou.get("when_changed"),
                    "description": ou.get("description"),
                    "managed_by": ou.get("managed_by"),
                    "department_name": dept_name,
                    "department_id": dept_match["department_id"],
                    "department_match_method": dept_match["match_method"],
                    "department_match_confidence": dept_match["confidence"],
                    "data_quality_score": self._calculate_quality_score(True),
                    "quality_flags": json.dumps([]),
                    "source_system": "active_directory",
                    "ingestion_run_id": run_id
                }
                
                # Handle potential serialization issues with timestamps
                if silver_record["ad_ou_created"]:
                    silver_record["ad_ou_created"] = str(silver_record["ad_ou_created"])
                if silver_record["ad_ou_modified"]:
                    silver_record["ad_ou_modified"] = str(silver_record["ad_ou_modified"])

                entity_hash = self._generate_hash(silver_record)
                silver_record["entity_hash"] = entity_hash
                
                if dry_run:
                    logger.info(f"🧪 DRY RUN: Would upsert AD Lab for {uniqname} (OU: {ou.get('name')})")
                    stats["processed"] += 1
                    continue

                try:
                    existing_query = "SELECT entity_hash FROM silver.ad_labs WHERE ad_lab_id = :id"
                    existing = self.db_adapter.query_to_dataframe(existing_query, {"id": uniqname})
                    
                    if not existing.empty and existing.iloc[0]["entity_hash"] == entity_hash and not full_sync:
                        stats["skipped"] += 1
                        continue
                        
                    upsert_sql = """
                    INSERT INTO silver.ad_labs (
                        ad_lab_id, pi_uniqname, lab_name,
                        has_ad_ou, ad_ou_dn, ad_ou_hierarchy, ad_parent_ou, ad_ou_depth,
                        ad_ou_created, ad_ou_modified, description, managed_by,
                        department_name, department_id, department_match_method, department_match_confidence,
                        data_quality_score, quality_flags, source_system, entity_hash, ingestion_run_id, updated_at
                    ) VALUES (
                        :ad_lab_id, :pi_uniqname, :lab_name,
                        :has_ad_ou, :ad_ou_dn, :ad_ou_hierarchy, :ad_parent_ou, :ad_ou_depth,
                        :ad_ou_created, :ad_ou_modified, :description, :managed_by,
                        :department_name, :department_id, :department_match_method, :department_match_confidence,
                        :data_quality_score, :quality_flags, :source_system, :entity_hash, :ingestion_run_id, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (ad_lab_id) DO UPDATE SET
                        lab_name = EXCLUDED.lab_name,
                        has_ad_ou = EXCLUDED.has_ad_ou,
                        ad_ou_dn = EXCLUDED.ad_ou_dn,
                        ad_ou_hierarchy = EXCLUDED.ad_ou_hierarchy,
                        ad_parent_ou = EXCLUDED.ad_parent_ou,
                        ad_ou_depth = EXCLUDED.ad_ou_depth,
                        ad_ou_created = EXCLUDED.ad_ou_created,
                        ad_ou_modified = EXCLUDED.ad_ou_modified,
                        description = EXCLUDED.description,
                        managed_by = EXCLUDED.managed_by,
                        department_name = EXCLUDED.department_name,
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
                        conn.execute(text(upsert_sql), silver_record)
                        conn.commit()
                    
                    if existing.empty:
                        stats["created"] += 1
                        logger.info(f"🆕 Created AD Lab for {uniqname}")
                    else:
                        stats["updated"] += 1
                        logger.info(f"📝 Updated AD Lab for {uniqname}")
                        
                except SQLAlchemyError as e:
                    logger.error(f"❌ Failed to upsert AD Lab for {uniqname}: {e}")

            stats["processed"] += 1

        logger.info("📊 Aggregation Summary:")
        logger.info(f"   ├─ Processed PIs: {stats['processed']}")
        logger.info(f"   ├─ Created: {stats['created']}")
        logger.info(f"   ├─ Updated: {stats['updated']}")
        logger.info(f"   └─ Skipped: {stats['skipped']}")
        logger.info("📊 Department Matching Summary:")
        logger.info(f"   ├─ Matched: {stats['dept_matched']}")
        logger.info(f"   └─ No match: {stats['dept_none']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate AD Labs")
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
        service = ADLabAggregationService(database_url)
        service.aggregate_labs(dry_run=args.dry_run, full_sync=args.full_sync)
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)
