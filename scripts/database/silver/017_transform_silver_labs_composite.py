#!/usr/bin/env python3
"""
Silver Layer Transformation: Composite Labs

Consolidates lab data from multiple intermediate tables into the composite silver.labs entity.
This is a Tier 3 Composite Entity.

Sources:
- silver.users (PIs) - Primary Driver
- silver.tdx_labs (Pipeline Helper)
- silver.award_labs (Pipeline Helper)
- silver.ad_labs (Pipeline Helper)

Logic:
1. Start with all PIs from silver.users
2. Left join with all aggregation tables
3. Construct composite record
4. Calculate quality scores and flags
5. Upsert into silver.labs
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


class CompositeLabsTransformationService:
    """Service for transforming composite labs."""

    def __init__(self, database_url: str):
        self.db_adapter = PostgresAdapter(database_url=database_url)
        logger.info("✨ Composite Labs Transformation Service initialized")

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

    def _get_composite_data(self) -> List[Dict[str, Any]]:
        """Fetch and join data from all sources."""
        query = """
        SELECT 
            u.uniqname as pi_uniqname,
            u.full_name,
            u.department_id as pi_department_id,
            
            -- TDX Data
            tl.tdx_lab_id,
            tl.lab_name as tdx_lab_name,
            tl.computer_count as tdx_computer_count,
            tl.has_tdx_presence,
            tl.department_id as tdx_department_id,
            tl.department_match_method as tdx_dept_match_method,
            tl.department_match_confidence as tdx_dept_confidence,
            
            -- Award Data
            al.award_lab_id,
            al.lab_name as award_lab_name,
            al.total_award_dollars,
            al.total_direct_dollars,
            al.total_indirect_dollars,
            al.award_count,
            al.active_award_count,
            al.earliest_award_start,
            al.latest_award_end,
            al.primary_department_id as award_primary_dept,
            al.department_ids as award_dept_ids,
            
            -- AD Data
            adl.ad_lab_id,
            adl.lab_name as ad_lab_name,
            adl.has_ad_ou,
            adl.ad_ou_dn,
            adl.ad_ou_hierarchy,
            adl.ad_parent_ou,
            adl.ad_ou_depth,
            adl.ad_ou_created,
            adl.ad_ou_modified,
            adl.department_id as ad_department_id,
            adl.department_name as ad_department_name,
            adl.department_match_confidence as ad_dept_confidence
            
        FROM silver.users u
        LEFT JOIN silver.tdx_labs tl ON tl.pi_uniqname = u.uniqname
        LEFT JOIN silver.award_labs al ON al.pi_uniqname = u.uniqname
        LEFT JOIN silver.ad_labs adl ON adl.pi_uniqname = u.uniqname
        WHERE u.is_pi = true
        """
        return self.db_adapter.query_to_dataframe(query).to_dict("records")

    def _determine_lab_name(self, record: Dict[str, Any]) -> str:
        """Determine the best lab name from available sources."""
        # Priority: AD > TDX > Award > Generated
        if record.get("ad_lab_name"):
            return record["ad_lab_name"]
        if record.get("tdx_lab_name"):
            return record["tdx_lab_name"]
        if record.get("award_lab_name"):
            return record["award_lab_name"]
        return f"{record['full_name']} Lab"

    def _determine_primary_dept(self, record: Dict[str, Any]) -> Optional[str]:
        """Determine primary department with cascading priority.
        
        Priority:
        1. Award primary department (most authoritative for research labs)
        2. TDX matched department (operational data)
        3. AD extracted department (organizational structure)
        4. PI's department (fallback)
        """
        # Priority 1: Award department (current logic)
        if record.get("award_primary_dept"):
            return record["award_primary_dept"]
        
        # Priority 2: TDX matched department
        if record.get("tdx_department_id"):
            return record["tdx_department_id"]
        
        # Priority 3: AD extracted department
        if record.get("ad_department_id"):
            return record["ad_department_id"]
        
        # Priority 4: PI's department (fallback)
        if record.get("pi_department_id"):
            return record["pi_department_id"]
        
        return None

    def _calculate_quality_score(self, record: Dict[str, Any]) -> float:
        """Calculate overall data quality score."""
        score = 0.0
        count = 0
        
        if record.get("tdx_lab_id"):
            score += 1.0
            count += 1
        if record.get("award_lab_id"):
            score += 1.0
            count += 1
        if record.get("ad_lab_id"):
            score += 1.0
            count += 1
            
        if count == 0:
            return 0.0
        return round(score / 3.0, 2) # Normalize to 0-1 range roughly, or just use average of available sources?
        # Actually, let's use weighted:
        # 3 sources = 1.0
        # 2 sources = 0.8
        # 1 source = 0.6
        # 0 sources = 0.0 (shouldn't happen for PIs)
        
        if count >= 3: return 1.0
        if count == 2: return 0.8
        if count == 1: return 0.6
        return 0.1

    def _generate_hash(self, record: Dict[str, Any]) -> str:
        """Generate SHA-256 hash of the record content."""
        # Hash key fields to detect changes
        content = f"{record['pi_uniqname']}|{record['lab_name']}|{record['data_source']}|{record['total_award_dollars']}|{record['computer_count']}"
        return hashlib.sha256(content.encode()).hexdigest()

    def _sanitize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize record for SQL insertion."""
        cleaned = {}
        for k, v in record.items():
            # Handle pandas/numpy NaN
            if pd.isna(v):
                cleaned[k] = None
            # Handle NaT string representation if it slipped through
            elif str(v) == "NaT":
                cleaned[k] = None
            else:
                cleaned[k] = v
        return cleaned

    def transform_labs(self, dry_run: bool = False, full_sync: bool = False):
        """Run the transformation process."""
        logger.info("🔄 Starting Composite Labs transformation...")
        
        if dry_run:
            run_id = "dry_run"
        else:
            run_id = self._create_ingestion_run("composite", "lab")

        raw_data = self._get_composite_data()
        logger.info(f"👥 Found {len(raw_data)} PIs to process")

        stats = {"processed": 0, "created": 0, "updated": 0, "skipped": 0}

        for row in raw_data:
            uniqname = row["pi_uniqname"]
            
            # Determine data sources
            sources = []
            if row.get("tdx_lab_id"): sources.append("tdx")
            if row.get("award_lab_id"): sources.append("lab_award")
            if row.get("ad_lab_id"): sources.append("active_directory")
            
            data_source = "+".join(sorted(sources))
            
            # Construct silver record
            silver_record = {
                "lab_id": uniqname,
                "pi_uniqname": uniqname,
                "lab_name": self._determine_lab_name(row),
                "lab_display_name": self._determine_lab_name(row), # Same for now
                
                # Departments
                "primary_department_id": self._determine_primary_dept(row),
                "department_ids": row.get("award_dept_ids") or "[]", # Already JSON string from source? No, likely None or list
                "department_names": json.dumps([]), # Placeholder
                
                # Financial
                "total_award_dollars": row.get("total_award_dollars") or 0.0,
                "total_direct_dollars": row.get("total_direct_dollars") or 0.0,
                "total_indirect_dollars": row.get("total_indirect_dollars") or 0.0,
                "award_count": row.get("award_count") or 0,
                "active_award_count": row.get("active_award_count") or 0,
                "earliest_award_start": row.get("earliest_award_start"),
                "latest_award_end": row.get("latest_award_end"),
                
                # AD OU
                "has_ad_ou": row.get("has_ad_ou") or False,
                "ad_ou_dn": row.get("ad_ou_dn"),
                "ad_ou_hierarchy": row.get("ad_ou_hierarchy") or "[]",
                "ad_parent_ou": row.get("ad_parent_ou"),
                "ad_ou_depth": row.get("ad_ou_depth"),
                "ad_ou_created": row.get("ad_ou_created"),
                "ad_ou_modified": row.get("ad_ou_modified"),
                
                # TDX
                "has_tdx_presence": row.get("has_tdx_presence") or False,
                "computer_count": row.get("tdx_computer_count") or 0,
                
                # Flags
                "is_active": True, # Default to true for PIs
                "has_active_awards": (row.get("active_award_count") or 0) > 0,
                "has_active_ou": row.get("has_ad_ou") or False, # Assume OU existence implies activity?
                
                # Completeness
                "has_award_data": row.get("award_lab_id") is not None,
                "has_ou_data": row.get("ad_lab_id") is not None,
                "has_tdx_data": row.get("tdx_lab_id") is not None,
                "data_source": data_source,
                
                # Quality
                "data_quality_score": self._calculate_quality_score(row),
                "quality_flags": json.dumps([]),
                "source_system": "composite",
                "ingestion_run_id": run_id
            }
            
            # Handle JSON serialization for list/dict fields if they aren't strings
            if not isinstance(silver_record["department_ids"], str):
                 silver_record["department_ids"] = json.dumps(silver_record["department_ids"])
            if not isinstance(silver_record["ad_ou_hierarchy"], str):
                 silver_record["ad_ou_hierarchy"] = json.dumps(silver_record["ad_ou_hierarchy"])

            # Handle timestamps
            for ts_field in ["earliest_award_start", "latest_award_end", "ad_ou_created", "ad_ou_modified"]:
                if silver_record[ts_field]:
                    silver_record[ts_field] = str(silver_record[ts_field])

            # Sanitize record (handle NaN, NaT)
            silver_record = self._sanitize_record(silver_record)

            entity_hash = self._generate_hash(silver_record)
            silver_record["entity_hash"] = entity_hash
            
            if dry_run:
                logger.info(f"🧪 DRY RUN: Would upsert Composite Lab for {uniqname} (Source: {data_source})")
                stats["processed"] += 1
                continue

            try:
                existing_query = "SELECT entity_hash FROM silver.labs WHERE lab_id = :id"
                existing = self.db_adapter.query_to_dataframe(existing_query, {"id": uniqname})
                
                if not existing.empty and existing.iloc[0]["entity_hash"] == entity_hash and not full_sync:
                    stats["skipped"] += 1
                    continue
                    
                upsert_sql = """
                INSERT INTO silver.labs (
                    lab_id, pi_uniqname, lab_name, lab_display_name,
                    primary_department_id, department_ids, department_names,
                    total_award_dollars, total_direct_dollars, total_indirect_dollars,
                    award_count, active_award_count, earliest_award_start, latest_award_end,
                    has_ad_ou, ad_ou_dn, ad_ou_hierarchy, ad_parent_ou, ad_ou_depth, ad_ou_created, ad_ou_modified,
                    has_tdx_presence, computer_count,
                    is_active, has_active_awards, has_active_ou,
                    has_award_data, has_ou_data, has_tdx_data, data_source,
                    data_quality_score, quality_flags, source_system, entity_hash, ingestion_run_id, updated_at
                ) VALUES (
                    :lab_id, :pi_uniqname, :lab_name, :lab_display_name,
                    :primary_department_id, :department_ids, :department_names,
                    :total_award_dollars, :total_direct_dollars, :total_indirect_dollars,
                    :award_count, :active_award_count, :earliest_award_start, :latest_award_end,
                    :has_ad_ou, :ad_ou_dn, :ad_ou_hierarchy, :ad_parent_ou, :ad_ou_depth, :ad_ou_created, :ad_ou_modified,
                    :has_tdx_presence, :computer_count,
                    :is_active, :has_active_awards, :has_active_ou,
                    :has_award_data, :has_ou_data, :has_tdx_data, :data_source,
                    :data_quality_score, :quality_flags, :source_system, :entity_hash, :ingestion_run_id, CURRENT_TIMESTAMP
                )
                ON CONFLICT (lab_id) DO UPDATE SET
                    lab_name = EXCLUDED.lab_name,
                    lab_display_name = EXCLUDED.lab_display_name,
                    primary_department_id = EXCLUDED.primary_department_id,
                    department_ids = EXCLUDED.department_ids,
                    department_names = EXCLUDED.department_names,
                    total_award_dollars = EXCLUDED.total_award_dollars,
                    total_direct_dollars = EXCLUDED.total_direct_dollars,
                    total_indirect_dollars = EXCLUDED.total_indirect_dollars,
                    award_count = EXCLUDED.award_count,
                    active_award_count = EXCLUDED.active_award_count,
                    earliest_award_start = EXCLUDED.earliest_award_start,
                    latest_award_end = EXCLUDED.latest_award_end,
                    has_ad_ou = EXCLUDED.has_ad_ou,
                    ad_ou_dn = EXCLUDED.ad_ou_dn,
                    ad_ou_hierarchy = EXCLUDED.ad_ou_hierarchy,
                    ad_parent_ou = EXCLUDED.ad_parent_ou,
                    ad_ou_depth = EXCLUDED.ad_ou_depth,
                    ad_ou_created = EXCLUDED.ad_ou_created,
                    ad_ou_modified = EXCLUDED.ad_ou_modified,
                    has_tdx_presence = EXCLUDED.has_tdx_presence,
                    computer_count = EXCLUDED.computer_count,
                    is_active = EXCLUDED.is_active,
                    has_active_awards = EXCLUDED.has_active_awards,
                    has_active_ou = EXCLUDED.has_active_ou,
                    has_award_data = EXCLUDED.has_award_data,
                    has_ou_data = EXCLUDED.has_ou_data,
                    has_tdx_data = EXCLUDED.has_tdx_data,
                    data_source = EXCLUDED.data_source,
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
                    logger.info(f"🆕 Created Composite Lab for {uniqname}")
                else:
                    stats["updated"] += 1
                    logger.info(f"📝 Updated Composite Lab for {uniqname}")
                    
            except SQLAlchemyError as e:
                logger.error(f"❌ Failed to upsert Composite Lab for {uniqname}: {e}")

            stats["processed"] += 1

        logger.info("📊 Transformation Summary:")
        logger.info(f"   ├─ Processed PIs: {stats['processed']}")
        logger.info(f"   ├─ Created: {stats['created']}")
        logger.info(f"   ├─ Updated: {stats['updated']}")
        logger.info(f"   └─ Skipped: {stats['skipped']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Composite Labs")
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
        service = CompositeLabsTransformationService(database_url)
        service.transform_labs(dry_run=args.dry_run, full_sync=args.full_sync)
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)
