#!/usr/bin/env python3
"""
Transform bronze lab_award and organizational_unit records into silver.labs.

Simplified version using direct psycopg2 connections like transform_silver_groups.py
"""

import hashlib
import json
import logging
import os
import sys
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_values

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


class LabSilverTransformationService:
    """Service for transforming bronze lab data into silver.labs."""

    def __init__(self, db_config: Dict[str, str]):
        """Initialize with database configuration."""
        self.db_config = db_config
        self.conn = psycopg2.connect(**db_config)
        self.conn.autocommit = False
        
        self.dept_cache: Dict[str, str] = {}
        self.dept_id_cache: Dict[str, Dict] = {}
        self.user_cache: Set[str] = set()
        
        logger.info("Connected to database for lab transformation")
    
    def _load_caches(self) -> None:
        """Load departments and users into memory."""
        logger.info("📚 Loading caches...")
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Load departments
            cur.execute("SELECT dept_id, department_name FROM silver.departments")
            for row in cur.fetchall():
                dept_id = row['dept_id']
                dept_name = row['department_name']
                self.dept_id_cache[dept_id] = dict(row)
                self.dept_cache[dept_name.lower()] = dept_id
            
            logger.info(f"   Loaded {len(self.dept_id_cache)} departments")
            
            # Load users
            cur.execute("SELECT uniqname FROM silver.users")
            self.user_cache = {row['uniqname'] for row in cur.fetchall()}
            
            logger.info(f"   Loaded {len(self.user_cache)} users")
    
    def _fetch_bronze_data(self) -> Tuple[Dict[str, List[Dict]], Dict[str, Dict]]:
        """Fetch lab_award and organizational_unit records."""
        logger.info("🔬 Fetching bronze data...")
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Fetch lab_award records
            cur.execute("""
                SELECT 
                    raw_id,
                    LOWER(raw_data->>'Person Uniqname') as uniqname,
                    raw_data
                FROM bronze.raw_entities
                WHERE entity_type = 'lab_award'
                ORDER BY ingested_at DESC
            """)
            
            award_records = {}
            count = 0
            for row in cur.fetchall():
                uniqname = row['uniqname']
                if uniqname not in award_records:
                    award_records[uniqname] = []
                award_records[uniqname].append({
                    'raw_id': row['raw_id'],
                    'raw_data': row['raw_data']
                })
                count += 1
            
            logger.info(f"   Found {count} award records for {len(award_records)} PIs")
            
            # Fetch OU records
            cur.execute("""
                WITH ranked_ous AS (
                    SELECT 
                        raw_id,
                        LOWER(raw_data->>'_extracted_uniqname') as uniqname,
                        raw_data,
                        ROW_NUMBER() OVER (
                            PARTITION BY LOWER(raw_data->>'_extracted_uniqname')
                            ORDER BY (raw_data->>'whenChanged')::timestamp DESC NULLS LAST
                        ) as rn
                    FROM bronze.raw_entities
                    WHERE entity_type = 'organizational_unit'
                      AND raw_data->>'_extracted_uniqname' IS NOT NULL
                )
                SELECT uniqname, raw_id, raw_data
                FROM ranked_ous
                WHERE rn = 1
            """)
            
            ou_records = {}
            for row in cur.fetchall():
                ou_records[row['uniqname']] = {
                    'raw_id': row['raw_id'],
                    'raw_data': row['raw_data']
                }
            
            logger.info(f"   Found {len(ou_records)} lab OUs")
            
            return award_records, ou_records
    
    def _parse_dollar(self, dollar_str: Optional[str]) -> Decimal:
        """Parse dollar string like '$60,000' to Decimal."""
        if not dollar_str:
            return Decimal('0.00')
        
        cleaned = str(dollar_str).replace('$', '').replace(',', '').strip()
        try:
            return Decimal(cleaned)
        except:
            return Decimal('0.00')
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """Parse date string to date object."""
        if not date_str:
            return None
        
        for fmt in ['%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d']:
            try:
                return datetime.strptime(str(date_str).strip(), fmt).date()
            except ValueError:
                continue
        return None
    
    def _resolve_department(
        self,
        award_records: List[Dict],
        ou_hierarchy: Optional[List[str]]
    ) -> Optional[str]:
        """Resolve primary department from awards or OU."""
        # Try awards first
        if award_records:
            dept_ids = []
            for award in award_records:
                dept_id_str = award['raw_data'].get('Person Appt Department Id')
                if dept_id_str:
                    dept_id = str(int(float(dept_id_str)))
                    if dept_id in self.dept_id_cache:
                        dept_ids.append(dept_id)
            
            if dept_ids:
                return Counter(dept_ids).most_common(1)[0][0]
        
        # Fall back to OU hierarchy
        if ou_hierarchy and len(ou_hierarchy) >= 2:
            for pos in [1, 2]:
                if pos < len(ou_hierarchy):
                    dept_name = ou_hierarchy[pos].lower()
                    if dept_name in self.dept_cache:
                        return self.dept_cache[dept_name]
        
        return None
    
    def _merge_lab_record(
        self,
        uniqname: str,
        award_records: List[Dict],
        ou_record: Optional[Dict],
        current_date: date
    ) -> Dict[str, Any]:
        """Merge bronze data into silver lab record."""
        has_award_data = len(award_records) > 0
        has_ou_data = ou_record is not None
        
        # Determine data source
        if has_award_data and has_ou_data:
            data_source = 'award+ou'
            source_system = 'lab_award+organizational_unit'
        elif has_award_data:
            data_source = 'award_only'
            source_system = 'lab_award'
        else:
            data_source = 'ou_only'
            source_system = 'organizational_unit'
        
        # Aggregate financials
        total_award = Decimal('0.00')
        total_direct = Decimal('0.00')
        total_indirect = Decimal('0.00')
        active_count = 0
        earliest_start = None
        latest_end = None
        
        for award in award_records:
            data = award['raw_data']
            total_award += self._parse_dollar(data.get('Award Total Dollars'))
            total_direct += self._parse_dollar(data.get('Award Direct Dollars'))
            total_indirect += self._parse_dollar(data.get('Award Indirect Dollars'))
            
            start = self._parse_date(data.get('Award Project Start Date'))
            end = self._parse_date(data.get('Award Project End Date'))
            
            if start:
                if not earliest_start or start < earliest_start:
                    earliest_start = start
            if end:
                if not latest_end or end > latest_end:
                    latest_end = end
                if start and start <= current_date <= end:
                    active_count += 1
        
        # Get OU data
        ou_hierarchy = ou_record['raw_data'].get('_ou_hierarchy', []) if ou_record else None
        primary_dept = self._resolve_department(award_records, ou_hierarchy)
        
        # Generate lab name
        ou_name = ou_record['raw_data'].get('ou') if ou_record else None
        if ou_name and ou_name.lower() != uniqname.lower():
            lab_name = ou_name
        elif award_records:
            first = award_records[0]['raw_data']
            fname = first.get('Person First Name', '').strip()
            lname = first.get('Person Last Name', '').strip()
            if fname and lname:
                lab_name = f"{fname} {lname} Lab"
            else:
                lab_name = f"{uniqname} Lab"
        else:
            lab_name = f"{uniqname} Lab"
        
        # OU structure
        ad_ou_dn = None
        ad_ou_hierarchy = []
        computer_count = 0
        has_active_ou = False
        
        if ou_record:
            ou_data = ou_record['raw_data']
            ad_ou_dn = ou_data.get('dn')
            ad_ou_hierarchy = ou_data.get('_ou_hierarchy', [])
            computer_count = ou_data.get('_direct_computer_count', 0)
            has_active_ou = computer_count > 0
        
        # Activity status
        has_active_awards = active_count > 0
        is_active = has_active_awards or has_active_ou
        
        # Quality score
        score = 1.0
        flags = []
        
        if uniqname not in self.user_cache:
            score -= 0.20
            flags.append('pi_not_in_silver_users')
        if not primary_dept:
            score -= 0.10
            flags.append('no_department')
        if not has_award_data:
            score -= 0.15
            flags.append('no_awards')
        if not has_ou_data:
            score -= 0.10
            flags.append('no_ad_ou')
        if not is_active:
            score -= 0.05
            flags.append('inactive')
        
        quality_score = max(0.0, min(1.0, score))
        
        # Entity hash
        hash_str = '|'.join([
            uniqname,
            str(primary_dept or ''),
            str(total_award),
            str(len(award_records)),
            str(active_count),
            str(ad_ou_dn or ''),
            str(computer_count)
        ])
        entity_hash = hashlib.sha256(hash_str.encode()).hexdigest()
        
        return {
            'lab_id': uniqname,
            'pi_uniqname': uniqname,
            'lab_name': lab_name,
            'lab_display_name': lab_name,
            'primary_department_id': primary_dept,
            'department_ids': json.dumps([primary_dept] if primary_dept else []),
            'department_names': json.dumps([]),
            'total_award_dollars': total_award,
            'total_direct_dollars': total_direct,
            'total_indirect_dollars': total_indirect,
            'award_count': len(award_records),
            'active_award_count': active_count,
            'earliest_award_start': earliest_start,
            'latest_award_end': latest_end,
            'has_ad_ou': has_ou_data,
            'ad_ou_dn': ad_ou_dn,
            'ad_ou_hierarchy': json.dumps(ad_ou_hierarchy),
            'ad_parent_ou': None,
            'ad_ou_depth': len(ad_ou_hierarchy) if ad_ou_hierarchy else None,
            'computer_count': computer_count,
            'has_computer_children': False,
            'has_child_ous': False,
            'ad_ou_created': None,
            'ad_ou_modified': None,
            'pi_count': 0,
            'investigator_count': 0,
            'member_count': 0,
            'is_active': is_active,
            'has_active_awards': has_active_awards,
            'has_active_ou': has_active_ou,
            'has_award_data': has_award_data,
            'has_ou_data': has_ou_data,
            'data_source': data_source,
            'data_quality_score': quality_score,
            'quality_flags': json.dumps(flags),
            'source_system': source_system,
            'entity_hash': entity_hash
        }
    
    def _bulk_upsert_labs(self, lab_records: List[Dict], run_id: str) -> Tuple[int, int]:
        """Bulk insert/update lab records."""
        if not lab_records:
            return 0, 0
        
        # Fetch existing hashes
        lab_ids = [lab['lab_id'] for lab in lab_records]
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT lab_id, entity_hash FROM silver.labs WHERE lab_id = ANY(%s)",
                (lab_ids,)
            )
            existing_hashes = {row['lab_id']: row['entity_hash'] for row in cur.fetchall()}
        
        # Filter changed
        labs_to_upsert = [
            lab for lab in lab_records
            if lab['lab_id'] not in existing_hashes or
               existing_hashes[lab['lab_id']] != lab['entity_hash']
        ]
        
        if not labs_to_upsert:
            return 0, 0
        
        logger.info(f"💾 Upserting {len(labs_to_upsert)} labs...")
        
        query = """
            INSERT INTO silver.labs (
                lab_id, pi_uniqname, lab_name, lab_display_name,
                primary_department_id, department_ids, department_names,
                total_award_dollars, total_direct_dollars, total_indirect_dollars,
                award_count, active_award_count,
                earliest_award_start, latest_award_end,
                has_ad_ou, ad_ou_dn, ad_ou_hierarchy, ad_parent_ou, ad_ou_depth,
                computer_count, has_computer_children, has_child_ous,
                ad_ou_created, ad_ou_modified,
                pi_count, investigator_count, member_count,
                is_active, has_active_awards, has_active_ou,
                has_award_data, has_ou_data, data_source,
                data_quality_score, quality_flags,
                source_system, entity_hash, ingestion_run_id
            ) VALUES %s
            ON CONFLICT (lab_id) DO UPDATE SET
                pi_uniqname = EXCLUDED.pi_uniqname,
                lab_name = EXCLUDED.lab_name,
                primary_department_id = EXCLUDED.primary_department_id,
                total_award_dollars = EXCLUDED.total_award_dollars,
                total_direct_dollars = EXCLUDED.total_direct_dollars,
                total_indirect_dollars = EXCLUDED.total_indirect_dollars,
                award_count = EXCLUDED.award_count,
                active_award_count = EXCLUDED.active_award_count,
                earliest_award_start = EXCLUDED.earliest_award_start,
                latest_award_end = EXCLUDED.latest_award_end,
                has_ad_ou = EXCLUDED.has_ad_ou,
                ad_ou_dn = EXCLUDED.ad_ou_dn,
                computer_count = EXCLUDED.computer_count,
                is_active = EXCLUDED.is_active,
                has_active_awards = EXCLUDED.has_active_awards,
                has_active_ou = EXCLUDED.has_active_ou,
                data_quality_score = EXCLUDED.data_quality_score,
                quality_flags = EXCLUDED.quality_flags,
                entity_hash = EXCLUDED.entity_hash,
                updated_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted
        """
        
        values = []
        for lab in labs_to_upsert:
            values.append((
                lab['lab_id'], lab['pi_uniqname'], lab['lab_name'], lab['lab_display_name'],
                lab['primary_department_id'], lab['department_ids'], lab['department_names'],
                lab['total_award_dollars'], lab['total_direct_dollars'], lab['total_indirect_dollars'],
                lab['award_count'], lab['active_award_count'],
                lab['earliest_award_start'], lab['latest_award_end'],
                lab['has_ad_ou'], lab['ad_ou_dn'], lab['ad_ou_hierarchy'], lab['ad_parent_ou'], lab['ad_ou_depth'],
                lab['computer_count'], lab['has_computer_children'], lab['has_child_ous'],
                lab['ad_ou_created'], lab['ad_ou_modified'],
                lab['pi_count'], lab['investigator_count'], lab['member_count'],
                lab['is_active'], lab['has_active_awards'], lab['has_active_ou'],
                lab['has_award_data'], lab['has_ou_data'], lab['data_source'],
                lab['data_quality_score'], lab['quality_flags'],
                lab['source_system'], lab['entity_hash'], run_id
            ))
        
        with self.conn.cursor() as cur:
            execute_values(cur, query, values)
            results = cur.fetchall()
            self.conn.commit()
            
            created = sum(1 for r in results if r[0])
            updated = len(results) - created
            
            return created, updated
    
    def transform_labs(self) -> Dict[str, Any]:
        """Main transformation method."""
        start_time = datetime.now()
        run_id = str(uuid4())
        
        try:
            # Create run
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (run_id, 'transform_silver_labs', 'lab', start_time, 'running'))
                self.conn.commit()
            
            logger.info(f"📝 Created run: {run_id}\n")
            
            # Load caches
            self._load_caches()
            
            # Fetch bronze data
            award_records, ou_records = self._fetch_bronze_data()
            
            # Get all unique labs
            unique_uniqnames = set(award_records.keys()) | set(ou_records.keys())
            logger.info(f"🔍 Found {len(unique_uniqnames)} unique labs\n")
            
            # Process each lab
            logger.info("⚙️  Processing labs...")
            lab_records = []
            current_date = date.today()
            
            for i, uniqname in enumerate(sorted(unique_uniqnames), 1):
                try:
                    awards = award_records.get(uniqname, [])
                    ou = ou_records.get(uniqname)
                    
                    lab_record = self._merge_lab_record(uniqname, awards, ou, current_date)
                    lab_records.append(lab_record)
                    
                    if i % 50 == 0 or i == len(unique_uniqnames):
                        logger.info(f"   Processed {i}/{len(unique_uniqnames)} labs")
                
                except Exception as e:
                    logger.error(f"   ❌ Failed {uniqname}: {e}")
                    continue
            
            logger.info(f"   ✓ Processed {len(lab_records)} labs\n")
            
            # Upsert labs
            created, updated = self._bulk_upsert_labs(lab_records, run_id)
            skipped = len(lab_records) - created - updated
            
            logger.info(f"   Created: {created}, Updated: {updated}, Skipped: {skipped}\n")
            
            # Calculate stats
            duration = (datetime.now() - start_time).total_seconds()
            avg_quality = sum(lab['data_quality_score'] for lab in lab_records) / len(lab_records)
            source_counts = Counter(lab['data_source'] for lab in lab_records)
            
            stats = {
                'records_processed': len(unique_uniqnames),
                'labs_created': created,
                'labs_updated': updated,
                'labs_skipped': skipped,
                'avg_quality_score': round(avg_quality, 3),
                'award_only': source_counts.get('award_only', 0),
                'ou_only': source_counts.get('ou_only', 0),
                'award_plus_ou': source_counts.get('award+ou', 0),
                'duration': round(duration, 2)
            }
            
            # Complete run
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = %s, status = %s,
                        records_processed = %s, records_created = %s, records_updated = %s,
                        metadata = %s
                    WHERE run_id = %s
                """, (datetime.now(), 'completed', stats['records_processed'],
                      created, updated, json.dumps(stats), run_id))
                self.conn.commit()
            
            # Print summary
            logger.info("✅ Transformation completed!\n")
            logger.info("╔════════════════════════════════════════╗")
            logger.info("║  SILVER LABS TRANSFORMATION SUMMARY   ║")
            logger.info("╠════════════════════════════════════════╣")
            logger.info(f"║ Total labs processed     │ {stats['records_processed']:>10} ║")
            logger.info(f"║ Labs created             │ {stats['labs_created']:>10} ║")
            logger.info(f"║ Labs updated             │ {stats['labs_updated']:>10} ║")
            logger.info(f"║ Labs unchanged           │ {stats['labs_skipped']:>10} ║")
            logger.info(f"║ Average quality score    │ {stats['avg_quality_score']:>10.3f} ║")
            logger.info("╠════════════════════════════════════════╣")
            logger.info("║ Data source breakdown:               ║")
            logger.info(f"║   award_only             │ {stats['award_only']:>10} ║")
            logger.info(f"║   ou_only                │ {stats['ou_only']:>10} ║")
            logger.info(f"║   award+ou               │ {stats['award_plus_ou']:>10} ║")
            logger.info("╠════════════════════════════════════════╣")
            logger.info(f"║ Duration: {stats['duration']:.1f}s")
            logger.info(f"║ Run ID: {run_id}")
            logger.info("╚════════════════════════════════════════╝")
            
            return stats
        
        except Exception as e:
            logger.error(f"\n❌ Transformation failed: {e}")
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = %s, status = %s, error_message = %s
                    WHERE run_id = %s
                """, (datetime.now(), 'failed', str(e), run_id))
                self.conn.commit()
            raise
        finally:
            self.close()
    
    def close(self):
        """Close connection."""
        if self.conn:
            self.conn.close()
            logger.info("Connection closed")


def main():
    """CLI entry point."""
    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("❌ DATABASE_URL not set")
        sys.exit(1)
    
    # Parse URL to get connection parameters
    parsed = urlparse(database_url)
    db_config = {
        'host': parsed.hostname,
        'port': parsed.port or 5432,
        'database': parsed.path[1:],  # Remove leading /
        'user': parsed.username,
        'password': parsed.password
    }
    
    print("🔬 Starting silver labs transformation...\n")
    
    service = LabSilverTransformationService(db_config)
    
    try:
        service.transform_labs()
        print("\n✅ Transformation completed successfully")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Transformation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
