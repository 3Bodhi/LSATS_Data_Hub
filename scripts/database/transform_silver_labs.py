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
from urllib.parse import urlparse
from uuid import uuid4

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
                dept_id = row["dept_id"]
                dept_name = row["department_name"]
                self.dept_id_cache[dept_id] = dict(row)
                self.dept_cache[dept_name.lower()] = dept_id

            logger.info(f"   Loaded {len(self.dept_id_cache)} departments")

            # Load users
            cur.execute("SELECT uniqname FROM silver.users")
            self.user_cache = {row["uniqname"] for row in cur.fetchall()}

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
                uniqname = row["uniqname"]
                if uniqname not in award_records:
                    award_records[uniqname] = []
                award_records[uniqname].append(
                    {"raw_id": row["raw_id"], "raw_data": row["raw_data"]}
                )
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
                ou_records[row["uniqname"]] = {
                    "raw_id": row["raw_id"],
                    "raw_data": row["raw_data"],
                }

            logger.info(f"   Found {len(ou_records)} lab OUs")

            return award_records, ou_records

    def _parse_dollar(self, dollar_str: Optional[str]) -> Decimal:
        """Parse dollar string like '$60,000' to Decimal."""
        if not dollar_str:
            return Decimal("0.00")

        cleaned = str(dollar_str).replace("$", "").replace(",", "").strip()
        try:
            return Decimal(cleaned)
        except:
            return Decimal("0.00")

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """Parse date string to date object."""
        if not date_str:
            return None

        for fmt in ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"]:
            try:
                return datetime.strptime(str(date_str).strip(), fmt).date()
            except ValueError:
                continue
        return None

    def _parse_dept_id(self, dept_id_raw: Optional[str]) -> Optional[str]:
        """Parse department ID and validate it exists in dept_cache."""
        if not dept_id_raw:
            return None

        try:
            # Convert to int then string to remove decimals (175000.0 -> 175000)
            dept_id = str(int(float(dept_id_raw)))
            # Only return if it exists in our department cache
            if dept_id in self.dept_id_cache:
                return dept_id
            return None
        except:
            return None

    def _resolve_department(
        self, award_records: List[Dict], ou_hierarchy: Optional[List[str]]
    ) -> Optional[str]:
        """Resolve primary department from awards or OU."""
        # Try awards first
        if award_records:
            dept_ids = []
            for award in award_records:
                dept_id_str = award["raw_data"].get("Person Appt Department Id")
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
        current_date: date,
    ) -> Dict[str, Any]:
        """Merge bronze data into silver lab record."""
        has_award_data = len(award_records) > 0
        has_ou_data = ou_record is not None

        # Determine data source
        if has_award_data and has_ou_data:
            data_source = "award+ou"
            source_system = "lab_award+organizational_unit"
        elif has_award_data:
            data_source = "award_only"
            source_system = "lab_award"
        else:
            data_source = "ou_only"
            source_system = "organizational_unit"

        # Aggregate financials
        total_award = Decimal("0.00")
        total_direct = Decimal("0.00")
        total_indirect = Decimal("0.00")
        active_count = 0
        earliest_start = None
        latest_end = None

        for award in award_records:
            data = award["raw_data"]
            total_award += self._parse_dollar(data.get("Award Total Dollars"))
            total_direct += self._parse_dollar(data.get("Award Direct Dollars"))
            total_indirect += self._parse_dollar(data.get("Award Indirect Dollars"))

            start = self._parse_date(data.get("Award Project Start Date"))
            end = self._parse_date(data.get("Award Project End Date"))

            if start:
                if not earliest_start or start < earliest_start:
                    earliest_start = start
            if end:
                if not latest_end or end > latest_end:
                    latest_end = end
                if start and start <= current_date <= end:
                    active_count += 1

        # Get OU data
        ou_hierarchy = (
            ou_record["raw_data"].get("_ou_hierarchy", []) if ou_record else None
        )
        primary_dept = self._resolve_department(award_records, ou_hierarchy)

        # Generate lab name
        ou_name = ou_record["raw_data"].get("ou") if ou_record else None
        if ou_name and ou_name.lower() != uniqname.lower():
            lab_name = ou_name
        elif award_records:
            first = award_records[0]["raw_data"]
            fname = first.get("Person First Name", "").strip()
            lname = first.get("Person Last Name", "").strip()
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
            ou_data = ou_record["raw_data"]
            ad_ou_dn = ou_data.get("dn")
            ad_ou_hierarchy = ou_data.get("_ou_hierarchy", [])
            computer_count = ou_data.get("_direct_computer_count", 0)
            has_active_ou = computer_count > 0

        # Activity status
        has_active_awards = active_count > 0
        is_active = has_active_awards or has_active_ou

        # Quality score
        score = 1.0
        flags = []

        if uniqname not in self.user_cache:
            score -= 0.20
            flags.append("pi_not_in_silver_users")
        if not primary_dept:
            score -= 0.10
            flags.append("no_department")
        if not has_award_data:
            score -= 0.15
            flags.append("no_awards")
        if not has_ou_data:
            score -= 0.10
            flags.append("no_ad_ou")
        if not is_active:
            score -= 0.05
            flags.append("inactive")

        quality_score = max(0.0, min(1.0, score))

        # Entity hash
        hash_str = "|".join(
            [
                uniqname,
                str(primary_dept or ""),
                str(total_award),
                str(len(award_records)),
                str(active_count),
                str(ad_ou_dn or ""),
                str(computer_count),
            ]
        )
        entity_hash = hashlib.sha256(hash_str.encode()).hexdigest()

        return {
            "lab_id": uniqname,
            "pi_uniqname": uniqname,
            "lab_name": lab_name,
            "lab_display_name": lab_name,
            "primary_department_id": primary_dept,
            "department_ids": json.dumps([primary_dept] if primary_dept else []),
            "department_names": json.dumps([]),
            "total_award_dollars": total_award,
            "total_direct_dollars": total_direct,
            "total_indirect_dollars": total_indirect,
            "award_count": len(award_records),
            "active_award_count": active_count,
            "earliest_award_start": earliest_start,
            "latest_award_end": latest_end,
            "has_ad_ou": has_ou_data,
            "ad_ou_dn": ad_ou_dn,
            "ad_ou_hierarchy": json.dumps(ad_ou_hierarchy),
            "ad_parent_ou": None,
            "ad_ou_depth": len(ad_ou_hierarchy) if ad_ou_hierarchy else None,
            "computer_count": computer_count,
            "has_computer_children": False,
            "has_child_ous": False,
            "ad_ou_created": None,
            "ad_ou_modified": None,
            "pi_count": 0,
            "investigator_count": 0,
            "member_count": 0,
            "is_active": is_active,
            "has_active_awards": has_active_awards,
            "has_active_ou": has_active_ou,
            "has_award_data": has_award_data,
            "has_ou_data": has_ou_data,
            "data_source": data_source,
            "data_quality_score": quality_score,
            "quality_flags": json.dumps(flags),
            "source_system": source_system,
            "entity_hash": entity_hash,
        }

    def _bulk_upsert_labs(
        self, lab_records: List[Dict], run_id: str
    ) -> Tuple[int, int]:
        """Bulk insert/update lab records."""
        if not lab_records:
            return 0, 0

        # Fetch existing hashes
        lab_ids = [lab["lab_id"] for lab in lab_records]

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT lab_id, entity_hash FROM silver.labs WHERE lab_id = ANY(%s)",
                (lab_ids,),
            )
            existing_hashes = {
                row["lab_id"]: row["entity_hash"] for row in cur.fetchall()
            }

        # Filter changed
        labs_to_upsert = [
            lab
            for lab in lab_records
            if lab["lab_id"] not in existing_hashes
            or existing_hashes[lab["lab_id"]] != lab["entity_hash"]
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
            values.append(
                (
                    lab["lab_id"],
                    lab["pi_uniqname"],
                    lab["lab_name"],
                    lab["lab_display_name"],
                    lab["primary_department_id"],
                    lab["department_ids"],
                    lab["department_names"],
                    lab["total_award_dollars"],
                    lab["total_direct_dollars"],
                    lab["total_indirect_dollars"],
                    lab["award_count"],
                    lab["active_award_count"],
                    lab["earliest_award_start"],
                    lab["latest_award_end"],
                    lab["has_ad_ou"],
                    lab["ad_ou_dn"],
                    lab["ad_ou_hierarchy"],
                    lab["ad_parent_ou"],
                    lab["ad_ou_depth"],
                    lab["computer_count"],
                    lab["has_computer_children"],
                    lab["has_child_ous"],
                    lab["ad_ou_created"],
                    lab["ad_ou_modified"],
                    lab["pi_count"],
                    lab["investigator_count"],
                    lab["member_count"],
                    lab["is_active"],
                    lab["has_active_awards"],
                    lab["has_active_ou"],
                    lab["has_award_data"],
                    lab["has_ou_data"],
                    lab["data_source"],
                    lab["data_quality_score"],
                    lab["quality_flags"],
                    lab["source_system"],
                    lab["entity_hash"],
                    run_id,
                )
            )

        with self.conn.cursor() as cur:
            execute_values(cur, query, values)
            results = cur.fetchall()
            self.conn.commit()

            created = sum(1 for r in results if r[0])
            updated = len(results) - created

            return created, updated

    def _extract_lab_members_from_groups(self, run_id: str) -> List[Dict[str, Any]]:
        """
        Extract lab members from group membership data.

        For each lab in v_lab_groups:
        1. Get all unique group members (deduplicated by uniqname)
        2. Enrich with silver.users data (job_title, full_name, dept, email)
        3. Mark is_pi=true if member_uniqname == lab_id
        4. Track which group_ids they belong to

        Args:
            run_id: Current ingestion run ID

        Returns:
            List of lab_member records ready for insertion
        """
        logger.info("👥 Extracting members from lab groups...")

        query = """
            WITH lab_group_members AS (
                -- Get all members from all groups associated with each lab
                SELECT
                    vlg.lab_id,
                    vlg.pi_uniqname as lab_pi,
                    gm.member_uniqname,
                    ARRAY_AGG(DISTINCT gm.group_id) as group_ids
                FROM silver.v_lab_groups vlg
                INNER JOIN silver.group_members gm
                    ON vlg.group_id = gm.group_id
                WHERE gm.member_type = 'user'
                GROUP BY vlg.lab_id, vlg.pi_uniqname, gm.member_uniqname
            ),
            enriched_members AS (
                -- Enrich with silver.users data
                SELECT
                    lgm.lab_id,
                    lgm.member_uniqname,
                    lgm.group_ids,
                    u.full_name as member_full_name,
                    u.first_name as member_first_name,
                    u.last_name as member_last_name,
                    u.job_title as member_job_title,
                    u.department_id as member_department_id,
                    d.department_name as member_department_name,
                    u.primary_email,
                    CASE WHEN u.uniqname IS NOT NULL THEN true ELSE false END as silver_user_exists,
                    CASE WHEN lgm.member_uniqname = lgm.lab_pi THEN true ELSE false END as is_pi_from_lab_id
                FROM lab_group_members lgm
                LEFT JOIN silver.users u ON lgm.member_uniqname = u.uniqname
                LEFT JOIN silver.departments d ON u.department_id = d.dept_id
            )
            SELECT * FROM enriched_members
            ORDER BY lab_id, is_pi_from_lab_id DESC, member_uniqname
        """

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()

        member_records = []
        for row in rows:
            member_record = {
                "lab_id": row["lab_id"],
                "member_uniqname": row["member_uniqname"],
                # Role fields
                "member_role": row["member_job_title"],  # Primary role from job_title
                "award_role": None,  # Populated in enrichment phase
                "is_pi": row["is_pi_from_lab_id"],  # Default from lab_id match
                "is_investigator": False,  # Default, updated if award data exists
                # Name fields
                "member_first_name": row["member_first_name"],
                "member_last_name": row["member_last_name"],
                "member_full_name": row["member_full_name"],
                # Department
                "member_department_id": row["member_department_id"],
                "member_department_name": row["member_department_name"],
                # Job title
                "member_job_title": row["member_job_title"],
                # Flags
                "silver_user_exists": row["silver_user_exists"],
                # Source tracking
                "source_system": "lab_groups",  # Will update to 'lab_groups+lab_award' if enriched
                "source_group_ids": json.dumps(row["group_ids"])
                if row["group_ids"]
                else json.dumps([]),
                "source_award_ids": json.dumps([]),  # Empty initially
            }

            member_records.append(member_record)

        logger.info(f"   Extracted {len(member_records)} unique members from groups")
        return member_records

    def _enrich_members_with_award_data(
        self, member_records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Enrich member records with award role data.

        For members who appear in bronze lab_award:
        1. Set award_role field
        2. Update is_pi if role contains "Principal Investigator"
        3. Set is_investigator if role contains "Investigator"
        4. Track award IDs in source_award_ids
        5. Update source_system to 'lab_groups+lab_award'

        Args:
            member_records: List of member dicts from groups

        Returns:
            Enriched member records
        """
        logger.info("🏆 Enriching members with award role data...")

        query = """
            SELECT
                LOWER(raw_data->>'Person Uniqname') as lab_id,
                LOWER(raw_data->>'Person Uniqname') as person_uniqname,
                raw_data->>'Person Role' as award_role,
                raw_data->>'Award Id' as award_id
            FROM bronze.raw_entities
            WHERE entity_type = 'lab_award'
        """

        # Fetch all award person records
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            award_rows = cur.fetchall()

        # Build lookup: (lab_id, uniqname) → award data
        award_data = {}
        for row in award_rows:
            key = (row["lab_id"], row["person_uniqname"])
            if key not in award_data:
                award_data[key] = {
                    "award_role": row["award_role"],
                    "award_ids": [row["award_id"]],
                }
            else:
                award_data[key]["award_ids"].append(row["award_id"])

        # Enrich member records
        enriched_count = 0
        for member in member_records:
            key = (member["lab_id"], member["member_uniqname"])

            if key in award_data:
                award_info = award_data[key]

                # Set award fields
                member["award_role"] = award_info["award_role"]
                member["source_award_ids"] = json.dumps(award_info["award_ids"])
                member["source_system"] = "lab_groups+lab_award"

                # Update PI status if role indicates PI
                if "Principal Investigator" in award_info["award_role"]:
                    member["is_pi"] = True

                # Set investigator status
                if "Investigator" in award_info["award_role"]:
                    member["is_investigator"] = True

                enriched_count += 1

        logger.info(f"   Enriched {enriched_count} members with award data")
        return member_records

    def _bulk_upsert_lab_members(
        self, member_records: List[Dict], run_id: str
    ) -> Tuple[int, int]:
        """
        Bulk insert/update lab_members using execute_values.

        Args:
            member_records: List of member dictionaries
            run_id: Current ingestion run ID

        Returns:
            Tuple of (created_count, updated_count)
        """
        if not member_records:
            return 0, 0

        logger.info(f"💾 Upserting {len(member_records)} lab members...")

        query = """
            INSERT INTO silver.lab_members (
                lab_id, member_uniqname, member_role, award_role,
                is_pi, is_investigator,
                member_first_name, member_last_name, member_full_name,
                member_department_id, member_department_name,
                silver_user_exists, member_job_title,
                source_system, source_group_ids, source_award_ids,
                created_at, updated_at
            ) VALUES %s
            ON CONFLICT (lab_id, member_uniqname) DO UPDATE SET
                member_role = EXCLUDED.member_role,
                award_role = EXCLUDED.award_role,
                is_pi = EXCLUDED.is_pi,
                is_investigator = EXCLUDED.is_investigator,
                member_full_name = EXCLUDED.member_full_name,
                member_department_id = EXCLUDED.member_department_id,
                member_job_title = EXCLUDED.member_job_title,
                source_group_ids = EXCLUDED.source_group_ids,
                source_award_ids = EXCLUDED.source_award_ids,
                source_system = EXCLUDED.source_system,
                silver_user_exists = EXCLUDED.silver_user_exists,
                updated_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted
        """

        values = []
        for member in member_records:
            values.append(
                (
                    member["lab_id"],
                    member["member_uniqname"],
                    member["member_role"],
                    member["award_role"],
                    member["is_pi"],
                    member["is_investigator"],
                    member["member_first_name"],
                    member["member_last_name"],
                    member["member_full_name"],
                    member["member_department_id"],
                    member["member_department_name"],
                    member["silver_user_exists"],
                    member["member_job_title"],
                    member["source_system"],
                    member["source_group_ids"],
                    member["source_award_ids"],
                    datetime.now(),
                    datetime.now(),
                )
            )

        with self.conn.cursor() as cur:
            execute_values(cur, query, values)
            results = cur.fetchall()
            self.conn.commit()

            created = sum(1 for r in results if r[0])
            updated = len(results) - created

            return created, updated

    def _extract_lab_awards(
        self, current_date: date, run_id: str
    ) -> List[Dict[str, Any]]:
        """
        Extract individual award records with all details.

        One record per (Award Id, Person Uniqname, Person Role).
        Calculates is_active based on date range.

        Args:
            current_date: Date for active status calculation
            run_id: Current ingestion run ID

        Returns:
            List of lab_award records ready for insertion
        """
        logger.info("🏅 Extracting lab award details...")

        query = """
            SELECT
                raw_id,
                LOWER(raw_data->>'Person Uniqname') as lab_id,
                raw_data
            FROM bronze.raw_entities
            WHERE entity_type = 'lab_award'
            ORDER BY raw_data->>'Award Id'
        """

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()

        award_records = []
        for row in rows:
            data = row["raw_data"]
            lab_id = row["lab_id"]

            # Parse dates
            start_date = self._parse_date(data.get("Award Project Start Date"))
            end_date = self._parse_date(data.get("Award Project End Date"))
            pre_nce_end = self._parse_date(data.get("Pre NCE Project End Date"))
            publish_date = self._parse_date(data.get("Award Publish Date"))

            # Calculate is_active
            is_active = False
            if start_date and end_date:
                is_active = start_date <= current_date <= end_date

            # Parse dollar amounts
            total_dollars = self._parse_dollar(data.get("Award Total Dollars"))
            direct_dollars = self._parse_dollar(data.get("Award Direct Dollars"))
            indirect_dollars = self._parse_dollar(data.get("Award Indirect Dollars"))

            # Parse facilities rate
            fac_rate_str = data.get("Facilities & Admin Rate (%)", "")
            try:
                fac_rate = (
                    Decimal(str(fac_rate_str).replace("%", "").strip())
                    if fac_rate_str
                    else None
                )
            except:
                fac_rate = None

            award_record = {
                "lab_id": lab_id,
                "award_id": data.get("Award Id"),
                "project_grant_id": data.get("Project/Grant"),
                "award_title": data.get("Award Title"),
                "award_class": data.get("Award Class"),
                "award_total_dollars": total_dollars,
                "award_direct_dollars": direct_dollars,
                "award_indirect_dollars": indirect_dollars,
                "facilities_admin_rate": fac_rate,
                "award_start_date": start_date,
                "award_end_date": end_date,
                "pre_nce_end_date": pre_nce_end,
                "award_publish_date": publish_date,
                "direct_sponsor_name": data.get("Direct Sponsor Name"),
                "direct_sponsor_category": data.get("Direct Sponsor Category"),
                "direct_sponsor_subcategory": data.get("Direct Sponsor Subcategory"),
                "direct_sponsor_reference": data.get(
                    "Direct Sponsor Award Reference Number+\n(Current Budget Period)"
                ),
                "prime_sponsor_name": data.get("Prime Sponsor Name"),
                "prime_sponsor_category": data.get("Prime Sponsor Category"),
                "prime_sponsor_subcategory": data.get("Prime Sponsor Subcategory"),
                "prime_sponsor_reference": data.get(
                    "Prime Sponsor Award Reference Number"
                ),
                "award_admin_department": data.get("Award Admin Department"),
                "award_admin_school_college": data.get("Award Admin School/College"),
                "person_uniqname": data.get("Person Uniqname", "").lower().strip(),
                "person_role": data.get("Person Role"),
                "person_first_name": data.get("Person First Name"),
                "person_last_name": data.get("Person Last Name"),
                "person_appt_department": data.get("Person Appt Department"),
                "person_appt_department_id": self._parse_dept_id(
                    data.get("Person Appt Department Id")
                ),
                "person_appt_school_college": data.get("Person Appt School/College"),
                "is_active": is_active,
                "bronze_raw_id": row["raw_id"],
                "source_file": data.get("_source_file"),
                "content_hash": data.get("_content_hash"),
            }

            award_records.append(award_record)

        # Deduplicate by (award_id, person_uniqname, person_role)
        seen_keys = set()
        deduplicated_records = []
        for record in award_records:
            key = (record["award_id"], record["person_uniqname"], record["person_role"])
            if key not in seen_keys:
                seen_keys.add(key)
                deduplicated_records.append(record)

        if len(deduplicated_records) < len(award_records):
            logger.info(
                f"   Deduplicated {len(award_records)} -> {len(deduplicated_records)} award records"
            )
        else:
            logger.info(f"   Extracted {len(award_records)} award records")
        return deduplicated_records

    def _bulk_upsert_lab_awards(
        self, award_records: List[Dict], run_id: str
    ) -> Tuple[int, int]:
        """
        Bulk insert/update lab_awards using execute_values.

        Args:
            award_records: List of award dictionaries
            run_id: Current ingestion run ID

        Returns:
            Tuple of (created_count, updated_count)
        """
        if not award_records:
            return 0, 0

        logger.info(f"💾 Upserting {len(award_records)} lab awards...")

        query = """
            INSERT INTO silver.lab_awards (
                lab_id, award_id, project_grant_id, award_title, award_class,
                award_total_dollars, award_direct_dollars, award_indirect_dollars,
                facilities_admin_rate, award_start_date, award_end_date,
                pre_nce_end_date, award_publish_date,
                direct_sponsor_name, direct_sponsor_category, direct_sponsor_subcategory,
                direct_sponsor_reference, prime_sponsor_name, prime_sponsor_category,
                prime_sponsor_subcategory, prime_sponsor_reference,
                award_admin_department, award_admin_school_college,
                person_uniqname, person_role, person_first_name, person_last_name,
                person_appt_department, person_appt_department_id, person_appt_school_college,
                is_active, bronze_raw_id, source_file, content_hash,
                created_at, updated_at
            ) VALUES %s
            ON CONFLICT (award_id, person_uniqname, person_role) DO UPDATE SET
                award_title = EXCLUDED.award_title,
                award_total_dollars = EXCLUDED.award_total_dollars,
                award_end_date = EXCLUDED.award_end_date,
                is_active = EXCLUDED.is_active,
                content_hash = EXCLUDED.content_hash,
                updated_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted
        """

        values = []
        for award in award_records:
            values.append(
                (
                    award["lab_id"],
                    award["award_id"],
                    award["project_grant_id"],
                    award["award_title"],
                    award["award_class"],
                    award["award_total_dollars"],
                    award["award_direct_dollars"],
                    award["award_indirect_dollars"],
                    award["facilities_admin_rate"],
                    award["award_start_date"],
                    award["award_end_date"],
                    award["pre_nce_end_date"],
                    award["award_publish_date"],
                    award["direct_sponsor_name"],
                    award["direct_sponsor_category"],
                    award["direct_sponsor_subcategory"],
                    award["direct_sponsor_reference"],
                    award["prime_sponsor_name"],
                    award["prime_sponsor_category"],
                    award["prime_sponsor_subcategory"],
                    award["prime_sponsor_reference"],
                    award["award_admin_department"],
                    award["award_admin_school_college"],
                    award["person_uniqname"],
                    award["person_role"],
                    award["person_first_name"],
                    award["person_last_name"],
                    award["person_appt_department"],
                    award["person_appt_department_id"],
                    award["person_appt_school_college"],
                    award["is_active"],
                    award["bronze_raw_id"],
                    award["source_file"],
                    award["content_hash"],
                    datetime.now(),
                    datetime.now(),
                )
            )

        with self.conn.cursor() as cur:
            execute_values(cur, query, values)
            results = cur.fetchall()
            self.conn.commit()

            created = sum(1 for r in results if r[0])
            updated = len(results) - created

            return created, updated

    def _update_lab_member_counts(self) -> int:
        """
        Update member_count, pi_count, investigator_count in silver.labs.

        Aggregates from silver.lab_members after population.

        Returns:
            Number of labs updated
        """
        logger.info("🔢 Updating lab member counts...")

        query = """
            UPDATE silver.labs l
            SET
                member_count = COALESCE(counts.total_members, 0),
                pi_count = COALESCE(counts.pi_count, 0),
                investigator_count = COALESCE(counts.investigator_count, 0),
                updated_at = CURRENT_TIMESTAMP
            FROM (
                SELECT
                    lab_id,
                    COUNT(*) as total_members,
                    COUNT(*) FILTER (WHERE is_pi = true) as pi_count,
                    COUNT(*) FILTER (WHERE is_investigator = true) as investigator_count
                FROM silver.lab_members
                GROUP BY lab_id
            ) counts
            WHERE l.lab_id = counts.lab_id
        """

        with self.conn.cursor() as cur:
            cur.execute(query)
            updated_count = cur.rowcount
            self.conn.commit()

        logger.info(f"   Updated {updated_count} labs with member counts")
        return updated_count

    def transform_labs(self) -> Dict[str, Any]:
        """Main transformation method."""
        start_time = datetime.now()
        run_id = str(uuid4())

        try:
            # Create run
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status
                    ) VALUES (%s, %s, %s, %s, %s)
                """,
                    (run_id, "transform_silver_labs", "lab", start_time, "running"),
                )
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

                    lab_record = self._merge_lab_record(
                        uniqname, awards, ou, current_date
                    )
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

            logger.info(
                f"   Created: {created}, Updated: {updated}, Skipped: {skipped}\n"
            )

            # NEW: Extract and insert lab members from groups
            logger.info("\n👥 Processing Lab Members")
            logger.info("=" * 60)

            member_records = self._extract_lab_members_from_groups(run_id)
            member_records = self._enrich_members_with_award_data(member_records)

            members_created, members_updated = self._bulk_upsert_lab_members(
                member_records, run_id
            )
            logger.info(
                f"   ✓ Created: {members_created}, Updated: {members_updated}\n"
            )

            # NEW: Extract and insert lab awards
            logger.info("🏆 Processing Lab Awards")
            logger.info("=" * 60)

            award_detail_records = self._extract_lab_awards(current_date, run_id)
            awards_created, awards_updated = self._bulk_upsert_lab_awards(
                award_detail_records, run_id
            )
            logger.info(f"   ✓ Created: {awards_created}, Updated: {awards_updated}\n")

            # NEW: Update member counts in labs table
            logger.info("🔢 Updating Lab Member Counts")
            logger.info("=" * 60)

            labs_count_updated = self._update_lab_member_counts()
            logger.info(f"   ✓ Updated {labs_count_updated} labs\n")

            # Calculate stats
            duration = (datetime.now() - start_time).total_seconds()
            avg_quality = sum(lab["data_quality_score"] for lab in lab_records) / len(
                lab_records
            )
            source_counts = Counter(lab["data_source"] for lab in lab_records)

            stats = {
                "records_processed": len(unique_uniqnames),
                "labs_created": created,
                "labs_updated": updated,
                "labs_skipped": skipped,
                "avg_quality_score": round(avg_quality, 3),
                "award_only": source_counts.get("award_only", 0),
                "ou_only": source_counts.get("ou_only", 0),
                "award_plus_ou": source_counts.get("award+ou", 0),
                "members_created": members_created,
                "members_updated": members_updated,
                "members_from_groups": len(member_records),
                "members_enriched_with_awards": sum(
                    1 for m in member_records if m.get("award_role") is not None
                ),
                "awards_created": awards_created,
                "awards_updated": awards_updated,
                "labs_with_updated_counts": labs_count_updated,
                "duration": round(duration, 2),
            }

            # Complete run
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE meta.ingestion_runs
                    SET completed_at = %s, status = %s,
                        records_processed = %s, records_created = %s, records_updated = %s,
                        metadata = %s
                    WHERE run_id = %s
                """,
                    (
                        datetime.now(),
                        "completed",
                        stats["records_processed"],
                        created,
                        updated,
                        json.dumps(stats),
                        run_id,
                    ),
                )
                self.conn.commit()

            # Print summary
            logger.info("✅ Transformation completed!\n")
            logger.info("╔════════════════════════════════════════╗")
            logger.info("║  SILVER LABS TRANSFORMATION SUMMARY   ║")
            logger.info("╠════════════════════════════════════════╣")
            logger.info(
                f"║ Total labs processed     │ {stats['records_processed']:>10} ║"
            )
            logger.info(f"║ Labs created             │ {stats['labs_created']:>10} ║")
            logger.info(f"║ Labs updated             │ {stats['labs_updated']:>10} ║")
            logger.info(f"║ Labs unchanged           │ {stats['labs_skipped']:>10} ║")
            logger.info(
                f"║ Average quality score    │ {stats['avg_quality_score']:>10.3f} ║"
            )
            logger.info("╠════════════════════════════════════════╣")
            logger.info("║ Data source breakdown:               ║")
            logger.info(f"║   award_only             │ {stats['award_only']:>10} ║")
            logger.info(f"║   ou_only                │ {stats['ou_only']:>10} ║")
            logger.info(f"║   award+ou               │ {stats['award_plus_ou']:>10} ║")
            logger.info("╠════════════════════════════════════════╣")
            logger.info("║ Lab Members & Awards:                ║")
            logger.info(
                f"║   Members created        │ {stats['members_created']:>10} ║"
            )
            logger.info(
                f"║   Members updated        │ {stats['members_updated']:>10} ║"
            )
            logger.info(
                f"║   Members w/ award roles │ {stats['members_enriched_with_awards']:>10} ║"
            )
            logger.info(f"║   Awards created         │ {stats['awards_created']:>10} ║")
            logger.info(f"║   Awards updated         │ {stats['awards_updated']:>10} ║")
            logger.info("╠════════════════════════════════════════╣")
            logger.info(f"║ Duration: {stats['duration']:.1f}s")
            logger.info(f"║ Run ID: {run_id}")
            logger.info("╚════════════════════════════════════════╝")

            return stats

        except Exception as e:
            logger.error(f"\n❌ Transformation failed: {e}")
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE meta.ingestion_runs
                    SET completed_at = %s, status = %s, error_message = %s
                    WHERE run_id = %s
                """,
                    (datetime.now(), "failed", str(e), run_id),
                )
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
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        print("❌ DATABASE_URL not set")
        sys.exit(1)

    # Parse URL to get connection parameters
    parsed = urlparse(database_url)
    db_config = {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "database": parsed.path[1:],  # Remove leading /
        "user": parsed.username,
        "password": parsed.password,
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


if __name__ == "__main__":
    main()
