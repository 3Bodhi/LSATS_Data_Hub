#!/usr/bin/env python3
"""
Lab Members Transformation Service

Populates silver.lab_members using PI-centric discovery from properly layered
group relationships. Replaces legacy award-centric discovery with group-based
membership extraction.

Key features:
- Starts with all PI users (is_pi = true)
- Finds groups related to each PI (member, owner, name match, OU match)
- Extracts all members from PI's groups
- Enriches with silver.users data (job_title, department, etc.)
- Optional award role enrichment
- Full refresh strategy for simplicity
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

# Add LSATS project to path
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

# LSATS imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter

# Set up logging
script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "/var/log/lsats/silver"
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


class LabMembersTransformationService:
    """
    Service for transforming PI groups into lab membership records.
    """

    def __init__(self, database_url: str):
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        self.user_cache: Dict[str, Dict[str, Any]] = {}
        logger.info("✨ Lab members transformation service initialized")

    def _load_user_cache(self):
        """Load all users into memory for fast lookups."""
        logger.info("📚 Loading user cache...")
        query = """
            SELECT
                uniqname,
                first_name,
                last_name,
                full_name,
                job_title,
                department_id,
                department_name
            FROM silver.users
        """
        df = self.db_adapter.query_to_dataframe(query)

        for _, row in df.iterrows():
            self.user_cache[row["uniqname"]] = dict(row)

        logger.info(f"   Loaded {len(self.user_cache)} users into cache")

    def _create_ingestion_run(self) -> str:
        """Create a new ingestion run record."""
        run_id = str(uuid.uuid4())
        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(
                            """
                            INSERT INTO meta.ingestion_runs (
                                run_id, source_system, entity_type, started_at, status
                            ) VALUES (
                                :run_id, 'silver_transformation', 'lab_members', :started_at, 'running'
                            )
                        """
                        ),
                        {"run_id": run_id, "started_at": datetime.now(timezone.utc)},
                    )
            return run_id
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create ingestion run: {e}")
            raise

    def _complete_ingestion_run(
        self, run_id: str, status: str, error_message: Optional[str] = None
    ):
        """Mark ingestion run as complete."""
        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(
                            """
                            UPDATE meta.ingestion_runs
                            SET completed_at = :completed_at,
                                status = :status,
                                error_message = :error_message
                            WHERE run_id = :run_id
                        """
                        ),
                        {
                            "run_id": run_id,
                            "completed_at": datetime.now(timezone.utc),
                            "status": status,
                            "error_message": error_message,
                        },
                    )
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete ingestion run: {e}")

    def _get_all_pis(self) -> List[Dict[str, str]]:
        """Get all PI users."""
        query = """
            SELECT uniqname, full_name
            FROM silver.users
            WHERE is_pi = true
            ORDER BY uniqname
        """
        df = self.db_adapter.query_to_dataframe(query)
        return df.to_dict("records")

    def _find_pi_groups(self, pi_uniqname: str) -> List[str]:
        """
        Find all groups related to a PI using STRICT criteria.

        Strict Criteria (only deliberate/structural relationships):
        - PI is an OWNER of the group (strongest signal - PI manages the group)
        - Group DN contains OU with PI uniqname (AD organizational structure)
        - Group name/ID contains PI uniqname as WHOLE WORD (explicit match, not substring)

        Removed overly broad criteria:
        - PI is member (too broad - includes institutional/departmental groups)
        - Substring matching (too many false positives, e.g., "ter" in "International")

        Args:
            pi_uniqname: The PI's uniqname

        Returns:
            List of group_ids
        """
        query = """
            SELECT DISTINCT g.group_id
            FROM silver.groups g
            WHERE
                -- PI is owner (STRONG SIGNAL - PI manages this group)
                EXISTS (
                    SELECT 1 FROM silver.group_owners go
                    WHERE go.group_id = g.group_id
                      AND go.owner_uniqname = :pi_uniqname
                      AND go.owner_type = 'user'
                )
                -- Group DN contains OU (STRUCTURAL - AD organizational hierarchy)
                OR g.distinguished_name ILIKE '%OU=' || :pi_uniqname || ',%'
                -- Group name contains PI as whole word (EXPLICIT - not substring)
                -- Regex: (^|[^a-z])uniqname([^a-z]|$) ensures word boundaries
                OR g.group_name ~* ('(^|[^a-z])' || :pi_uniqname || '([^a-z]|$)')
                OR g.group_id ~* ('(^|[^a-z])' || :pi_uniqname || '([^a-z]|$)')
        """

        df = self.db_adapter.query_to_dataframe(query, {"pi_uniqname": pi_uniqname})
        return df["group_id"].tolist()

    def _extract_group_members(
        self, group_ids: List[str], max_group_size: int = 200
    ) -> List[Dict]:
        """
        Extract all unique members from given groups.

        Filters out very large groups (e.g., institutional seminar lists, notification groups)
        to avoid noise in lab membership.

        Args:
            group_ids: List of group IDs to extract members from
            max_group_size: Maximum number of members a group can have (default 200)

        Returns:
            List of dicts with member_uniqname, source_system, source_group_ids
        """
        if not group_ids:
            return []

        query = """
            WITH group_sizes AS (
                SELECT
                    group_id,
                    COUNT(*) as member_count
                FROM silver.group_members
                WHERE group_id = ANY(:group_ids)
                  AND member_type = 'user'
                GROUP BY group_id
            ),
            filtered_groups AS (
                SELECT group_id
                FROM group_sizes
                WHERE member_count <= :max_group_size
            )
            SELECT
                gm.member_uniqname,
                array_agg(DISTINCT gm.source_system) as source_systems,
                array_agg(DISTINCT gm.group_id) as source_group_ids
            FROM silver.group_members gm
            JOIN filtered_groups fg ON fg.group_id = gm.group_id
            WHERE gm.member_type = 'user'
              AND gm.member_uniqname IS NOT NULL
            GROUP BY gm.member_uniqname
        """

        df = self.db_adapter.query_to_dataframe(
            query, {"group_ids": group_ids, "max_group_size": max_group_size}
        )
        return df.to_dict("records")

    def _lookup_user(self, uniqname: str) -> Optional[Dict[str, Any]]:
        """Lookup user in cache."""
        return self.user_cache.get(uniqname)

    def _enrich_with_user_data(
        self, members: List[Dict], lab_id: str
    ) -> List[Dict[str, Any]]:
        """
        Enrich members with data from silver.users.

        Args:
            members: List of raw member dicts from group extraction
            lab_id: The PI's uniqname (lab identifier)

        Returns:
            List of enriched member records ready for insertion
        """
        enriched = []

        for member in members:
            uniqname = member["member_uniqname"]
            user = self._lookup_user(uniqname)

            # Determine source_system (prefer most authoritative)
            source_systems = member["source_systems"]
            if "ad+mcommunity" in source_systems:
                source_system = "lab_groups"  # Consolidated from both
            elif "ad" in source_systems:
                source_system = "lab_groups"
            elif "mcommunity" in source_systems:
                source_system = "lab_groups"
            else:
                source_system = "lab_groups"

            enriched_member = {
                "lab_id": lab_id,
                "member_uniqname": uniqname,
                "member_role": user.get("job_title") if user else None,
                "member_first_name": user.get("first_name") if user else None,
                "member_last_name": user.get("last_name") if user else None,
                "member_full_name": user.get("full_name") if user else None,
                "member_department_id": user.get("department_id") if user else None,
                "member_department_name": user.get("department_name") if user else None,
                "silver_user_exists": user is not None,
                "member_job_title": user.get("job_title") if user else None,
                "source_system": source_system,
                "source_group_ids": json.dumps(member["source_group_ids"]),
                "source_award_ids": "[]",  # Will be enriched in future if needed
                "is_pi": (uniqname == lab_id),
                "is_investigator": False,  # Will be enriched from awards in future
                "award_role": None,
            }

            enriched.append(enriched_member)

        return enriched

    def transform_lab_members(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Main transformation logic.

        Args:
            dry_run: If True, don't actually write to database

        Returns:
            Dict with transformation statistics
        """
        run_id = self._create_ingestion_run()
        start_time = datetime.now(timezone.utc)
        logger.info(f"🚀 Starting lab members transformation (Run ID: {run_id})")

        stats = {
            "run_id": run_id,
            "pis_processed": 0,
            "pis_with_groups": 0,
            "total_groups_found": 0,
            "total_members_extracted": 0,
            "members_inserted": 0,
            "members_with_user_data": 0,
            "members_without_user_data": 0,
            "started_at": start_time,
        }

        try:
            # Load user cache
            self._load_user_cache()

            # Get all PIs
            pis = self._get_all_pis()
            total_pis = len(pis)
            logger.info(f"📦 Processing {total_pis} PIs")

            if total_pis == 0:
                logger.warning("⚠️ No PIs found in silver.users")
                self._complete_ingestion_run(run_id, "completed")
                stats["completed_at"] = datetime.now(timezone.utc)
                self._log_final_summary(stats)
                return stats

            # Collect all members across all PIs
            all_members_to_insert = []

            for idx, pi in enumerate(pis):
                pi_uniqname = pi["uniqname"]

                try:
                    # Find PI's groups
                    pi_groups = self._find_pi_groups(pi_uniqname)
                    stats["total_groups_found"] += len(pi_groups)

                    if pi_groups:
                        stats["pis_with_groups"] += 1

                        # Extract members from groups
                        members = self._extract_group_members(pi_groups)
                        stats["total_members_extracted"] += len(members)

                        # Enrich with user data
                        enriched_members = self._enrich_with_user_data(
                            members, pi_uniqname
                        )

                        # Track user data availability
                        for member in enriched_members:
                            if member["silver_user_exists"]:
                                stats["members_with_user_data"] += 1
                            else:
                                stats["members_without_user_data"] += 1

                        all_members_to_insert.extend(enriched_members)

                    stats["pis_processed"] += 1

                    # Progress logging
                    if stats["pis_processed"] % 50 == 0:
                        logger.info(
                            f"📊 Progress: {stats['pis_processed']}/{total_pis} PIs processed "
                            f"({stats['pis_with_groups']} with groups, "
                            f"{stats['total_members_extracted']} members found)"
                        )

                except Exception as pi_error:
                    logger.error(f"❌ Error processing PI {pi_uniqname}: {pi_error}")
                    # Continue with next PI
                    stats["pis_processed"] += 1
                    continue

            logger.info(
                f"🔍 Extracted {len(all_members_to_insert)} total lab member records"
            )

            if dry_run:
                logger.info("🔍 [DRY RUN] Would insert members. Skipping DB writes.")
                stats["members_inserted"] = len(all_members_to_insert)
                self._complete_ingestion_run(run_id, "completed")
                stats["completed_at"] = datetime.now(timezone.utc)
                self._log_final_summary(stats)
                return stats

            # Insert into database (full refresh)
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    # Clear existing data
                    logger.info("🗑️  Clearing existing lab_members...")
                    conn.execute(text("TRUNCATE TABLE silver.lab_members CASCADE"))

                    # Batch insert
                    if all_members_to_insert:
                        logger.info(
                            f"✍️  Inserting {len(all_members_to_insert)} members..."
                        )
                        chunk_size = 5000

                        for i in range(0, len(all_members_to_insert), chunk_size):
                            chunk = all_members_to_insert[i : i + chunk_size]
                            conn.execute(
                                text(
                                    """
                                    INSERT INTO silver.lab_members (
                                        lab_id, member_uniqname, member_role,
                                        member_first_name, member_last_name, member_full_name,
                                        member_department_id, member_department_name,
                                        silver_user_exists, member_job_title,
                                        source_system, source_group_ids, source_award_ids,
                                        is_pi, is_investigator, award_role
                                    ) VALUES (
                                        :lab_id, :member_uniqname, :member_role,
                                        :member_first_name, :member_last_name, :member_full_name,
                                        :member_department_id, :member_department_name,
                                        :silver_user_exists, :member_job_title,
                                        :source_system, :source_group_ids, :source_award_ids,
                                        :is_pi, :is_investigator, :award_role
                                    )
                                """
                                ),
                                chunk,
                            )
                            stats["members_inserted"] += len(chunk)
                            logger.info(
                                f"  ✅ Inserted chunk {i // chunk_size + 1}/{(len(all_members_to_insert) + chunk_size - 1) // chunk_size}"
                            )

            self._complete_ingestion_run(run_id, "completed")
            stats["completed_at"] = datetime.now(timezone.utc)
            self._log_final_summary(stats)
            logger.info("✅ Lab members transformation complete")
            return stats

        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}", exc_info=True)
            self._complete_ingestion_run(run_id, "failed", str(e))
            stats["completed_at"] = datetime.now(timezone.utc)
            stats["error"] = str(e)
            self._log_final_summary(stats)
            raise

    def _log_final_summary(self, stats: Dict[str, Any]):
        """Log comprehensive final summary."""
        duration = (stats["completed_at"] - stats["started_at"]).total_seconds()

        logger.info(f"{'=' * 60}")
        logger.info(f"📊 FINAL RESULTS SUMMARY")
        logger.info(f"{'=' * 60}")
        logger.info(f"Run ID: {stats['run_id']}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"PIs Processed: {stats['pis_processed']}")
        logger.info(f"├─ PIs with Groups: {stats['pis_with_groups']}")
        logger.info(f"├─ Total Groups Found: {stats['total_groups_found']}")
        logger.info(f"├─ Total Members Extracted: {stats['total_members_extracted']}")
        logger.info(f"├─ Members Inserted: {stats['members_inserted']}")
        logger.info(f"├─ Members with User Data: {stats['members_with_user_data']}")
        logger.info(
            f"└─ Members without User Data: {stats['members_without_user_data']}"
        )
        if stats.get("error"):
            logger.error(f"❌ Error: {stats['error']}")
        logger.info(f"{'=' * 60}")

    def close(self):
        """Clean up connections."""
        if self.db_adapter:
            self.db_adapter.close()


def main():
    parser = argparse.ArgumentParser(description="Transform lab members")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    args = parser.parse_args()

    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)

    service = LabMembersTransformationService(database_url)

    try:
        results = service.transform_lab_members(dry_run=args.dry_run)

        # Display results
        print(f"\n📊 Results Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   PIs Processed: {results['pis_processed']}")
        print(f"   PIs with Groups: {results['pis_with_groups']}")
        print(f"   Members Inserted: {results['members_inserted']}")
        print(f"   Members with User Data: {results['members_with_user_data']}")

        duration = (results["completed_at"] - results["started_at"]).total_seconds()
        print(f"   Duration: {duration:.2f} seconds")

        service.close()
        print("✅ Completed successfully!")

    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
        print(f"❌ Failed: {e}")
        service.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
