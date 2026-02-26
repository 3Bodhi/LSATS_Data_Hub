#!/usr/bin/env python3
"""
Group Relationships Transformation Service

Populates silver.group_members and silver.group_owners from the consolidated
silver.groups table. This replaces the legacy direct-from-bronze extraction.

Key features:
- Extracts relationships from silver.groups JSONB fields
- Parses DNs/identifiers to determine member types (user vs group)
- Populates silver.group_members and silver.group_owners
- Maintains source_system traceability
- Deduplicates members/owners within groups
"""

import argparse
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone

# Add LSATS project to path
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


class GroupRelationshipsService:
    """
    Service for extracting and populating group relationships.
    """

    # DN parsing regex patterns (reused from legacy script for consistency)
    DN_PATTERNS = {
        "user_uid": re.compile(r"(?:uid|cn)=([^,]+)", re.IGNORECASE),
        "group_cn": re.compile(r"cn=([^,]+)", re.IGNORECASE),
    }

    def __init__(self, database_url: str):
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        logger.info("✨ Group relationships service initialized")

    def _parse_identifier(self, identifier: str) -> Tuple[str, str]:
        """
        Parse an identifier (DN or simple name) to extract ID and type.

        Args:
            identifier: DN string or simple name

        Returns:
            Tuple of (id, type) where type is 'user' or 'group'
        """
        if not identifier:
            return "", "unknown"

        identifier_lower = identifier.lower()

        # Check if it looks like a DN
        if "=" in identifier:
            # Check for user DN patterns
            if (
                "ou=people" in identifier_lower
                or "ou=accounts" in identifier_lower
                or "ou=privileged" in identifier_lower
            ):
                match = self.DN_PATTERNS["user_uid"].search(identifier)
                if match:
                    return match.group(1), "user"

            # Check for group DN patterns
            if (
                "ou=user groups" in identifier_lower
                or "ou=groups" in identifier_lower
                or "ou=mcommadsync" in identifier_lower
            ):
                match = self.DN_PATTERNS["group_cn"].search(identifier)
                if match:
                    return match.group(1), "group"

            # Fallback for other DNs
            match = self.DN_PATTERNS["user_uid"].search(identifier)
            if match:
                val = match.group(1)
                # Heuristic for groups
                if "lsa-" in val or "arcts-" in val or "turbo" in val:
                    return val, "group"
                return val, "user"
        else:
            # Simple identifier (not a DN)
            # If it's a uniqname, it's a user. If it has spaces or looks like a group name, it's a group.
            # This is ambiguous, but we can check against known patterns
            if " " in identifier or "lsa-" in identifier_lower:
                return identifier, "group"
            return identifier, "user"

        return identifier, "unknown"

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
                                :run_id, 'silver_transformation', 'group_relationships', :started_at, 'running'
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

    def transform_relationships(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Main transformation logic.

        Returns:
            Dict with transformation statistics
        """
        run_id = self._create_ingestion_run()
        start_time = datetime.now(timezone.utc)
        logger.info(f"🚀 Starting relationship transformation (Run ID: {run_id})")

        stats = {
            "run_id": run_id,
            "groups_processed": 0,
            "members_extracted": 0,
            "owners_extracted": 0,
            "members_inserted": 0,
            "owners_inserted": 0,
            "started_at": start_time,
        }

        try:
            # 1. Fetch all groups with their member/owner data
            query = """
                SELECT
                    group_id,
                    members,
                    owners,
                    direct_members,
                    source_system
                FROM silver.groups
            """
            groups_df = self.db_adapter.query_to_dataframe(query)

            if groups_df.empty:
                logger.warning("⚠️ No groups found in silver.groups")
                self._complete_ingestion_run(run_id, "completed")
                stats["completed_at"] = datetime.now(timezone.utc)
                self._log_final_summary(stats)
                return stats

            total_groups = len(groups_df)
            logger.info(f"📦 Processing {total_groups} groups")

            all_members = []
            all_owners = []

            # Track unique combinations to prevent duplicates
            seen_members = set()
            seen_owners = set()

            for idx, row in groups_df.iterrows():
                group_id = row["group_id"]
                source_system = row["source_system"]

                # Parse Members
                members_list = (
                    row["members"] if isinstance(row["members"], list) else []
                )
                direct_members_list = (
                    row["direct_members"]
                    if isinstance(row["direct_members"], list)
                    else []
                )
                direct_members_set = set(str(m) for m in direct_members_list)

                # Deduplicate members list (some groups have duplicate DNs in JSONB)
                unique_members = list(dict.fromkeys(str(m) for m in members_list if m))

                for member_str in unique_members:
                    m_id, m_type = self._parse_identifier(member_str)

                    if m_type in ("user", "group"):
                        is_direct = member_str in direct_members_set
                        # For AD, everything is effectively direct or we assume so if source is AD
                        if "active_directory" in source_system:
                            is_direct = True

                        # Create unique key for deduplication
                        unique_key = (group_id, m_type, m_id, source_system)

                        if unique_key not in seen_members:
                            seen_members.add(unique_key)
                            all_members.append(
                                {
                                    "group_id": group_id,
                                    "member_type": m_type,
                                    "member_uniqname": m_id
                                    if m_type == "user"
                                    else None,
                                    "member_group_id": m_id
                                    if m_type == "group"
                                    else None,
                                    "is_direct_member": is_direct,
                                    "source_system": source_system,
                                }
                            )

                # Parse Owners
                owners_list = row["owners"] if isinstance(row["owners"], list) else []

                # Deduplicate owners list
                unique_owners = list(dict.fromkeys(str(o) for o in owners_list if o))

                for owner_str in unique_owners:
                    o_id, o_type = self._parse_identifier(owner_str)

                    if o_type in ("user", "group"):
                        # Create unique key for deduplication
                        unique_key = (group_id, o_type, o_id, source_system)

                        if unique_key not in seen_owners:
                            seen_owners.add(unique_key)
                            all_owners.append(
                                {
                                    "group_id": group_id,
                                    "owner_type": o_type,
                                    "owner_uniqname": o_id
                                    if o_type == "user"
                                    else None,
                                    "owner_group_id": o_id
                                    if o_type == "group"
                                    else None,
                                    "source_system": source_system,
                                }
                            )

                stats["groups_processed"] += 1

                # Progress logging
                if stats["groups_processed"] % 1000 == 0:
                    logger.info(
                        f"📊 Progress: {stats['groups_processed']}/{total_groups} groups processed"
                    )

            stats["members_extracted"] = len(all_members)
            stats["owners_extracted"] = len(all_owners)

            logger.info(
                f"🔍 Extracted {stats['members_extracted']} unique members and {stats['owners_extracted']} unique owners"
            )

            if dry_run:
                logger.info(
                    "🔍 [DRY RUN] Would upsert relationships. Skipping DB writes."
                )
                stats["members_inserted"] = stats["members_extracted"]
                stats["owners_inserted"] = stats["owners_extracted"]
                self._complete_ingestion_run(run_id, "completed")
                stats["completed_at"] = datetime.now(timezone.utc)
                self._log_final_summary(stats)
                return stats

            # 2. Upsert (Delete + Insert for full refresh)
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    logger.info("🗑️  Clearing existing relationships...")
                    conn.execute(text("TRUNCATE TABLE silver.group_members"))
                    conn.execute(text("TRUNCATE TABLE silver.group_owners"))

                    # Insert Members
                    if all_members:
                        logger.info(f"✍️  Writing {len(all_members)} members...")
                        chunk_size = 5000
                        for i in range(0, len(all_members), chunk_size):
                            chunk = all_members[i : i + chunk_size]
                            conn.execute(
                                text(
                                    """
                                    INSERT INTO silver.group_members (
                                        group_id, member_type, member_uniqname, member_group_id,
                                        is_direct_member, source_system
                                    ) VALUES (
                                        :group_id, :member_type, :member_uniqname, :member_group_id,
                                        :is_direct_member, :source_system
                                    )
                                """
                                ),
                                chunk,
                            )
                            stats["members_inserted"] += len(chunk)
                            logger.info(
                                f"  ✅ Inserted member chunk {i // chunk_size + 1}/{(len(all_members) + chunk_size - 1) // chunk_size}"
                            )

                    # Insert Owners
                    if all_owners:
                        logger.info(f"✍️  Writing {len(all_owners)} owners...")
                        chunk_size = 5000
                        for i in range(0, len(all_owners), chunk_size):
                            chunk = all_owners[i : i + chunk_size]
                            conn.execute(
                                text(
                                    """
                                    INSERT INTO silver.group_owners (
                                        group_id, owner_type, owner_uniqname, owner_group_id,
                                        source_system
                                    ) VALUES (
                                        :group_id, :owner_type, :owner_uniqname, :owner_group_id,
                                        :source_system
                                    )
                                """
                                ),
                                chunk,
                            )
                            stats["owners_inserted"] += len(chunk)
                            logger.info(
                                f"  ✅ Inserted owner chunk {i // chunk_size + 1}/{(len(all_owners) + chunk_size - 1) // chunk_size}"
                            )

            self._complete_ingestion_run(run_id, "completed")
            stats["completed_at"] = datetime.now(timezone.utc)
            self._log_final_summary(stats)
            logger.info("✅ Relationship transformation complete")
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
        logger.info(f"Groups Processed: {stats['groups_processed']}")
        logger.info(f"├─ Members Extracted: {stats['members_extracted']}")
        logger.info(f"├─ Members Inserted: {stats['members_inserted']}")
        logger.info(f"├─ Owners Extracted: {stats['owners_extracted']}")
        logger.info(f"└─ Owners Inserted: {stats['owners_inserted']}")
        if stats.get("error"):
            logger.error(f"❌ Error: {stats['error']}")
        logger.info(f"{'=' * 60}")

    def close(self):
        """Clean up connections."""
        if self.db_adapter:
            self.db_adapter.close()


def main():
    parser = argparse.ArgumentParser(description="Transform group relationships")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    args = parser.parse_args()

    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)

    service = GroupRelationshipsService(database_url)

    try:
        results = service.transform_relationships(dry_run=args.dry_run)

        # Display results
        print(f"\n📊 Results Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Groups Processed: {results['groups_processed']}")
        print(f"   Members Inserted: {results['members_inserted']}")
        print(f"   Owners Inserted: {results['owners_inserted']}")

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
