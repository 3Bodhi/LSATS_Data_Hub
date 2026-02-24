#!/usr/bin/env python3
"""
Sync TDX Lab CI IDs to silver.labs

Fetches lab CIs from TeamDynamix and writes back their IDs to the database.

Key features:
- Fetches all lab CIs from TDX (Type ID 10132)
- Matches CIs using name regex and PI UID verification
- Updates silver.labs.tdx_ci_id column
- Tracks processing statistics in meta.ingestion_runs
- Supports dry-run mode

Usage:
    python scripts/tdx/002_sync_tdx_lab_ci_ids.py
    python scripts/tdx/002_sync_tdx_lab_ci_ids.py --dry-run
    python scripts/tdx/002_sync_tdx_lab_ci_ids.py --verbose
"""

import argparse
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add LSATS project to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# LSATS imports
from database.adapters.postgres_adapter import PostgresAdapter
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

# Logging configuration
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


class TDXLabCISyncService:
    """Service for syncing TDX lab CI IDs back to database."""

    def __init__(
        self, database_url: str, tdx_base_url: str, tdx_token: str = None,
        tdx_username: str = None, tdx_password: str = None,
        tdx_beid: str = None, tdx_web_services_key: str = None,
        tdx_app_id: int = 48
    ):
        """
        Initialize the sync service.

        Args:
            database_url: PostgreSQL connection string
            tdx_base_url: TeamDynamix base API URL
            tdx_token: TDX API authentication token (optional if using other auth)
            tdx_username: TDX username for JWT auth (optional)
            tdx_password: TDX password for JWT auth (optional)
            tdx_beid: TDX BEID for admin auth (optional)
            tdx_web_services_key: TDX web services key for admin auth (optional)
            tdx_app_id: TDX application ID (default: 48 for Asset/CI app)
        """
        self.db_adapter = PostgresAdapter(database_url=database_url)
        self.tdx = TeamDynamixFacade(
            tdx_base_url,
            tdx_app_id,
            api_token=tdx_token,
            username=tdx_username,
            password=tdx_password,
            beid=tdx_beid,
            web_services_key=tdx_web_services_key,
        )
        self.lab_id_pattern = re.compile(r"^([a-z]+)\s+[Ll]ab$")
        logger.info("✨ TDX Lab CI Sync service initialized")

    def fetch_all_lab_cis(self) -> List[Dict]:
        """
        Fetch all lab CIs from TeamDynamix.

        Uses search_ci_advanced with Type ID 10132 (Labs).

        Returns:
            List of CI dictionaries with ID, Name, OwnerUID, etc.
        """
        logger.info("📥 Fetching lab CIs from TeamDynamix (Type ID 10132)...")

        search_params = {"TypeIDs": [10132]}
        cis = self.tdx.configuration_items.search_ci_advanced(search_params)

        logger.info(f"   Found {len(cis)} lab CIs in TDX")
        return cis

    def extract_lab_id_from_name(self, ci_name: str) -> Optional[str]:
        """
        Extract lab_id from CI name using regex.

        Pattern: '<lab_id> Lab' or '<lab_id> lab'

        Examples:
            "aabol Lab" → "aabol"
            "csmonk lab" → "csmonk"
            "arnoldho Lab" → "arnoldho"

        Args:
            ci_name: The CI name from TDX

        Returns:
            lab_id (lowercase) if pattern matches, None otherwise
        """
        if not ci_name:
            return None

        match = self.lab_id_pattern.match(ci_name.strip())
        if match:
            return match.group(1).lower()
        return None

    def get_pi_tdx_uid_mapping(self) -> Dict[str, str]:
        """
        Get mapping of lab_id → pi_tdx_uid from database.

        Uses silver.v_lab_managers_tdx_reference view which contains
        TDX UIDs for both PIs and managers.

        Returns:
            Dict mapping lab_id to PI TDX UID (as string)
        """
        logger.info("📊 Loading PI TDX UID mappings...")

        query = """
            SELECT DISTINCT lab_id, pi_tdx_uid
            FROM silver.v_lab_managers_tdx_reference
            WHERE pi_tdx_uid IS NOT NULL
        """

        df = self.db_adapter.query_to_dataframe(query)
        mapping = {row["lab_id"]: str(row["pi_tdx_uid"]) for _, row in df.iterrows()}

        logger.info(f"   Loaded {len(mapping)} PI mappings")
        return mapping

    def match_cis_to_labs(
        self, cis: List[Dict], pi_mapping: Dict[str, str]
    ) -> Tuple[List[Tuple[str, int]], List[Dict]]:
        """
        Match TDX CIs to database lab records.

        Matching strategies (in priority order):
        1. Name regex match: Extract lab_id from CI name
        2. OwnerUID verification: Cross-check against pi_tdx_uid

        If name matches but UID doesn't, we log a warning but still accept
        the match (name takes priority over UID).

        Args:
            cis: List of CI dicts from TDX
            pi_mapping: Dict mapping lab_id → pi_tdx_uid

        Returns:
            Tuple of (matches, unmatched_cis)
            - matches: List of (lab_id, ci_id) tuples
            - unmatched_cis: List of CI dicts that couldn't be matched
        """
        logger.info("🔗 Matching CIs to database labs...")

        matches = []
        unmatched = []

        for ci in cis:
            ci_id = ci.get("ID")
            ci_name = ci.get("Name", "")
            owner_uid = ci.get("OwnerUID")

            # Strategy 1: Name pattern match
            lab_id = self.extract_lab_id_from_name(ci_name)

            if lab_id:
                # Verify owner UID if available
                expected_pi_uid = pi_mapping.get(lab_id)
                if expected_pi_uid:
                    if str(owner_uid) == expected_pi_uid:
                        matches.append((lab_id, ci_id))
                        logger.debug(
                            f"   ✅ Matched '{ci_name}' → {lab_id} (name + UID verified)"
                        )
                    else:
                        matches.append((lab_id, ci_id))
                        logger.warning(
                            f"   ⚠️  Matched '{ci_name}' → {lab_id} (name match, UID mismatch: expected {expected_pi_uid}, got {owner_uid})"
                        )
                else:
                    matches.append((lab_id, ci_id))
                    logger.debug(
                        f"   ✅ Matched '{ci_name}' → {lab_id} (name match, no PI UID in database)"
                    )
            else:
                # Strategy 2: Try owner UID fallback
                matched_by_uid = False
                if owner_uid:
                    owner_uid_str = str(owner_uid)
                    for lab_id_candidate, pi_uid in pi_mapping.items():
                        if owner_uid_str == pi_uid:
                            matches.append((lab_id_candidate, ci_id))
                            logger.warning(
                                f"   🔍 Matched '{ci_name}' → {lab_id_candidate} (UID only, name pattern failed)"
                            )
                            matched_by_uid = True
                            break

                if not matched_by_uid:
                    unmatched.append(ci)
                    logger.warning(f"   ❌ No match for CI: '{ci_name}' (ID: {ci_id})")

        logger.info(f"   ✅ Matched: {len(matches)}")
        logger.info(f"   ❌ Unmatched: {len(unmatched)}")

        return matches, unmatched

    def update_lab_ci_ids(
        self, matches: List[Tuple[str, int]], dry_run: bool = False
    ) -> int:
        """
        Update silver.labs.tdx_ci_id for matched labs.

        Uses a bulk UPDATE with VALUES clause for efficiency.

        Args:
            matches: List of (lab_id, ci_id) tuples
            dry_run: If True, preview changes without updating database

        Returns:
            Number of records updated
        """
        if not matches:
            logger.warning("⚠️  No matches to update")
            return 0

        logger.info(f"💾 Updating silver.labs with {len(matches)} CI IDs...")

        if dry_run:
            logger.info("   [DRY RUN] Would update:")
            for lab_id, ci_id in matches[:10]:  # Show first 10
                logger.info(f"      {lab_id} → CI ID {ci_id}")
            if len(matches) > 10:
                logger.info(f"      ... and {len(matches) - 10} more")
            return len(matches)

        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    # Build UPDATE with CASE statement for each match
                    for lab_id, ci_id in matches:
                        conn.execute(
                            text("""
                                UPDATE silver.labs
                                SET tdx_ci_id = :ci_id,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE lab_id = :lab_id
                            """),
                            {"lab_id": lab_id, "ci_id": ci_id},
                        )

            logger.info(f"   ✅ Updated {len(matches)} lab records")
            return len(matches)

        except SQLAlchemyError as e:
            logger.error(f"   ❌ Database update failed: {e}")
            raise

    def create_ingestion_run(self) -> str:
        """
        Create tracking record in meta.ingestion_runs.

        Returns:
            run_id UUID string
        """
        run_id = str(uuid.uuid4())

        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text("""
                            INSERT INTO meta.ingestion_runs (
                                run_id, source_system, entity_type,
                                started_at, status, metadata
                            )
                            VALUES (
                                :run_id, 'tdx_ci_sync', 'lab_ci_id',
                                :started_at, 'running', '{}'::jsonb
                            )
                        """),
                        {"run_id": run_id, "started_at": datetime.now(timezone.utc)},
                    )

            logger.info(f"📝 Created ingestion run: {run_id}")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create ingestion run: {e}")
            raise

    def complete_ingestion_run(
        self,
        run_id: str,
        status: str,
        records_processed: int = 0,
        records_created: int = 0,
        error_message: Optional[str] = None,
    ):
        """
        Update ingestion run with final statistics.

        Args:
            run_id: The run UUID
            status: 'completed' or 'failed'
            records_processed: Number of CIs fetched from TDX
            records_created: Number of labs updated in database
            error_message: Error details if failed
        """
        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text("""
                            UPDATE meta.ingestion_runs
                            SET completed_at = :completed_at,
                                status = :status,
                                records_processed = :records_processed,
                                records_created = :records_created,
                                error_message = :error_message
                            WHERE run_id = :run_id
                        """),
                        {
                            "run_id": run_id,
                            "completed_at": datetime.now(timezone.utc),
                            "status": status,
                            "records_processed": records_processed,
                            "records_created": records_created,
                            "error_message": error_message,
                        },
                    )

            emoji = "✅" if status == "completed" else "❌"
            logger.info(f"{emoji} Ingestion run {status}: {run_id}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to update ingestion run: {e}")

    def sync(self, dry_run: bool = False) -> Dict:
        """
        Main sync workflow.

        Steps:
        1. Create ingestion run tracking record
        2. Fetch all lab CIs from TDX
        3. Get PI UID mappings from database
        4. Match CIs to labs using name/UID strategies
        5. Update database with matched CI IDs
        6. Complete ingestion run with statistics

        Args:
            dry_run: If True, preview changes without updating database

        Returns:
            Dict with sync statistics:
            - total_cis: Number of CIs fetched
            - matched: Number of successful matches
            - unmatched: Number of CIs that couldn't be matched
            - updated: Number of database records updated
            - run_id: Ingestion run UUID
        """
        run_id = self.create_ingestion_run()

        try:
            # Step 1: Fetch CIs from TDX
            cis = self.fetch_all_lab_cis()

            # Step 2: Get PI mappings from database
            pi_mapping = self.get_pi_tdx_uid_mapping()

            # Step 3: Match CIs to labs
            matches, unmatched = self.match_cis_to_labs(cis, pi_mapping)

            # Step 4: Update database
            updated_count = self.update_lab_ci_ids(matches, dry_run=dry_run)

            # Complete run (skip if dry run)
            if not dry_run:
                self.complete_ingestion_run(
                    run_id=run_id,
                    status="completed",
                    records_processed=len(cis),
                    records_created=updated_count,
                )

            return {
                "total_cis": len(cis),
                "matched": len(matches),
                "unmatched": len(unmatched),
                "updated": updated_count,
                "run_id": run_id,
            }

        except Exception as e:
            logger.error(f"Sync failed: {e}")
            if not dry_run:
                self.complete_ingestion_run(
                    run_id=run_id, status="failed", error_message=str(e)
                )
            raise

    def close(self):
        """Close database connection."""
        self.db_adapter.close()
        logger.info("🔒 Database connection closed")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Sync TDX Lab CI IDs to silver.labs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview changes without updating
  python scripts/tdx/002_sync_tdx_lab_ci_ids.py --dry-run

  # Execute actual sync
  python scripts/tdx/002_sync_tdx_lab_ci_ids.py

  # Verbose output for debugging
  python scripts/tdx/002_sync_tdx_lab_ci_ids.py --dry-run --verbose
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without updating database",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging (DEBUG level)"
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)

    # Load environment variables
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    tdx_base_url = os.getenv("TDX_BASE_URL")
    tdx_token = os.getenv("TDX_API_TOKEN")
    tdx_username = os.getenv("TDX_USERNAME")
    tdx_password = os.getenv("TDX_PASSWORD")
    tdx_beid = os.getenv("TDX_BEID")
    tdx_web_services_key = os.getenv("TDX_WEB_SERVICES_KEY")

    has_credentials = (
        (tdx_beid and tdx_web_services_key)
        or (tdx_username and tdx_password)
        or tdx_token
    )
    if not database_url or not tdx_base_url or not has_credentials:
        logger.error("❌ Missing required environment variables")
        logger.error("   Required: DATABASE_URL, TDX_BASE_URL, and valid credentials (BEID+WebServicesKey, Username+Password, or API_TOKEN)")
        sys.exit(1)

    # Initialize service
    service = TDXLabCISyncService(
        database_url=database_url,
        tdx_base_url=tdx_base_url,
        tdx_token=tdx_token,
        tdx_username=tdx_username,
        tdx_password=tdx_password,
        tdx_beid=tdx_beid,
        tdx_web_services_key=tdx_web_services_key,
    )

    try:
        logger.info("=" * 60)
        logger.info("🚀 Starting TDX Lab CI ID Sync")
        if args.dry_run:
            logger.info("   [DRY RUN MODE - No changes will be made]")
        logger.info("=" * 60)

        stats = service.sync(dry_run=args.dry_run)

        logger.info("=" * 60)
        logger.info("📊 Sync Complete")
        logger.info(f"   Total CIs fetched: {stats['total_cis']}")
        logger.info(f"   Matched: {stats['matched']}")
        logger.info(f"   Unmatched: {stats['unmatched']}")
        logger.info(f"   Updated: {stats['updated']}")
        logger.info(f"   Run ID: {stats['run_id']}")
        logger.info("=" * 60)

        if stats["unmatched"] > 0:
            logger.warning(f"⚠️  {stats['unmatched']} CIs could not be matched")
            logger.warning("   Check logs for details on unmatched CIs")

        if args.dry_run:
            logger.info("✅ Dry run completed successfully")
            logger.info("   Run without --dry-run to apply changes")
        else:
            logger.info("✅ Sync completed successfully")

    except Exception as e:
        logger.error(f"❌ Sync failed: {e}")
        sys.exit(1)
    finally:
        service.close()


if __name__ == "__main__":
    main()
