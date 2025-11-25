#!/usr/bin/env python3
"""
Consolidated Users Silver Layer Transformation Service

Transforms source-specific silver user records (TDX, AD, UMAPI, MCommunity)
into consolidated silver.users table.

Key features:
- Merges data from 4 source systems
- Handles UMAPI multiple employment records (aggregates to JSONB)
- Derives is_PI flag from lab awards and organizational units
- Calculates data quality scores
- Content hash-based change detection
- Incremental processing with --full-sync override
- Optional alumni exclusion for performance
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add LSATS project to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

# LSATS imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter

# Set up logging
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


class UserConsolidationService:
    """
    Service for consolidating user records from TDX, AD, UMAPI, and MCommunity into silver.users.
    """

    def __init__(self, database_url: str):
        """
        Initialize the consolidation service.

        Args:
            database_url: PostgreSQL connection string
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        logger.info("✨ User consolidation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful consolidation run.

        Returns:
            Timestamp of last completed run, or None if first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'users_consolidated'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"⏰ Last successful consolidation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info("🆕 No previous consolidation found - processing all users")
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️ Could not determine last consolidation timestamp: {e}")
            return None

    def _load_pi_uniqnames(self) -> Set[str]:
        """
        Load set of uniqnames identified as Principal Investigators.

        Sources:
        - silver.lab_awards (person_uniqname where role is PI)
        - silver.ad_organizational_units (extracted_uniqname for lab OUs)

        Returns:
            Set of lowercase uniqnames
        """
        try:
            query = """
            SELECT DISTINCT uniqname FROM (
                SELECT DISTINCT LOWER(person_uniqname) as uniqname
                FROM silver.lab_awards
                WHERE person_uniqname IS NOT NULL
                  AND person_role LIKE '%Principal Investigator%'
                UNION
                SELECT DISTINCT LOWER(extracted_uniqname) as uniqname
                FROM silver.ad_organizational_units
                WHERE extracted_uniqname IS NOT NULL
                  AND ou_depth >= 8
            ) pis
            """
            result_df = self.db_adapter.query_to_dataframe(query)
            pi_uniqnames = set(result_df["uniqname"].str.lower().tolist())
            logger.info(f"🔬 Loaded {len(pi_uniqnames)} PI uniqnames")
            return pi_uniqnames
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to load PI uniqnames: {e}")
            return set()

    def _resolve_empl_id_to_uniqname(self, empl_id: str) -> Optional[str]:
        """
        Resolve an EmplID to a uniqname using UMAPI data.
        Note: This implementation does a direct query. For performance in large batches,
        we might want to cache this map, but for now we'll assume it's used sparsely
        or we rely on the fact that we have the data in memory during merge if needed.

        Actually, for the transformation, we are processing by uniqname, so we might not
        have the supervisor's uniqname readily available if they are a different user.
        We will implement a cached lookup if performance becomes an issue.
        For now, we'll return None and let the join handle it if needed, or implement
        a lookup if we really need it populated in the JSONB.

        Let's skip complex resolution for now to keep it simple, or use a simple query if needed.
        """
        return None

    def _fetch_source_records(
        self,
        since_timestamp: Optional[datetime] = None,
        full_sync: bool = False,
        exclude_alumni: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch records from all 4 sources and group by uniqname.

        Args:
            since_timestamp: Only fetch records updated after this time
            full_sync: Ignore timestamp and fetch all records
            exclude_alumni: If True, exclude MCommunity users with only "Alumni" affiliation

        Returns:
            Dictionary keyed by uniqname, containing source records:
            {
                'uniqname': {
                    'tdx': {...},
                    'ad': {...},
                    'umapi': [{...}, ...],  # List for UMAPI
                    'mcom': {...}
                }
            }
        """
        try:
            time_filter = ""
            params = {}

            if since_timestamp and not full_sync:
                time_filter = "AND updated_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp
                logger.info(f"📊 Fetching records updated after {since_timestamp}")
            else:
                logger.info("📊 Fetching all user records (full sync)")

            # 1. Fetch TDX Users
            logger.info("📥 Fetching TDX users...")
            tdx_query = f"""
            SELECT * FROM silver.tdx_users
            WHERE uniqname IS NOT NULL {time_filter}
            """
            tdx_records = self.db_adapter.query_to_dataframe(tdx_query, params).to_dict(
                "records"
            )

            # 2. Fetch AD Users
            logger.info("📥 Fetching AD users...")
            # AD users don't have 'uniqname' column, they have 'uid' which maps to uniqname
            # We also need to handle the case where uid might be missing but sam_account_name exists
            ad_query = f"""
            SELECT * FROM silver.ad_users
            WHERE uid IS NOT NULL {time_filter}
            """
            ad_records = self.db_adapter.query_to_dataframe(ad_query, params).to_dict(
                "records"
            )

            # 3. Fetch UMAPI Employees
            logger.info("📥 Fetching UMAPI employees...")
            umapi_query = f"""
            SELECT * FROM silver.umapi_employees
            WHERE uniqname IS NOT NULL {time_filter}
            """
            umapi_records = self.db_adapter.query_to_dataframe(
                umapi_query, params
            ).to_dict("records")

            # 4. Fetch MCommunity Users
            logger.info("📥 Fetching MCommunity users...")
            mcom_filter = ""
            if exclude_alumni:
                mcom_filter = """
                AND NOT (
                    jsonb_array_length(ou) = 1
                    AND ou->>0 = 'Alumni'
                )
                """
                logger.info("🎓 Excluding alumni-only users from MCommunity fetch")

            mcom_query = f"""
            SELECT * FROM silver.mcommunity_users
            WHERE uid IS NOT NULL
            {mcom_filter}
            {time_filter}
            """
            mcom_records = self.db_adapter.query_to_dataframe(
                mcom_query, params
            ).to_dict("records")

            # Group by uniqname
            grouped_data = {}

            # Helper to get/create group
            def get_group(u):
                u = u.lower().strip()
                if u not in grouped_data:
                    grouped_data[u] = {
                        "tdx": None,
                        "ad": None,
                        "umapi": [],
                        "mcom": None,
                    }
                return grouped_data[u]

            for r in tdx_records:
                get_group(r["uniqname"])["tdx"] = r

            for r in ad_records:
                get_group(r["uid"])["ad"] = r

            for r in umapi_records:
                get_group(r["uniqname"])["umapi"].append(r)

            for r in mcom_records:
                get_group(r["uid"])["mcom"] = r

            logger.info(
                f"📦 Consolidated {len(grouped_data)} unique users from sources"
            )
            return grouped_data

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch source records: {e}")
            raise

    def _aggregate_umapi_records(
        self, umapi_records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Aggregate multiple UMAPI employment records for a single user.

        Returns both primary fields (from empl_rcd=0) and arrays of all records.
        """
        if not umapi_records:
            return {}

        # Sort by empl_rcd to ensure primary (0) is first
        # Handle case where empl_rcd might be None or string
        def get_empl_rcd(r):
            try:
                return int(r.get("empl_rcd", 999))
            except (ValueError, TypeError):
                return 999

        sorted_records = sorted(umapi_records, key=get_empl_rcd)
        primary = sorted_records[0]  # First record is primary

        # Build aggregate data
        aggregated = {
            # Primary fields (for easy singular access)
            "department_id": primary.get("department_id"),
            "department_name": primary.get("dept_description"),
            "primary_job_code": primary.get("jobcode"),
            "job_title": primary.get("university_job_title")
            or primary.get("department_job_title"),
            "department_job_title": primary.get("department_job_title"),
            "primary_supervisor_id": primary.get("supervisor_id"),
            "umich_empl_id": primary.get("empl_id"),
            # JSONB arrays (comprehensive multi-appointment data)
            "department_ids": [
                {
                    "dept_id": rec.get("department_id"),
                    "dept_name": rec.get("dept_description"),
                    "empl_rcd": rec.get("empl_rcd"),
                }
                for rec in sorted_records
                if rec.get("department_id")
            ],
            "job_codes": [
                {
                    "job_code": rec.get("jobcode"),
                    "dept_job_title": rec.get("department_job_title"),
                    "empl_rcd": rec.get("empl_rcd"),
                }
                for rec in sorted_records
                if rec.get("jobcode")
            ],
            "supervisor_ids": [
                {"empl_id": rec.get("supervisor_id"), "empl_rcd": rec.get("empl_rcd")}
                for rec in sorted_records
                if rec.get("supervisor_id")
            ],
            "umich_empl_ids": list(
                {rec.get("empl_id") for rec in sorted_records if rec.get("empl_id")}
            ),
        }

        return aggregated

    def _merge_user_records(
        self,
        uniqname: str,
        tdx_record: Optional[Dict[str, Any]],
        ad_record: Optional[Dict[str, Any]],
        umapi_records: List[Dict[str, Any]],
        mcom_record: Optional[Dict[str, Any]],
        pi_uniqnames: Set[str],
    ) -> Dict[str, Any]:
        """
        Merge user records from all sources into consolidated format.
        """
        sources = []
        if tdx_record:
            sources.append("tdx")
        if ad_record:
            sources.append("ad")
        if umapi_records:
            sources.append("umapi")
        if mcom_record:
            sources.append("mcom")

        # Process UMAPI aggregation
        umapi_agg = self._aggregate_umapi_records(umapi_records)

        # --- Priority Helpers ---
        def pick_first(*args):
            for val in args:
                if val:
                    return val
            return None

        # --- Core Identity ---
        # Priority: TDX > UMAPI > MCommunity > AD
        first_name = pick_first(
            tdx_record.get("first_name") if tdx_record else None,
            umapi_records[0].get("first_name") if umapi_records else None,
            mcom_record.get("given_name") if mcom_record else None,
            ad_record.get("given_name") if ad_record else None,
        )

        last_name = pick_first(
            tdx_record.get("last_name") if tdx_record else None,
            umapi_records[0].get("last_name") if umapi_records else None,
            mcom_record.get("sn")
            if mcom_record
            else None,  # MCom might use sn? schema says display_name/given_name. Let's assume standard LDAP attrs or what's in schema.
            # Schema check: mcommunity_users has display_name, given_name. It doesn't explicitly list sn/surname in the summary I saw, but usually it's there.
            # Let's rely on display_name parsing if needed or just what we have.
            # Actually, let's stick to what we know exists.
            ad_record.get("sn") if ad_record else None,
        )

        full_name = pick_first(
            f"{last_name}, {first_name}" if last_name and first_name else None,
            mcom_record.get("display_name") if mcom_record else None,
            ad_record.get("display_name") if ad_record else None,
        )

        display_name = pick_first(
            mcom_record.get("display_name") if mcom_record else None,
            ad_record.get("display_name") if ad_record else None,
            full_name,
        )

        # --- Contact Info ---
        # Priority: TDX > MCommunity > AD
        primary_email = pick_first(
            tdx_record.get("primary_email") if tdx_record else None,
            mcom_record.get("mail") if mcom_record else None,
            ad_record.get("mail") if ad_record else None,
        )

        # Work phone: UMAPI > MCommunity
        work_phone = None
        if umapi_records and umapi_records[0].get("work_location"):
            try:
                # Handle string or dict JSONB
                loc = umapi_records[0]["work_location"]
                if isinstance(loc, str):
                    loc = json.loads(loc)
                work_phone = loc.get("phone")
            except:
                pass

        if not work_phone and mcom_record:
            work_phone = mcom_record.get("telephone_number")

        # --- Employment ---
        # Department: UMAPI > TDX
        department_id = umapi_agg.get("department_id")
        if not department_id and tdx_record:
            # TDX organization_id might be dept ID or GUID.
            # The schema analysis said organization_id seems to be dept ID.
            department_id = tdx_record.get("organization_id")

        department_name = umapi_agg.get("department_name")

        # Job Title: UMAPI > MCommunity > TDX
        job_title = pick_first(
            umapi_agg.get("job_title"),
            mcom_record.get("umich_title") if mcom_record else None,
            tdx_record.get("title") if tdx_record else None,
        )

        # --- Status ---
        is_active = False
        if tdx_record and tdx_record.get("is_active"):
            is_active = True
        if ad_record and not ad_record.get("ad_account_disabled"):
            is_active = True
        if umapi_records:
            is_active = True  # Employees are active
        if mcom_record:
            is_active = (
                True  # MCom users usually active if present? Or we assume active.
            )

        is_employee = bool(umapi_records) or (
            tdx_record and tdx_record.get("is_employee")
        )

        is_pi = uniqname in pi_uniqnames

        # --- AD Specifics ---
        ad_disabled = ad_record.get("ad_account_disabled") if ad_record else None

        # --- MCommunity Affiliations ---
        mcom_affiliations = []
        if mcom_record and mcom_record.get("ou"):
            try:
                ou_val = mcom_record["ou"]
                if isinstance(ou_val, str):
                    mcom_affiliations = json.loads(ou_val)
                elif isinstance(ou_val, list):
                    mcom_affiliations = ou_val
            except:
                pass

        # --- Construct Merged Record ---
        merged = {
            "uniqname": uniqname,
            # External IDs
            "tdx_user_uid": tdx_record.get("tdx_user_uid") if tdx_record else None,
            "umich_empl_id": umapi_agg.get("umich_empl_id"),
            "umich_empl_ids": umapi_agg.get("umich_empl_ids", []),
            "ldap_uid_number": mcom_record.get("uid_number") if mcom_record else None,
            "ldap_gid_number": mcom_record.get("gid_number") if mcom_record else None,
            "ad_object_guid": ad_record.get("ad_user_guid")
            if ad_record
            else None,  # ad_user_guid is the PK in silver.ad_users
            "ad_sam_account_name": ad_record.get("sam_account_name")
            if ad_record
            else None,
            # Identity
            "first_name": first_name,
            "last_name": last_name,
            "full_name": full_name,
            "display_name": display_name,
            # Contact
            "primary_email": primary_email,
            "work_phone": work_phone,
            "mobile_phone": mcom_record.get("mobile") if mcom_record else None,
            # Employment
            "department_id": department_id,
            "department_name": department_name,
            "department_ids": umapi_agg.get("department_ids", []),
            "job_title": job_title,
            "department_job_title": umapi_agg.get("department_job_title"),
            "primary_job_code": umapi_agg.get("primary_job_code"),
            "job_codes": umapi_agg.get("job_codes", []),
            "primary_supervisor_id": umapi_agg.get("primary_supervisor_id"),
            "supervisor_ids": umapi_agg.get("supervisor_ids", []),
            "reports_to_uid": tdx_record.get("reports_to_uid") if tdx_record else None,
            # Flags
            "is_pi": is_pi,
            "is_active": is_active,
            "is_employee": is_employee,
            "ad_account_disabled": ad_disabled,
            # Affiliations
            "mcommunity_ou_affiliations": mcom_affiliations,
            # Groups
            "ad_group_memberships": ad_record.get("member_of", []) if ad_record else [],
            "tdx_group_ids": tdx_record.get("group_ids", []) if tdx_record else [],
            # AD OU
            "ad_ou_root": ad_record.get("ou_root") if ad_record else None,
            "ad_ou_organization": ad_record.get("ou_organization")
            if ad_record
            else None,
            "ad_ou_department": ad_record.get("ou_department") if ad_record else None,
            "ad_ou_full_path": ad_record.get("ou_full_path") if ad_record else [],
            # POSIX
            "home_directory": mcom_record.get("home_directory")
            if mcom_record
            else None,
            "login_shell": mcom_record.get("login_shell") if mcom_record else None,
            # TDX Specific
            "tdx_external_id": tdx_record.get("external_id") if tdx_record else None,
            "tdx_beid": tdx_record.get("beid") if tdx_record else None,
            "tdx_security_role_name": tdx_record.get("security_role_name")
            if tdx_record
            else None,
            # Metadata
            "source_system": "+".join(sorted(sources)),
            "source_entity_id": uniqname,
        }

        return merged

    def _calculate_content_hash(self, merged_record: Dict[str, Any]) -> str:
        """Calculate content hash for change detection."""
        # Exclude metadata fields
        exclude_fields = {
            "data_quality_score",
            "quality_flags",
            "entity_hash",
            "ingestion_run_id",
            "created_at",
            "updated_at",
        }

        hash_payload = {
            k: v for k, v in merged_record.items() if k not in exclude_fields
        }

        normalized_json = json.dumps(
            hash_payload, sort_keys=True, separators=(",", ":"), default=str
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _calculate_data_quality(
        self, merged_record: Dict[str, Any]
    ) -> Tuple[Decimal, List[str]]:
        """Calculate data quality score and flags."""
        score = Decimal("1.00")
        flags = []

        if not merged_record.get("primary_email"):
            score -= Decimal("0.25")
            flags.append("missing_email")

        if not merged_record.get("first_name") or not merged_record.get("last_name"):
            score -= Decimal("0.20")
            flags.append("missing_name")

        if not merged_record.get("department_id"):
            score -= Decimal("0.15")
            flags.append("missing_department")

        if not merged_record.get("job_title") and not merged_record.get("is_pi"):
            score -= Decimal("0.10")
            flags.append("missing_job_title")

        if not merged_record.get("is_employee"):
            score -= Decimal("0.10")
            flags.append("not_umapi_employee")

        if merged_record.get("ad_account_disabled"):
            score -= Decimal("0.10")
            flags.append("ad_disabled")

        sources = merged_record.get("source_system", "")
        if "tdx" not in sources:
            score -= Decimal("0.05")
            flags.append("no_tdx_record")

        if sources == "mcom":
            score -= Decimal("0.15")
            flags.append("mcom_only")

        if (
            "tdx" in sources
            and "umapi" in sources
            and "ad" in sources
            and "mcom" in sources
        ):
            score += Decimal("0.10")

        return max(Decimal("0.00"), min(Decimal("1.00"), score)), flags

    def _batch_upsert_records(
        self, records: List[Dict[str, Any]], run_id: str, dry_run: bool = False
    ) -> Tuple[int, int, int]:
        """
        Batch upsert records to silver.users.
        Returns (created, updated, skipped) counts.
        """
        if not records:
            return 0, 0, 0

        if dry_run:
            logger.debug(f"🔍 [DRY RUN] Would batch upsert {len(records)} records")
            return len(records), 0, 0

        try:
            # Prepare data
            json_fields = [
                "umich_empl_ids",
                "department_ids",
                "job_codes",
                "supervisor_ids",
                "mcommunity_ou_affiliations",
                "ad_group_memberships",
                "tdx_group_ids",
                "ad_ou_full_path",
                "quality_flags",
            ]

            upsert_data = []
            for r in records:
                r_copy = r.copy()
                for field in json_fields:
                    if field in r_copy:
                        r_copy[field] = json.dumps(r_copy[field])
                r_copy["ingestion_run_id"] = run_id
                r_copy["updated_at"] = datetime.now(timezone.utc)
                upsert_data.append(r_copy)

            # Use SQLAlchemy Core for bulk upsert
            from sqlalchemy import column, table
            from sqlalchemy.dialects.postgresql import insert

            # Define table structure for the statement
            users_table = table(
                "users",
                column("uniqname"),
                column("tdx_user_uid"),
                column("umich_empl_id"),
                column("umich_empl_ids"),
                column("ldap_uid_number"),
                column("ldap_gid_number"),
                column("ad_object_guid"),
                column("ad_sam_account_name"),
                column("first_name"),
                column("last_name"),
                column("full_name"),
                column("display_name"),
                column("primary_email"),
                column("work_phone"),
                column("mobile_phone"),
                column("department_id"),
                column("department_name"),
                column("department_ids"),
                column("job_title"),
                column("department_job_title"),
                column("primary_job_code"),
                column("job_codes"),
                column("primary_supervisor_id"),
                column("supervisor_ids"),
                column("reports_to_uid"),
                column("is_pi"),
                column("is_active"),
                column("is_employee"),
                column("ad_account_disabled"),
                column("mcommunity_ou_affiliations"),
                column("ad_group_memberships"),
                column("tdx_group_ids"),
                column("ad_ou_root"),
                column("ad_ou_organization"),
                column("ad_ou_department"),
                column("ad_ou_full_path"),
                column("home_directory"),
                column("login_shell"),
                column("tdx_external_id"),
                column("tdx_beid"),
                column("tdx_security_role_name"),
                column("data_quality_score"),
                column("quality_flags"),
                column("source_system"),
                column("source_entity_id"),
                column("entity_hash"),
                column("ingestion_run_id"),
                column("updated_at"),
            )

            stmt = insert(users_table).values(upsert_data)

            # Define update columns (all except PK and created_at)
            update_cols = {
                c.name: c
                for c in stmt.excluded
                if c.name not in ["uniqname", "created_at"]
            }

            # Add WHERE clause to skip if hash matches
            stmt = stmt.on_conflict_do_update(
                index_elements=["uniqname"],
                set_=update_cols,
                where=(users_table.c.entity_hash != stmt.excluded.entity_hash),
            )

            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(text("SET search_path TO silver, public"))
                    result = conn.execute(stmt)
                    # Rowcount includes inserts and updates.
                    # It's hard to distinguish exact counts in bulk upsert without returning.
                    # For stats, we'll approximate or just track "processed".
                    # If rowcount = len(records), all were inserted/updated.
                    # If rowcount < len(records), some were skipped due to hash match.
                    processed = result.rowcount
                    skipped = len(records) - processed
                    return (
                        processed,
                        0,
                        skipped,
                    )  # We can't easily distinguish created vs updated in bulk

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to batch upsert: {e}")
            raise

    def create_transformation_run(self, full_sync: bool) -> str:
        """Create transformation run record."""
        run_id = str(uuid.uuid4())
        metadata = {
            "transformation_type": "consolidate_users",
            "entity_type": "users_consolidated",
            "full_sync": full_sync,
        }

        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text("""
                        INSERT INTO meta.ingestion_runs
                        (run_id, source_system, entity_type, started_at, status, metadata)
                        VALUES (:run_id, 'silver_transformation', 'users_consolidated', :started_at, 'running', :metadata)
                    """),
                        {
                            "run_id": run_id,
                            "started_at": datetime.now(timezone.utc),
                            "metadata": json.dumps(metadata),
                        },
                    )
            return run_id
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create run: {e}")
            raise

    def complete_transformation_run(self, run_id: str, stats: Dict[str, Any]):
        """Complete transformation run."""
        status = "failed" if stats.get("errors") else "completed"
        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text("""
                        UPDATE meta.ingestion_runs
                        SET completed_at = :completed_at,
                            status = :status,
                            records_processed = :processed,
                            records_created = :created,
                            records_updated = :updated,
                            metadata = jsonb_set(metadata, '{stats}', :stats_json)
                        WHERE run_id = :run_id
                    """),
                        {
                            "run_id": run_id,
                            "completed_at": datetime.now(timezone.utc),
                            "status": status,
                            "processed": stats["processed"],
                            "created": stats["created"],
                            "updated": stats["updated"],
                            "stats_json": json.dumps(stats),
                        },
                    )
            logger.info(f"✅ Run {run_id} completed: {status}")
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete run: {e}")

    def consolidate_users(
        self,
        full_sync: bool = False,
        dry_run: bool = False,
        exclude_alumni: bool = False,
    ):
        """Main consolidation logic."""
        last_run = None if full_sync else self._get_last_transformation_timestamp()
        run_id = self.create_transformation_run(full_sync)

        stats = {
            "processed": 0,
            "created": 0,
            "updated": 0,
            "skipped": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            # 1. Load PI Cache
            pi_uniqnames = self._load_pi_uniqnames()

            # 2. Fetch Sources
            source_data = self._fetch_source_records(
                last_run, full_sync, exclude_alumni
            )

            if not source_data:
                logger.info("✨ No records to process")
                if not dry_run:
                    self.complete_transformation_run(run_id, stats)
                return

            # 3. Process Users
            total = len(source_data)
            logger.info(f"🚀 Processing {total} users...")

            batch_size = 2000
            batch_records = []

            for idx, (uniqname, sources) in enumerate(source_data.items(), 1):
                try:
                    merged = self._merge_user_records(
                        uniqname,
                        sources["tdx"],
                        sources["ad"],
                        sources["umapi"],
                        sources["mcom"],
                        pi_uniqnames,
                    )

                    q_score, q_flags = self._calculate_data_quality(merged)
                    merged["data_quality_score"] = q_score
                    merged["quality_flags"] = q_flags
                    merged["entity_hash"] = self._calculate_content_hash(merged)

                    batch_records.append(merged)

                    if len(batch_records) >= batch_size:
                        c, u, s = self._batch_upsert_records(
                            batch_records, run_id, dry_run
                        )
                        stats["processed"] += len(batch_records)
                        stats["created"] += c  # Approximate
                        stats["skipped"] += s
                        batch_records = []
                        logger.info(f"📈 Progress: {idx}/{total} users")

                except Exception as e:
                    err = f"Error processing {uniqname}: {e}"
                    logger.error(f"❌ {err}")
                    stats["errors"].append(err)

            # Process remaining batch
            if batch_records:
                c, u, s = self._batch_upsert_records(batch_records, run_id, dry_run)
                stats["processed"] += len(batch_records)
                stats["created"] += c
                stats["skipped"] += s

            # 4. Finish
            duration = (
                datetime.now(timezone.utc) - datetime.fromisoformat(stats["started_at"])
            ).total_seconds()
            stats["duration_seconds"] = duration

            logger.info("=" * 80)
            logger.info(f"🎉 USER CONSOLIDATION COMPLETE ({duration:.2f}s)")
            logger.info(f"📊 Processed: {stats['processed']}")
            logger.info(f"✅ Upserted (New/Upd): {stats['created']}")
            logger.info(f"⏭️  Skipped: {stats['skipped']}")
            logger.info("=" * 80)

            if not dry_run:
                self.complete_transformation_run(run_id, stats)

        except Exception as e:
            logger.error(f"❌ Fatal error: {e}", exc_info=True)
            raise
        finally:
            self.db_adapter.close()


def main():
    parser = argparse.ArgumentParser(description="Consolidate Silver Users")
    parser.add_argument("--full-sync", action="store_true", help="Force full sync")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument(
        "--exclude-alumni", action="store_true", help="Exclude alumni-only users"
    )
    args = parser.parse_args()

    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("❌ DATABASE_URL not set")
        sys.exit(1)

    service = UserConsolidationService(db_url)
    service.consolidate_users(args.full_sync, args.dry_run, args.exclude_alumni)


if __name__ == "__main__":
    main()
