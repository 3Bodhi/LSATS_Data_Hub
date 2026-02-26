#!/usr/bin/env python3
"""
Consolidated Groups Silver Layer Transformation Service

Transforms source-specific silver group records (ad_groups + mcommunity_groups)
into consolidated silver.groups table.

Key features:
- Merges data from AD and MCommunity sources
- Natural CN-based business keys (no prefixes)
- All 1,129 CN overlaps merged into single records
- Data quality scoring
- Incremental processing with --full-sync override
- Comprehensive logging with em

oji standards
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


class GroupConsolidationService:
    """
    Service for consolidating group records from AD and MCommunity into silver.groups.
    
    Merge Strategy:
    - group_id: Natural CN/group_name (clean, no prefixes)
    - All 1,129 CN overlaps: Merged records (source_system = 'ad+mcommunity')
    - AD-only: 7,597 groups (source_system = 'ad')
    - MCommunity-only: 32,671 groups (source_system = 'mcommunity')
    - Total expected: 41,397 groups
    
    Field Priority:
    - group_name: AD.name > MCommunity.group_name
    - group_email: MCommunity.group_email > AD.mail
    - description: MCommunity.description > AD.description
    - members: UNION of both sources, deduplicated
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
        logger.info("✨ Group consolidation service initialized")

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
              AND entity_type = 'groups_consolidated'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"⏰ Last successful consolidation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info("🆕 No previous consolidation found - processing all groups")
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️ Could not determine last consolidation timestamp: {e}")
            return None

    def _fetch_source_records(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Fetch group records from both source-specific tables.

        Args:
            since_timestamp: Only fetch records updated after this time
            full_sync: Ignore timestamp and fetch all records

        Returns:
            Tuple of (ad_records, mcommunity_records)
        """
        try:
            time_filter = ""
            params = {}

            if since_timestamp and not full_sync:
                time_filter = "WHERE updated_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp
                logger.info(f"📊 Fetching records updated after {since_timestamp}")
            else:
                logger.info("📊 Fetching all group records (full sync)")

            # Fetch AD groups
            ad_query = f"""
            SELECT *
            FROM silver.ad_groups
            {time_filter}
            ORDER BY updated_at
            """
            ad_df = self.db_adapter.query_to_dataframe(ad_query, params)
            ad_records = ad_df.to_dict("records") if not ad_df.empty else []

            # Fetch MCommunity groups
            mcom_query = f"""
            SELECT *
            FROM silver.mcommunity_groups
            {time_filter}
            ORDER BY updated_at
            """
            mcom_df = self.db_adapter.query_to_dataframe(mcom_query, params)
            mcom_records = mcom_df.to_dict("records") if not mcom_df.empty else []

            logger.info(f"📦 Found {len(ad_records)} AD + {len(mcom_records)} MCommunity groups")

            return ad_records, mcom_records

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch source records: {e}")
            raise

    def _detect_cn_overlaps(
        self, ad_records: List[Dict[str, Any]], mcom_records: List[Dict[str, Any]]
    ) -> Set[str]:
        """
        Detect CN overlaps between AD and MCommunity groups.
        
        Args:
            ad_records: AD group records
            mcom_records: MCommunity group records
            
        Returns:
            Set of normalized CNs that appear in both systems
        """
        # Normalize CNs for matching
        ad_cns = {
            r["cn"].lower().strip(): r["cn"] 
            for r in ad_records 
            if r.get("cn")
        }
        
        mcom_names = {
            r["group_name"].lower().strip(): r["group_name"]
            for r in mcom_records
            if r.get("group_name")
        }
        
        # Find overlaps (case-insensitive)
        overlap_cns_normalized = set(ad_cns.keys()) & set(mcom_names.keys())
        
        # Return original casing from AD (canonical)
        overlaps = {ad_cns[normalized_cn] for normalized_cn in overlap_cns_normalized}
        
        logger.info(f"🔍 Detected {len(overlaps)} CN overlaps between systems")
        
        return overlaps

    def _merge_group_records(
        self,
        ad_record: Optional[Dict[str, Any]],
        mcom_record: Optional[Dict[str, Any]],
        is_overlap: bool,
    ) -> Dict[str, Any]:
        """
        Merge AD and MCommunity group records into consolidated format.

        Merge Priority:
        - group_id: Natural CN (AD.cn or MCommunity.group_name)
        - group_name: AD.name > MCommunity.group_name
        - group_email: MCommunity.group_email > AD.mail
        - description: MCommunity.description > AD.description
        - members: UNION of both, deduplicated

        Args:
            ad_record: Record from silver.ad_groups (or None)
            mcom_record: Record from silver.mcommunity_groups (or None)
            is_overlap: True if this CN exists in both systems

        Returns:
            Merged group record
        """
        sources = []
        
        # Determine group_id (natural CN/name)
        group_id = None
        if ad_record:
            group_id = ad_record.get("cn")
            sources.append("ad")
        if mcom_record:
            if not group_id:
                group_id = mcom_record.get("group_name")
            sources.append("mcommunity")

        if not group_id:
            raise ValueError("Cannot merge records without group_id (CN/name)")

        # Merge members (deduplicate)
        members = []
        if ad_record and ad_record.get("members"):
            members.extend(ad_record["members"] if isinstance(ad_record["members"], list) else [])
        if mcom_record and mcom_record.get("members"):
            mcom_members = mcom_record["members"] if isinstance(mcom_record["members"], list) else []
            members.extend([m for m in mcom_members if m not in members])

        # Merge owners
        owners = []
        if mcom_record and mcom_record.get("owners"):
            owners = mcom_record["owners"] if isinstance(mcom_record["owners"], list) else []
        # Add AD managed_by to owners if present
        if ad_record and ad_record.get("managed_by"):
            if ad_record["managed_by"] not in owners:
                owners.append(ad_record["managed_by"])

        # Detect MCommADSync
        is_mcomm_adsync = False
        if ad_record and ad_record.get("distinguished_name"):
            is_mcomm_adsync = "OU=MCommADSync" in ad_record["distinguished_name"]

        merged = {
            # Primary key
            "group_id": group_id,
            
            # External system identifiers
            "ad_group_guid": ad_record.get("ad_group_guid") if ad_record else None,
            "mcommunity_group_uid": mcom_record.get("mcommunity_group_uid") if mcom_record else None,
            
            # Core identity (merged with priority)
            "group_name": (
                ad_record.get("name") if ad_record
                else mcom_record.get("group_name") if mcom_record
                else group_id
            ),
            "group_email": (
                mcom_record.get("group_email") if mcom_record
                else ad_record.get("mail") if ad_record
                else None
            ),
            "sam_account_name": ad_record.get("sam_account_name") if ad_record else None,
            "cn": group_id,
            "distinguished_name": (
                ad_record.get("distinguished_name") if ad_record
                else mcom_record.get("distinguished_name") if mcom_record
                else None
            ),
            "description": (
                mcom_record.get("description") if mcom_record and mcom_record.get("description")
                else ad_record.get("description") if ad_record
                else None
            ),
            "display_name": ad_record.get("display_name") if ad_record else None,
            
            # Group type & classification
            "group_type": ad_record.get("group_type") if ad_record else None,
            "is_security_group": self._is_security_group(ad_record) if ad_record else None,
            "is_distribution_group": self._is_distribution_group(ad_record, mcom_record),
            "sam_account_type": ad_record.get("sam_account_type") if ad_record else None,
            "object_category": ad_record.get("object_category") if ad_record else None,
            
            # MCommunity-specific flags
            "is_private": mcom_record.get("is_private") if mcom_record else None,
            "is_members_only": mcom_record.get("is_members_only") if mcom_record else None,
            "is_joinable": mcom_record.get("is_joinable") if mcom_record else None,
            "expiry_timestamp": mcom_record.get("expiry_timestamp") if mcom_record else None,
            
            # MCommADSync detection
            "is_mcomm_adsync": is_mcomm_adsync,
            
            # Organization hierarchy (from AD)
            "ou_root": ad_record.get("ou_root") if ad_record else None,
            "ou_organization": ad_record.get("ou_organization") if ad_record else None,
            "ou_department": ad_record.get("ou_department") if ad_record else None,
            "ou_category": ad_record.get("ou_category") if ad_record else None,
            "ou_immediate_parent": ad_record.get("ou_immediate_parent") if ad_record else None,
            "ou_full_path": ad_record.get("ou_full_path") if ad_record else [],
            "ou_depth": ad_record.get("ou_depth") if ad_record else None,
            "parent_ou_dn": ad_record.get("parent_ou_dn") if ad_record else None,
            
            # Membership (merged)
            "members": members,
            "owners": owners,
            "member_of": ad_record.get("member_of", []) if ad_record else [],
            "direct_members": mcom_record.get("direct_members", []) if mcom_record else [],
            "nested_members": mcom_record.get("nested_members", []) if mcom_record else [],
            
            # Management
            "managed_by": ad_record.get("managed_by") if ad_record else None,
            
            # Contact info
            "contact_info": mcom_record.get("contact_info", {}) if mcom_record else {},
            "proxy_addresses": ad_record.get("proxy_addresses") if ad_record else None,
            
            # Timestamps (earliest/latest)
            "when_created": self._earliest_timestamp(
                ad_record.get("when_created") if ad_record else None,
                mcom_record.get("created_at") if mcom_record else None
            ),
            "when_changed": self._latest_timestamp(
                ad_record.get("when_changed") if ad_record else None,
                mcom_record.get("updated_at") if mcom_record else None
            ),
            
            # Traceability
            "ad_raw_id": ad_record.get("raw_id") if ad_record else None,
            "mcommunity_raw_id": mcom_record.get("raw_id") if mcom_record else None,
            
            # Metadata
            "sources": sources,
            "source_system": "+".join(sources) if sources else "unknown",
            "source_entity_id": group_id,
        }

        return merged

    def _is_security_group(self, ad_record: Optional[Dict[str, Any]]) -> Optional[bool]:
        """Determine if AD group is a security group based on groupType."""
        if not ad_record or not ad_record.get("group_type"):
            return None
        # AD groupType: bit 0x80000000 = security group
        return bool(ad_record["group_type"] & 0x80000000)

    def _is_distribution_group(
        self, ad_record: Optional[Dict[str, Any]], mcom_record: Optional[Dict[str, Any]]
    ) -> Optional[bool]:
        """Determine if group is a distribution group."""
        if ad_record and ad_record.get("group_type"):
            # If it's not a security group, it's distribution
            return not self._is_security_group(ad_record)
        if mcom_record:
            # MCommunity groups are typically distribution groups
            return True
        return None

    def _earliest_timestamp(self, ts1: Optional[datetime], ts2: Optional[datetime]) -> Optional[datetime]:
        """Return the earliest of two timestamps."""
        if ts1 and ts2:
            return min(ts1, ts2)
        return ts1 or ts2

    def _latest_timestamp(self, ts1: Optional[datetime], ts2: Optional[datetime]) -> Optional[datetime]:
        """Return the latest of two timestamps."""
        if ts1 and ts2:
            return max(ts1, ts2)
        return ts1 or ts2

    def _calculate_content_hash(self, merged_record: Dict[str, Any]) -> str:
        """
        Calculate content hash for change detection.

        Args:
            merged_record: The merged group record

        Returns:
            SHA-256 hash string
        """
        # Include significant fields in hash (exclude metadata)
        significant_fields = {
            "group_id": merged_record.get("group_id"),
            "group_name": merged_record.get("group_name"),
            "group_email": merged_record.get("group_email"),
            "description": merged_record.get("description"),
            "is_mcomm_adsync": merged_record.get("is_mcomm_adsync"),
            "ou_department": merged_record.get("ou_department"),
            "members": merged_record.get("members"),
            "owners": merged_record.get("owners"),
            "is_private": merged_record.get("is_private"),
            "is_security_group": merged_record.get("is_security_group"),
        }

        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":"), default=str
        )
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        return content_hash

    def _calculate_data_quality(
        self, merged_record: Dict[str, Any]
    ) -> Tuple[Decimal, List[str]]:
        """
        Calculate data quality score and identify quality flags.

        Scoring criteria (start at 1.00):
        - Missing group_name: -0.30
        - Missing group_email (for MCommunity): -0.10
        - No members: -0.10
        - Missing description: -0.05
        - Single-source only (not merged): -0.05
        - Bonus for merged (both sources): +0.10

        Args:
            merged_record: The merged group record

        Returns:
            Tuple of (quality_score, quality_flags_list)
        """
        score = Decimal("1.00")
        flags = []

        # Critical fields
        if not merged_record.get("group_name"):
            score -= Decimal("0.30")
            flags.append("missing_group_name")

        # Email (important for MCommunity groups)
        if "mcommunity" in merged_record.get("sources", []) and not merged_record.get("group_email"):
            score -= Decimal("0.10")
            flags.append("missing_group_email")

        # Membership
        members = merged_record.get("members", [])
        if not members or len(members) == 0:
            score -= Decimal("0.10")
            flags.append("no_members")

        # Description
        if not merged_record.get("description"):
            score -= Decimal("0.05")
            flags.append("missing_description")

        # Source completeness
        sources = merged_record.get("sources", [])
        if len(sources) == 1:
            score -= Decimal("0.05")
            flags.append(f"{sources[0]}_only")
        elif len(sources) > 1:
            # Bonus for merged groups
            score = min(Decimal("1.00"), score + Decimal("0.10"))
            flags.append("merged_from_multiple_sources")

        # Ensure score doesn't go below 0
        score = max(Decimal("0.00"), score)

        return score, flags

    def _upsert_consolidated_record(
        self, merged_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> str:
        """
        Insert or update a silver.groups record.

        Args:
            merged_record: The merged record to upsert
            run_id: The current transformation run ID
            dry_run: If True, only log what would be done

        Returns:
            Action taken: 'created', 'updated', or 'skipped'
        """
        group_id = merged_record["group_id"]

        if dry_run:
            logger.info(
                f"🔍 [DRY RUN] Would upsert group: "
                f"id={group_id}, "
                f"name={merged_record['group_name']}, "
                f"sources={merged_record['sources']}, "
                f"quality={merged_record['data_quality_score']}"
            )
            return "dry_run"

        try:
            # Check if exists and compare hash
            check_query = """
            SELECT entity_hash 
            FROM silver.groups 
            WHERE group_id = :group_id
            """
            existing_df = self.db_adapter.query_to_dataframe(
                check_query, {"group_id": group_id}
            )

            is_new = existing_df.empty
            existing_hash = None if is_new else existing_df.iloc[0]["entity_hash"]

            # Skip if unchanged
            if not is_new and existing_hash == merged_record["entity_hash"]:
                logger.debug(f"⏭️ Group unchanged, skipping: {group_id}")
                return "skipped"

            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    upsert_query = text("""
                        INSERT INTO silver.groups (
                            group_id, ad_group_guid, mcommunity_group_uid,
                            group_name, group_email, sam_account_name, cn, distinguished_name,
                            description, display_name,
                            group_type, is_security_group, is_distribution_group, sam_account_type, object_category,
                            is_private, is_members_only, is_joinable, expiry_timestamp,
                            is_mcomm_adsync,
                            ou_root, ou_organization, ou_department, ou_category, ou_immediate_parent,
                            ou_full_path, ou_depth, parent_ou_dn,
                            members, owners, member_of, direct_members, nested_members,
                            managed_by, contact_info, proxy_addresses,
                            when_created, when_changed,
                            data_quality_score, quality_flags,
                            source_system, source_entity_id, entity_hash,
                            ad_raw_id, mcommunity_raw_id,
                            ingestion_run_id, updated_at
                        ) VALUES (
                            :group_id, :ad_group_guid, :mcommunity_group_uid,
                            :group_name, :group_email, :sam_account_name, :cn, :distinguished_name,
                            :description, :display_name,
                            :group_type, :is_security_group, :is_distribution_group, :sam_account_type, :object_category,
                            :is_private, :is_members_only, :is_joinable, :expiry_timestamp,
                            :is_mcomm_adsync,
                            :ou_root, :ou_organization, :ou_department, :ou_category, :ou_immediate_parent,
                            CAST(:ou_full_path AS jsonb), :ou_depth, :parent_ou_dn,
                            CAST(:members AS jsonb), CAST(:owners AS jsonb), CAST(:member_of AS jsonb),
                            CAST(:direct_members AS jsonb), CAST(:nested_members AS jsonb),
                            :managed_by, CAST(:contact_info AS jsonb), :proxy_addresses,
                            :when_created, :when_changed,
                            :data_quality_score, CAST(:quality_flags AS jsonb),
                            :source_system, :source_entity_id, :entity_hash,
                            :ad_raw_id, :mcommunity_raw_id,
                            :ingestion_run_id, CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (group_id) DO UPDATE SET
                            ad_group_guid = EXCLUDED.ad_group_guid,
                            mcommunity_group_uid = EXCLUDED.mcommunity_group_uid,
                            group_name = EXCLUDED.group_name,
                            group_email = EXCLUDED.group_email,
                            sam_account_name = EXCLUDED.sam_account_name,
                            cn = EXCLUDED.cn,
                            distinguished_name = EXCLUDED.distinguished_name,
                            description = EXCLUDED.description,
                            display_name = EXCLUDED.display_name,
                            group_type = EXCLUDED.group_type,
                            is_security_group = EXCLUDED.is_security_group,
                            is_distribution_group = EXCLUDED.is_distribution_group,
                            sam_account_type = EXCLUDED.sam_account_type,
                            object_category = EXCLUDED.object_category,
                            is_private = EXCLUDED.is_private,
                            is_members_only = EXCLUDED.is_members_only,
                            is_joinable = EXCLUDED.is_joinable,
                            expiry_timestamp = EXCLUDED.expiry_timestamp,
                            is_mcomm_adsync = EXCLUDED.is_mcomm_adsync,
                            ou_root = EXCLUDED.ou_root,
                            ou_organization = EXCLUDED.ou_organization,
                            ou_department = EXCLUDED.ou_department,
                            ou_category = EXCLUDED.ou_category,
                            ou_immediate_parent = EXCLUDED.ou_immediate_parent,
                            ou_full_path = EXCLUDED.ou_full_path,
                            ou_depth = EXCLUDED.ou_depth,
                            parent_ou_dn = EXCLUDED.parent_ou_dn,
                            members = EXCLUDED.members,
                            owners = EXCLUDED.owners,
                            member_of = EXCLUDED.member_of,
                            direct_members = EXCLUDED.direct_members,
                            nested_members = EXCLUDED.nested_members,
                            managed_by = EXCLUDED.managed_by,
                            contact_info = EXCLUDED.contact_info,
                            proxy_addresses = EXCLUDED.proxy_addresses,
                            when_created = EXCLUDED.when_created,
                            when_changed = EXCLUDED.when_changed,
                            data_quality_score = EXCLUDED.data_quality_score,
                            quality_flags = EXCLUDED.quality_flags,
                            source_system = EXCLUDED.source_system,
                            source_entity_id = EXCLUDED.source_entity_id,
                            entity_hash = EXCLUDED.entity_hash,
                            ad_raw_id = EXCLUDED.ad_raw_id,
                            mcommunity_raw_id = EXCLUDED.mcommunity_raw_id,
                            ingestion_run_id = EXCLUDED.ingestion_run_id,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE silver.groups.entity_hash != EXCLUDED.entity_hash
                    """)

                    conn.execute(
                        upsert_query,
                        {
                            "group_id": merged_record["group_id"],
                            "ad_group_guid": merged_record["ad_group_guid"],
                            "mcommunity_group_uid": merged_record["mcommunity_group_uid"],
                            "group_name": merged_record["group_name"],
                            "group_email": merged_record["group_email"],
                            "sam_account_name": merged_record["sam_account_name"],
                            "cn": merged_record["cn"],
                            "distinguished_name": merged_record["distinguished_name"],
                            "description": merged_record["description"],
                            "display_name": merged_record["display_name"],
                            "group_type": merged_record["group_type"],
                            "is_security_group": merged_record["is_security_group"],
                            "is_distribution_group": merged_record["is_distribution_group"],
                            "sam_account_type": merged_record["sam_account_type"],
                            "object_category": merged_record["object_category"],
                            "is_private": merged_record["is_private"],
                            "is_members_only": merged_record["is_members_only"],
                            "is_joinable": merged_record["is_joinable"],
                            "expiry_timestamp": merged_record["expiry_timestamp"],
                            "is_mcomm_adsync": merged_record["is_mcomm_adsync"],
                            "ou_root": merged_record["ou_root"],
                            "ou_organization": merged_record["ou_organization"],
                            "ou_department": merged_record["ou_department"],
                            "ou_category": merged_record["ou_category"],
                            "ou_immediate_parent": merged_record["ou_immediate_parent"],
                            "ou_full_path": json.dumps(merged_record["ou_full_path"]),
                            "ou_depth": merged_record["ou_depth"],
                            "parent_ou_dn": merged_record["parent_ou_dn"],
                            "members": json.dumps(merged_record["members"]),
                            "owners": json.dumps(merged_record["owners"]),
                            "member_of": json.dumps(merged_record["member_of"]),
                            "direct_members": json.dumps(merged_record["direct_members"]),
                            "nested_members": json.dumps(merged_record["nested_members"]),
                            "managed_by": merged_record["managed_by"],
                            "contact_info": json.dumps(merged_record["contact_info"]),
                            "proxy_addresses": merged_record["proxy_addresses"],
                            "when_created": merged_record["when_created"],
                            "when_changed": merged_record["when_changed"],
                            "data_quality_score": merged_record["data_quality_score"],
                            "quality_flags": json.dumps(merged_record["quality_flags"]),
                            "source_system": merged_record["source_system"],
                            "source_entity_id": merged_record["source_entity_id"],
                            "entity_hash": merged_record["entity_hash"],
                            "ad_raw_id": merged_record["ad_raw_id"],
                            "mcommunity_raw_id": merged_record["mcommunity_raw_id"],
                           "ingestion_run_id": run_id,
                        },
                    )

            action = "created" if is_new else "updated"
            logger.debug(
                f"✅ {action.capitalize()} group: {group_id} ({merged_record['group_name']})"
            )
            return action

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to upsert group {group_id}: {e}")
            raise

    def create_transformation_run(
        self, incremental_since: Optional[datetime] = None, full_sync: bool = False
    ) -> str:
        """
        Create a transformation run record for tracking.

        Args:
            incremental_since: Timestamp for incremental processing
            full_sync: Whether this is a full sync

        Returns:
            Run ID (UUID string)
        """
        try:
            run_id = str(uuid.uuid4())

            metadata = {
                "transformation_type": "consolidate_groups",
                "entity_type": "groups_consolidated",
                "source_tables": ["silver.ad_groups", "silver.mcommunity_groups"],
                "target_table": "silver.groups",
                "tier": "consolidated",
                "full_sync": full_sync,
                "incremental_since": incremental_since.isoformat()
                if incremental_since
                else None,
            }

            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    insert_query = text("""
                        INSERT INTO meta.ingestion_runs (
                            run_id, source_system, entity_type, started_at, status, metadata
                        ) VALUES (
                            :run_id, 'silver_transformation', 'groups_consolidated', :started_at, 'running', :metadata
                        )
                    """)

                    conn.execute(
                        insert_query,
                        {
                            "run_id": run_id,
                            "started_at": datetime.now(timezone.utc),
                            "metadata": json.dumps(metadata),
                        },
                    )

            mode = "FULL SYNC" if full_sync else "INCREMENTAL"
            logger.info(f"📝 Created transformation run {run_id} ({mode})")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create transformation run: {e}")
            raise

    def complete_transformation_run(
        self,
        run_id: str,
        records_processed: int,
        records_created: int,
        records_updated: int,
        records_skipped: int,
        error_message: Optional[str] = None,
    ):
        """
        Mark a transformation run as completed.

        Args:
            run_id: The run ID to complete
            records_processed: Total groups processed
            records_created: New records created
            records_updated: Existing records updated
            records_skipped: Records skipped (unchanged)
            error_message: Error message if run failed
        """
        try:
            status = "failed" if error_message else "completed"

            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    update_query = text("""
                        UPDATE meta.ingestion_runs
                        SET completed_at = :completed_at,
                            status = :status,
                            records_processed = :records_processed,
                            records_created = :records_created,
                            records_updated = :records_updated,
                            error_message = :error_message,
                            metadata = jsonb_set(
                                metadata,
                                '{records_skipped}',
                                to_jsonb(CAST(:records_skipped AS int))
                            )
                        WHERE run_id = :run_id
                    """)

                    conn.execute(
                        update_query,
                        {
                            "run_id": run_id,
                            "completed_at": datetime.now(timezone.utc),
                            "status": status,
                            "records_processed": records_processed,
                            "records_created": records_created,
                            "records_updated": records_updated,
                            "records_skipped": records_skipped,
                            "error_message": error_message,
                        },
                    )

            logger.info(f"✅ Completed transformation run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete transformation run: {e}")

    def consolidate_groups(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Main entry point: Consolidate AD and MCommunity groups into silver.groups.

        Process flow:
        1. Determine last successful consolidation timestamp (unless full_sync)
        2. Fetch records from silver.ad_groups and silver.mcommunity_groups
        3. Detect CN overlaps between systems (1,129 expected)
        4. For each unique group_id:
           a. Merge AD and MCommunity records (if both exist)
           b. Calculate quality score
           c. Calculate entity hash
           d. Upsert to silver.groups
        5. Track statistics and return results

        Args:
            full_sync: If True, process all groups regardless of timestamp
            dry_run: If True, preview changes without committing to database

        Returns:
            Dictionary with transformation statistics
        """
        # Get timestamp of last successful consolidation
        last_consolidation = (
            None if full_sync else self._get_last_transformation_timestamp()
        )

        # Create transformation run
        run_id = self.create_transformation_run(last_consolidation, full_sync)

        stats = {
            "run_id": run_id,
            "incremental_since": last_consolidation,
            "full_sync": full_sync,
            "dry_run": dry_run,
            "groups_processed": 0,
            "records_created": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "ad_only": 0,
            "mcommunity_only": 0,
            "merged_overlaps": 0,
            "mcomm_adsync_count": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            if dry_run:
                logger.info("⚠️ DRY RUN MODE - No changes will be committed")

            if full_sync:
                logger.info("🔄 Full sync mode: Processing ALL groups")
            elif last_consolidation:
                logger.info(
                    f"⚡ Incremental mode: Processing groups since {last_consolidation}"
                )
            else:
                logger.info("🆕 First run: Processing ALL groups")

            logger.info("🚀 Starting group consolidation...")

            # Fetch source records
            ad_records, mcom_records = self._fetch_source_records(
                last_consolidation, full_sync
            )

            # Detect CN overlaps
            overlapping_cns = self._detect_cn_overlaps(ad_records, mcom_records)

            # Create lookup dictionaries
            ad_by_cn = {}
            for r in ad_records:
                if r.get("cn"):
                    ad_by_cn[r["cn"].lower().strip()] = r

            mcom_by_name = {}
            for r in mcom_records:
                if r.get("group_name"):
                    mcom_by_name[r["group_name"].lower().strip()] = r

            # Get all unique group identifiers (normalized for matching)
            all_cns_normalized = set(ad_by_cn.keys()) | set(mcom_by_name.keys())

            if not all_cns_normalized:
                logger.info("✨ All records up to date - no consolidation needed")
                if not dry_run:
                    self.complete_transformation_run(run_id, 0, 0, 0, 0)
                return stats

            logger.info(f"📊 Processing {len(all_cns_normalized)} unique groups")
            logger.info(f"   - Expected overlaps: {len(overlapping_cns)}")
            logger.info(f"   - AD-only: ~{len(ad_by_cn) - len(overlapping_cns)}")
            logger.info(f"   - MCommunity-only: ~{len(mcom_by_name) - len(overlapping_cns)}")

            # Process each unique group
            for idx, cn_normalized in enumerate(sorted(all_cns_normalized), 1):
                try:
                    ad_record = ad_by_cn.get(cn_normalized)
                    mcom_record = mcom_by_name.get(cn_normalized)

                    # Get original casing from AD (canonical) or MCommunity
                    original_cn = (
                        ad_record["cn"] if ad_record 
                        else mcom_record["group_name"] if mcom_record
                        else cn_normalized
                    )

                    is_overlap = original_cn in overlapping_cns

                    # Track source distribution
                    if ad_record and mcom_record:
                        stats["merged_overlaps"] +=1
                    elif ad_record:
                        stats["ad_only"] += 1
                    elif mcom_record:
                        stats["mcommunity_only"] += 1

                    # Merge records
                    merged_record = self._merge_group_records(
                        ad_record, mcom_record, is_overlap
                    )

                    # Track MCommADSync
                    if merged_record.get("is_mcomm_adsync"):
                        stats["mcomm_adsync_count"] += 1

                    # Calculate quality
                    quality_score, quality_flags = self._calculate_data_quality(
                        merged_record
                    )
                    merged_record["data_quality_score"] = quality_score
                    merged_record["quality_flags"] = quality_flags

                    # Calculate hash
                    merged_record["entity_hash"] = self._calculate_content_hash(
                        merged_record
                    )

                    # Upsert record
                    action = self._upsert_consolidated_record(
                        merged_record, run_id, dry_run
                    )

                    if action == "created":
                        stats["records_created"] += 1
                    elif action == "updated":
                        stats["records_updated"] += 1
                    elif action == "skipped":
                        stats["records_skipped"] += 1

                    stats["groups_processed"] += 1

                    # Log progress periodically
                    if idx % 1000 == 0:
                        logger.info(
                            f"📈 Progress: {idx}/{len(all_cns_normalized)} groups processed "
                            f"({stats['records_created']} created, {stats['records_updated']} updated, "
                            f"{stats['records_skipped']} skipped)"
                        )

                except Exception as record_error:
                    error_msg = (
                        f"Error processing group {cn_normalized}: {str(record_error)}"
                    )
                    logger.error(f"❌ {error_msg}")
                    stats["errors"].append(error_msg)
                    # Continue processing other groups

            # Calculate duration
            stats["duration_seconds"] = (
                datetime.now(timezone.utc) - stats["started_at"]
            ).total_seconds()

            # Complete run
            if not dry_run:
                self.complete_transformation_run(
                    run_id,
                    stats["groups_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                )

            # Log summary
            logger.info("=" * 80)
            logger.info("🎉 GROUP CONSOLIDATION COMPLETE")
            logger.info("=" * 80)
            logger.info(f"📊 Groups processed: {stats['groups_processed']}")
            logger.info(f"✅ Created: {stats['records_created']}")
            logger.info(f"📝 Updated: {stats['records_updated']}")
            logger.info(f"⏭️  Skipped (unchanged): {stats['records_skipped']}")
            logger.info(f"   - AD-only: {stats['ad_only']}")
            logger.info(f"   - MCommunity-only: {stats['mcommunity_only']}")
            logger.info(f"   - Merged (CN overlap): {stats['merged_overlaps']}")
            logger.info(f"   - MCommADSync groups: {stats['mcomm_adsync_count']}")
            logger.info(f"⏱️  Duration: {stats['duration_seconds']:.2f}s")
            logger.info(f"🔑 Run ID: {run_id}")
            if stats["errors"]:
                logger.warning(f"⚠️  Errors: {len(stats['errors'])}")
            logger.info("=" * 80)

            return stats

        except Exception as e:
            error_msg = f"Consolidation failed: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)
            if not dry_run:
                self.complete_transformation_run(run_id, 0, 0, 0, 0, error_msg)
            raise

    def close(self):
        """Close database connection."""
        self.db_adapter.close()
        logger.info("🔌 Database connection closed")


def main():
    """Main entry point for group consolidation."""
    parser = argparse.ArgumentParser(
        description="Consolidate AD and MCommunity groups to silver.groups"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Force full consolidation instead of incremental",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    try:
        service = GroupConsolidationService(database_url)
        service.consolidate_groups(full_sync=args.full_sync, dry_run=args.dry_run)
        service.close()
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
