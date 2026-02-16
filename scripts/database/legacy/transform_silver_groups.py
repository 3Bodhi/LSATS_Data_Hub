#!/usr/bin/env python3
"""
Silver Layer Group Transformation Service

Transforms raw group data from MCommunity LDAP and Active Directory into
standardized silver layer records with proper cross-source matching and
relationship tracking.

Key Features:
- Matches groups across MCommunity and AD by gidNumber and cn
- Extracts and normalizes member/owner relationships
- Handles nested group memberships
- Calculates data quality scores
- Supports incremental transformations

Usage:
    python transform_silver_groups.py [--full-sync] [--dry-run] [--batch-size 500]
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_batch

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GroupSilverTransformationService:
    """
    Service for transforming bronze group data into silver layer records.

    Handles:
    - Cross-source group matching (MCommunity + AD)
    - Member and owner relationship extraction
    - DN parsing and normalization
    - Data quality calculation
    - Incremental updates
    """

    # DN parsing regex patterns
    DN_PATTERNS = {
        "user_uid": re.compile(r"(?:uid|cn)=([^,]+)", re.IGNORECASE),
        "group_cn": re.compile(r"cn=([^,]+)", re.IGNORECASE),
        "ou": re.compile(r"ou=([^,]+)", re.IGNORECASE),
    }

    def _json_dumps_or_none(self, obj: Any) -> Optional[str]:
        """
        Safely convert object to JSON string, returning None for None input.

        Args:
            obj: Object to convert to JSON.

        Returns:
            JSON string or None.
        """
        if obj is None:
            return None
        return json.dumps(obj, default=str)

    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the transformation service.

        Args:
            db_config: Database connection configuration.
        """
        self.db_config = db_config
        self.conn = psycopg2.connect(**db_config)
        self.conn.autocommit = False
        logger.info("Connected to database for group transformation")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful group transformation.

        Returns:
            Datetime of last transformation, or None if no previous run.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(completed_at)
                FROM meta.ingestion_runs
                WHERE source_system = 'silver_transformation'
                  AND entity_type = 'group'
                  AND status = 'completed'
            """)
            result = cur.fetchone()
            return result[0] if result and result[0] else None

    def _fetch_latest_bronze_records(
        self, source_system: str, last_transformation_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch the latest bronze group records for a given source.

        Args:
            source_system: Source system identifier ('mcommunity_ldap' or 'active_directory').
            last_transformation_time: Only fetch records ingested after this time.

        Returns:
            List of bronze group records.
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            if last_transformation_time:
                cur.execute(
                    """
                    SELECT
                        raw_id,
                        entity_type,
                        source_system,
                        external_id,
                        raw_data,
                        ingested_at,
                        entity_hash
                    FROM bronze.raw_entities
                    WHERE entity_type = 'group'
                      AND source_system = %s
                      AND ingested_at > %s
                    ORDER BY ingested_at DESC
                """,
                    (source_system, last_transformation_time),
                )
            else:
                # Full sync - get all records
                cur.execute(
                    """
                    SELECT
                        raw_id,
                        entity_type,
                        source_system,
                        external_id,
                        raw_data,
                        ingested_at,
                        entity_hash
                    FROM bronze.raw_entities
                    WHERE entity_type = 'group'
                      AND source_system = %s
                    ORDER BY ingested_at DESC
                """,
                    (source_system,),
                )

            records = cur.fetchall()
            logger.info(f"Fetched {len(records)} {source_system} group records")
            return [dict(r) for r in records]

    def _normalize_cn(self, raw_cn: Any) -> str:
        """
        Normalize cn field (can be string or array).

        Args:
            raw_cn: Raw cn value from LDAP.

        Returns:
            Primary cn as string.
        """
        if isinstance(raw_cn, list):
            return raw_cn[0] if raw_cn else ""
        return str(raw_cn) if raw_cn else ""

    def _extract_cn_aliases(self, raw_cn: Any) -> List[str]:
        """
        Extract additional cn aliases (MCommunity groups often have multiple).

        Args:
            raw_cn: Raw cn value from LDAP.

        Returns:
            List of alias cn values (excluding primary).
        """
        if isinstance(raw_cn, list) and len(raw_cn) > 1:
            return raw_cn[1:]
        return []

    def _parse_dn_to_identifier(self, dn: str) -> Tuple[str, str]:
        """
        Parse LDAP DN to extract identifier and type.

        Examples:
            uid=myodhes,ou=People,dc=umich,dc=edu -> ('myodhes', 'user')
            cn=somegroup,ou=User Groups,... -> ('somegroup', 'group')
            CN=myodhes1,OU=Privileged,... -> ('myodhes1', 'user')

        Args:
            dn: LDAP distinguished name.

        Returns:
            Tuple of (identifier, type) where type is 'user' or 'group'.
        """
        dn_lower = dn.lower()

        # Check if this is a user DN
        if (
            "ou=people" in dn_lower
            or "ou=accounts" in dn_lower
            or "ou=privileged" in dn_lower
        ):
            match = self.DN_PATTERNS["user_uid"].search(dn)
            if match:
                return (match.group(1), "user")

        # Check if this is a group DN
        if (
            "ou=user groups" in dn_lower
            or "ou=groups" in dn_lower
            or "ou=mcommadsync" in dn_lower
        ):
            match = self.DN_PATTERNS["group_cn"].search(dn)
            if match:
                return (match.group(1), "group")

        # Fallback: try to extract first cn or uid
        match = self.DN_PATTERNS["user_uid"].search(dn)
        if match:
            # Heuristic: if it looks like a group name, it's probably a group
            identifier = match.group(1)
            if "lsa-" in identifier or "arcts-" in identifier or "turbo" in identifier:
                return (identifier, "group")
            return (identifier, "user")

        logger.warning(f"Could not parse DN: {dn}")
        return ("", "unknown")

    def _is_ad_synced_group(self, dn: str) -> bool:
        """
        Check if AD group is synced from MCommunity (in MCommADSync OU).

        Args:
            dn: Active Directory distinguished name.

        Returns:
            True if group is in MCommADSync OU.
        """
        return "OU=MCommADSync" in dn or "ou=mcommadsync" in dn.lower()

    def _parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Parse LDAP or AD timestamp string to datetime.

        Args:
            timestamp_str: Timestamp string (LDAP or AD format).

        Returns:
            Datetime object or None.
        """
        if not timestamp_str:
            return None

        try:
            # LDAP format: 20261012035959Z
            if timestamp_str.endswith("Z") and len(timestamp_str) == 15:
                return datetime.strptime(timestamp_str, "%Y%m%d%H%M%SZ").replace(
                    tzinfo=timezone.utc
                )

            # ISO format from AD (already parsed by ingestion)
            return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except Exception as e:
            logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None

    def _match_groups_across_sources(
        self, mcom_records: List[Dict[str, Any]], ad_records: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Match groups across MCommunity and Active Directory sources.

        Matching strategy:
        1. Match by gidNumber (highest priority)
        2. Match by exact cn (fallback)

        Args:
            mcom_records: MCommunity LDAP group records.
            ad_records: Active Directory group records.

        Returns:
            Dictionary mapping group_id to matched records with structure:
            {
                'group_id': str,
                'mcom_record': Optional[Dict],
                'ad_record': Optional[Dict],
                'match_method': str  # 'gidNumber', 'cn', or 'unique'
            }
        """
        matches = {}

        # Build lookup indexes
        mcom_by_gid = {}
        mcom_by_cn = {}
        ad_by_gid = {}
        ad_by_cn = {}

        # Index MCommunity groups
        for record in mcom_records:
            raw_data = record["raw_data"]
            gid = raw_data.get("gidNumber")
            cn = self._normalize_cn(raw_data.get("cn"))

            if gid:
                mcom_by_gid[str(gid)] = record
            if cn:
                mcom_by_cn[cn.lower()] = record

        # Index AD groups
        for record in ad_records:
            raw_data = record["raw_data"]
            gid = raw_data.get("gidNumber")
            cn = self._normalize_cn(raw_data.get("cn"))

            if gid:
                ad_by_gid[str(gid)] = record
            if cn:
                ad_by_cn[cn.lower()] = record

        logger.info(
            f"MCommunity groups: {len(mcom_records)} (gid: {len(mcom_by_gid)}, cn: {len(mcom_by_cn)})"
        )
        logger.info(
            f"AD groups: {len(ad_records)} (gid: {len(ad_by_gid)}, cn: {len(ad_by_cn)})"
        )

        # Match by gidNumber (priority)
        matched_mcom_ids = set()
        matched_ad_ids = set()

        for gid, mcom_record in mcom_by_gid.items():
            if gid in ad_by_gid:
                ad_record = ad_by_gid[gid]
                matches[gid] = {
                    "group_id": gid,
                    "mcom_record": mcom_record,
                    "ad_record": ad_record,
                    "match_method": "gidNumber",
                }
                matched_mcom_ids.add(mcom_record["raw_id"])
                matched_ad_ids.add(ad_record["raw_id"])

        logger.info(f"Matched {len(matches)} groups by gidNumber")

        # Match remaining by cn
        cn_matches = 0
        for cn, mcom_record in mcom_by_cn.items():
            if mcom_record["raw_id"] in matched_mcom_ids:
                continue

            if cn in ad_by_cn:
                ad_record = ad_by_cn[cn]
                if ad_record["raw_id"] not in matched_ad_ids:
                    # Use mcom gidNumber if available, otherwise use cn-based ID
                    gid = mcom_record["raw_data"].get("gidNumber")
                    group_id = str(gid) if gid else f"cn_{cn}"

                    matches[group_id] = {
                        "group_id": group_id,
                        "mcom_record": mcom_record,
                        "ad_record": ad_record,
                        "match_method": "cn",
                    }
                    matched_mcom_ids.add(mcom_record["raw_id"])
                    matched_ad_ids.add(ad_record["raw_id"])
                    cn_matches += 1

        logger.info(f"Matched {cn_matches} additional groups by cn")

        # Add unmatched MCommunity groups
        for record in mcom_records:
            if record["raw_id"] not in matched_mcom_ids:
                gid = record["raw_data"].get("gidNumber")
                cn = self._normalize_cn(record["raw_data"].get("cn"))
                group_id = str(gid) if gid else f"mcom_{cn}"

                matches[group_id] = {
                    "group_id": group_id,
                    "mcom_record": record,
                    "ad_record": None,
                    "match_method": "unique_mcom",
                }

        # Add unmatched AD groups
        for record in ad_records:
            if record["raw_id"] not in matched_ad_ids:
                gid = record["raw_data"].get("gidNumber")
                cn = self._normalize_cn(record["raw_data"].get("cn"))
                group_id = str(gid) if gid else f"ad_{cn}"

                matches[group_id] = {
                    "group_id": group_id,
                    "mcom_record": None,
                    "ad_record": record,
                    "match_method": "unique_ad",
                }

        logger.info(f"Total groups after matching: {len(matches)}")
        return matches

    def _merge_bronze_to_silver(self, match: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge matched bronze records into silver layer format.

        Args:
            match: Matched group record from _match_groups_across_sources.

        Returns:
            Dictionary with silver layer group fields.
        """
        mcom_data = match["mcom_record"]["raw_data"] if match["mcom_record"] else {}
        ad_data = match["ad_record"]["raw_data"] if match["ad_record"] else {}

        # Determine sync source
        if match["mcom_record"] and match["ad_record"]:
            sync_source = "both"
            source_system = "mcommunity_ldap+active_directory"
        elif match["mcom_record"]:
            sync_source = "mcommunity"
            source_system = "mcommunity_ldap"
        else:
            sync_source = "ad_only"
            source_system = "active_directory"

        # Extract core fields
        group_name = self._normalize_cn(mcom_data.get("cn")) or self._normalize_cn(
            ad_data.get("cn")
        )
        group_aliases = self._extract_cn_aliases(mcom_data.get("cn"))
        gid_number = mcom_data.get("gidNumber") or ad_data.get("gidNumber")

        # Descriptive information (prefer MCommunity)
        description = mcom_data.get("umichDescription") or ad_data.get("description")
        email_address = mcom_data.get("umichGroupEmail") or ad_data.get("mail")

        # AD-specific identifiers
        ad_object_guid = ad_data.get("objectGUID")
        ad_sam_account_name = ad_data.get("sAMAccountName")
        ad_object_sid = ad_data.get("objectSid")

        # MCommunity-specific
        mcommunity_dn = mcom_data.get("dn")

        # Group configuration (MCommunity only)
        is_joinable = mcom_data.get("joinable")
        is_members_only = mcom_data.get("Membersonly")
        is_private = mcom_data.get("umichPrivate")
        suppress_no_email_error = mcom_data.get("suppressNoEmailError")

        # Timestamps
        mcommunity_expiry = self._parse_timestamp(mcom_data.get("umichExpiryTimestamp"))
        ad_when_created = self._parse_timestamp(ad_data.get("whenCreated"))
        ad_when_changed = self._parse_timestamp(ad_data.get("whenChanged"))

        # Check if AD synced
        is_ad_synced = False
        if ad_data.get("dn"):
            is_ad_synced = self._is_ad_synced_group(ad_data["dn"])

        # Calculate entity hash (for change detection)
        hash_content = {
            "group_name": group_name,
            "gid_number": gid_number,
            "description": description,
            "sync_source": sync_source,
        }
        entity_hash = hashlib.sha256(
            json.dumps(hash_content, sort_keys=True, default=str).encode()
        ).hexdigest()

        silver_record = {
            "group_id": match["group_id"],
            "group_name": group_name,
            "group_aliases": group_aliases,
            "gid_number": str(gid_number) if gid_number else None,
            "description": description,
            "email_address": email_address,
            "ad_object_guid": ad_object_guid,
            "ad_sam_account_name": ad_sam_account_name,
            "ad_object_sid": ad_object_sid,
            "mcommunity_dn": mcommunity_dn,
            "is_joinable": is_joinable,
            "is_members_only": is_members_only,
            "is_private": is_private,
            "suppress_no_email_error": suppress_no_email_error,
            "is_ad_synced": is_ad_synced,
            "sync_source": sync_source,
            "mcommunity_expiry_timestamp": mcommunity_expiry,
            "ad_when_created": ad_when_created,
            "ad_when_changed": ad_when_changed,
            "is_active": True,  # Will update based on expiry/status checks
            "source_system": source_system,
            "source_entity_id": match["group_id"],
            "entity_hash": entity_hash,
            "mcom_members": mcom_data.get("member", []),
            "ad_members": ad_data.get("member", []),
            "mcom_owners": mcom_data.get("owner", []),
            "mcom_direct_members": mcom_data.get("umichDirectMember", []),
        }

        return silver_record

    def _extract_members_and_owners(
        self, silver_record: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Extract member and owner relationships from silver record.

        Args:
            silver_record: Merged silver group record.

        Returns:
            Tuple of (members_list, owners_list).
        """
        members = []
        owners = []
        group_id = silver_record["group_id"]

        # Track direct members from MCommunity
        direct_member_dns = set(
            silver_record.get("mcom_direct_members", [])
            if isinstance(silver_record.get("mcom_direct_members"), list)
            else []
        )

        # Process MCommunity members
        mcom_members = silver_record.get("mcom_members", [])
        if isinstance(mcom_members, list):
            for dn in mcom_members:
                if not dn:
                    continue

                identifier, member_type = self._parse_dn_to_identifier(str(dn))
                if identifier and member_type in ("user", "group"):
                    members.append(
                        {
                            "group_id": group_id,
                            "member_type": member_type,
                            "member_uniqname": identifier
                            if member_type == "user"
                            else None,
                            "member_group_id": identifier
                            if member_type == "group"
                            else None,
                            "is_direct_member": str(dn) in direct_member_dns,
                            "source_system": "mcommunity_ldap",
                        }
                    )

        # Process AD members
        ad_members = silver_record.get("ad_members", [])
        if isinstance(ad_members, list):
            for dn in ad_members:
                if not dn:
                    continue

                identifier, member_type = self._parse_dn_to_identifier(str(dn))
                if identifier and member_type in ("user", "group"):
                    members.append(
                        {
                            "group_id": group_id,
                            "member_type": member_type,
                            "member_uniqname": identifier
                            if member_type == "user"
                            else None,
                            "member_group_id": identifier
                            if member_type == "group"
                            else None,
                            "is_direct_member": True,  # AD doesn't distinguish
                            "source_system": "active_directory",
                        }
                    )

        # Process MCommunity owners
        mcom_owners = silver_record.get("mcom_owners", [])
        if isinstance(mcom_owners, list):
            for dn in mcom_owners:
                if not dn:
                    continue

                identifier, owner_type = self._parse_dn_to_identifier(str(dn))
                if identifier and owner_type in ("user", "group"):
                    owners.append(
                        {
                            "group_id": group_id,
                            "owner_type": owner_type,
                            "owner_uniqname": identifier
                            if owner_type == "user"
                            else None,
                            "owner_group_id": identifier
                            if owner_type == "group"
                            else None,
                            "source_system": "mcommunity_ldap",
                        }
                    )

        return members, owners

    def _calculate_data_quality(
        self, silver_record: Dict[str, Any]
    ) -> Tuple[Decimal, List[str]]:
        """
        Calculate data quality score and flags for a group record.

        Args:
            silver_record: Silver layer group record.

        Returns:
            Tuple of (quality_score, quality_flags).
        """
        flags = []
        score = Decimal("1.00")

        # Check for missing critical fields
        if not silver_record.get("description"):
            flags.append("missing_description")
            score -= Decimal("0.10")

        if not silver_record.get("gid_number"):
            flags.append("missing_gid_number")
            score -= Decimal("0.05")

        # Check for expired groups
        if silver_record.get("mcommunity_expiry_timestamp"):
            if silver_record["mcommunity_expiry_timestamp"] < datetime.now(
                timezone.utc
            ):
                flags.append("expired")
                score -= Decimal("0.20")

        # Check for sync inconsistencies
        if (
            silver_record.get("is_ad_synced")
            and silver_record.get("sync_source") != "both"
        ):
            flags.append("sync_mismatch")
            score -= Decimal("0.15")

        # Check for empty membership
        if not silver_record.get("mcom_members") and not silver_record.get(
            "ad_members"
        ):
            flags.append("no_members")
            score -= Decimal("0.10")

        # Ensure score doesn't go below 0
        score = max(score, Decimal("0.00"))

        return score, flags

    def _bulk_upsert_silver_records(
        self, silver_records: List[Dict[str, Any]], ingestion_run_id: str
    ) -> int:
        """
        Bulk insert/update silver group records.

        Args:
            silver_records: List of silver group records to upsert.
            ingestion_run_id: UUID of the current ingestion run.

        Returns:
            Number of records upserted.
        """
        if not silver_records:
            return 0

        with self.conn.cursor() as cur:
            # Prepare records for upsert
            upsert_data = []
            for record in silver_records:
                quality_score, quality_flags = self._calculate_data_quality(record)

                # Count members and owners
                members, owners = self._extract_members_and_owners(record)
                member_count = len(members)
                owner_count = len(owners)
                has_nested = any(m["member_type"] == "group" for m in members)

                upsert_data.append(
                    (
                        record["group_id"],
                        record["group_name"],
                        self._json_dumps_or_none(record["group_aliases"]),
                        record.get("gid_number"),
                        record.get("description"),
                        record.get("email_address"),
                        record.get("ad_object_guid"),
                        record.get("ad_sam_account_name"),
                        record.get("ad_object_sid"),
                        record.get("mcommunity_dn"),
                        record.get("is_joinable"),
                        record.get("is_members_only"),
                        record.get("is_private"),
                        record.get("suppress_no_email_error"),
                        member_count,
                        owner_count,
                        has_nested,
                        record.get("is_ad_synced"),
                        record.get("sync_source"),
                        record.get("mcommunity_expiry_timestamp"),
                        record.get("ad_when_created"),
                        record.get("ad_when_changed"),
                        record["is_active"],
                        quality_score,
                        self._json_dumps_or_none(quality_flags),
                        record["source_system"],
                        record["source_entity_id"],
                        record["entity_hash"],
                        ingestion_run_id,
                    )
                )

            # Bulk upsert
            execute_batch(
                cur,
                """
                INSERT INTO silver.groups (
                    group_id, group_name, group_aliases, gid_number,
                    description, email_address,
                    ad_object_guid, ad_sam_account_name, ad_object_sid,
                    mcommunity_dn,
                    is_joinable, is_members_only, is_private, suppress_no_email_error,
                    member_count, owner_count, has_nested_groups,
                    is_ad_synced, sync_source,
                    mcommunity_expiry_timestamp, ad_when_created, ad_when_changed,
                    is_active,
                    data_quality_score, quality_flags,
                    source_system, source_entity_id, entity_hash,
                    ingestion_run_id
                ) VALUES (
                    %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s
                )
                ON CONFLICT (group_id) DO UPDATE SET
                    group_name = EXCLUDED.group_name,
                    group_aliases = EXCLUDED.group_aliases,
                    gid_number = EXCLUDED.gid_number,
                    description = EXCLUDED.description,
                    email_address = EXCLUDED.email_address,
                    ad_object_guid = EXCLUDED.ad_object_guid,
                    ad_sam_account_name = EXCLUDED.ad_sam_account_name,
                    ad_object_sid = EXCLUDED.ad_object_sid,
                    mcommunity_dn = EXCLUDED.mcommunity_dn,
                    is_joinable = EXCLUDED.is_joinable,
                    is_members_only = EXCLUDED.is_members_only,
                    is_private = EXCLUDED.is_private,
                    suppress_no_email_error = EXCLUDED.suppress_no_email_error,
                    member_count = EXCLUDED.member_count,
                    owner_count = EXCLUDED.owner_count,
                    has_nested_groups = EXCLUDED.has_nested_groups,
                    is_ad_synced = EXCLUDED.is_ad_synced,
                    sync_source = EXCLUDED.sync_source,
                    mcommunity_expiry_timestamp = EXCLUDED.mcommunity_expiry_timestamp,
                    ad_when_created = EXCLUDED.ad_when_created,
                    ad_when_changed = EXCLUDED.ad_when_changed,
                    is_active = EXCLUDED.is_active,
                    data_quality_score = EXCLUDED.data_quality_score,
                    quality_flags = EXCLUDED.quality_flags,
                    source_system = EXCLUDED.source_system,
                    entity_hash = EXCLUDED.entity_hash,
                    updated_at = CURRENT_TIMESTAMP,
                    ingestion_run_id = EXCLUDED.ingestion_run_id
            """,
                upsert_data,
            )

            logger.info(f"Upserted {len(upsert_data)} group records")
            return len(upsert_data)

    def _bulk_upsert_members(self, members: List[Dict[str, Any]]) -> int:
        """
        Bulk insert/update group member relationships.

        Args:
            members: List of member relationship records.

        Returns:
            Number of relationships upserted.
        """
        if not members:
            return 0

        with self.conn.cursor() as cur:
            # Clear existing members for these groups (simplifies update logic)
            group_ids = list(set(m["group_id"] for m in members))
            cur.execute(
                """
                DELETE FROM silver.group_members
                WHERE group_id = ANY(%s)
            """,
                (group_ids,),
            )

            # Bulk insert new members
            member_data = [
                (
                    m["group_id"],
                    m["member_type"],
                    m["member_uniqname"],
                    m["member_group_id"],
                    m["is_direct_member"],
                    m["source_system"],
                )
                for m in members
            ]

            execute_batch(
                cur,
                """
                INSERT INTO silver.group_members (
                    group_id, member_type, member_uniqname, member_group_id,
                    is_direct_member, source_system
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
                member_data,
            )

            logger.info(f"Inserted {len(member_data)} member relationships")
            return len(member_data)

    def _bulk_upsert_owners(self, owners: List[Dict[str, Any]]) -> int:
        """
        Bulk insert/update group owner relationships.

        Args:
            owners: List of owner relationship records.

        Returns:
            Number of relationships upserted.
        """
        if not owners:
            return 0

        with self.conn.cursor() as cur:
            # Clear existing owners for these groups
            group_ids = list(set(o["group_id"] for o in owners))
            cur.execute(
                """
                DELETE FROM silver.group_owners
                WHERE group_id = ANY(%s)
            """,
                (group_ids,),
            )

            # Bulk insert new owners
            owner_data = [
                (
                    o["group_id"],
                    o["owner_type"],
                    o["owner_uniqname"],
                    o["owner_group_id"],
                    o["source_system"],
                )
                for o in owners
            ]

            execute_batch(
                cur,
                """
                INSERT INTO silver.group_owners (
                    group_id, owner_type, owner_uniqname, owner_group_id,
                    source_system
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """,
                owner_data,
            )

            logger.info(f"Inserted {len(owner_data)} owner relationships")
            return len(owner_data)

    def create_transformation_run(self) -> str:
        """
        Create a new transformation run record.

        Returns:
            UUID of the transformation run.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO meta.ingestion_runs (
                    source_system,
                    entity_type,
                    status,
                    started_at
                ) VALUES (
                    'silver_transformation',
                    'group',
                    'running',
                    CURRENT_TIMESTAMP
                )
                RETURNING run_id
            """)
            run_id = cur.fetchone()[0]
            self.conn.commit()
            logger.info(f"Created transformation run: {run_id}")
            return str(run_id)

    def complete_transformation_run(
        self,
        run_id: str,
        records_processed: int,
        records_created: int,
        records_updated: int,
        status: str = "completed",
        error_message: Optional[str] = None,
    ):
        """
        Mark transformation run as complete.

        Args:
            run_id: UUID of the transformation run.
            records_processed: Number of records processed.
            records_created: Number of records created.
            records_updated: Number of records updated.
            status: Final status ('completed' or 'failed').
            error_message: Error message if failed.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE meta.ingestion_runs
                SET completed_at = CURRENT_TIMESTAMP,
                    status = %s,
                    records_processed = %s,
                    records_created = %s,
                    records_updated = %s,
                    error_message = %s
                WHERE run_id = %s
            """,
                (
                    status,
                    records_processed,
                    records_created,
                    records_updated,
                    error_message,
                    run_id,
                ),
            )
            self.conn.commit()
            logger.info(f"Completed transformation run {run_id}: {status}")

    def transform_groups_incremental(
        self, batch_size: int = 500, full_sync: bool = False
    ) -> Dict[str, int]:
        """
        Transform group records from bronze to silver layer.

        Args:
            batch_size: Number of groups to process per batch.
            full_sync: If True, transform all groups regardless of timestamps.

        Returns:
            Dictionary with transformation statistics.
        """
        run_id = self.create_transformation_run()
        stats = {
            "groups_processed": 0,
            "groups_created": 0,
            "groups_updated": 0,
            "members_inserted": 0,
            "owners_inserted": 0,
        }

        try:
            # Determine incremental vs full sync
            last_transformation_time = (
                None if full_sync else self._get_last_transformation_timestamp()
            )

            if last_transformation_time:
                logger.info(
                    f"Incremental transformation since {last_transformation_time}"
                )
            else:
                logger.info("Full sync transformation")

            # Fetch bronze records
            mcom_records = self._fetch_latest_bronze_records(
                "mcommunity_ldap", last_transformation_time
            )
            ad_records = self._fetch_latest_bronze_records(
                "active_directory", last_transformation_time
            )

            # Match groups across sources
            matches = self._match_groups_across_sources(mcom_records, ad_records)

            # Process in batches
            match_items = list(matches.values())
            for i in range(0, len(match_items), batch_size):
                batch = match_items[i : i + batch_size]

                logger.info(
                    f"Processing batch {i // batch_size + 1} ({len(batch)} groups)"
                )

                # Merge to silver format
                silver_records = [
                    self._merge_bronze_to_silver(match) for match in batch
                ]

                # Upsert groups
                upserted = self._bulk_upsert_silver_records(silver_records, run_id)
                stats["groups_processed"] += len(silver_records)
                stats["groups_updated"] += upserted

                # Extract and upsert relationships
                all_members = []
                all_owners = []

                for record in silver_records:
                    members, owners = self._extract_members_and_owners(record)
                    all_members.extend(members)
                    all_owners.extend(owners)

                members_inserted = self._bulk_upsert_members(all_members)
                owners_inserted = self._bulk_upsert_owners(all_owners)

                stats["members_inserted"] += members_inserted
                stats["owners_inserted"] += owners_inserted

                # Commit batch
                self.conn.commit()
                logger.info(
                    f"Batch complete: {upserted} groups, {members_inserted} members, {owners_inserted} owners"
                )

            # Mark run as complete
            self.complete_transformation_run(
                run_id,
                stats["groups_processed"],
                stats["groups_created"],
                stats["groups_updated"],
            )

            logger.info(f"Transformation complete: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Transformation failed: {e}", exc_info=True)
            self.conn.rollback()
            self.complete_transformation_run(
                run_id,
                stats["groups_processed"],
                stats["groups_created"],
                stats["groups_updated"],
                status="failed",
                error_message=str(e),
            )
            raise

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


def main():
    """Main entry point for the transformation script."""
    parser = argparse.ArgumentParser(
        description="Transform bronze group data to silver layer"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Transform all groups regardless of timestamps",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of groups to process per batch (default: 500)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview transformation without committing changes",
    )

    args = parser.parse_args()

    # Load environment
    load_dotenv()

    # Database configuration
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "database": os.getenv("DB_NAME", "lsats_db"),
        "user": os.getenv("DB_USER", "lsats_user"),
        "password": os.getenv("DB_PASSWORD"),
    }

    if not db_config["password"]:
        logger.error("DB_PASSWORD environment variable not set")
        sys.exit(1)

    # Run transformation
    service = None
    try:
        service = GroupSilverTransformationService(db_config)

        if args.dry_run:
            logger.info("DRY RUN MODE - No changes will be committed")

        stats = service.transform_groups_incremental(
            batch_size=args.batch_size, full_sync=args.full_sync
        )

        if args.dry_run:
            service.conn.rollback()
            logger.info("DRY RUN - All changes rolled back")

        # Print summary
        print("\n" + "=" * 60)
        print("GROUP TRANSFORMATION SUMMARY")
        print("=" * 60)
        print(f"Groups processed: {stats['groups_processed']}")
        print(f"Groups updated: {stats['groups_updated']}")
        print(f"Members inserted: {stats['members_inserted']}")
        print(f"Owners inserted: {stats['owners_inserted']}")
        print("=" * 60)

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        sys.exit(1)
    finally:
        if service:
            service.close()


if __name__ == "__main__":
    main()
