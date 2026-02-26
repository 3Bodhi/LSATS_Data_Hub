#!/usr/bin/env python3
"""
Silver Layer Lab Computers Transformation Script

Transforms silver.computers → silver.lab_computers with enhanced multi-criteria
additive confidence scoring.

Phase: 6 of Lab Modernization Plan
Strategy: Full refresh (TRUNCATE + INSERT)

Key Features:
- 5 discovery methods (AD OU, PI ownership, group membership, lab membership)
- Additive confidence scoring (start at 1.0, subtract penalties)
- Multi-criteria tracking (6 boolean fields)
- Function-based scoring (Research +, Admin/Dev -, Classroom +)
- Filters computers with NO discovery criteria (prevents all computers in table)
- Primary lab selection (highest confidence per computer)

Discovery Methods:
1. AD OU Nested (0.95): Computer DN contains lab's AD OU
2. Owner is PI (0.90): computer.owner_uniqname = lab.pi_uniqname
3. Group Membership (0.70): Computer in groups matching lab/PI
4. Owner is Member (0.60): computer.owner_uniqname in lab_members
5. Last User is Member (0.45): computer.last_user in lab_members

Additive Scoring Penalties (subtracted from 1.0):
- Owner not PI: -0.15
- Financial owner not PI: -0.10
- Owner not member: -0.20
- Financial owner not member: -0.15
- Function not Research/Classroom: -0.10 (or -0.20 for Admin/Dev)
- Function Classroom bonus: +0.05

Minimum confidence after discovery: 0.30

Usage:
    source venv/bin/activate
    python scripts/database/silver/014_transform_lab_computers.py [--dry-run]
"""

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

# Add LSATS project to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from dotenv import load_dotenv
from sqlalchemy import text

from database.adapters.postgres_adapter import PostgresAdapter

# Load environment
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class LabComputersTransformationService:
    """
    Transform silver.computers → silver.lab_computers with enhanced scoring.

    Strategy: Full refresh (TRUNCATE + INSERT) for simplicity and data consistency.
    """

    # TDX Function IDs
    FUNCTION_RESEARCH = "27316"
    FUNCTION_CLASSROOM = "27312"
    FUNCTION_ADMIN_STAFF = "27311"
    FUNCTION_DEV_TESTING = "27313"

    def __init__(self, db_adapter: PostgresAdapter, dry_run: bool = False):
        """
        Initialize transformation service.

        Args:
            db_adapter: PostgresAdapter instance
            dry_run: If True, don't commit changes
        """
        self.db_adapter = db_adapter
        self.dry_run = dry_run
        self.run_id = None  # Track current ingestion run (Phase 4)

        # Caches (loaded once for performance)
        self.lab_members_cache = {}  # {lab_id: set(uniqnames)}
        self.function_cache = {}  # {computer_id: function_id}
        self.labs_cache = {}  # {lab_id: lab_dict}
        self.computers_cache = {}  # {computer_id: computer_dict}

        logger.info(
            f"🔧 Initialized LabComputersTransformationService (dry_run={dry_run})"
        )

    # ========================================================================
    # CACHE LOADING
    # ========================================================================

    def _load_lab_members_cache(self):
        """Load all lab members grouped by lab_id."""
        logger.info("📚 Loading lab members cache...")

        query = """
            SELECT lab_id, member_uniqname
            FROM silver.lab_members
            WHERE member_uniqname IS NOT NULL
        """

        df = self.db_adapter.query_to_dataframe(query)

        # Group by lab_id
        for _, row in df.iterrows():
            lab_id = row["lab_id"]
            uniqname = row["member_uniqname"]

            if lab_id not in self.lab_members_cache:
                self.lab_members_cache[lab_id] = set()

            self.lab_members_cache[lab_id].add(uniqname)

        logger.info(f"   Loaded {len(self.lab_members_cache)} labs with members")
        logger.info(f"   Total member entries: {len(df)}")

    def _load_function_cache(self):
        """Load computer function attributes."""
        logger.info("📚 Loading computer function cache...")

        query = """
            SELECT
                computer_id,
                (tdx_attributes->'function'->>'id')::text as function_id
            FROM silver.computers
            WHERE tdx_attributes->'function' IS NOT NULL
              AND is_active = true
        """

        df = self.db_adapter.query_to_dataframe(query)

        for _, row in df.iterrows():
            # Convert float string to int string (27316.0 -> '27316')
            func_id = row["function_id"]
            if func_id:
                func_id = str(int(float(func_id)))
            self.function_cache[row["computer_id"]] = func_id

        logger.info(f"   Loaded {len(self.function_cache)} computer functions")

        # Show distribution
        function_counts = {}
        for func_id in self.function_cache.values():
            function_counts[func_id] = function_counts.get(func_id, 0) + 1

        logger.info(f"   Function distribution:")
        for func_id in sorted(
            function_counts.keys(), key=lambda x: function_counts[x], reverse=True
        ):
            func_name = self._get_function_name(func_id)
            logger.info(f"     {func_id} ({func_name}): {function_counts[func_id]}")

    def _load_labs_cache(self):
        """Load all labs with key fields."""
        logger.info("📚 Loading labs cache...")

        query = """
            SELECT
                lab_id,
                pi_uniqname,
                ad_ou_dn,
                has_ad_ou
            FROM silver.labs
            WHERE is_active = true
        """

        df = self.db_adapter.query_to_dataframe(query)

        for _, row in df.iterrows():
            self.labs_cache[row["lab_id"]] = dict(row)

        logger.info(f"   Loaded {len(self.labs_cache)} active labs")

    def _load_computers_cache(self):
        """Load all computers with key fields."""
        logger.info("📚 Loading computers cache...")

        query = """
            SELECT
                computer_id,
                computer_name,
                owner_uniqname,
                financial_owner_uniqname,
                last_user,
                ad_distinguished_name,
                has_recent_activity
            FROM silver.computers
            WHERE is_active = true
        """

        df = self.db_adapter.query_to_dataframe(query)

        for _, row in df.iterrows():
            self.computers_cache[row["computer_id"]] = dict(row)

        logger.info(f"   Loaded {len(self.computers_cache)} active computers")

    # ========================================================================
    # HELPER METHODS
    # ========================================================================

    def _get_function_name(self, function_id: Optional[str]) -> str:
        """Get human-readable function name."""
        mapping = {
            "27311": "Administrative/Staff",
            "27312": "Classroom/Computer Lab",
            "27313": "Development/Testing",
            "27314": "General Office",
            "27315": "Special Purpose/Other",
            "27316": "Research",
            "27317": "Server",
        }
        return mapping.get(function_id, "Unknown")

    # ========================================================================
    # DISCOVERY METHODS
    # ========================================================================

    def _discover_by_ad_ou(self) -> List[Dict[str, Any]]:
        """
        Method 1: AD OU Nested (confidence 0.80).

        Find computers whose AD DN contains a lab's AD OU DN.
        """
        logger.info("🔍 Discovery Method 1: AD OU Nested...")

        associations = []

        for lab_id, lab in self.labs_cache.items():
            if not lab.get("has_ad_ou") or not lab.get("ad_ou_dn"):
                continue

            lab_ou = lab["ad_ou_dn"]

            # Find computers in this OU
            for computer_id, computer in self.computers_cache.items():
                if not computer.get("ad_distinguished_name"):
                    continue

                # Check if computer DN contains lab OU DN
                if lab_ou.lower() in computer["ad_distinguished_name"].lower():
                    associations.append(
                        {
                            "computer_id": computer_id,
                            "lab_id": lab_id,
                            "method": "ad_ou_nested",
                            "base_confidence": Decimal("0.80"),
                            "matched_ou": lab_ou,
                            "matched_group_id": None,
                            "matched_user": None,
                        }
                    )

        logger.info(f"   Found {len(associations)} AD OU matches")
        return associations

    def _discover_by_owner_pi(self) -> List[Dict[str, Any]]:
        """
        Method 2: Owner is PI (confidence 0.85).

        Find computers whose owner_uniqname matches lab PI.
        """
        logger.info("🔍 Discovery Method 2: Owner is PI...")

        associations = []

        for lab_id, lab in self.labs_cache.items():
            pi = lab.get("pi_uniqname")
            if not pi:
                continue

            # Find computers owned by this PI
            for computer_id, computer in self.computers_cache.items():
                if computer.get("owner_uniqname") == pi:
                    associations.append(
                        {
                            "computer_id": computer_id,
                            "lab_id": lab_id,
                            "method": "owner_is_pi",
                            "base_confidence": Decimal("0.85"),
                            "matched_ou": None,
                            "matched_group_id": None,
                            "matched_user": pi,
                        }
                    )

        logger.info(f"   Found {len(associations)} PI owner matches")
        return associations

    def _discover_by_financial_owner_pi(self) -> List[Dict[str, Any]]:
        """
        Method 2b: Financial Owner is PI (confidence 0.80).

        Find computers whose financial_owner_uniqname matches lab PI.
        This is a STRONG signal - financial ownership represents budget/grant allocation.

        Returns:
            List of association dictionaries with financial owner PI matches
        """
        logger.info("🔍 Discovery Method 2b: Financial Owner is PI...")

        associations = []

        for lab_id, lab in self.labs_cache.items():
            pi = lab.get("pi_uniqname")
            if not pi:
                continue

            # Find computers financially owned by this PI
            for computer_id, computer in self.computers_cache.items():
                if computer.get("financial_owner_uniqname") == pi:
                    associations.append(
                        {
                            "computer_id": computer_id,
                            "lab_id": lab_id,
                            "method": "fin_owner_is_pi",
                            "base_confidence": Decimal("0.80"),
                            "matched_ou": None,
                            "matched_group_id": None,
                            "matched_user": pi,
                        }
                    )

        logger.info(f"   Found {len(associations)} financial owner-PI matches")
        return associations

    def _discover_by_name_pattern_pi(self) -> List[Dict[str, Any]]:
        """
        Method 3: Name Pattern Contains PI (confidence 0.70).

        Find computers whose name contains the lab PI's uniqname.
        Common patterns: MAC-CRSEIJAS02, PSYC-CRSEIJAS01, etc.

        Returns:
            List of association dictionaries with name pattern matches
        """
        logger.info("🔍 Discovery Method 3: Name Pattern Contains PI...")

        associations = []

        for lab_id, lab in self.labs_cache.items():
            pi = lab.get("pi_uniqname")
            if not pi:
                continue

            pi_lower = pi.lower()

            # Search for PI uniqname in computer names
            for computer_id, computer in self.computers_cache.items():
                computer_name = computer.get("computer_name", "").lower()

                if pi_lower in computer_name:
                    associations.append(
                        {
                            "computer_id": computer_id,
                            "lab_id": lab_id,
                            "method": "name_pattern_pi",
                            "base_confidence": Decimal("0.70"),
                            "matched_ou": None,
                            "matched_group_id": None,
                            "matched_user": pi,
                        }
                    )

        logger.info(f"   Found {len(associations)} name pattern-PI matches")
        return associations

    def _discover_by_group_membership(self) -> List[Dict[str, Any]]:
        """
        Method 4: Group Membership (confidence 0.45).

        Find computers in groups matching lab ID or PI name.
        This is a simplified version - full implementation would query computer_groups.
        """
        logger.info("🔍 Discovery Method 4: Group Membership...")

        # This method requires querying computer_groups table
        # Skip for now as it's complex and may not add many associations
        logger.info("   Skipped (requires computer_groups join)")
        return []

    def _discover_by_owner_member(self) -> List[Dict[str, Any]]:
        """
        Method 5: Owner is Member (confidence 0.35).

        Find computers whose owner is a lab member (not the PI).
        """
        logger.info("🔍 Discovery Method 5: Owner is Lab Member...")

        associations = []

        for lab_id, lab in self.labs_cache.items():
            pi = lab.get("pi_uniqname")
            members = self.lab_members_cache.get(lab_id, set())

            if not members:
                continue

            # Find computers owned by members (excluding PI)
            for computer_id, computer in self.computers_cache.items():
                owner = computer.get("owner_uniqname")

                if owner and owner in members and owner != pi:
                    associations.append(
                        {
                            "computer_id": computer_id,
                            "lab_id": lab_id,
                            "method": "owner_member",
                            "base_confidence": Decimal("0.35"),
                            "matched_ou": None,
                            "matched_group_id": None,
                            "matched_user": owner,
                        }
                    )

        logger.info(f"   Found {len(associations)} owner-member matches")
        return associations

    def _discover_by_last_user_member(self) -> List[Dict[str, Any]]:
        """
        Method 6: Last User is Member (confidence 0.30).

        Find computers whose last_user is a lab member.
        """
        logger.info("🔍 Discovery Method 6: Last User is Lab Member...")

        associations = []

        for lab_id, lab in self.labs_cache.items():
            members = self.lab_members_cache.get(lab_id, set())

            if not members:
                continue

            # Find computers where last_user is a member
            for computer_id, computer in self.computers_cache.items():
                last_user = computer.get("last_user")

                if last_user:
                    # Normalize to lowercase for matching
                    last_user_lower = last_user.lower()

                    if last_user_lower in members:
                        associations.append(
                            {
                                "computer_id": computer_id,
                                "lab_id": lab_id,
                                "method": "last_user_member",
                                "base_confidence": Decimal("0.30"),
                                "matched_ou": None,
                                "matched_group_id": None,
                                "matched_user": last_user_lower,
                            }
                        )

        logger.info(f"   Found {len(associations)} last-user matches")
        return associations

    # ========================================================================
    # DEDUPLICATION
    # ========================================================================

    def _deduplicate_associations(
        self, associations: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Deduplicate associations, keeping highest base confidence per computer-lab pair.

        Args:
            associations: List of association dicts

        Returns:
            Deduplicated list
        """
        logger.info("🔗 Deduplicating associations...")

        # Track unique computer-lab pairs
        unique_pairs = {}

        for assoc in associations:
            key = (assoc["computer_id"], assoc["lab_id"])

            if key not in unique_pairs:
                unique_pairs[key] = assoc
            else:
                # Keep association with higher base confidence
                if assoc["base_confidence"] > unique_pairs[key]["base_confidence"]:
                    unique_pairs[key] = assoc

        result = list(unique_pairs.values())

        logger.info(f"   {len(associations)} total → {len(result)} unique")
        return result

    # ========================================================================
    # ADDITIVE CONFIDENCE SCORING
    # ========================================================================

    def _calculate_additive_confidence(
        self, computer_id: str, lab_id: str, base_method: str, base_confidence: Decimal
    ) -> Tuple[Decimal, Dict[str, bool]]:
        """
        Calculate additive confidence score with hierarchical tier enforcement.

        Hierarchical Scoring Model (Phase 3):
        - Tier 1 methods (ad_ou_nested, owner_is_pi, fin_owner_is_pi, name_pattern_pi):
          * Start with base confidence (0.70-0.85)
          * Apply bonuses for supporting evidence
          * Apply penalties for contradicting evidence
          * ENFORCE FLOOR: minimum 0.70 (strong methods ALWAYS high confidence)

        - Tier 2 methods (group_membership, owner_member, last_user_member):
          * Start with base confidence (0.30-0.45)
          * Apply bonuses for supporting evidence
          * Apply penalties for contradicting evidence
          * ENFORCE CEILING: maximum 0.50 (weak methods NEVER high confidence)

        Args:
            computer_id: Computer ID
            lab_id: Lab ID
            base_method: Discovery method that found this association
            base_confidence: Initial confidence from discovery method

        Returns:
            Tuple of (final_confidence_score, criteria_dict)
        """
        # Start with discovery method's base confidence
        score = base_confidence

        # Get records
        computer = self.computers_cache.get(computer_id, {})
        lab = self.labs_cache.get(lab_id, {})
        members = self.lab_members_cache.get(lab_id, set())

        # Extract fields
        owner = computer.get("owner_uniqname")
        fin_owner = computer.get("financial_owner_uniqname")
        pi = lab.get("pi_uniqname")
        function_id = self.function_cache.get(computer_id)
        computer_name = (computer.get("computer_name") or "").lower()
        ad_dn = (computer.get("ad_distinguished_name") or "").lower()
        lab_ou = (lab.get("ad_ou_dn") or "").lower()

        # Check criteria (these become the boolean fields)
        owner_is_pi = owner == pi
        fin_owner_is_pi = fin_owner == pi
        owner_is_member = (owner in members and owner != pi) if owner else False
        fin_owner_is_member = (
            (fin_owner in members and fin_owner != pi) if fin_owner else False
        )
        function_is_research = function_id == self.FUNCTION_RESEARCH
        function_is_classroom = function_id == self.FUNCTION_CLASSROOM

        # Additional signals for scoring (not stored as fields)
        name_contains_pi = pi and pi.lower() in computer_name
        in_lab_ou = lab_ou and lab_ou in ad_dn

        # =================================================================
        # APPLY BONUSES (supporting evidence not part of primary discovery)
        # =================================================================

        if fin_owner_is_pi and base_method != "fin_owner_is_pi":
            score += Decimal("0.15")

        if owner_is_pi and base_method != "owner_is_pi":
            score += Decimal("0.12")

        if in_lab_ou and base_method != "ad_ou_nested":
            score += Decimal("0.10")

        if name_contains_pi and base_method != "name_pattern_pi":
            score += Decimal("0.08")

        if function_is_research:
            score += Decimal("0.05")

        if function_is_classroom:
            score += Decimal("0.03")

        # =================================================================
        # APPLY PENALTIES (contradicting evidence)
        # =================================================================

        if not owner_is_pi and not owner_is_member and owner:
            score -= Decimal("0.10")

        if not fin_owner_is_pi and not fin_owner_is_member and fin_owner:
            score -= Decimal("0.08")

        if function_id == self.FUNCTION_ADMIN_STAFF:
            score -= Decimal("0.12")

        if function_id == self.FUNCTION_DEV_TESTING:
            score -= Decimal("0.12")

        if (
            function_id
            not in [
                self.FUNCTION_RESEARCH,
                self.FUNCTION_CLASSROOM,
                self.FUNCTION_ADMIN_STAFF,
                self.FUNCTION_DEV_TESTING,
            ]
            and function_id
        ):
            score -= Decimal("0.05")

        # =================================================================
        # ENFORCE HIERARCHICAL TIER BOUNDS
        # =================================================================

        # Tier 1: Strong discovery methods (floor: 0.70, ceiling: 1.00)
        TIER_1_METHODS = {
            "ad_ou_nested",
            "owner_is_pi",
            "fin_owner_is_pi",
            "name_pattern_pi",
        }

        # Tier 2: Weak discovery methods (floor: 0.20, ceiling: 0.50)
        TIER_2_METHODS = {"group_membership", "owner_member", "last_user_member"}

        if base_method in TIER_1_METHODS:
            # Tier 1: Enforce floor of 0.70 and ceiling of 1.00
            score = max(Decimal("0.70"), min(Decimal("1.00"), score))
        elif base_method in TIER_2_METHODS:
            # Tier 2: Enforce floor of 0.20 and ceiling of 0.50
            score = max(Decimal("0.20"), min(Decimal("0.50"), score))
        else:
            # Unknown method: use absolute bounds
            score = max(Decimal("0.20"), min(Decimal("1.00"), score))

        criteria = {
            "owner_is_pi": owner_is_pi,
            "fin_owner_is_pi": fin_owner_is_pi,
            "owner_is_member": owner_is_member,
            "fin_owner_is_member": fin_owner_is_member,
            "function_is_research": function_is_research,
            "function_is_classroom": function_is_classroom,
        }

        return score, criteria

    # ========================================================================
    # DATABASE OPERATIONS
    # ========================================================================

    def _insert_associations(self, associations: List[Dict[str, Any]]) -> int:
        """
        Insert associations into silver.lab_computers (full refresh).

        Args:
            associations: List of association dicts with all fields

        Returns:
            Number of associations inserted
        """
        if not associations:
            logger.warning("⚠️  No associations to insert")
            return 0

        if self.dry_run:
            logger.info(f"[DRY RUN] Would insert {len(associations)} associations")
            return len(associations)

        logger.info(f"✍️  Inserting {len(associations)} associations...")

        # Full refresh: TRUNCATE + INSERT
        with self.db_adapter.engine.connect() as conn:
            with conn.begin():
                # Clear existing data
                logger.info("   🗑️  Truncating lab_computers...")
                conn.execute(text("TRUNCATE TABLE silver.lab_computers CASCADE"))

                # Batch insert
                chunk_size = 5000
                for i in range(0, len(associations), chunk_size):
                    chunk = associations[i : i + chunk_size]

                    conn.execute(
                        text("""
                            INSERT INTO silver.lab_computers (
                                computer_id,
                                lab_id,
                                association_method,
                                confidence_score,
                                owner_is_pi,
                                fin_owner_is_pi,
                                owner_is_member,
                                fin_owner_is_member,
                                function_is_research,
                                function_is_classroom,
                                matched_ou,
                                matched_group_id,
                                matched_user,
                                is_primary,
                                quality_flags
                            ) VALUES (
                                :computer_id,
                                :lab_id,
                                :association_method,
                                :confidence_score,
                                :owner_is_pi,
                                :fin_owner_is_pi,
                                :owner_is_member,
                                :fin_owner_is_member,
                                :function_is_research,
                                :function_is_classroom,
                                :matched_ou,
                                :matched_group_id,
                                :matched_user,
                                :is_primary,
                                CAST(:quality_flags AS jsonb)
                            )
                        """),
                        chunk,
                    )

                    logger.info(
                        f"   Inserted chunk {i // chunk_size + 1}/{(len(associations) + chunk_size - 1) // chunk_size}"
                    )

        logger.info(f"✅ Inserted {len(associations)} associations")
        return len(associations)

    def _update_primary_labs(self):
        """
        Update computers.primary_lab_id with highest confidence association.
        """
        if self.dry_run:
            logger.info("[DRY RUN] Would update primary labs in silver.computers")
            return

        logger.info("🔄 Updating primary labs in silver.computers...")

        query = """
            WITH primary_labs AS (
                SELECT DISTINCT ON (computer_id)
                    computer_id,
                    lab_id,
                    association_method,
                    confidence_score
                FROM silver.lab_computers
                ORDER BY computer_id, confidence_score DESC, lab_id
            )
            UPDATE silver.computers c
            SET
                primary_lab_id = pl.lab_id,
                primary_lab_method = pl.association_method,
                lab_association_count = (
                    SELECT COUNT(*)
                    FROM silver.lab_computers lc
                    WHERE lc.computer_id = c.computer_id
                )
            FROM primary_labs pl
            WHERE c.computer_id = pl.computer_id
        """

        with self.db_adapter.engine.connect() as conn:
            with conn.begin():
                result = conn.execute(text(query))
                logger.info(f"   Updated {result.rowcount} computers with primary lab")

    # ========================================================================
    # QUALITY METRICS (Phase 5)
    # ========================================================================

    def _calculate_quality_flags(
        self,
        computer_id: str,
        lab_id: str,
        confidence_score: Decimal,
        criteria: Dict[str, bool],
    ) -> List[str]:
        """
        Calculate quality flags for association (Phase 5).

        Flags indicate potential issues or interesting patterns for monitoring.

        Args:
            computer_id: Computer ID
            lab_id: Lab ID
            confidence_score: Calculated confidence score
            criteria: Criteria dict from confidence calculation

        Returns:
            List of quality flag strings
        """
        flags = []

        # Confidence-based flags
        if confidence_score < Decimal("0.40"):
            flags.append("low_confidence")
        elif confidence_score >= Decimal("0.90"):
            flags.append("high_confidence")

        # Ownership pattern flags
        if criteria["owner_is_pi"] and criteria["fin_owner_is_pi"]:
            flags.append("fully_pi_owned")

        if not criteria["owner_is_pi"] and not criteria["owner_is_member"]:
            flags.append("owner_not_affiliated")

        if not criteria["fin_owner_is_pi"] and not criteria["fin_owner_is_member"]:
            flags.append("fin_owner_not_affiliated")

        # Function-based flags
        computer = self.computers_cache.get(computer_id, {})
        function_id = self.function_cache.get(computer_id)

        if function_id == self.FUNCTION_ADMIN_STAFF:
            flags.append("admin_function")
        elif function_id == self.FUNCTION_DEV_TESTING:
            flags.append("dev_function")
        elif not function_id:
            flags.append("no_function")

        return flags

    # ========================================================================
    # META TRACKING (Phase 4)
    # ========================================================================

    def create_ingestion_run(self) -> str:
        """Create meta.ingestion_runs tracking record."""
        run_id = str(uuid4())

        if self.dry_run:
            logger.info(f"[DRY RUN] Would create ingestion run: {run_id}")
            return run_id

        with self.db_adapter.engine.connect() as conn:
            conn.execute(
                text("""
                INSERT INTO meta.ingestion_runs (
                    run_id, source_system, entity_type, started_at, status, metadata
                ) VALUES (
                    :run_id, 'silver_transformation', 'lab_computers',
                    :started_at, 'running', :metadata
                )
            """),
                {
                    "run_id": run_id,
                    "started_at": datetime.now(timezone.utc),
                    "metadata": json.dumps(
                        {
                            "script": "014_transform_lab_computers.py",
                            "strategy": "full_refresh",
                            "dry_run": self.dry_run,
                        }
                    ),
                },
            )
            conn.commit()

        logger.info(f"📝 Created ingestion run: {run_id}")
        return run_id

    def complete_ingestion_run(
        self,
        records_processed: int,
        records_created: int,
        error_message: Optional[str] = None,
    ):
        """
        Complete meta.ingestion_runs tracking record.

        Handles all failure scenarios including exceptions and keyboard interrupts.
        """
        if self.dry_run or not self.run_id:
            return

        status = "failed" if error_message else "completed"

        try:
            with self.db_adapter.engine.connect() as conn:
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
                        "run_id": self.run_id,
                        "completed_at": datetime.now(timezone.utc),
                        "status": status,
                        "records_processed": records_processed,
                        "records_created": records_created,
                        "error_message": error_message,
                    },
                )
                conn.commit()
                logger.info(
                    f"✅ Completed ingestion run: {self.run_id} (status: {status})"
                )
        except Exception as meta_error:
            # If meta tracking fails, log but don't crash
            logger.error(f"❌ Failed to update meta.ingestion_runs: {meta_error}")

    # ========================================================================
    # MAIN TRANSFORMATION
    # ========================================================================

    def transform(self) -> Dict[str, Any]:
        """
        Main transformation logic with robust error handling (Phase 4).

        Ensures meta tracking is updated even on errors or keyboard interrupts.

        Returns:
            Dict with statistics
        """
        logger.info("=" * 80)
        logger.info("🚀 Starting Lab Computers Transformation (Phases 1-6)")
        logger.info("=" * 80)

        start_time = datetime.now(timezone.utc)

        # Create tracking record (Phase 4)
        self.run_id = self.create_ingestion_run()

        stats = {
            "run_id": self.run_id,
            "total_associations": 0,
            "unique_computers": 0,
            "unique_labs": 0,
            "primary_associations": 0,
            "duration_seconds": 0,
            "dry_run": self.dry_run,
        }

        try:
            # Step 1: Load caches
            logger.info("\n📊 Step 1: Loading Caches")
            logger.info("-" * 80)
            self._load_labs_cache()
            self._load_computers_cache()
            self._load_lab_members_cache()
            self._load_function_cache()

            # Step 2: Discover associations
            logger.info("\n🔍 Step 2: Discovering Associations")
            logger.info("-" * 80)
            all_associations = []
            all_associations.extend(self._discover_by_ad_ou())
            all_associations.extend(self._discover_by_owner_pi())
            all_associations.extend(
                self._discover_by_financial_owner_pi()
            )  # NEW - Phase 1
            all_associations.extend(
                self._discover_by_name_pattern_pi()
            )  # NEW - Phase 2
            all_associations.extend(self._discover_by_group_membership())
            all_associations.extend(self._discover_by_owner_member())
            all_associations.extend(self._discover_by_last_user_member())

            logger.info(f"\n📈 Total discovered: {len(all_associations)}")

            # Step 3: Deduplicate
            logger.info("\n🔗 Step 3: Deduplicating")
            logger.info("-" * 80)
            unique_associations = self._deduplicate_associations(all_associations)

            # Step 4: Calculate additive confidence
            logger.info("\n🎯 Step 4: Calculating Additive Confidence")
            logger.info("-" * 80)
            enhanced_associations = []

            for assoc in unique_associations:
                score, criteria = self._calculate_additive_confidence(
                    assoc["computer_id"],
                    assoc["lab_id"],
                    assoc["method"],
                    assoc["base_confidence"],  # NEW - Pass base confidence for Phase 3
                )

                assoc["confidence_score"] = score
                assoc["owner_is_pi"] = criteria["owner_is_pi"]
                assoc["fin_owner_is_pi"] = criteria["fin_owner_is_pi"]
                assoc["owner_is_member"] = criteria["owner_is_member"]
                assoc["fin_owner_is_member"] = criteria["fin_owner_is_member"]
                assoc["function_is_research"] = criteria["function_is_research"]
                assoc["function_is_classroom"] = criteria["function_is_classroom"]
                assoc["association_method"] = assoc["method"]
                assoc["is_primary"] = False  # Will be updated later

                # Calculate quality flags (Phase 5)
                quality_flags = self._calculate_quality_flags(
                    assoc["computer_id"], assoc["lab_id"], score, criteria
                )
                assoc["quality_flags"] = json.dumps(
                    quality_flags
                )  # Convert to JSON string for JSONB

                enhanced_associations.append(assoc)

            # Show confidence distribution
            confidence_buckets = {
                "Perfect (1.0)": 0,
                "Very High (0.80-0.99)": 0,
                "High (0.60-0.79)": 0,
                "Medium (0.40-0.59)": 0,
                "Low (<0.40)": 0,
            }

            for assoc in enhanced_associations:
                score = float(assoc["confidence_score"])
                if score == 1.0:
                    confidence_buckets["Perfect (1.0)"] += 1
                elif score >= 0.80:
                    confidence_buckets["Very High (0.80-0.99)"] += 1
                elif score >= 0.60:
                    confidence_buckets["High (0.60-0.79)"] += 1
                elif score >= 0.40:
                    confidence_buckets["Medium (0.40-0.59)"] += 1
                else:
                    confidence_buckets["Low (<0.40)"] += 1

            logger.info("   Confidence Distribution:")
            for bucket, count in confidence_buckets.items():
                pct = (
                    100.0 * count / len(enhanced_associations)
                    if enhanced_associations
                    else 0
                )
                logger.info(f"     {bucket}: {count} ({pct:.1f}%)")

            # Step 5: Mark primary associations
            logger.info("\n🏆 Step 5: Marking Primary Associations")
            logger.info("-" * 80)

            # Group by computer_id, find highest confidence
            primary_map = {}
            for assoc in enhanced_associations:
                computer_id = assoc["computer_id"]
                if computer_id not in primary_map:
                    primary_map[computer_id] = assoc
                elif (
                    assoc["confidence_score"]
                    > primary_map[computer_id]["confidence_score"]
                ):
                    primary_map[computer_id] = assoc

            # Mark as primary
            for computer_id, primary_assoc in primary_map.items():
                primary_assoc["is_primary"] = True

            primary_count = sum(1 for a in enhanced_associations if a["is_primary"])
            logger.info(f"   Marked {primary_count} primary associations")

            # Step 6: Insert into database
            logger.info("\n💾 Step 6: Inserting Into Database")
            logger.info("-" * 80)
            inserted = self._insert_associations(enhanced_associations)

            # Step 7: Update primary labs in computers
            logger.info("\n🔄 Step 7: Updating Primary Labs")
            logger.info("-" * 80)
            self._update_primary_labs()

            # Calculate final statistics
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            unique_computers = len(set(a["computer_id"] for a in enhanced_associations))
            unique_labs = len(set(a["lab_id"] for a in enhanced_associations))

            stats.update(
                {
                    "total_associations": len(enhanced_associations),
                    "unique_computers": unique_computers,
                    "unique_labs": unique_labs,
                    "primary_associations": primary_count,
                    "confidence_distribution": confidence_buckets,
                    "duration_seconds": duration,
                }
            )

            # Mark run as successful (Phase 4)
            self.complete_ingestion_run(
                records_processed=len(enhanced_associations), records_created=inserted
            )

            logger.info("\n" + "=" * 80)
            logger.info("✅ Transformation Complete!")
            logger.info("=" * 80)
            logger.info(f"   Total Associations: {stats['total_associations']}")
            logger.info(f"   Unique Computers: {stats['unique_computers']}")
            logger.info(f"   Unique Labs: {stats['unique_labs']}")
            logger.info(f"   Duration: {duration:.2f} seconds")

            return stats

        except KeyboardInterrupt:
            # Handle Ctrl+C gracefully (Phase 4)
            error_msg = "Transformation interrupted by user (Ctrl+C)"
            logger.warning(f"\n⚠️  {error_msg}")

            self.complete_ingestion_run(
                records_processed=stats.get("total_associations", 0),
                records_created=0,
                error_message=error_msg,
            )
            raise

        except Exception as e:
            # Handle any other errors (Phase 4)
            error_msg = f"Transformation failed: {str(e)}"
            logger.error(f"\n❌ {error_msg}", exc_info=True)

            self.complete_ingestion_run(
                records_processed=stats.get("total_associations", 0),
                records_created=0,
                error_message=error_msg,
            )
            raise

    def close(self):
        """Close database connection."""
        self.db_adapter.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Transform silver.computers → silver.lab_computers"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )

    args = parser.parse_args()

    # Database connection
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("❌ DATABASE_URL not set in environment")
        sys.exit(1)

    db_adapter = PostgresAdapter(
        database_url=database_url, pool_size=5, max_overflow=10
    )

    try:
        service = LabComputersTransformationService(db_adapter, dry_run=args.dry_run)
        stats = service.transform()

        if args.dry_run:
            logger.info("\n" + "=" * 80)
            logger.info("🧪 DRY RUN - No changes committed")
            logger.info("=" * 80)

        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}", exc_info=True)
        sys.exit(1)

    finally:
        db_adapter.close()


if __name__ == "__main__":
    main()
