"""
Lab Manager Identification Service

Identifies up to 3 lab managers per legitimate lab based on member roles and job codes.
Replaces database functions calculate_lab_manager_score() and populate_lab_managers().

This service implements the lab manager scoring algorithm with two tiers:
- Tier 1 (Scores 1-4): Automatic assignment - always selected if found
- Tier 2 (Scores 5-10): Conditional assignment - only if no Tier 1 exists

Key features:
- Score-based prioritization (1 = highest confidence, 10 = lowest)
- Special handling for small labs (≤3 non-PI members)
- Maximum 3 managers per lab
- First-match-wins pattern evaluation
"""

import logging
from typing import Any, Dict, List, Optional

from database.adapters.postgres_adapter import PostgresAdapter

logger = logging.getLogger(__name__)


class ScoringRule:
    """
    Represents a single scoring rule for manager identification.

    Attributes:
        score: Confidence score (1-10, lower is better)
        role_pattern: SQL ILIKE pattern for role matching (None if no role check)
        role_exact: Exact role match string (takes precedence over pattern)
        job_code: Job code to match (None if no job code check)
        detection_reason: Human-readable explanation
        tier: 1 (automatic) or 2 (conditional)
    """

    def __init__(
        self,
        score: int,
        detection_reason: str,
        tier: int = 1,
        role_pattern: Optional[str] = None,
        role_exact: Optional[str] = None,
        job_code: Optional[str] = None,
    ):
        self.score = score
        self.detection_reason = detection_reason
        self.tier = tier
        self.role_pattern = role_pattern
        self.role_exact = role_exact
        self.job_code = job_code

    def matches(
        self, member_role: Optional[str], job_codes: Optional[List[str]]
    ) -> bool:
        """
        Check if this rule matches the member's role or job codes.

        Uses OR logic: Either role OR job code qualifies.

        Args:
            member_role: The member's role text
            job_codes: List of job code strings

        Returns:
            True if rule matches, False otherwise
        """
        role_match = False
        job_match = False

        # Check role matching
        if member_role:
            if self.role_exact:
                # Exact match (case-sensitive)
                role_match = member_role == self.role_exact
            elif self.role_pattern:
                # Pattern match (case-insensitive)
                role_match = self._ilike_match(member_role, self.role_pattern)

        # Check job code matching
        if self.job_code and job_codes:
            job_match = self.job_code in job_codes

        # OR logic: either qualifies
        return role_match or job_match

    @staticmethod
    def _ilike_match(text: str, pattern: str) -> bool:
        """
        Implement SQL ILIKE pattern matching (case-insensitive substring).

        Args:
            text: Text to search in
            pattern: Pattern like '%substring%'

        Returns:
            True if pattern matches
        """
        # Remove % wildcards and do case-insensitive substring match
        search_str = pattern.replace("%", "")
        return search_str.lower() in text.lower()


class LabManagerIdentificationService:
    """
    Service for identifying lab managers using score-based rules.

    Replaces database functions:
    - calculate_lab_manager_score()
    - populate_lab_managers()

    The service implements a two-tier scoring system:
    - Tier 1 (Scores 1-4): High-confidence roles (Lab Manager, Coordinator, etc.)
    - Tier 2 (Scores 5-10): Lower-confidence fallback roles (Research Fellow, grad students)

    Tier 2 is ONLY used if no Tier 1 managers exist for a lab.
    """

    # Define scoring rules in priority order (first match wins)
    SCORING_RULES = [
        # Tier 1 - Score 1: Explicit Managers (Automatic)
        ScoringRule(
            score=1,
            role_pattern="%Lab Manager%",
            job_code="102945",
            detection_reason="Explicit Lab Manager (role or job code 102945)",
            tier=1,
        ),
        ScoringRule(
            score=1,
            role_pattern="%Lab Coordinator%",
            job_code="102946",
            detection_reason="Lab Coordinator (role or job code 102946)",
            tier=1,
        ),
        ScoringRule(
            score=1,
            role_pattern="%Laboratory Manager%",
            job_code="102929",
            detection_reason="Laboratory Manager (role or job code 102929)",
            tier=1,
        ),
        # Tier 1 - Score 2: Administrative Coordinators (Automatic)
        ScoringRule(
            score=2,
            role_exact="Admin Coord/Project Coord",
            detection_reason="Administrative/Project Coordinator",
            tier=1,
        ),
        ScoringRule(
            score=2,
            role_pattern="%Project Coordinator%",
            detection_reason="Project Coordinator (variant)",
            tier=1,
        ),
        ScoringRule(
            score=2,
            role_pattern="%Administrative Coordinator%",
            detection_reason="Administrative Coordinator",
            tier=1,
        ),
        # Tier 1 - Score 3: Specialist Leads (Automatic)
        ScoringRule(
            score=3,
            role_exact="Research Lab Specialist Lead",
            job_code="102909",
            detection_reason="Research Lab Specialist Lead (role or job code 102909)",
            tier=1,
        ),
        # Tier 1 - Score 4: Specialist Lead Variants (Automatic)
        ScoringRule(
            score=4,
            role_pattern="%Research Lab Specialist Lead%",
            detection_reason="Research Lab Specialist Lead (variant)",
            tier=1,
        ),
        # Tier 2 - Score 5: Research Fellows (Conditional)
        ScoringRule(
            score=5,
            role_pattern="Research Fellow%",
            detection_reason="Research Fellow",
            tier=2,
        ),
        # Tier 2 - Score 6: Senior Technicians (Conditional)
        ScoringRule(
            score=6,
            role_pattern="%Tech%Sr%",
            job_code="102944",
            detection_reason="Senior Technician (Tech Sr or job code 102944)",
            tier=2,
        ),
        # Tier 2 - Score 7: Leadership Roles (Conditional)
        ScoringRule(
            score=7,
            role_pattern="%Lead%",
            detection_reason='Leadership role (contains "Lead")',
            tier=2,
        ),
        # Tier 2 - Score 8: Research Scientists (Conditional)
        ScoringRule(
            score=8,
            role_pattern="%Research Scientist%",
            detection_reason="Research Scientist",
            tier=2,
        ),
        # Tier 2 - Score 9: Fallback Roles (Conditional)
        ScoringRule(
            score=9,
            role_exact="Graduate Student Instructor and Graduate Student Research Assistant",
            detection_reason="Graduate Student (dual GSI/GSRA)",
            tier=2,
        ),
        ScoringRule(
            score=9,
            role_exact="Graduate Student Research Assistant and Graduate Student Instructor",
            detection_reason="Graduate Student (dual GSRA/GSI)",
            tier=2,
        ),
        ScoringRule(
            score=9,
            role_pattern="%Research Lab Specialist Senior%",
            detection_reason="Research Lab Specialist Senior",
            tier=2,
        ),
        # Tier 2 - Score 10: Graduate Students (Last Resort)
        ScoringRule(
            score=10,
            role_pattern="%Graduate Student Instructor%",
            detection_reason="Graduate Student (GSI)",
            tier=2,
        ),
        ScoringRule(
            score=10,
            role_pattern="%Graduate Student Research Assistant%",
            detection_reason="Graduate Student (GSRA)",
            tier=2,
        ),
    ]

    def __init__(self, database_url: str):
        """
        Initialize the lab manager identification service.

        Args:
            database_url: PostgreSQL connection string
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        logger.info("✨ Lab manager identification service initialized")

    def calculate_manager_score(
        self, member_role: Optional[str], job_codes: Optional[List[str]]
    ) -> Optional[Dict[str, Any]]:
        """
        Calculate confidence score for a potential lab manager.

        Port of calculate_lab_manager_score() database function.
        Evaluates scoring rules in order; first match wins.

        Args:
            member_role: The member's role text
            job_codes: List of job code strings

        Returns:
            Dict with 'confidence_score', 'detection_reason', 'tier' or None if no match
        """
        # Handle None/empty inputs
        if not member_role and not job_codes:
            return None

        # Evaluate rules in priority order (first match wins)
        for rule in self.SCORING_RULES:
            if rule.matches(member_role, job_codes):
                return {
                    "confidence_score": rule.score,
                    "detection_reason": rule.detection_reason,
                    "tier": rule.tier,
                }

        # No match found
        return None

    def identify_managers_for_lab(self, lab_id: str) -> List[Dict[str, Any]]:
        """
        Identify up to 3 managers for a single lab using role-based scoring.

        Algorithm:
        1. Score all eligible members using scoring rules (Tier 1 and Tier 2)
        2. If any Tier 1 (scores 1-4), use only Tier 1
        3. If no Tier 1, use Tier 2 (scores 5-10)
        4. Rank by score (ascending) then role (alphabetical)
        5. Return top 3

        Tier 1 (High Confidence):
        - Lab Managers, Coordinators, Specialist Leads (scores 1-4)

        Tier 2 (Fallback):
        - Research Fellows, Graduate Students, Scientists (scores 5-10)

        Args:
            lab_id: The lab identifier

        Returns:
            List of manager dicts with uniqname, rank, score, reason
        """
        # Step 1: Get lab info from v_legitimate_labs
        lab_query = """
            SELECT lab_id, member_count, pi_uniqname
            FROM silver.v_legitimate_labs
            WHERE lab_id = :lab_id
        """

        lab_df = self.db_adapter.query_to_dataframe(lab_query, {"lab_id": lab_id})

        if lab_df.empty:
            logger.warning(
                f"   Lab '{lab_id}' not found in v_legitimate_labs - skipping"
            )
            return []

        lab = lab_df.iloc[0]

        # Step 2: Get eligible members from view
        eligible_query = """
            SELECT
                membership_id,
                lab_id,
                member_uniqname,
                member_role,
                member_job_title,
                is_pi,
                is_investigator,
                job_codes,
                tdx_user_uid
            FROM silver.v_eligible_lab_members
            WHERE lab_id = :lab_id
        """

        eligible_df = self.db_adapter.query_to_dataframe(
            eligible_query, {"lab_id": lab_id}
        )

        if eligible_df.empty:
            logger.info(f"   Lab '{lab_id}': No eligible members")
            return []

        # Step 3: Score all eligible members
        scored_members = []

        for idx, member in eligible_df.iterrows():
            # Convert JSONB job_codes to list of strings
            job_codes = None
            if member["job_codes"]:
                if isinstance(member["job_codes"], list):
                    job_codes = [str(code) for code in member["job_codes"]]
                elif isinstance(member["job_codes"], dict):
                    # Handle case where job_codes is stored as JSON object
                    job_codes = [str(code) for code in member["job_codes"].values()]

            score_result = self.calculate_manager_score(
                member["member_role"], job_codes
            )

            if score_result:
                scored_members.append(
                    {
                        "lab_id": lab_id,
                        "manager_uniqname": member["member_uniqname"],
                        "manager_tdx_uid": member["tdx_user_uid"],
                        "manager_role": member["member_role"],
                        "manager_job_codes": member["job_codes"],
                        "manager_confidence_score": score_result["confidence_score"],
                        "detection_reason": score_result["detection_reason"],
                        "tier": score_result["tier"],
                    }
                )

        if not scored_members:
            logger.info(f"   Lab '{lab_id}': No members match scoring criteria")
            return []

        # Step 4: Check for Tier 1 (scores 1-4) managers
        has_tier1 = any(m["tier"] == 1 for m in scored_members)

        # Step 5: Filter based on tier logic
        if has_tier1:
            # Only use Tier 1 managers
            final_managers = [m for m in scored_members if m["tier"] == 1]
            logger.debug(
                f"   Lab '{lab_id}': Found {len(final_managers)} Tier 1 managers - excluding Tier 2"
            )
        else:
            # No Tier 1 found - use Tier 2
            final_managers = scored_members
            logger.debug(
                f"   Lab '{lab_id}': No Tier 1 managers - using {len(final_managers)} Tier 2 candidates"
            )

        # Step 6: Sort by score (ascending) then role (alphabetical)
        final_managers.sort(
            key=lambda x: (x["manager_confidence_score"], x["manager_role"])
        )

        # Step 7: Assign ranks and limit to 3
        for rank, manager in enumerate(final_managers[:3], start=1):
            manager["manager_rank"] = rank
            # Remove tier field (internal use only)
            manager.pop("tier", None)

        return final_managers[:3]

    def close(self):
        """Close database connections."""
        if self.db_adapter:
            self.db_adapter.close()
            logger.info("Database connections closed")
