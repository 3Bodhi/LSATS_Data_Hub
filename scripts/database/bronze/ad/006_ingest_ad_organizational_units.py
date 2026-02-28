#!/usr/bin/env python3
"""
Active Directory LDAP Organizational Unit (OU) Ingestion Service

This service ingests organizational unit data from the University of Michigan Active
Directory LDAP directory (adsroot.itcs.umich.edu) into the bronze layer for
cross-referencing and analysis.

Ingests OUs from two primary branches:
1. Research and Instrumentation - Lab-focused organizational structure
2. Workstations - Computer workstation organizational structure

Both structures enable lab entity identification and computer compliance management workflows.

Active Directory OU structure provides:
- Organizational hierarchy (distinguishedName, parent-child relationships)
- OU metadata (name, description, managedBy)
- Child entity information (computers, sub-OUs)
- Group policy assignments (gPLink)

All OU records are stored with objectGUID as the unique external identifier.
OUs are filtered by objectClass=organizationalUnit.

Bronze Layer Approach:
- Captures ALL organizational units within configured search bases
- Enriches with structural metadata (hierarchy, child counts, patterns)
- Does NOT classify or filter for "labs" - that's silver layer responsibility
- Preserves complete change history via content hashing

IMPORTANT: The enrichment metadata (_has_computer_children, _child_ou_count, etc.)
is computed during ingestion because it requires active LDAP queries that would be
expensive to recreate later.
"""

import argparse
import base64
import hashlib
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# Core Python imports for PostgreSQL operations
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.pool import QueuePool

# Add your LSATS project to Python path
# Add your LSATS project to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter, create_postgres_adapter
from ldap.adapters.ldap_adapter import LDAPAdapter

script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "/var/log/lsats/bronze"
os.makedirs(log_dir, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"{log_dir}/{script_name}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class ActiveDirectoryOUIngestionService:
    """
    Organizational Unit ingestion service for University of Michigan Active Directory.

    Uses content hashing for change detection since LDAP doesn't provide
    modification timestamps in a consistent way. This approach:

    1. Fetches current OU data from Active Directory LDAP (configurable search bases)
    2. Enriches with structural metadata (hierarchy, child counts, name patterns)
    3. Calculates content hashes for each OU (including enrichment)
    4. Compares against stored hashes from previous ingestions
    5. Only creates new bronze records when OU content has actually changed
    6. Preserves complete change history for OU analysis

    Key Features:
    - Configurable search bases for flexible scope
    - Inline enrichment with hierarchical parsing
    - Child entity queries (computers, sub-OUs) during ingestion
    - Name pattern detection for lab identification hints
    - Efficient change detection without requiring timestamps
    - Comprehensive audit trail for OU changes
    - Detailed ingestion statistics and monitoring

    Design Philosophy:
    - Bronze layer captures ALL OUs without business logic filtering
    - Enrichment metadata enables silver layer lab classification
    - Entity type is always "organizational_unit" regardless of purpose
    - Lab identification happens in silver layer transformation
    """

    def __init__(
        self,
        database_url: str,
        ldap_config: Dict[str, Any],
        search_bases: Optional[List[str]] = None,
        force_full_sync: bool = False,
        dry_run: bool = False,
        batch_size: int = 500,
    ):
        """
        Initialize the Active Directory OU ingestion service.

        Args:
            database_url: PostgreSQL connection string
            ldap_config: LDAP connection configuration dictionary
            search_bases: List of DN search bases to ingest OUs from.
                         If None, defaults to Research and Instrumentation OU.
            force_full_sync: If True, bypass timestamp filtering and perform full sync
            dry_run: If True, preview changes without committing to database
            batch_size: Batch size for processing
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        # Initialize LDAP adapter for Active Directory
        self.ldap_adapter = LDAPAdapter(ldap_config)

        # Store configuration
        self.force_full_sync = force_full_sync
        self.dry_run = dry_run
        self.batch_size = batch_size

        # Test LDAP connection
        if not self.ldap_adapter.test_connection():
            raise Exception("Failed to connect to Active Directory LDAP")

        # Configure search bases (default to Research & Instrumentation + Workstations)
        self.search_bases = search_bases or [
            "OU=Research and Instrumentation,OU=LSA,OU=Organizations,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
            "OU=Workstations,OU=LSA,OU=Organizations,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
        ]

        logger.info(
            f"Active Directory OU ingestion service initialized with {len(self.search_bases)} search base(s)"
        )
        for search_base in self.search_bases:
            logger.info(f"  - {search_base}")

    def _normalize_ldap_attribute(self, value: Any) -> Any:
        """
        Normalize LDAP attribute values for consistent hashing and JSON serialization.

        LDAP attributes can be single values, lists, bytes, or None. Binary fields
        are converted to base64 to avoid issues with null bytes in PostgreSQL JSON/JSONB.

        Args:
            value: Raw LDAP attribute value

        Returns:
            Normalized value suitable for JSON serialization
        """
        if value is None:
            return ""
        elif isinstance(value, bytes):
            # Handle binary attributes using base64 encoding
            try:
                # Try to decode as UTF-8 for string-like fields
                decoded = value.decode("utf-8")
                # Check if it contains null bytes or other problematic characters
                if "\x00" in decoded or any(
                    ord(c) < 32 and c not in "\t\n\r" for c in decoded
                ):
                    # Contains binary data, use base64
                    return base64.b64encode(value).decode("ascii")
                else:
                    return decoded.strip()
            except UnicodeDecodeError:
                # Not valid UTF-8, definitely binary data
                return base64.b64encode(value).decode("ascii")
        elif isinstance(value, datetime):
            # Handle datetime objects
            return value.isoformat()
        elif isinstance(value, list):
            if len(value) == 0:
                return ""
            elif len(value) == 1:
                item = value[0]
                if isinstance(item, bytes):
                    try:
                        decoded = item.decode("utf-8")
                        if "\x00" in decoded or any(
                            ord(c) < 32 and c not in "\t\n\r" for c in decoded
                        ):
                            return base64.b64encode(item).decode("ascii")
                        else:
                            return decoded.strip()
                    except UnicodeDecodeError:
                        return base64.b64encode(item).decode("ascii")
                elif isinstance(item, datetime):
                    return item.isoformat()
                else:
                    return str(item).strip()
            else:
                # Sort multi-value attributes for consistent hashing
                normalized_items = []
                for item in value:
                    if isinstance(item, bytes):
                        try:
                            decoded = item.decode("utf-8")
                            if "\x00" in decoded or any(
                                ord(c) < 32 and c not in "\t\n\r" for c in decoded
                            ):
                                normalized_items.append(
                                    base64.b64encode(item).decode("ascii")
                                )
                            else:
                                normalized_items.append(decoded.strip())
                        except UnicodeDecodeError:
                            normalized_items.append(
                                base64.b64encode(item).decode("ascii")
                            )
                    elif isinstance(item, datetime):
                        normalized_items.append(item.isoformat())
                    else:
                        normalized_items.append(str(item).strip())
                return sorted(normalized_items)
        else:
            return str(value).strip()

    def _normalize_raw_data_for_json(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively normalize all values in a dictionary for JSON serialization.

        This ensures that datetime objects, bytes, and other non-JSON-serializable
        types are converted before inserting into the database.

        Args:
            data: Dictionary with raw LDAP data

        Returns:
            Dictionary with all values normalized for JSON serialization
        """
        normalized = {}
        for key, value in data.items():
            normalized[key] = self._normalize_ldap_attribute(value)
        return normalized

    def _parse_ou_hierarchy(self, distinguished_name: str) -> List[str]:
        """
        Parse a distinguished name into an ordered list of OU components.

        Args:
            distinguished_name: Full DN like "OU=lab,OU=dept,OU=region,OU=current,..."

        Returns:
            List of OU names from most specific to least specific
            Example: ["lab", "dept", "region", "current", "Research and Instrumentation", ...]
        """
        if not distinguished_name:
            return []

        # Extract OU components (ignore DC components)
        ou_pattern = re.compile(r"OU=([^,]+)", re.IGNORECASE)
        matches = ou_pattern.findall(distinguished_name)

        # Return in order from most specific (closest to object) to least specific (root)
        return matches

    def _extract_parent_ou(self, distinguished_name: str) -> str:
        """
        Extract the immediate parent OU distinguished name.

        Args:
            distinguished_name: Full DN of the current OU

        Returns:
            DN of parent OU, or empty string if no parent
        """
        if not distinguished_name:
            return ""

        # Find first comma after "OU="
        # Parent DN is everything after the first OU component
        parts = distinguished_name.split(",", 1)
        if len(parts) == 2:
            return parts[1]
        return ""

    def _extract_uniqname(self, ou_name: str) -> Optional[str]:
        """
        Attempt to extract a uniqname from OU name using common patterns.

        Common lab OU patterns:
        - "psyc-danweiss" -> "danweiss"
        - "danweiss" -> "danweiss"
        - "danweiss-Lab" -> "danweiss"
        - "kramer-lab" -> "kramer"

        Args:
            ou_name: The OU name to analyze

        Returns:
            Extracted uniqname if found, None otherwise
        """
        if not ou_name:
            return None

        ou_name = ou_name.strip()

        # Pattern 1: uniqname-Lab (e.g., "danweiss-Lab", "kramer-lab")
        # CHECK THIS FIRST to avoid matching "kramer-lab" as "dept-uniqname"
        match = re.match(r"^([a-z]{3,8})-lab$", ou_name, re.IGNORECASE)
        if match:
            return match.group(1).lower()

        # Pattern 2: dept-uniqname (e.g., "psyc-danweiss")
        # More restrictive: exclude if second part is "lab"
        match = re.match(r"^[a-z]{2,6}-([a-z]{3,8})$", ou_name, re.IGNORECASE)
        if match and match.group(1).lower() != "lab":
            return match.group(1).lower()

        # Pattern 3: just uniqname (e.g., "danweiss")
        match = re.match(r"^[a-z]{3,8}$", ou_name, re.IGNORECASE)
        if match:
            return ou_name.lower()

        return None

    def _categorize_ou_depth(
        self, depth: int, hierarchy: List[str], search_base_origin: str
    ) -> str:
        """
        Categorize OU based on depth with search-base-specific logic.

        Different AD branches have different organizational structures.
        Categories are normalized where possible:
        - potential_lab: Individual researcher/lab OUs (most specific)
        - lab_grouping: Intermediate lab groupings (Workstations only)
        - department: Academic departments
        - region: Support/service groups (normalized across both branches)
        - high_level: Top-level organizational structure

        Depth Mappings:

        Research & Instrumentation:
          Example: UMICH → Orgs → LSA → R&I → Current → RSN → Chemistry → chem-kramer
          - Depth 8+: potential_lab (e.g., "chem-kramer", "psyc-danweiss")
          - Depth 7:  department (e.g., "Chemistry", "Physics", "Psychology")
          - Depth 6:  region (e.g., "RSN", "EHTS", "Randall" support groups)
          - Depth 5-: high_level (e.g., "Current", "Legacy", "Staging")

        Workstations:
          Example: UMICH → Orgs → LSA → Workstations → EHTS → Psychology → Standard → Psyc-Faculty → psyc-nestorl
          - Depth 8-9: potential_lab (e.g., "psyc-nestorl", "Raithel")
          - Depth 7:   lab_grouping (e.g., "Lab", "Standard", "Psyc-Faculty-and-Researchers")
          - Depth 6:   department (e.g., "Physics", "Psychology", "Chemistry")
          - Depth 5:   region (e.g., "CaTS", "EHTS", "RSN" support groups)
          - Depth 4-:  high_level (e.g., "Workstations" root)

        Args:
            depth: Number of OU levels in hierarchy
            hierarchy: OU path list (e.g., ["LSA", "Chemistry", "jdoe"])
            search_base_origin: Search base DN where this OU was found

        Returns:
            Category string for downstream silver layer processing

        Note:
            Silver layer will use this as ONE of many heuristics for lab identification.
            Other factors include: name patterns, computer presence, extracted uniqnames.
        """
        # Research & Instrumentation branch
        if "Research and Instrumentation" in search_base_origin:
            if depth >= 8:
                return "potential_lab"
            elif depth == 7:
                return "department"
            elif depth == 6:
                return "region"  # RSN, EHTS, Randall support groups
            else:
                return "high_level"

        # Workstations branch
        elif "Workstations" in search_base_origin:
            if depth >= 8:
                return "potential_lab"
            elif depth == 7:
                return "lab_grouping"  # Lab vs Standard categories
            elif depth == 6:
                return "department"
            elif depth == 5:
                return "region"  # CaTS, EHTS, RSN support groups (normalized)
            else:
                return "high_level"

        # Generic fallback for unknown or future search bases
        else:
            logger.debug(
                f"Unknown search base pattern: {search_base_origin}. "
                f"Using generic categorization for depth {depth}"
            )
            if depth >= 8:
                return "potential_lab"
            elif depth >= 6:
                return "department"
            else:
                return "high_level"

    def _count_computers(self, ou_dn: str) -> int:
        """
        Count computer objects directly in this OU (not recursive).

        This requires an LDAP query and is expensive, which is why we do it
        during ingestion rather than trying to recreate it later.

        Args:
            ou_dn: Distinguished name of the OU

        Returns:
            Number of computer objects in this OU (direct children only)
        """
        try:
            results = self.ldap_adapter.search_as_dicts(
                search_filter="(objectClass=computer)",
                search_base=ou_dn,
                scope="level",  # Only immediate children
                attributes=["cn"],  # Minimal attributes for speed
            )
            count = len(results) if results else 0
            return count
        except Exception as e:
            logger.warning(f"Failed to count computers in {ou_dn}: {e}")
            return 0

    def _count_child_ous(self, ou_dn: str) -> int:
        """
        Count sub-OUs directly under this OU (not recursive).

        Args:
            ou_dn: Distinguished name of the OU

        Returns:
            Number of child OUs (direct children only)
        """
        try:
            results = self.ldap_adapter.search_as_dicts(
                search_filter="(objectClass=organizationalUnit)",
                search_base=ou_dn,
                scope="level",  # Only immediate children
                attributes=["ou"],  # Minimal attributes for speed
            )
            count = len(results) if results else 0
            return count
        except Exception as e:
            logger.warning(f"Failed to count child OUs in {ou_dn}: {e}")
            return 0

    def _enrich_ou_metadata(self, ou_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich OU data with technical metadata during ingestion.

        This captures information that would be expensive or impossible
        to recompute later without re-querying Active Directory.

        Enrichment includes:
        - Hierarchical structure parsing (cheap string operations)
        - Child entity counts (expensive LDAP queries)
        - Name pattern analysis (cheap regex matching)
        - Extracted identifiers (cheap string parsing)

        Args:
            ou_data: Raw OU data from LDAP

        Returns:
            Dictionary with enrichment metadata (keys prefixed with _)
        """
        enrichment = {}

        dn = self._normalize_ldap_attribute(ou_data.get("distinguishedName"))
        ou_name = self._normalize_ldap_attribute(ou_data.get("ou"))

        # Hierarchical structure (cheap string parsing)
        hierarchy = self._parse_ou_hierarchy(dn)
        enrichment["_ou_depth"] = len(hierarchy)
        enrichment["_ou_hierarchy"] = hierarchy
        enrichment["_parent_ou"] = self._extract_parent_ou(dn)

        # Child entity queries (requires LDAP access NOW)
        # These are the expensive operations that justify inline enrichment
        computer_count = self._count_computers(dn)
        child_ou_count = self._count_child_ous(dn)

        enrichment["_direct_computer_count"] = computer_count
        enrichment["_has_computer_children"] = computer_count > 0
        enrichment["_child_ou_count"] = child_ou_count
        enrichment["_has_child_ous"] = child_ou_count > 0

        # Name pattern analysis (cheap regex - provides hints for silver layer)
        enrichment["_name_patterns"] = {
            "dept_uniqname": bool(
                re.match(r"^[a-z]{2,6}-[a-z]{3,8}$", ou_name or "", re.IGNORECASE)
            ),
            "uniqname_only": bool(
                re.match(r"^[a-z]{3,8}$", ou_name or "", re.IGNORECASE)
            ),
            "lab_suffix": bool(re.search(r"lab$", ou_name or "", re.IGNORECASE)),
            "has_hyphen": "-" in (ou_name or ""),
        }

        # Extract potential identifiers (cheap string parsing)
        enrichment["_extracted_uniqname"] = self._extract_uniqname(ou_name)

        # Categorize depth level (search-base-aware for different OU structures)
        search_base_origin = ou_data.get("_search_base_origin", "")
        enrichment["_depth_category"] = self._categorize_ou_depth(
            len(hierarchy), hierarchy, search_base_origin
        )

        return enrichment

    def _calculate_ou_content_hash(
        self, ou_data: Dict[str, Any], enrichment: Dict[str, Any]
    ) -> str:
        """
        Calculate a content hash for Active Directory OU data to detect meaningful changes.

        This hash represents the "content fingerprint" of the OU record, including
        both raw LDAP attributes AND enrichment metadata. This means changes to
        child computers or sub-OUs will trigger a new bronze record.

        Args:
            ou_data: Raw OU data from Active Directory LDAP
            enrichment: Enrichment metadata from _enrich_ou_metadata()

        Returns:
            SHA-256 hash of the normalized OU content
        """
        # Extract significant fields for change detection
        significant_fields = {
            # Core identifiers
            "name": self._normalize_ldap_attribute(ou_data.get("name")),
            "ou": self._normalize_ldap_attribute(ou_data.get("ou")),
            "distinguishedName": self._normalize_ldap_attribute(
                ou_data.get("distinguishedName")
            ),
            "objectGUID": self._normalize_ldap_attribute(ou_data.get("objectGUID")),
            # OU metadata
            "description": self._normalize_ldap_attribute(ou_data.get("description")),
            "managedBy": self._normalize_ldap_attribute(ou_data.get("managedBy")),
            "street": self._normalize_ldap_attribute(ou_data.get("street")),
            "l": self._normalize_ldap_attribute(ou_data.get("l")),  # Locality
            "postalCode": self._normalize_ldap_attribute(ou_data.get("postalCode")),
            # Group policy
            "gPLink": self._normalize_ldap_attribute(ou_data.get("gPLink")),
            "gPOptions": self._normalize_ldap_attribute(ou_data.get("gPOptions")),
            # Object metadata
            "objectCategory": self._normalize_ldap_attribute(
                ou_data.get("objectCategory")
            ),
            "objectClass": self._normalize_ldap_attribute(ou_data.get("objectClass")),
            # Timestamps
            "whenCreated": self._normalize_ldap_attribute(ou_data.get("whenCreated")),
            "whenChanged": self._normalize_ldap_attribute(ou_data.get("whenChanged")),
            # USN (Update Sequence Number) for change tracking
            "uSNCreated": self._normalize_ldap_attribute(ou_data.get("uSNCreated")),
            "uSNChanged": self._normalize_ldap_attribute(ou_data.get("uSNChanged")),
            # Directory replication metadata
            "dSCorePropagationData": self._normalize_ldap_attribute(
                ou_data.get("dSCorePropagationData")
            ),
            # Instance type
            "instanceType": self._normalize_ldap_attribute(ou_data.get("instanceType")),
            # System flags
            "systemFlags": self._normalize_ldap_attribute(ou_data.get("systemFlags")),
            # Enrichment metadata (changes to children trigger new record)
            "_direct_computer_count": enrichment.get("_direct_computer_count"),
            "_child_ou_count": enrichment.get("_child_ou_count"),
            "_ou_hierarchy": enrichment.get("_ou_hierarchy"),
        }

        # Create normalized JSON for consistent hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )

        # Generate SHA-256 hash
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        name = ou_data.get("name", "unknown")
        object_guid = self._normalize_ldap_attribute(ou_data.get("objectGUID"))
        logger.debug(
            f"Content hash for OU {name} (objectGUID: {object_guid}): {content_hash}"
        )

        return content_hash

    def _get_existing_ou_hashes(self) -> Dict[str, str]:
        """
        Retrieve the latest content hash for each Active Directory OU from the bronze layer.

        This uses a window function to get only the most recent record for each
        OU, allowing efficient comparison with new data.

        Returns:
            Dictionary mapping objectGUID -> latest_content_hash
        """
        try:
            # Query to get the most recent record for each OU
            query = """
            WITH latest_ous AS (
                SELECT
                    external_id,
                    raw_data,
                    ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'organizational_unit'
                AND source_system = 'active_directory'
            )
            SELECT
                external_id,
                raw_data
            FROM latest_ous
            WHERE row_num = 1
            """

            results_df = self.db_adapter.query_to_dataframe(query)

            # Calculate content hashes for existing records
            existing_hashes = {}
            for _, row in results_df.iterrows():
                object_guid = row["external_id"]
                raw_data = row["raw_data"]  # JSONB comes back as dict

                # Reconstruct enrichment from stored data for hash calculation
                enrichment = {
                    "_direct_computer_count": raw_data.get("_direct_computer_count", 0),
                    "_child_ou_count": raw_data.get("_child_ou_count", 0),
                    "_ou_hierarchy": raw_data.get("_ou_hierarchy", []),
                }

                content_hash = self._calculate_ou_content_hash(raw_data, enrichment)
                existing_hashes[object_guid] = content_hash

            logger.info(
                f"Retrieved content hashes for {len(existing_hashes)} existing Active Directory OUs"
            )
            return existing_hashes

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve existing OU hashes: {e}")
            raise

    def _get_last_successful_sync_time(self) -> Optional[datetime]:
        """
        Retrieve the completion timestamp of the last successful ingestion run.

        This is used for LDAP timestamp-based pre-filtering to avoid fetching
        unchanged records from Active Directory.

        Returns:
            datetime: Timestamp of last successful sync, or None if this is first run
                     or if force_full_sync is enabled
        """
        # If full sync is forced, return None to bypass timestamp filtering
        if self.force_full_sync:
            logger.info(
                "Full sync forced via --full-sync flag, bypassing timestamp filtering"
            )
            return None

        try:
            query = """
            SELECT completed_at
            FROM meta.ingestion_runs
            WHERE source_system = 'active_directory'
            AND entity_type = 'organizational_unit'
            AND status = 'completed'
            AND completed_at IS NOT NULL
            ORDER BY completed_at DESC
            LIMIT 1
            """

            results_df = self.db_adapter.query_to_dataframe(query)

            if results_df.empty:
                logger.info(
                    "No previous successful sync found - this appears to be first run"
                )
                return None

            last_sync = results_df.iloc[0]["completed_at"]
            logger.info(f"Last successful sync completed at: {last_sync}")
            return last_sync

        except SQLAlchemyError as e:
            logger.warning(
                f"Failed to retrieve last sync time, proceeding with full sync: {e}"
            )
            return None

    def _build_ldap_filter_with_timestamp(
        self, last_sync_time: Optional[datetime]
    ) -> str:
        """
        Build LDAP filter with optional whenChanged timestamp pre-filtering.

        Active Directory's whenChanged attribute uses GeneralizedTime format (YYYYMMDDHHMMSS.0Z).
        We use >= comparison to get all records modified since last sync.

        Args:
            last_sync_time: Timestamp of last successful sync, or None for full sync

        Returns:
            str: LDAP filter string
        """
        base_filter = "(objectClass=organizationalUnit)"

        if last_sync_time is None:
            logger.info("Building filter for FULL sync (no previous sync time)")
            return base_filter

        # Convert datetime to LDAP GeneralizedTime format: YYYYMMDDHHMMSS.0Z
        # Active Directory stores whenChanged in UTC
        if last_sync_time.tzinfo is None:
            # Assume UTC if no timezone
            sync_time_utc = last_sync_time.replace(tzinfo=timezone.utc)
        else:
            sync_time_utc = last_sync_time.astimezone(timezone.utc)

        ldap_timestamp = sync_time_utc.strftime("%Y%m%d%H%M%S.0Z")

        # Build filter: (&(objectClass=organizationalUnit)(whenChanged>=ldap_timestamp))
        timestamp_filter = (
            f"(&(objectClass=organizationalUnit)(whenChanged>={ldap_timestamp}))"
        )

        logger.info(
            f"Building filter for INCREMENTAL sync: whenChanged >= {sync_time_utc.isoformat()} "
            f"(LDAP format: {ldap_timestamp})"
        )

        return timestamp_filter

    def create_ingestion_run(
        self, source_system: str, entity_type: str, metadata: Dict[str, Any] = None
    ) -> str:
        """Create a new ingestion run record for tracking purposes."""
        try:
            run_id = str(uuid.uuid4())

            # Metadata specific to Active Directory LDAP OU ingestion
            base_metadata = {
                "ingestion_type": "content_hash_based",
                "source_api": "active_directory_ldap",
                "ldap_server": "adsroot.itcs.umich.edu",
                "search_bases": self.search_bases,
                "search_filter": "(objectClass=organizationalUnit)",
                "change_detection_method": "sha256_content_hash",
                "includes_child_counts": True,
                "includes_enrichment": True,
                "enrichment_fields": [
                    "_ou_depth",
                    "_ou_hierarchy",
                    "_parent_ou",
                    "_direct_computer_count",
                    "_has_computer_children",
                    "_child_ou_count",
                    "_has_child_ous",
                    "_name_patterns",
                    "_extracted_uniqname",
                    "_depth_category",
                ],
            }

            if metadata:
                base_metadata.update(metadata)

            with self.db_adapter.engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status, metadata
                    ) VALUES (
                        :run_id, :source_system, :entity_type, :started_at, 'running', :metadata
                    )
                """)

                conn.execute(
                    insert_query,
                    {
                        "run_id": run_id,
                        "source_system": source_system,
                        "entity_type": entity_type,
                        "started_at": datetime.now(timezone.utc),
                        "metadata": json.dumps(base_metadata),
                    },
                )

                conn.commit()

            logger.info(
                f"Created Active Directory ingestion run {run_id} for {source_system}/{entity_type}"
            )
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create ingestion run: {e}")
            raise

    def complete_ingestion_run(
        self,
        run_id: str,
        records_processed: int,
        records_created: int,
        records_skipped: int = 0,
        error_message: Optional[str] = None,
    ):
        """Mark an ingestion run as completed with comprehensive statistics."""
        try:
            status = "failed" if error_message else "completed"

            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = :completed_at,
                        status = :status,
                        records_processed = :records_processed,
                        records_created = :records_created,
                        records_updated = :records_skipped,
                        error_message = :error_message
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
                        "records_skipped": records_skipped,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"Completed Active Directory ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete ingestion run: {e}")

    def ingest_ad_ous_with_change_detection(self) -> Dict[str, Any]:
        """
        Ingest University of Michigan Active Directory OUs using intelligent content hashing.

        This method:
        1. Fetches OU data from configured Active Directory search bases
           (using timestamp filtering if available)
        2. Enriches each OU with structural metadata (inline during ingestion)
        3. Calculates content hashes for each OU (including enrichment)
        4. Compares against existing bronze records
        5. Only creates new records when content has actually changed
        6. Provides detailed statistics about OU changes detected

        Returns:
            Dictionary with comprehensive ingestion statistics
        """
        # Create ingestion run for tracking
        run_metadata = {"full_sync": self.force_full_sync, "dry_run": self.dry_run}
        if not self.dry_run:
            run_id = self.create_ingestion_run(
                "active_directory", "organizational_unit", run_metadata
            )
        else:
            run_id = "DRY_RUN_" + str(uuid.uuid4())
            logger.info(f"🧪 Dry run mode enabled. Run ID: {run_id}")

        ingestion_stats = {
            "run_id": run_id,
            "records_processed": 0,
            "records_created": 0,
            "records_skipped_unchanged": 0,
            "new_ous": 0,
            "changed_ous": 0,
            "ous_with_computers": 0,
            "total_computers": 0,
            "ous_with_child_ous": 0,
            "total_child_ous": 0,
            "potential_labs": 0,
            "dept_uniqname_pattern": 0,
            "uniqname_only_pattern": 0,
            "lab_suffix_pattern": 0,
            "ous_with_description": 0,
            "ous_with_managed_by": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info(
                "Starting Active Directory OU ingestion with content hash change detection..."
            )

            # Step 1: Get last successful sync time for timestamp-based pre-filtering
            last_sync_time = self._get_last_successful_sync_time()

            # Step 2: Get existing OU content hashes from bronze layer
            existing_hashes = {}
            if not self.force_full_sync:
                existing_hashes = self._get_existing_ou_hashes()
            else:
                logger.info("🔄 Full sync requested - ignoring existing hashes")

            # Step 3: Fetch current data from Active Directory LDAP (all search bases)
            all_ous = []

            # Build LDAP filter with optional timestamp pre-filtering
            search_filter = self._build_ldap_filter_with_timestamp(last_sync_time)

            for search_base in self.search_bases:
                logger.info(f"Fetching OU data from {search_base}...")

                raw_ous = self.ldap_adapter.search_as_dicts(
                    search_filter=search_filter,
                    search_base=search_base,
                    scope="subtree",
                    attributes=None,  # Return all attributes
                    use_pagination=True,
                )

                if raw_ous:
                    # Tag each OU with its search base origin for traceability
                    for ou in raw_ous:
                        ou["_search_base_origin"] = search_base
                    all_ous.extend(raw_ous)
                    logger.info(f"  Retrieved {len(raw_ous)} OUs from {search_base}")
                else:
                    logger.warning(f"  No OUs found in {search_base}")

            if not all_ous:
                logger.warning(
                    "No OUs found in any configured search bases in Active Directory LDAP"
                )
                return ingestion_stats

            logger.info(
                f"Total OUs retrieved from Active Directory LDAP: {len(all_ous)}"
            )

            # Step 3: Process each OU with enrichment and content hash change detection
            for ou_data in all_ous:
                try:
                    # Extract OU identifiers
                    name = self._normalize_ldap_attribute(ou_data.get("name"))
                    object_guid = self._normalize_ldap_attribute(
                        ou_data.get("objectGUID")
                    )
                    ou_name = self._normalize_ldap_attribute(ou_data.get("ou"))
                    dn = self._normalize_ldap_attribute(
                        ou_data.get("distinguishedName")
                    )

                    # Skip if no objectGUID (required as external_id)
                    if not object_guid:
                        logger.warning(
                            f"Skipping OU {name} - missing objectGUID attribute"
                        )
                        continue

                    # INLINE ENRICHMENT - capture metadata that requires LDAP queries
                    enrichment_metadata = self._enrich_ou_metadata(ou_data)

                    # Track analytics for reporting using enrichment data
                    if enrichment_metadata["_has_computer_children"]:
                        ingestion_stats["ous_with_computers"] += 1
                        ingestion_stats["total_computers"] += enrichment_metadata[
                            "_direct_computer_count"
                        ]

                    if enrichment_metadata["_has_child_ous"]:
                        ingestion_stats["ous_with_child_ous"] += 1
                        ingestion_stats["total_child_ous"] += enrichment_metadata[
                            "_child_ou_count"
                        ]

                    # Track depth categories
                    if enrichment_metadata["_depth_category"] == "potential_lab":
                        ingestion_stats["potential_labs"] += 1

                    # Track name patterns
                    patterns = enrichment_metadata["_name_patterns"]
                    if patterns["dept_uniqname"]:
                        ingestion_stats["dept_uniqname_pattern"] += 1
                    if patterns["uniqname_only"]:
                        ingestion_stats["uniqname_only_pattern"] += 1
                    if patterns["lab_suffix"]:
                        ingestion_stats["lab_suffix_pattern"] += 1

                    # Track descriptive attributes
                    if ou_data.get("description"):
                        ingestion_stats["ous_with_description"] += 1
                    if ou_data.get("managedBy"):
                        ingestion_stats["ous_with_managed_by"] += 1

                    # Calculate content hash for this OU (including enrichment)
                    current_hash = self._calculate_ou_content_hash(
                        ou_data, enrichment_metadata
                    )

                    # Check if this OU is new or has changed
                    existing_hash = existing_hashes.get(object_guid)

                    if existing_hash is None:
                        # This is a completely new OU
                        logger.info(
                            f"New OU detected: {name} ({ou_name}, objectGUID: {object_guid})"
                        )
                        should_insert = True
                        ingestion_stats["new_ous"] += 1

                    elif existing_hash != current_hash:
                        # This OU exists but has changed
                        logger.info(
                            f"OU changed: {name} ({ou_name}, objectGUID: {object_guid})"
                        )
                        logger.debug(f"   Old hash: {existing_hash}")
                        logger.debug(f"   New hash: {current_hash}")
                        should_insert = True
                        ingestion_stats["changed_ous"] += 1

                    else:
                        # This OU exists and hasn't changed - skip it
                        logger.debug(
                            f"OU unchanged, skipping: {name} ({ou_name}, objectGUID: {object_guid})"
                        )
                        should_insert = False
                        ingestion_stats["records_skipped_unchanged"] += 1

                    # Only insert if the OU is new or changed
                    if should_insert:
                        if self.dry_run:
                            logger.info(
                                f"[DRY RUN] Would insert OU: {name} ({object_guid})"
                            )
                        else:
                            # Normalize all raw data for JSON serialization
                            normalized_data = self._normalize_raw_data_for_json(ou_data)

                            # Add enrichment metadata to the bronze record
                            normalized_data.update(enrichment_metadata)

                            # Add standard bronze metadata
                            normalized_data["_content_hash"] = current_hash
                            normalized_data["_change_detection"] = "content_hash_based"
                            normalized_data["_ldap_server"] = "adsroot.itcs.umich.edu"

                            # Insert into bronze layer using objectGUID as external_id
                            entity_id = self.db_adapter.insert_raw_entity(
                                entity_type="organizational_unit",
                                source_system="active_directory",
                                external_id=object_guid,
                                raw_data=normalized_data,
                                ingestion_run_id=run_id,
                            )

                        ingestion_stats["records_created"] += 1

                    # Log progress periodically
                    if (
                        ingestion_stats["records_processed"] % 50 == 0
                        and ingestion_stats["records_processed"] > 0
                    ):
                        logger.info(
                            f"Progress: {ingestion_stats['records_processed']} OUs processed "
                            f"({ingestion_stats['records_created']} new/changed, "
                            f"{ingestion_stats['records_skipped_unchanged']} unchanged)"
                        )

                except Exception as record_error:
                    name_safe = (
                        ou_data.get("name", "unknown")
                        if "name" in ou_data
                        else "unknown"
                    )
                    guid_safe = (
                        ou_data.get("objectGUID", "unknown")
                        if "objectGUID" in ou_data
                        else "unknown"
                    )
                    error_msg = f"Failed to process OU {name_safe} (objectGUID: {guid_safe}): {record_error}"
                    logger.error(error_msg)
                    ingestion_stats["errors"].append(error_msg)

                ingestion_stats["records_processed"] += 1

            # Complete the ingestion run
            error_summary = None
            if ingestion_stats["errors"]:
                error_summary = f"{len(ingestion_stats['errors'])} individual record errors occurred"

            if not self.dry_run:
                self.complete_ingestion_run(
                    run_id=run_id,
                    records_processed=ingestion_stats["records_processed"],
                    records_created=ingestion_stats["records_created"],
                    records_skipped=ingestion_stats["records_skipped_unchanged"],
                    error_message=error_summary,
                )

            ingestion_stats["completed_at"] = datetime.now(timezone.utc)
            duration = (
                ingestion_stats["completed_at"] - ingestion_stats["started_at"]
            ).total_seconds()

            # Log comprehensive results
            logger.info(
                f"Active Directory OU ingestion completed in {duration:.2f} seconds"
            )
            logger.info(f"Results Summary:")
            logger.info(f"   Total Processed: {ingestion_stats['records_processed']}")
            logger.info(f"   New Records Created: {ingestion_stats['records_created']}")
            logger.info(f"   ├─ New OUs: {ingestion_stats['new_ous']}")
            logger.info(f"   └─ Changed OUs: {ingestion_stats['changed_ous']}")
            logger.info(
                f"   Skipped (Unchanged): {ingestion_stats['records_skipped_unchanged']}"
            )
            logger.info(f"   OU Analytics:")
            logger.info(
                f"   ├─ OUs with Computers: {ingestion_stats['ous_with_computers']}"
            )
            logger.info(
                f"   ├─ Total Computers: {ingestion_stats['total_computers']} (across all OUs)"
            )
            logger.info(
                f"   ├─ OUs with Child OUs: {ingestion_stats['ous_with_child_ous']}"
            )
            logger.info(f"   ├─ Total Child OUs: {ingestion_stats['total_child_ous']}")
            logger.info(
                f"   ├─ Potential Labs (by depth): {ingestion_stats['potential_labs']}"
            )
            logger.info(f"   ├─ Name Patterns:")
            logger.info(
                f"   │  ├─ dept-uniqname: {ingestion_stats['dept_uniqname_pattern']}"
            )
            logger.info(
                f"   │  ├─ uniqname only: {ingestion_stats['uniqname_only_pattern']}"
            )
            logger.info(
                f"   │  └─ *-Lab suffix: {ingestion_stats['lab_suffix_pattern']}"
            )
            logger.info(
                f"   ├─ OUs with Description: {ingestion_stats['ous_with_description']}"
            )
            logger.info(
                f"   └─ OUs with ManagedBy: {ingestion_stats['ous_with_managed_by']}"
            )
            logger.info(f"   Errors: {len(ingestion_stats['errors'])}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"Active Directory OU ingestion failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            if not self.dry_run:
                self.complete_ingestion_run(
                    run_id=run_id,
                    records_processed=ingestion_stats["records_processed"],
                    records_created=ingestion_stats["records_created"],
                    records_skipped=ingestion_stats["records_skipped_unchanged"],
                    error_message=error_msg,
                )

            raise

    def get_ou_analytics(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze Active Directory OU data from bronze layer.

        This provides insights into the OU structure and can help
        identify patterns for lab classification in the silver layer.

        Returns:
            Dictionary containing DataFrames for different OU analyses
        """
        try:
            # Query for OU analytics using Active Directory LDAP fields and enrichment
            analytics_query = """
            WITH latest_ous AS (
                SELECT
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'organizational_unit'
                AND source_system = 'active_directory'
            )
            SELECT
                raw_data->>'name' as name,
                raw_data->>'ou' as ou_name,
                raw_data->>'objectGUID' as object_guid,
                raw_data->>'description' as description,
                raw_data->>'distinguishedName' as distinguished_name,
                raw_data->>'managedBy' as managed_by,
                (raw_data->>'_ou_depth')::int as ou_depth,
                raw_data->>'_depth_category' as depth_category,
                (raw_data->>'_direct_computer_count')::int as computer_count,
                (raw_data->>'_child_ou_count')::int as child_ou_count,
                raw_data->>'_extracted_uniqname' as extracted_uniqname,
                (raw_data->'_name_patterns'->>'dept_uniqname')::boolean as pattern_dept_uniqname,
                (raw_data->'_name_patterns'->>'uniqname_only')::boolean as pattern_uniqname_only,
                (raw_data->'_name_patterns'->>'lab_suffix')::boolean as pattern_lab_suffix,
                raw_data->>'_search_base_origin' as search_base_origin
            FROM latest_ous
            WHERE row_num = 1
            ORDER BY distinguished_name
            """

            analytics_df = self.db_adapter.query_to_dataframe(analytics_query)

            # Create summary analyses
            analyses = {}

            # Depth distribution
            if not analytics_df.empty and "ou_depth" in analytics_df.columns:
                depth_summary = (
                    analytics_df.groupby("ou_depth").size().reset_index(name="ou_count")
                )
                analyses["depth_summary"] = depth_summary.sort_values(
                    "ou_depth", ascending=False
                )

            # Depth category distribution
            if not analytics_df.empty and "depth_category" in analytics_df.columns:
                category_summary = (
                    analytics_df.groupby("depth_category")
                    .size()
                    .reset_index(name="ou_count")
                )
                analyses["category_summary"] = category_summary

            # Computer count distribution
            if not analytics_df.empty and "computer_count" in analytics_df.columns:
                computer_summary = (
                    analytics_df[analytics_df["computer_count"] > 0]
                    .groupby("computer_count")
                    .size()
                    .reset_index(name="ou_count")
                )
                analyses["computer_summary"] = computer_summary.sort_values(
                    "computer_count", ascending=False
                )

            # Name pattern distribution
            if not analytics_df.empty:
                pattern_summary = {
                    "dept_uniqname_pattern": analytics_df[
                        "pattern_dept_uniqname"
                    ].sum(),
                    "uniqname_only_pattern": analytics_df[
                        "pattern_uniqname_only"
                    ].sum(),
                    "lab_suffix_pattern": analytics_df["pattern_lab_suffix"].sum(),
                }
                analyses["pattern_summary"] = pd.DataFrame([pattern_summary])

            # Overall features summary
            if not analytics_df.empty:
                features_summary = {
                    "total_ous": len(analytics_df),
                    "ous_with_computers": (analytics_df["computer_count"] > 0).sum(),
                    "total_computers": analytics_df["computer_count"].sum(),
                    "ous_with_child_ous": (analytics_df["child_ou_count"] > 0).sum(),
                    "ous_with_description": analytics_df["description"].notna().sum(),
                    "ous_with_managed_by": analytics_df["managed_by"].notna().sum(),
                    "ous_with_extracted_uniqname": analytics_df["extracted_uniqname"]
                    .notna()
                    .sum(),
                    "avg_depth": analytics_df["ou_depth"].mean(),
                    "max_depth": analytics_df["ou_depth"].max(),
                    "avg_computers_per_ou": analytics_df["computer_count"].mean(),
                    "max_computers_in_ou": analytics_df["computer_count"].max(),
                }
                analyses["features_summary"] = pd.DataFrame([features_summary])

            # Full OU list
            analyses["full_ou_list"] = analytics_df

            logger.info(
                f"Generated OU analytics with {len(analytics_df)} OUs from Active Directory"
            )
            return analyses

        except SQLAlchemyError as e:
            logger.error(f"Failed to generate OU analytics: {e}")
            raise

    def get_ou_change_history(self, object_guid: str) -> pd.DataFrame:
        """
        Get the complete change history for a specific Active Directory OU.

        Args:
            object_guid: The Active Directory objectGUID

        Returns:
            DataFrame with all historical versions of the OU
        """
        try:
            query = """
            SELECT
                raw_id,
                raw_data->>'name' as name,
                raw_data->>'ou' as ou_name,
                raw_data->>'description' as description,
                raw_data->>'distinguishedName' as distinguished_name,
                (raw_data->>'_ou_depth')::int as ou_depth,
                (raw_data->>'_direct_computer_count')::int as computer_count,
                (raw_data->>'_child_ou_count')::int as child_ou_count,
                raw_data->>'_extracted_uniqname' as extracted_uniqname,
                raw_data->>'_content_hash' as content_hash,
                ingested_at,
                ingestion_run_id
            FROM bronze.raw_entities
            WHERE entity_type = 'organizational_unit'
            AND source_system = 'active_directory'
            AND external_id = :object_guid
            ORDER BY ingested_at DESC
            """

            history_df = self.db_adapter.query_to_dataframe(
                query, {"object_guid": object_guid}
            )

            logger.info(
                f"Retrieved {len(history_df)} historical records for Active Directory OU {object_guid}"
            )
            return history_df

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve OU history: {e}")
            raise

    def close(self):
        """Clean up database and LDAP connections."""
        if self.db_adapter:
            self.db_adapter.close()
        if self.ldap_adapter:
            # LDAPAdapter doesn't have explicit close, connection is managed internally
            pass
        logger.info("Active Directory OU ingestion service closed")


def main():
    """
    Main function to run Active Directory OU ingestion from command line.
    """
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(
            description="Ingest Active Directory Organizational Units into Bronze Layer"
        )
        parser.add_argument(
            "--full-sync",
            action="store_true",
            help="Force full sync (ignore last sync timestamp)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without committing to database",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=500,
            help="Batch size for processing (default: 500)",
        )
        args = parser.parse_args()


        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv("DATABASE_URL")

        # Active Directory LDAP configuration — sourced from environment variables.
        # On the production server, AD_PASSWORD is injected by systemd via
        # LoadCredential= and exported by the orchestrator shell script.
        # In local dev, set AD_PASSWORD (and optionally AD_KEYRING_SERVICE) in .env.
        ad_config = {
            "server": os.getenv("AD_SERVER", "adsroot.itcs.umich.edu"),
            "search_base": os.getenv(
                "AD_SEARCH_BASE",
                "OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
            ),
            "user": os.getenv("AD_USER"),
            "password": os.getenv("AD_PASSWORD"),
            "keyring_service": os.getenv("AD_KEYRING_SERVICE", "ldap_umich"),
            "port": int(os.getenv("AD_PORT", "636")),
            "use_ssl": True,
        }

        # Configure search bases (default to Research & Instrumentation + Workstations)
        search_bases = [
            "OU=Research and Instrumentation,OU=LSA,OU=Organizations,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
            "OU=Workstations,OU=LSA,OU=Organizations,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu",
        ]

        # Validate configuration
        if not database_url:
            raise ValueError("Missing required environment variable: DATABASE_URL")

        if not ad_config["user"]:
            raise ValueError("Missing required environment variable: AD_USER")

        # Create and run Active Directory OU ingestion service
        ingestion_service = ActiveDirectoryOUIngestionService(
            database_url=database_url,
            ldap_config=ad_config,
            search_bases=search_bases,
            force_full_sync=args.full_sync,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
        )

        # Run the content hash-based ingestion process with inline enrichment
        print(
            "🏢 Starting Active Directory OU ingestion with content hashing and enrichment..."
        )
        if args.dry_run:
            print("🧪 DRY RUN MODE: No changes will be committed to database")

        results = ingestion_service.ingest_ad_ous_with_change_detection()

        # Display comprehensive summary
        print(f"\n📊 Active Directory OU Ingestion Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Total OUs Processed: {results['records_processed']}")
        print(f"   New Records Created: {results['records_created']}")
        print(f"     ├─ Brand New OUs: {results['new_ous']}")
        print(f"     └─ OUs with Changes: {results['changed_ous']}")
        print(f"   Skipped (No Changes): {results['records_skipped_unchanged']}")
        print(f"   OU Analytics:")
        print(f"     ├─ OUs with Computers: {results['ous_with_computers']}")
        print(f"     ├─ Total Computers: {results['total_computers']} (across all OUs)")
        print(f"     ├─ OUs with Child OUs: {results['ous_with_child_ous']}")
        print(f"     ├─ Total Child OUs: {results['total_child_ous']}")
        print(
            f"     ├─ Potential Labs (by depth): {results['potential_labs']} OUs at typical lab depth"
        )
        print(f"     ├─ Name Patterns:")
        print(f"     │  ├─ dept-uniqname: {results['dept_uniqname_pattern']} OUs")
        print(f"     │  ├─ uniqname only: {results['uniqname_only_pattern']} OUs")
        print(f"     │  └─ *-Lab suffix: {results['lab_suffix_pattern']} OUs")
        print(f"     ├─ OUs with Description: {results['ous_with_description']}")
        print(f"     └─ OUs with ManagedBy: {results['ous_with_managed_by']}")
        print(f"   Errors: {len(results['errors'])}")

        if results["records_skipped_unchanged"] > 0:
            efficiency_percentage = (
                results["records_skipped_unchanged"] / results["records_processed"]
            ) * 100
            print(
                f"\n⚡ Efficiency: {efficiency_percentage:.1f}% of OUs were unchanged and skipped"
            )

        # Show OU analytics
        print("\n🔍 Analyzing OU data...")
        ou_analyses = ingestion_service.get_ou_analytics()

        # Depth distribution
        if "depth_summary" in ou_analyses:
            print("\n📏 OU Depth Distribution:")
            depth_summary = ou_analyses["depth_summary"]
            for _, row in depth_summary.iterrows():
                print(f"   - Depth {row['ou_depth']}: {row['ou_count']} OUs")

        # Depth category distribution
        if "category_summary" in ou_analyses:
            print("\n🏷️  Depth Category Distribution:")
            category_summary = ou_analyses["category_summary"]
            for _, row in category_summary.iterrows():
                print(f"   - {row['depth_category']}: {row['ou_count']} OUs")

        # Computer distribution (top 20)
        if "computer_summary" in ou_analyses:
            print("\n💻 Top 20 OUs by Computer Count:")
            computer_summary = ou_analyses["computer_summary"]
            for _, row in computer_summary.head(20).iterrows():
                print(f"   - {row['computer_count']} computers: {row['ou_count']} OUs")

            if len(computer_summary) > 20:
                remaining_count = computer_summary.iloc[20:]["ou_count"].sum()
                print(f"   - ... and {remaining_count} more OUs with computers")

        # Name pattern summary
        if "pattern_summary" in ou_analyses:
            print("\n🔤 Name Pattern Analysis:")
            patterns = ou_analyses["pattern_summary"].iloc[0]
            print(
                f"   - dept-uniqname pattern (e.g., psyc-danweiss): {patterns['dept_uniqname_pattern']} OUs"
            )
            print(
                f"   - uniqname only pattern (e.g., danweiss): {patterns['uniqname_only_pattern']} OUs"
            )
            print(
                f"   - Lab suffix pattern (e.g., danweiss-Lab): {patterns['lab_suffix_pattern']} OUs"
            )

        # Features summary
        if "features_summary" in ou_analyses:
            print("\n📈 Overall OU Statistics:")
            features = ou_analyses["features_summary"].iloc[0]
            print(f"   - Total OUs: {features['total_ous']}")
            print(f"   - OUs with Computers: {features['ous_with_computers']}")
            print(
                f"   - Total Computers: {features['total_computers']} (across all OUs)"
            )
            print(f"   - OUs with Child OUs: {features['ous_with_child_ous']}")
            print(f"   - OUs with Description: {features['ous_with_description']}")
            print(f"   - OUs with ManagedBy: {features['ous_with_managed_by']}")
            print(
                f"   - OUs with Extracted Uniqname: {features['ous_with_extracted_uniqname']}"
            )
            print(f"   - Average Depth: {features['avg_depth']:.1f}")
            print(f"   - Max Depth: {features['max_depth']}")
            print(f"   - Avg Computers per OU: {features['avg_computers_per_ou']:.2f}")
            print(f"   - Max Computers in an OU: {features['max_computers_in_ou']}")

        # Clean up
        ingestion_service.close()

        print("\n✅ Active Directory OU ingestion completed successfully!\n")

    except Exception as e:
        logger.error(f"Active Directory OU ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
