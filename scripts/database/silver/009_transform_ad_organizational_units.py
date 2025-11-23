#!/usr/bin/env python3
"""
Active Directory Organizational Units Source-Specific Silver Layer Transformation Service

This service transforms bronze Active Directory organizational unit records into the 
source-specific silver.ad_organizational_units table. This is TIER 1 of the two-tier 
silver architecture.

Key features:
- Extracts all AD LDAP OU fields from JSONB to typed columns
- Universal OU hierarchy extraction method for cross-entity matching (computers, groups, users, OUs)
- Preserves enrichment metadata computed during bronze ingestion (_extracted_uniqname, computer counts, etc.)
- Content hash-based change detection
- Incremental processing (only transform OUs with new bronze data)
- Comprehensive logging with emoji standards
- Dry-run mode for validation
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add LSATS project to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# LSATS imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter

# Set up logging
script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "logs/silver"
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


class ADOrganizationalUnitTransformationService:
    """
    Service for transforming bronze AD organizational unit records into source-specific silver layer.

    This service creates silver.ad_organizational_units records from bronze.raw_entities where:
    - entity_type = 'organizational_unit'
    - source_system = 'active_directory'

    Transformation Logic:
    - Extract AD LDAP fields from JSONB to typed columns
    - Universal OU parsing from distinguishedName (root→leaf extraction)
    - Preserve enrichment metadata (_extracted_uniqname, _depth_category, computer counts)
    - Calculate entity_hash for change detection
    - Track raw_id for traceability back to bronze
    """

    def __init__(self, database_url: str):
        """
        Initialize the transformation service.

        Args:
            database_url: PostgreSQL connection string
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        logger.info("🔌 AD organizational units silver transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful AD OU transformation.

        Returns:
            Timestamp of last completed run, or None if this is the first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'ad_organizational_unit'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "🆕 No previous transformation found - processing all OUs"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last transformation timestamp: {e}")
            return None

    def _get_ous_needing_transformation(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Set[str]:
        """
        Find AD OU object GUIDs that have new/updated bronze records.

        Args:
            since_timestamp: Only include OUs with bronze records after this time
            full_sync: If True, return ALL AD OUs regardless of timestamp

        Returns:
            Set of object GUIDs (strings) that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                external_id as object_guid
            FROM bronze.raw_entities
            WHERE entity_type = 'organizational_unit'
              AND source_system = 'active_directory'
              {time_filter}
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            object_guids = set(result_df["object_guid"].tolist())

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"🔍 Found {len(object_guids)} AD OUs needing transformation ({sync_mode} mode)"
            )
            return object_guids

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get OUs needing transformation: {e}")
            raise

    def _fetch_latest_bronze_record(
        self, object_guid: str
    ) -> Optional[Tuple[Dict, str]]:
        """
        Fetch the latest bronze record for an AD OU.

        Args:
            object_guid: The objectGUID (e.g., "{a7e39396-706e-4861-9851-18776ed41a9d}")

        Returns:
            Tuple of (raw_data dict, raw_id UUID) or None if not found
        """
        try:
            query = """
            SELECT raw_data, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'organizational_unit'
              AND source_system = 'active_directory'
              AND external_id = :object_guid
            ORDER BY ingested_at DESC
            LIMIT 1
            """

            result_df = self.db_adapter.query_to_dataframe(
                query, {"object_guid": object_guid}
            )

            if result_df.empty:
                return None

            return result_df.iloc[0]["raw_data"], result_df.iloc[0]["raw_id"]

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to fetch bronze record for {object_guid}: {e}"
            )
            raise

    def _calculate_content_hash(self, silver_record: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 content hash for change detection.

        Args:
            silver_record: The prepared silver record

        Returns:
            SHA-256 hash string
        """
        # Exclude metadata fields from hash calculation
        exclude_fields = {
            "raw_id",
            "entity_hash",
            "ingestion_run_id",
            "created_at",
            "updated_at",
            "source_system",
        }

        content_to_hash = {
            k: v for k, v in silver_record.items() if k not in exclude_fields
        }

        normalized_json = json.dumps(
            content_to_hash, sort_keys=True, separators=(",", ":"), default=str
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _parse_ou_hierarchy(self, distinguished_name: str) -> Dict[str, Any]:
        """
        Parse OU hierarchy from distinguishedName using OU-specific extraction method.

        CRITICAL: OU objects have their own name at position [0], unlike CN objects (computers, groups, users).
        Therefore, we use different offsets than ad_computers/ad_groups/ad_users:
        - OU objects: get_ou_from_end(offset) uses array[length-offset] (e.g., array[length-1] for root)
        - CN objects: get_ou_from_end(offset) uses array[length-offset+1] (e.g., array[length] for root)

        Pattern for OU (from root → leaf):
        DC=edu,DC=umich,DC=itcs,DC=adsroot,OU=UMICH,OU=Organizations,OU=LSA,OU=Current,OU=Research and Instrumentation,OU=Psychology,OU=psyc-berridge

        Extraction positions for OU objects (from root):
        - ou_root = UMICH (array[length-1], last element)
        - ou_organization_type = Organizations (array[length-2])
        - ou_organization = LSA (array[length-3])
        - ou_category = Research and Instrumentation (array[length-4])
        - ou_status = Current (array[length-5]) **NEW**
        - ou_division = Psychology (array[length-6])
        - ou_department = (array[length-7])
        - ou_subdepartment = (array[length-8])
        - ou_immediate_parent = (array[1], first parent OU - NOT array[0] which is the OU itself)

        Args:
            distinguished_name: Full AD DN string

        Returns:
            Dictionary with parsed OU fields
        """
        import re

        try:
            # Remove DC parts
            dn_no_dc = re.sub(r",DC=.*$", "", distinguished_name)

            # Split on ",OU=" to get OU components (leaf → root order)
            ou_parts = [part.strip() for part in dn_no_dc.split(",OU=")]

            # Remove leading "OU=" from first element if present
            if ou_parts and ou_parts[0].startswith("OU="):
                ou_parts[0] = ou_parts[0][3:]

            ou_count = len(ou_parts)

            # Helper function to safely get OU at position from end (for OU objects)
            # Note: This is different from CN objects which use array[length-offset+1]
            def get_ou_from_end(offset: int) -> Optional[str]:
                idx = ou_count - offset
                if 0 <= idx < ou_count:
                    return ou_parts[idx] if ou_parts[idx] else None
                return None

            # For OU objects, immediate_parent is array[1] (NOT array[0] which is the OU itself)
            def get_ou_immediate_parent() -> Optional[str]:
                if ou_count > 1:
                    return ou_parts[1] if ou_parts[1] else None
                return None

            return {
                "ou_root": get_ou_from_end(1),  # array[length-1] (last element)
                "ou_organization_type": get_ou_from_end(2),  # array[length-2]
                "ou_organization": get_ou_from_end(3),  # array[length-3]
                "ou_category": get_ou_from_end(4),  # array[length-4]
                "ou_status": get_ou_from_end(5),  # array[length-5] **NEW**
                "ou_division": get_ou_from_end(6),  # array[length-6]
                "ou_department": get_ou_from_end(7),  # array[length-7]
                "ou_subdepartment": get_ou_from_end(8),  # array[length-8]
                "ou_immediate_parent": get_ou_immediate_parent(),  # array[1]
                "ou_full_path": ou_parts,  # Complete array
                "ou_depth": ou_count,  # Depth count
            }

        except Exception as e:
            logger.warning(
                f"⚠️  Failed to parse OU hierarchy for DN: {distinguished_name[:100]}... Error: {e}"
            )
            return {
                "ou_root": None,
                "ou_organization_type": None,
                "ou_organization": None,
                "ou_category": None,
                "ou_status": None,
                "ou_division": None,
                "ou_department": None,
                "ou_subdepartment": None,
                "ou_immediate_parent": None,
                "ou_full_path": [],
                "ou_depth": 0,
            }

    def _parse_parent_ou_dn(self, distinguished_name: str) -> Optional[str]:
        """
        Extract the immediate parent OU distinguished name.

        Args:
            distinguished_name: Full DN of the current OU

        Returns:
            DN of parent OU, or None if no parent
        """
        if not distinguished_name:
            return None

        # Find first comma after "OU="
        # Parent DN is everything after the first OU component
        parts = distinguished_name.split(",", 1)
        if len(parts) == 2:
            return parts[1]
        return None

    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        """
        Parse AD timestamp to datetime with timezone.

        Args:
            value: Timestamp string or None

        Returns:
            Datetime object with UTC timezone or None
        """
        if value is None:
            return None

        try:
            if isinstance(value, datetime):
                return value
            dt = pd.to_datetime(value, errors="coerce")
            if pd.isna(dt):
                return None
            # Ensure timezone-aware
            if dt.tzinfo is None:
                dt = dt.tz_localize("UTC")
            return dt
        except Exception:
            return None

    def _to_int(self, value: Any) -> Optional[int]:
        """Safely convert to integer."""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _to_bigint(self, value: Any) -> Optional[int]:
        """Safely convert to bigint."""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _normalize_to_list(self, value: Any) -> List[str]:
        """
        Normalize a field that might be a string or a list into a list of strings.

        Args:
            value: String, list, or None

        Returns:
            List of strings (empty list if None)
        """
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item) for item in value]
        return [str(value)]

    def _extract_ou_fields(
        self, raw_data: Dict[str, Any], raw_id: str
    ) -> Dict[str, Any]:
        """
        Extract and type-cast AD OU fields from bronze JSONB to silver columns.

        Args:
            raw_data: Raw JSONB data from bronze.raw_entities
            raw_id: UUID of the bronze record

        Returns:
            Dictionary with all silver.ad_organizational_units columns
        """

        # Parse OU hierarchy
        dn = raw_data.get("distinguishedName") or raw_data.get("dn", "")
        ou_fields = self._parse_ou_hierarchy(dn)

        # Parse parent OU DN
        parent_ou_dn = self._parse_parent_ou_dn(dn)

        # Normalize object_class (can be string or array)
        object_class_raw = raw_data.get("objectClass")
        object_class = self._normalize_to_list(object_class_raw)

        # Extract enrichment metadata from bronze (prefixed with _)
        extracted_uniqname = raw_data.get("_extracted_uniqname")
        depth_category = raw_data.get("_depth_category")
        direct_computer_count = self._to_int(raw_data.get("_direct_computer_count", 0))
        has_computer_children = raw_data.get("_has_computer_children", False)
        child_ou_count = self._to_int(raw_data.get("_child_ou_count", 0))
        has_child_ous = raw_data.get("_has_child_ous", False)
        name_patterns = raw_data.get("_name_patterns")

        # Build silver record
        silver_record = {
            # Primary identifier
            "object_guid": raw_data.get("objectGUID"),
            # Core AD fields
            "distinguished_name": dn,
            "ou_name": raw_data.get("ou") or raw_data.get("name"),
            "name": raw_data.get("name"),
            "description": raw_data.get("description"),
            "managed_by": raw_data.get("managedBy"),
            "gp_link": raw_data.get("gPLink"),
            "gp_options": raw_data.get("gPOptions"),
            "object_category": raw_data.get("objectCategory"),
            "object_class": object_class,
            "instance_type": self._to_int(raw_data.get("instanceType")),
            "system_flags": self._to_int(raw_data.get("systemFlags")),
            # OU hierarchy (from parsing function)
            "ou_root": ou_fields["ou_root"],
            "ou_organization_type": ou_fields["ou_organization_type"],
            "ou_organization": ou_fields["ou_organization"],
            "ou_category": ou_fields["ou_category"],
            "ou_status": ou_fields["ou_status"],
            "ou_division": ou_fields["ou_division"],
            "ou_department": ou_fields["ou_department"],
            "ou_subdepartment": ou_fields["ou_subdepartment"],
            "ou_immediate_parent": ou_fields["ou_immediate_parent"],
            "ou_full_path": ou_fields["ou_full_path"],
            "ou_depth": ou_fields["ou_depth"],
            "parent_ou_dn": parent_ou_dn,
            # Enrichment metadata
            "direct_computer_count": direct_computer_count or 0,
            "has_computer_children": has_computer_children,
            "child_ou_count": child_ou_count or 0,
            "has_child_ous": has_child_ous,
            "depth_category": depth_category,
            "extracted_uniqname": extracted_uniqname,
            "name_patterns": name_patterns,
            # AD timestamps
            "when_created": self._parse_timestamp(raw_data.get("whenCreated")),
            "when_changed": self._parse_timestamp(raw_data.get("whenChanged")),
            "usn_created": self._to_bigint(raw_data.get("uSNCreated")),
            "usn_changed": self._to_bigint(raw_data.get("uSNChanged")),
            # Metadata
            "ds_core_propagation_data": self._normalize_to_list(
                raw_data.get("dSCorePropagationData")
            ),
            # Traceability
            "raw_id": raw_id,
            # Standard metadata (will be set later)
            "source_system": "active_directory",
        }

        # Calculate entity hash
        silver_record["entity_hash"] = self._calculate_content_hash(silver_record)

        return silver_record

    def _upsert_silver_record(
        self,
        silver_record: Dict[str, Any],
        ingestion_run_id: uuid.UUID,
        dry_run: bool = False,
    ) -> str:
        """
        Insert or update a silver.ad_organizational_units record.

        Args:
            silver_record: Dictionary with silver table columns
            ingestion_run_id: UUID of the transformation run
            dry_run: If True, log action but don't execute

        Returns:
            "inserted", "updated", or "unchanged"
        """
        object_guid = silver_record["object_guid"]

        try:
            # Check if record exists and compare hash
            check_query = """
            SELECT entity_hash
            FROM silver.ad_organizational_units
            WHERE object_guid = :object_guid
            """

            existing_df = self.db_adapter.query_to_dataframe(
                check_query, {"object_guid": object_guid}
            )

            if existing_df.empty:
                # Insert new record
                if dry_run:
                    logger.info(
                        f"🔵 [DRY RUN] Would insert new OU: {silver_record.get('ou_name')}"
                    )
                    return "inserted"

                insert_query = """
                INSERT INTO silver.ad_organizational_units (
                    object_guid, distinguished_name, ou_name, name,
                    description, managed_by, gp_link, gp_options,
                    object_category, object_class, instance_type, system_flags,
                    ou_root, ou_organization_type, ou_organization, ou_category,
                    ou_status, ou_division, ou_department, ou_subdepartment, ou_immediate_parent,
                    ou_full_path, ou_depth, parent_ou_dn,
                    direct_computer_count, has_computer_children,
                    child_ou_count, has_child_ous,
                    depth_category, extracted_uniqname, name_patterns,
                    when_created, when_changed, usn_created, usn_changed,
                    ds_core_propagation_data,
                    raw_id, source_system, entity_hash, ingestion_run_id,
                    created_at, updated_at
                ) VALUES (
                    :object_guid, :distinguished_name, :ou_name, :name,
                    :description, :managed_by, :gp_link, :gp_options,
                    :object_category, :object_class, :instance_type, :system_flags,
                    :ou_root, :ou_organization_type, :ou_organization, :ou_category,
                    :ou_status, :ou_division, :ou_department, :ou_subdepartment, :ou_immediate_parent,
                    :ou_full_path, :ou_depth, :parent_ou_dn,
                    :direct_computer_count, :has_computer_children,
                    :child_ou_count, :has_child_ous,
                    :depth_category, :extracted_uniqname, :name_patterns,
                    :when_created, :when_changed, :usn_created, :usn_changed,
                    :ds_core_propagation_data,
                    :raw_id, :source_system, :entity_hash, :ingestion_run_id,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                """

                params = {**silver_record, "ingestion_run_id": str(ingestion_run_id)}
                # Convert JSONB fields to JSON strings
                params["object_class"] = json.dumps(params["object_class"])
                params["ou_full_path"] = json.dumps(params["ou_full_path"])
                params["ds_core_propagation_data"] = json.dumps(
                    params["ds_core_propagation_data"]
                )
                params["name_patterns"] = (
                    json.dumps(params["name_patterns"])
                    if params["name_patterns"]
                    else None
                )

                with self.db_adapter.engine.connect() as conn:
                    conn.execute(text(insert_query), params)
                    conn.commit()
                return "inserted"

            else:
                # Check if content has changed
                existing_hash = existing_df.iloc[0]["entity_hash"]
                if existing_hash == silver_record["entity_hash"]:
                    # No changes
                    return "unchanged"

                # Update existing record
                if dry_run:
                    logger.info(
                        f"🟡 [DRY RUN] Would update OU: {silver_record.get('ou_name')}"
                    )
                    return "updated"

                update_query = """
                UPDATE silver.ad_organizational_units SET
                    distinguished_name = :distinguished_name,
                    ou_name = :ou_name,
                    name = :name,
                    description = :description,
                    managed_by = :managed_by,
                    gp_link = :gp_link,
                    gp_options = :gp_options,
                    object_category = :object_category,
                    object_class = :object_class,
                    instance_type = :instance_type,
                    system_flags = :system_flags,
                    ou_root = :ou_root,
                    ou_organization_type = :ou_organization_type,
                    ou_organization = :ou_organization,
                    ou_category = :ou_category,
                    ou_status = :ou_status,
                    ou_division = :ou_division,
                    ou_department = :ou_department,
                    ou_subdepartment = :ou_subdepartment,
                    ou_immediate_parent = :ou_immediate_parent,
                    ou_full_path = :ou_full_path,
                    ou_depth = :ou_depth,
                    parent_ou_dn = :parent_ou_dn,
                    direct_computer_count = :direct_computer_count,
                    has_computer_children = :has_computer_children,
                    child_ou_count = :child_ou_count,
                    has_child_ous = :has_child_ous,
                    depth_category = :depth_category,
                    extracted_uniqname = :extracted_uniqname,
                    name_patterns = :name_patterns,
                    when_created = :when_created,
                    when_changed = :when_changed,
                    usn_created = :usn_created,
                    usn_changed = :usn_changed,
                    ds_core_propagation_data = :ds_core_propagation_data,
                    raw_id = :raw_id,
                    entity_hash = :entity_hash,
                    ingestion_run_id = :ingestion_run_id,
                    updated_at = CURRENT_TIMESTAMP
                WHERE object_guid = :object_guid
                """

                params = {**silver_record, "ingestion_run_id": str(ingestion_run_id)}
                # Convert JSONB fields to JSON strings
                params["object_class"] = json.dumps(params["object_class"])
                params["ou_full_path"] = json.dumps(params["ou_full_path"])
                params["ds_core_propagation_data"] = json.dumps(
                    params["ds_core_propagation_data"]
                )
                params["name_patterns"] = (
                    json.dumps(params["name_patterns"])
                    if params["name_patterns"]
                    else None
                )

                with self.db_adapter.engine.connect() as conn:
                    conn.execute(text(update_query), params)
                    conn.commit()
                return "updated"

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to upsert silver record for {object_guid}: {e}"
            )
            raise

    def transform_ad_ous(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, int]:
        """
        Main transformation function - processes AD OUs from bronze → silver.

        Args:
            full_sync: If True, process all OUs. If False, only process new/updated
            dry_run: If True, log actions but don't modify database

        Returns:
            Dictionary with transformation statistics
        """
        logger.info("🚀 Starting AD organizational units transformation to silver layer")
        logger.info(f"🔧 Mode: {'FULL SYNC' if full_sync else 'INCREMENTAL'}")
        logger.info(f"🔧 Dry run: {'YES' if dry_run else 'NO'}")

        stats = {
            "processed": 0,
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "errors": 0,
        }

        # Create transformation run record
        run_id = uuid.uuid4()
        if not dry_run:
            try:
                with self.db_adapter.engine.connect() as conn:
                    conn.execute(
                        text("""
                        INSERT INTO meta.ingestion_runs (
                            run_id, source_system, entity_type, started_at, status
                        ) VALUES (
                            :run_id, 'silver_transformation', 'ad_organizational_unit', CURRENT_TIMESTAMP, 'running'
                        )
                        """),
                        {"run_id": str(run_id)},
                    )
                    conn.commit()
            except SQLAlchemyError as e:
                logger.warning(f"⚠️  Could not create transformation run record: {e}")

        try:
            # Get OUs needing transformation
            last_timestamp = (
                None if full_sync else self._get_last_transformation_timestamp()
            )
            ous_to_process = self._get_ous_needing_transformation(
                since_timestamp=last_timestamp, full_sync=full_sync
            )

            logger.info(f"📊 Processing {len(ous_to_process)} OUs...")

            # Process each OU
            for idx, object_guid in enumerate(ous_to_process, 1):
                try:
                    # Fetch latest bronze record
                    bronze_result = self._fetch_latest_bronze_record(object_guid)
                    if not bronze_result:
                        logger.warning(
                            f"⚠️  No bronze record found for {object_guid}"
                        )
                        stats["errors"] += 1
                        continue

                    raw_data, raw_id = bronze_result

                    # Extract fields to silver schema
                    silver_record = self._extract_ou_fields(raw_data, raw_id)

                    # Upsert to silver
                    action = self._upsert_silver_record(silver_record, run_id, dry_run)

                    stats[action] += 1
                    stats["processed"] += 1

                    # Progress logging
                    if stats["processed"] % 100 == 0:
                        logger.info(
                            f"📊 Progress: {stats['processed']}/{len(ous_to_process)} OUs processed "
                            f"({stats['inserted']} inserted, {stats['updated']} updated, "
                            f"{stats['unchanged']} unchanged)"
                        )

                except Exception as ou_error:
                    logger.error(
                        f"❌ Failed to process OU {object_guid}: {ou_error}"
                    )
                    stats["errors"] += 1

            # Complete the run
            if not dry_run:
                try:
                    status = "failed" if stats["errors"] > 0 else "completed"
                    with self.db_adapter.engine.connect() as conn:
                        conn.execute(
                            text("""
                            UPDATE meta.ingestion_runs
                            SET completed_at = CURRENT_TIMESTAMP,
                                status = :status,
                                records_processed = :processed,
                                records_created = :inserted,
                                records_updated = :updated
                            WHERE run_id = :run_id
                            """),
                            {
                                "run_id": str(run_id),
                                "status": status,
                                "processed": stats["processed"],
                                "inserted": stats["inserted"],
                                "updated": stats["updated"],
                            },
                        )
                        conn.commit()
                except SQLAlchemyError as e:
                    logger.warning(f"⚠️  Could not update transformation run: {e}")

            # Log results
            logger.info("🎉 AD organizational units transformation completed")
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Total Processed: {stats['processed']}")
            logger.info(f"   Inserted: {stats['inserted']}")
            logger.info(f"   Updated: {stats['updated']}")
            logger.info(f"   Unchanged: {stats['unchanged']}")
            logger.info(f"   Errors: {stats['errors']}")

            return stats

        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}")
            raise


def main():
    """Main entry point for the transformation script."""
    parser = argparse.ArgumentParser(
        description="Transform AD organizational units from bronze to silver layer"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all OUs regardless of timestamps (default: incremental)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Get database URL
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    # Run transformation
    try:
        service = ADOrganizationalUnitTransformationService(database_url)
        stats = service.transform_ad_ous(
            full_sync=args.full_sync, dry_run=args.dry_run
        )

        # Exit with error code if there were errors
        if stats["errors"] > 0:
            logger.warning(f"⚠️  Completed with {stats['errors']} errors")
            sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
