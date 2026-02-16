#!/usr/bin/env python3
"""
Department Silver Layer Transformation Service

Merges bronze department data from two sources (umich_api + tdx) into unified silver records.
Key features:
- Matches departments by DeptID (umich_api) == Code (tdx)
- Prioritizes UMICH_API for org hierarchy, TDX for operational data
- Enforces unique constraint on dept_id in silver layer
- Incremental processing (only transforms departments with new bronze records)
- Comprehensive data quality scoring and validation
"""

import hashlib
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import dateutil.parser

# Core imports
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add LSATS project to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# LSATS imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/silver_department_transformation.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class DepartmentSilverTransformationService:
    """
    Service for transforming bronze department records into silver layer.

    This service merges data from two bronze sources:
    1. umich_api: Provides hierarchical organizational data
    2. tdx: Provides operational data, timestamps, and write-back IDs

    The merge creates a unified silver record with dept_id as the primary key.
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
        logger.info("Department silver transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful silver transformation run.

        This enables incremental processing - we only transform departments
        that have new bronze records since the last transformation.

        Returns:
            datetime of last successful transformation, or None for first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
            AND entity_type = 'department'
            AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"Last successful transformation was at: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "No previous transformation found - processing all departments"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"Could not determine last transformation timestamp: {e}")
            return None

    def _get_departments_needing_transformation(
        self, since_timestamp: Optional[datetime] = None
    ) -> Set[str]:
        """
        Find department IDs that have new/updated bronze records.

        Args:
            since_timestamp: Only include departments with bronze records after this time

        Returns:
            Set of dept_ids that need transformation
        """
        try:
            # Build query to find departments with recent bronze updates
            time_filter = ""
            params = {}

            if since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                COALESCE(
                    raw_data->>'DeptId',  -- From umich_api
                    raw_data->>'Code',     -- From tdx
                    external_id            -- Fallback
                ) as dept_id
            FROM bronze.raw_entities
            WHERE entity_type = 'department'
            AND source_system IN ('umich_api', 'tdx')
            {time_filter}
            AND COALESCE(raw_data->>'DeptId', raw_data->>'Code', external_id) IS NOT NULL
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            dept_ids = set(result_df["dept_id"].tolist())

            logger.info(f"Found {len(dept_ids)} departments needing transformation")
            return dept_ids

        except SQLAlchemyError as e:
            logger.error(f"Failed to get departments needing transformation: {e}")
            raise

    def _fetch_latest_bronze_records(
        self, dept_id: str
    ) -> Tuple[Optional[Dict], Optional[Dict]]:
        """
        Fetch the latest bronze records for a department from both sources.

        Args:
            dept_id: The department identifier to look up

        Returns:
            Tuple of (umich_api_record, tdx_record) - either may be None if not found
        """
        try:
            # Fetch latest umich_api record
            umich_query = """
            SELECT raw_data, ingested_at, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'department'
            AND source_system = 'umich_api'
            AND external_id = :dept_id
            ORDER BY ingested_at DESC
            LIMIT 1
            """

            umich_df = self.db_adapter.query_to_dataframe(
                umich_query, {"dept_id": dept_id}
            )
            umich_record = umich_df.iloc[0]["raw_data"] if not umich_df.empty else None

            # Fetch latest tdx record
            tdx_query = """
            SELECT raw_data, ingested_at, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'department'
            AND source_system = 'tdx'
            AND raw_data->>'Code' = :dept_id
            ORDER BY ingested_at DESC
            LIMIT 1
            """

            tdx_df = self.db_adapter.query_to_dataframe(tdx_query, {"dept_id": dept_id})
            tdx_record = tdx_df.iloc[0]["raw_data"] if not tdx_df.empty else None

            return umich_record, tdx_record

        except SQLAlchemyError as e:
            logger.error(f"Failed to fetch bronze records for dept {dept_id}: {e}")
            raise

    def _clean_department_name(self, raw_name: str) -> str:
        """
        Clean and standardize department names.

        Removes department codes that appear at the end (e.g., "LSA DAAS  190300" -> "LSA DAAS")
        Trims whitespace and normalizes spacing.

        Args:
            raw_name: Raw department name from source

        Returns:
            Cleaned department name
        """
        if not raw_name:
            return ""

        # Remove department codes at the end (pattern: multiple spaces + digits)
        cleaned = re.sub(r"\s{2,}\d+\s*$", "", raw_name)

        # Normalize whitespace
        cleaned = " ".join(cleaned.split())

        # Trim
        cleaned = cleaned.strip()

        return cleaned

    def _parse_tdx_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Parse TeamDynamix timestamp strings into Python datetime objects.

        Args:
            timestamp_str: ISO format timestamp string (e.g., "2020-02-18T22:10:00Z")

        Returns:
            datetime object with timezone, or None if parsing fails
        """
        if not timestamp_str:
            return None

        try:
            parsed_dt = dateutil.parser.isoparse(timestamp_str)

            # Ensure timezone info
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)

            return parsed_dt

        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None

    def _build_location_info(self, tdx_data: Optional[Dict]) -> Dict[str, Any]:
        """
        Build structured location information from TDX data.

        Args:
            tdx_data: TDX bronze record

        Returns:
            Dictionary with location fields (empty dict if no data)
        """
        if not tdx_data:
            return {}

        location = {}

        # Extract location fields from TDX
        if tdx_data.get("City"):
            location["city"] = tdx_data["City"]
        if tdx_data.get("Address1"):
            location["address1"] = tdx_data["Address1"]
        if tdx_data.get("Address2"):
            location["address2"] = tdx_data["Address2"]
        if tdx_data.get("Address3"):
            location["address3"] = tdx_data["Address3"]
        if tdx_data.get("Address4"):
            location["address4"] = tdx_data["Address4"]
        if tdx_data.get("PostalCode"):
            location["postal_code"] = tdx_data["PostalCode"]
        if tdx_data.get("Phone"):
            location["phone"] = tdx_data["Phone"]
        if tdx_data.get("Fax"):
            location["fax"] = tdx_data["Fax"]
        if tdx_data.get("Url"):
            location["url"] = tdx_data["Url"]
        if tdx_data.get("StateAbbr"):
            location["state_abbr"] = tdx_data["StateAbbr"]
        if tdx_data.get("StateName"):
            location["state_name"] = tdx_data["StateName"]
        if tdx_data.get("Country"):
            location["country"] = tdx_data["Country"]

        return location

    def _merge_bronze_to_silver(
        self, dept_id: str, umich_data: Optional[Dict], tdx_data: Optional[Dict]
    ) -> Dict[str, Any]:
        """
        Merge bronze records from both sources into a unified silver record.

        Field Priority Rules:
        - dept_id: Primary key (from dept_id parameter)
        - tdx_id: From TDX 'ID' field
        - department_name: From UMICH 'DeptDescription' (cleaned), fallback to TDX 'Name'
        - Hierarchical fields: From UMICH only (campus, college, vp_area, path)
        - is_active: From TDX 'IsActive'
        - Timestamps: From TDX (created, modified)
        - Location: From TDX

        Args:
            dept_id: The department identifier
            umich_data: Bronze record from umich_api (may be None)
            tdx_data: Bronze record from tdx (may be None)

        Returns:
            Dictionary representing silver record
        """
        silver_record = {
            "dept_id": dept_id,
            "department_code": dept_id,  # Duplicate for compatibility
        }

        # Determine source system
        if umich_data and tdx_data:
            silver_record["source_system"] = "umich_api+tdx"
        elif umich_data:
            silver_record["source_system"] = "umich_api"
        elif tdx_data:
            silver_record["source_system"] = "tdx"
        else:
            raise ValueError(f"No bronze data found for department {dept_id}")

        # Extract TDX operational fields
        if tdx_data:
            silver_record["tdx_id"] = tdx_data.get("ID")
            silver_record["is_active"] = tdx_data.get("IsActive", True)
            silver_record["tdx_created_date"] = self._parse_tdx_timestamp(
                tdx_data.get("CreatedDate")
            )
            silver_record["tdx_modified_date"] = self._parse_tdx_timestamp(
                tdx_data.get("ModifiedDate")
            )
            silver_record["location_info"] = self._build_location_info(tdx_data)
            silver_record["description"] = tdx_data.get("Notes") or None
        else:
            # Defaults when TDX data is missing
            silver_record["tdx_id"] = None
            silver_record["is_active"] = True  # Assume active if unknown
            silver_record["tdx_created_date"] = None
            silver_record["tdx_modified_date"] = None
            silver_record["location_info"] = {}
            silver_record["description"] = None

        # Extract UMICH hierarchical fields
        if umich_data:
            raw_dept_name = umich_data.get("DeptDescription", "")
            silver_record["department_name"] = self._clean_department_name(
                raw_dept_name
            )
            silver_record["campus_name"] = umich_data.get("DeptGroupCampusDescr")
            silver_record["college_group"] = umich_data.get("DeptGroupDescription")
            silver_record["vp_area"] = umich_data.get("DeptGroupVPAreaDescr")
            silver_record["hierarchical_path"] = umich_data.get("_hierarchical_path")
        else:
            # Fallback to TDX name if UMICH data is missing
            if tdx_data:
                raw_tdx_name = tdx_data.get("Name", "")
                silver_record["department_name"] = self._clean_department_name(
                    raw_tdx_name
                )
            else:
                silver_record["department_name"] = f"Department {dept_id}"

            # Hierarchy fields remain None
            silver_record["campus_name"] = None
            silver_record["college_group"] = None
            silver_record["vp_area"] = None
            silver_record["hierarchical_path"] = None

        # Set source entity ID
        silver_record["source_entity_id"] = dept_id

        return silver_record

    def _calculate_data_quality(
        self, silver_record: Dict, umich_data: Optional[Dict], tdx_data: Optional[Dict]
    ) -> Tuple[float, List[str]]:
        """
        Calculate data quality score and identify quality flags.

        Scoring criteria:
        - Start at 1.0 (perfect)
        - Deduct 0.1 if missing umich_api source
        - Deduct 0.1 if missing tdx source
        - Deduct 0.05 per missing hierarchical field (campus, college, vp_area)
        - Deduct 0.05 if department names don't match between sources

        Args:
            silver_record: The merged silver record
            umich_data: UMICH bronze record (may be None)
            tdx_data: TDX bronze record (may be None)

        Returns:
            Tuple of (quality_score, quality_flags_list)
        """
        score = 1.0
        flags = []

        # Check for missing sources
        if not umich_data:
            score -= 0.1
            flags.append("missing_umich_source")

        if not tdx_data:
            score -= 0.1
            flags.append("missing_tdx_source")

        # Check for missing hierarchical fields
        hierarchical_fields = ["campus_name", "college_group", "vp_area"]
        for field in hierarchical_fields:
            if not silver_record.get(field):
                score -= 0.05
                flags.append(f"missing_{field}")

        # Check for name mismatches between sources
        if umich_data and tdx_data:
            umich_name = self._clean_department_name(
                umich_data.get("DeptDescription", "")
            )
            tdx_name = self._clean_department_name(tdx_data.get("Name", ""))

            # Compare cleaned names (case-insensitive)
            if umich_name.lower() != tdx_name.lower():
                score -= 0.05
                flags.append("name_mismatch")
                logger.debug(
                    f"Name mismatch for {silver_record['dept_id']}: "
                    f"UMICH='{umich_name}' vs TDX='{tdx_name}'"
                )

        # Check for missing location data
        if not silver_record.get("location_info") or not silver_record["location_info"]:
            flags.append("no_location_data")

        # Ensure score doesn't go below 0
        score = max(0.0, score)

        return round(score, 2), flags

    def _calculate_entity_hash(self, silver_record: Dict) -> str:
        """
        Calculate content hash for the merged silver record.

        This hash represents the "fingerprint" of the merged data,
        used for change detection in future transformations.

        Args:
            silver_record: The silver record dictionary

        Returns:
            SHA-256 hash string
        """
        # Include significant fields in hash
        significant_fields = {
            "dept_id": silver_record.get("dept_id"),
            "tdx_id": silver_record.get("tdx_id"),
            "department_name": silver_record.get("department_name"),
            "campus_name": silver_record.get("campus_name"),
            "college_group": silver_record.get("college_group"),
            "vp_area": silver_record.get("vp_area"),
            "is_active": silver_record.get("is_active"),
            "location_info": silver_record.get("location_info"),
        }

        # Create normalized JSON for hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        return content_hash

    def _upsert_silver_record(self, silver_record: Dict, run_id: str):
        """
        Insert or update a silver department record.

        Uses PostgreSQL UPSERT (INSERT ... ON CONFLICT) to handle both
        new departments and updates to existing ones.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
        """
        try:
            with self.db_adapter.engine.connect() as conn:
                upsert_query = text("""
                    INSERT INTO silver.departments (
                        dept_id, tdx_id, department_name, department_code, description,
                        campus_name, college_group, vp_area, hierarchical_path,
                        is_active, tdx_created_date, tdx_modified_date, location_info,
                        data_quality_score, quality_flags, source_system, source_entity_id,
                        entity_hash, ingestion_run_id, created_at, updated_at
                    ) VALUES (
                        :dept_id, :tdx_id, :department_name, :department_code, :description,
                        :campus_name, :college_group, :vp_area, :hierarchical_path,
                        :is_active, :tdx_created_date, :tdx_modified_date, CAST(:location_info AS jsonb),
                        :data_quality_score, CAST(:quality_flags AS jsonb), :source_system, :source_entity_id,
                        :entity_hash, :ingestion_run_id, :created_at, :updated_at
                    )
                    ON CONFLICT (dept_id) DO UPDATE SET
                        tdx_id = EXCLUDED.tdx_id,
                        department_name = EXCLUDED.department_name,
                        department_code = EXCLUDED.department_code,
                        description = EXCLUDED.description,
                        campus_name = EXCLUDED.campus_name,
                        college_group = EXCLUDED.college_group,
                        vp_area = EXCLUDED.vp_area,
                        hierarchical_path = EXCLUDED.hierarchical_path,
                        is_active = EXCLUDED.is_active,
                        tdx_created_date = EXCLUDED.tdx_created_date,
                        tdx_modified_date = EXCLUDED.tdx_modified_date,
                        location_info = EXCLUDED.location_info,
                        data_quality_score = EXCLUDED.data_quality_score,
                        quality_flags = EXCLUDED.quality_flags,
                        source_system = EXCLUDED.source_system,
                        source_entity_id = EXCLUDED.source_entity_id,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = EXCLUDED.updated_at
                """)

                conn.execute(
                    upsert_query,
                    {
                        "dept_id": silver_record["dept_id"],
                        "tdx_id": silver_record.get("tdx_id"),
                        "department_name": silver_record["department_name"],
                        "department_code": silver_record.get("department_code"),
                        "description": silver_record.get("description"),
                        "campus_name": silver_record.get("campus_name"),
                        "college_group": silver_record.get("college_group"),
                        "vp_area": silver_record.get("vp_area"),
                        "hierarchical_path": silver_record.get("hierarchical_path"),
                        "is_active": silver_record.get("is_active", True),
                        "tdx_created_date": silver_record.get("tdx_created_date"),
                        "tdx_modified_date": silver_record.get("tdx_modified_date"),
                        "location_info": json.dumps(
                            silver_record.get("location_info", {})
                        ),
                        "data_quality_score": silver_record.get("data_quality_score"),
                        "quality_flags": json.dumps(
                            silver_record.get("quality_flags", [])
                        ),
                        "source_system": silver_record["source_system"],
                        "source_entity_id": silver_record["source_entity_id"],
                        "entity_hash": silver_record["entity_hash"],
                        "ingestion_run_id": run_id,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )

                conn.commit()

        except SQLAlchemyError as e:
            logger.error(
                f"Failed to upsert silver record for dept {silver_record['dept_id']}: {e}"
            )
            raise

    def create_transformation_run(
        self, incremental_since: Optional[datetime] = None
    ) -> str:
        """
        Create a transformation run record for tracking.

        Args:
            incremental_since: Timestamp for incremental processing

        Returns:
            Run ID (UUID string)
        """
        try:
            run_id = str(uuid.uuid4())

            metadata = {
                "transformation_type": "bronze_to_silver",
                "entity_type": "department",
                "incremental_since": incremental_since.isoformat()
                if incremental_since
                else None,
                "merge_sources": ["umich_api", "tdx"],
            }

            with self.db_adapter.engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status, metadata
                    ) VALUES (
                        :run_id, 'silver_transformation', 'department', :started_at, 'running', :metadata
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

                conn.commit()

            logger.info(f"Created transformation run {run_id}")
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"Failed to create transformation run: {e}")
            raise

    def complete_transformation_run(
        self,
        run_id: str,
        records_processed: int,
        records_created: int,
        records_updated: int,
        error_message: Optional[str] = None,
    ):
        """
        Mark a transformation run as completed.

        Args:
            run_id: The run ID to complete
            records_processed: Total departments processed
            records_created: New silver records created
            records_updated: Existing silver records updated
            error_message: Error message if run failed
        """
        try:
            status = "failed" if error_message else "completed"

            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = :completed_at,
                        status = :status,
                        records_processed = :records_processed,
                        records_created = :records_created,
                        records_updated = :records_updated,
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
                        "records_updated": records_updated,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"Completed transformation run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete transformation run: {e}")

    def transform_departments_incremental(self) -> Dict[str, Any]:
        """
        Main entry point: Transform bronze departments to silver layer incrementally.

        Process flow:
        1. Determine last successful transformation timestamp
        2. Find departments with bronze records newer than that timestamp
        3. For each department:
           a. Fetch latest bronze records from both sources
           b. Merge into unified silver record
           c. Calculate data quality metrics
           d. Upsert to silver.departments
        4. Track statistics and return results

        Returns:
            Dictionary with transformation statistics
        """
        # Get timestamp of last successful transformation
        last_transformation = self._get_last_transformation_timestamp()

        # Create transformation run
        run_id = self.create_transformation_run(last_transformation)

        stats = {
            "run_id": run_id,
            "incremental_since": last_transformation,
            "departments_processed": 0,
            "records_created": 0,
            "records_updated": 0,
            "source_distribution": {"umich_only": 0, "tdx_only": 0, "merged": 0},
            "quality_issues": [],
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info("Starting incremental silver transformation...")

            # Find departments needing transformation
            dept_ids = self._get_departments_needing_transformation(last_transformation)

            if not dept_ids:
                logger.info("No departments need transformation - all up to date")
                self.complete_transformation_run(run_id, 0, 0, 0)
                return stats

            logger.info(f"Processing {len(dept_ids)} departments")

            # Process each department
            for dept_id in dept_ids:
                try:
                    # Fetch latest bronze records
                    umich_data, tdx_data = self._fetch_latest_bronze_records(dept_id)

                    # Skip if no data found at all
                    if not umich_data and not tdx_data:
                        logger.warning(f"No bronze data found for department {dept_id}")
                        stats["errors"].append(f"No bronze data for {dept_id}")
                        continue

                    # Track source distribution
                    if umich_data and tdx_data:
                        stats["source_distribution"]["merged"] += 1
                    elif umich_data:
                        stats["source_distribution"]["umich_only"] += 1
                    elif tdx_data:
                        stats["source_distribution"]["tdx_only"] += 1

                    # Merge bronze records into silver
                    silver_record = self._merge_bronze_to_silver(
                        dept_id, umich_data, tdx_data
                    )

                    # Calculate data quality
                    quality_score, quality_flags = self._calculate_data_quality(
                        silver_record, umich_data, tdx_data
                    )
                    silver_record["data_quality_score"] = quality_score
                    silver_record["quality_flags"] = quality_flags

                    # Track quality issues
                    if quality_flags:
                        stats["quality_issues"].append(
                            {
                                "dept_id": dept_id,
                                "flags": quality_flags,
                                "score": quality_score,
                            }
                        )

                    # Calculate entity hash
                    silver_record["entity_hash"] = self._calculate_entity_hash(
                        silver_record
                    )

                    # Check if this is a new record or update
                    # (We'll count creates vs updates based on conflict resolution)
                    existing_query = """
                    SELECT dept_id FROM silver.departments WHERE dept_id = :dept_id
                    """
                    existing_df = self.db_adapter.query_to_dataframe(
                        existing_query, {"dept_id": dept_id}
                    )
                    is_update = not existing_df.empty

                    # Upsert to silver layer
                    self._upsert_silver_record(silver_record, run_id)

                    if is_update:
                        stats["records_updated"] += 1
                    else:
                        stats["records_created"] += 1

                    stats["departments_processed"] += 1

                    # Log progress periodically
                    if stats["departments_processed"] % 50 == 0:
                        logger.info(
                            f"Progress: {stats['departments_processed']}/{len(dept_ids)} departments processed"
                        )

                except Exception as record_error:
                    error_msg = (
                        f"Failed to transform department {dept_id}: {record_error}"
                    )
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)

            # Complete the transformation run
            error_summary = None
            if stats["errors"]:
                error_summary = (
                    f"{len(stats['errors'])} departments failed to transform"
                )

            self.complete_transformation_run(
                run_id=run_id,
                records_processed=stats["departments_processed"],
                records_created=stats["records_created"],
                records_updated=stats["records_updated"],
                error_message=error_summary,
            )

            stats["completed_at"] = datetime.now(timezone.utc)
            duration = (stats["completed_at"] - stats["started_at"]).total_seconds()

            # Log comprehensive results
            logger.info(f"Silver transformation completed in {duration:.2f} seconds")
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Departments Processed: {stats['departments_processed']}")
            logger.info(f"   ├─ New Records Created: {stats['records_created']}")
            logger.info(f"   └─ Existing Records Updated: {stats['records_updated']}")
            logger.info(f"   Source Distribution:")
            logger.info(
                f"   ├─ Merged (UMICH + TDX): {stats['source_distribution']['merged']}"
            )
            logger.info(
                f"   ├─ UMICH Only: {stats['source_distribution']['umich_only']}"
            )
            logger.info(f"   └─ TDX Only: {stats['source_distribution']['tdx_only']}")
            logger.info(f"   Quality Issues: {len(stats['quality_issues'])}")
            logger.info(f"   Errors: {len(stats['errors'])}")

            return stats

        except Exception as e:
            error_msg = f"Silver transformation failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            self.complete_transformation_run(
                run_id=run_id,
                records_processed=stats["departments_processed"],
                records_created=stats["records_created"],
                records_updated=stats["records_updated"],
                error_message=error_msg,
            )

            raise

    def get_transformation_summary(self) -> pd.DataFrame:
        """
        Get a summary of silver department records with quality metrics.

        Returns:
            DataFrame with department summaries
        """
        try:
            query = """
            SELECT
                dept_id,
                department_name,
                campus_name,
                college_group,
                vp_area,
                is_active,
                source_system,
                data_quality_score,
                quality_flags,
                tdx_modified_date,
                updated_at
            FROM silver.departments
            ORDER BY department_name
            """

            return self.db_adapter.query_to_dataframe(query)

        except SQLAlchemyError as e:
            logger.error(f"Failed to get transformation summary: {e}")
            raise

    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()
        logger.info("Silver transformation service closed")


def main():
    """
    Main function to run silver transformation from command line.
    """
    try:
        # Load environment variables
        load_dotenv()

        database_url = os.getenv("DATABASE_URL")

        if not database_url:
            raise ValueError("DATABASE_URL environment variable is required")

        # Create transformation service
        transformation_service = DepartmentSilverTransformationService(database_url)

        # Run incremental transformation
        print("🔄 Starting department silver transformation...")
        results = transformation_service.transform_departments_incremental()

        # Display results
        print(f"\n📊 Silver Transformation Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(
            f"   Incremental Since: {results['incremental_since'] or 'First Run (Full Sync)'}"
        )
        print(f"   Departments Processed: {results['departments_processed']}")
        print(f"   ├─ New Records Created: {results['records_created']}")
        print(f"   └─ Existing Records Updated: {results['records_updated']}")
        print(f"\n   Source Distribution:")
        print(f"   ├─ Merged (UMICH + TDX): {results['source_distribution']['merged']}")
        print(f"   ├─ UMICH API Only: {results['source_distribution']['umich_only']}")
        print(f"   └─ TeamDynamix Only: {results['source_distribution']['tdx_only']}")
        print(f"\n   Data Quality:")
        print(f"   ├─ Departments with Issues: {len(results['quality_issues'])}")
        print(f"   └─ Errors: {len(results['errors'])}")

        # Show quality issues if any
        if results["quality_issues"]:
            print(f"\n⚠️  Quality Issues Detected (showing first 5):")
            for issue in results["quality_issues"][:5]:
                print(
                    f"   - {issue['dept_id']}: {', '.join(issue['flags'])} (score: {issue['score']})"
                )

        # Show summary of silver records
        print("\n📋 Getting summary of silver records...")
        summary_df = transformation_service.get_transformation_summary()
        print(f"   Total silver department records: {len(summary_df)}")

        # Show quality distribution
        if not summary_df.empty:
            avg_quality = summary_df["data_quality_score"].mean()
            print(f"   Average quality score: {avg_quality:.2f}")

            quality_ranges = [
                (1.0, 1.0, "Perfect"),
                (0.9, 0.99, "Excellent"),
                (0.8, 0.89, "Good"),
                (0.0, 0.79, "Needs Review"),
            ]

            print(f"\n   Quality Distribution:")
            for low, high, label in quality_ranges:
                count = len(
                    summary_df[
                        (summary_df["data_quality_score"] >= low)
                        & (summary_df["data_quality_score"] <= high)
                    ]
                )
                if count > 0:
                    print(f"   ├─ {label} ({low}-{high}): {count} departments")

        # Clean up
        transformation_service.close()

        print("\n✅ Silver transformation completed successfully!")

    except Exception as e:
        logger.error(f"Silver transformation failed: {e}", exc_info=True)
        print(f"❌ Transformation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
