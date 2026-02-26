#!/usr/bin/env python3
"""
TDX Department Silver Layer Transformation Service

Transforms bronze TDX department data into source-specific silver.tdx_departments table.
This is a source-specific table that preserves complete TDX fidelity including custom attributes.

Key features:
- Single-source transformation (TDX only, no merging)
- Preserves all TDX fields including custom Attributes
- Handles both basic and enriched bronze records
- JSONB consolidation for location and attributes
- Incremental processing using tdx_modified_date
- Comprehensive data quality scoring
- Handles manager_uid NULL value (00000000-0000-0000-0000-000000000000)
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

import dateutil.parser
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


class TdxDepartmentTransformationService:
    """
    Service for transforming bronze TDX department records into source-specific silver layer.

    This service creates silver.tdx_departments records with complete TDX fidelity,
    separate from the merged silver.departments table.
    """

    # Null UUID that TDX uses to represent NULL manager_uid
    TDX_NULL_UUID = "00000000-0000-0000-0000-000000000000"

    def __init__(self, database_url: str):
        """
        Initialize the transformation service.

        Args:
            database_url: PostgreSQL connection string
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        logger.info("✨ TDX department transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful transformation run.

        Enables incremental processing by tracking when we last transformed.

        Returns:
            datetime of last successful transformation, or None for first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
            AND entity_type = 'tdx_department'
            AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"⏰ Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "🆕 No previous transformation found - processing all TDX departments"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️ Could not determine last transformation timestamp: {e}")
            return None

    def _fetch_bronze_records(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Fetch TDX department bronze records for transformation.

        Args:
            since_timestamp: Only fetch records modified after this time
            full_sync: Ignore timestamp and fetch all records

        Returns:
            List of dictionaries with raw_id, raw_data, ingested_at
        """
        try:
            params = {}
            time_filter = ""

            if since_timestamp and not full_sync:
                # Incremental: only fetch records with tdx_modified_date > last run
                time_filter = """
                AND (raw_data->>'ModifiedDate')::timestamp with time zone > :since_timestamp
                """
                params["since_timestamp"] = since_timestamp
                logger.info(
                    f"📊 Fetching TDX departments modified after {since_timestamp}"
                )
            else:
                logger.info("📊 Fetching all TDX departments (full transformation)")

            query = f"""
            SELECT
                raw_id,
                raw_data,
                ingested_at,
                external_id
            FROM bronze.raw_entities
            WHERE source_system = 'tdx'
            AND entity_type = 'department'
            {time_filter}
            ORDER BY (raw_data->>'ModifiedDate')::timestamp with time zone
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)

            if result_df.empty:
                logger.info("✅ No TDX departments to transform")
                return []

            records = result_df.to_dict("records")
            logger.info(f"📦 Found {len(records)} TDX department records to transform")
            return records

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to fetch bronze records: {e}")
            raise

    def _parse_tdx_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Parse TeamDynamix timestamp strings into Python datetime objects.

        Args:
            timestamp_str: ISO format timestamp (e.g., "2020-02-18T22:10:00Z")

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
            logger.warning(f"⚠️ Failed to parse timestamp '{timestamp_str}': {e}")
            return None

    def _normalize_manager_uid(self, manager_uid: Optional[str]) -> Optional[str]:
        """
        Normalize manager_uid field, treating TDX null UUID as Python None.

        TDX uses "00000000-0000-0000-0000-000000000000" to represent NULL.

        Args:
            manager_uid: Raw ManagerUID from TDX

        Returns:
            UUID string or None
        """
        if not manager_uid or manager_uid == self.TDX_NULL_UUID:
            return None
        return manager_uid

    def _build_location_info(self, tdx_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build structured location information JSONB from TDX fields.

        Consolidates 12 location fields into a single JSONB object.

        Args:
            tdx_data: TDX bronze record

        Returns:
            Dictionary with location fields (empty dict if no data)
        """
        location = {}

        # Extract all location fields
        location_fields = {
            "address1": "Address1",
            "address2": "Address2",
            "address3": "Address3",
            "address4": "Address4",
            "city": "City",
            "state_abbr": "StateAbbr",
            "state_name": "StateName",
            "postal_code": "PostalCode",
            "country": "Country",
            "phone": "Phone",
            "fax": "Fax",
            "url": "Url",
        }

        for json_key, tdx_key in location_fields.items():
            value = tdx_data.get(tdx_key)
            if value:
                location[json_key] = value

        return location

    def _extract_attributes(self, tdx_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract TDX custom attributes array.

        Attributes come from the enrichment process (get_account API).
        Non-enriched records will have missing or empty Attributes.

        Args:
            tdx_data: TDX bronze record

        Returns:
            List of attribute dictionaries with id, name, value, value_text
        """
        attributes = tdx_data.get("Attributes")

        if not attributes:
            return []

        # Attributes should be a list
        if not isinstance(attributes, list):
            logger.warning(
                f"⚠️ Unexpected Attributes format for TDX ID {tdx_data.get('ID')}: {type(attributes)}"
            )
            return []

        return attributes

    def _is_enriched(self, tdx_data: Dict[str, Any]) -> bool:
        """
        Determine if this record has been enriched with Attributes.

        Args:
            tdx_data: TDX bronze record

        Returns:
            True if Attributes field is present and populated
        """
        attributes = tdx_data.get("Attributes")
        return bool(attributes and isinstance(attributes, list) and len(attributes) > 0)

    def _transform_bronze_to_silver(
        self, bronze_record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Transform a single bronze TDX department record to silver format.

        Args:
            bronze_record: Dictionary with raw_id, raw_data, ingested_at

        Returns:
            Dictionary representing silver.tdx_departments record
        """
        raw_data = bronze_record["raw_data"]
        raw_id = bronze_record["raw_id"]

        # Extract core TDX fields
        # Handle empty string dept_code - use tdx_id as fallback to satisfy NOT NULL constraint
        dept_code = raw_data.get("Code", "").strip()
        if not dept_code:
            dept_code = f"TDX_{raw_data.get('ID')}"  # Use TDX ID as fallback

        silver_record = {
            "tdx_id": raw_data.get("ID"),
            "dept_code": dept_code,
            "dept_name": raw_data.get("Name", ""),
            "dept_notes": raw_data.get("Notes"),
            "is_active": raw_data.get("IsActive", True),
            "parent_id": raw_data.get("ParentID"),
            "manager_uid": self._normalize_manager_uid(raw_data.get("ManagerUID")),
            "tdx_created_date": self._parse_tdx_timestamp(raw_data.get("CreatedDate")),
            "tdx_modified_date": self._parse_tdx_timestamp(
                raw_data.get("ModifiedDate")
            ),
            "location_info": self._build_location_info(raw_data),
            "attributes": self._extract_attributes(raw_data),
            "is_enriched": self._is_enriched(raw_data),
            "source_bronze_id": raw_id,
        }

        return silver_record

    def _calculate_content_hash(self, silver_record: Dict[str, Any]) -> str:
        """
        Calculate content hash for the silver record.

        Hash represents the "fingerprint" of the TDX data for change detection.

        Args:
            silver_record: The silver record dictionary

        Returns:
            SHA-256 hash string
        """
        # Include significant fields in hash
        significant_fields = {
            "tdx_id": silver_record.get("tdx_id"),
            "dept_code": silver_record.get("dept_code"),
            "dept_name": silver_record.get("dept_name"),
            "is_active": silver_record.get("is_active"),
            "parent_id": silver_record.get("parent_id"),
            "manager_uid": silver_record.get("manager_uid"),
            "tdx_modified_date": (
                silver_record["tdx_modified_date"].isoformat()
                if silver_record.get("tdx_modified_date")
                else None
            ),
            "location_info": silver_record.get("location_info"),
            "attributes": silver_record.get("attributes"),
        }

        # Create normalized JSON for hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":"), default=str
        )
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        return content_hash

    def _calculate_data_quality(
        self, silver_record: Dict[str, Any]
    ) -> Tuple[Decimal, List[str]]:
        """
        Calculate data quality score and identify quality flags.

        Scoring criteria (start at 1.00):
        - Missing dept_code: -0.30
        - Fallback dept_code (TDX_*): -0.20
        - Missing dept_name: -0.30
        - Not enriched (no Attributes): -0.10
        - Modified before created (timestamp anomaly): -0.05
        - No location data: flag only
        - Missing active status: -0.10

        Args:
            silver_record: The transformed silver record

        Returns:
            Tuple of (quality_score, quality_flags_list)
        """
        score = Decimal("1.00")
        flags = []

        # Critical fields missing
        dept_code = silver_record.get("dept_code", "")

        if not dept_code:
            score -= Decimal("0.30")
            flags.append("missing_dept_code")
        elif dept_code.startswith("TDX_"):
            # Fallback dept_code used (empty Code field in TDX)
            score -= Decimal("0.20")
            flags.append("fallback_dept_code")

        if not silver_record.get("dept_name"):
            score -= Decimal("0.30")
            flags.append("missing_dept_name")

        # Important field missing
        if silver_record.get("is_active") is None:
            score -= Decimal("0.10")
            flags.append("missing_active_status")

        # Not enriched (missing Attributes)
        if not silver_record.get("is_enriched"):
            score -= Decimal("0.10")
            flags.append("not_enriched")

        # Timestamp anomalies
        created = silver_record.get("tdx_created_date")
        modified = silver_record.get("tdx_modified_date")

        if created and modified and modified < created:
            score -= Decimal("0.05")
            flags.append("modified_before_created")

        # Empty location info (flag only, no penalty)
        if not silver_record.get("location_info") or not silver_record["location_info"]:
            flags.append("no_location_data")

        # Ensure score doesn't go below 0
        score = max(Decimal("0.00"), score)

        return score, flags

    def _upsert_silver_record(
        self, silver_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> bool:
        """
        Insert or update a silver.tdx_departments record.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
            dry_run: If True, only log what would be done

        Returns:
            True if record was inserted/updated, False if skipped
        """
        if dry_run:
            logger.info(
                f"🔍 [DRY RUN] Would upsert TDX department: "
                f"tdx_id={silver_record['tdx_id']}, "
                f"code={silver_record['dept_code']}, "
                f"name={silver_record['dept_name']}, "
                f"quality={silver_record['data_quality_score']}"
            )
            return True

        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    upsert_query = text("""
                        INSERT INTO silver.tdx_departments (
                            tdx_id, dept_code, dept_name, dept_notes,
                            is_active, parent_id, manager_uid,
                            tdx_created_date, tdx_modified_date,
                            location_info, attributes,
                            data_quality_score, quality_flags,
                            entity_hash, is_enriched, source_bronze_id,
                            ingestion_run_id, updated_at
                        ) VALUES (
                            :tdx_id, :dept_code, :dept_name, :dept_notes,
                            :is_active, :parent_id, :manager_uid,
                            :tdx_created_date, :tdx_modified_date,
                            :location_info, :attributes,
                            :data_quality_score, :quality_flags,
                            :entity_hash, :is_enriched, :source_bronze_id,
                            :ingestion_run_id, CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (tdx_id) DO UPDATE SET
                            dept_code = EXCLUDED.dept_code,
                            dept_name = EXCLUDED.dept_name,
                            dept_notes = EXCLUDED.dept_notes,
                            is_active = EXCLUDED.is_active,
                            parent_id = EXCLUDED.parent_id,
                            manager_uid = EXCLUDED.manager_uid,
                            tdx_created_date = EXCLUDED.tdx_created_date,
                            tdx_modified_date = EXCLUDED.tdx_modified_date,
                            location_info = EXCLUDED.location_info,
                            attributes = EXCLUDED.attributes,
                            data_quality_score = EXCLUDED.data_quality_score,
                            quality_flags = EXCLUDED.quality_flags,
                            entity_hash = EXCLUDED.entity_hash,
                            is_enriched = EXCLUDED.is_enriched,
                            source_bronze_id = EXCLUDED.source_bronze_id,
                            ingestion_run_id = EXCLUDED.ingestion_run_id,
                            updated_at = CURRENT_TIMESTAMP
                    """)

                    conn.execute(
                        upsert_query,
                        {
                            "tdx_id": silver_record["tdx_id"],
                            "dept_code": silver_record["dept_code"],
                            "dept_name": silver_record["dept_name"],
                            "dept_notes": silver_record["dept_notes"],
                            "is_active": silver_record["is_active"],
                            "parent_id": silver_record["parent_id"],
                            "manager_uid": silver_record["manager_uid"],
                            "tdx_created_date": silver_record["tdx_created_date"],
                            "tdx_modified_date": silver_record["tdx_modified_date"],
                            "location_info": json.dumps(silver_record["location_info"]),
                            "attributes": json.dumps(silver_record["attributes"]),
                            "data_quality_score": silver_record["data_quality_score"],
                            "quality_flags": json.dumps(silver_record["quality_flags"]),
                            "entity_hash": silver_record["entity_hash"],
                            "is_enriched": silver_record["is_enriched"],
                            "source_bronze_id": silver_record["source_bronze_id"],
                            "ingestion_run_id": run_id,
                        },
                    )

            return True

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to upsert silver record for TDX ID {silver_record['tdx_id']}: {e}"
            )
            raise

    def create_transformation_run(self) -> str:
        """
        Create a new ingestion run record in meta.ingestion_runs.

        Returns:
            UUID string of the new run
        """
        run_id = str(uuid.uuid4())

        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
                    insert_query = text("""
                        INSERT INTO meta.ingestion_runs (
                            run_id, source_system, entity_type, started_at, status
                        ) VALUES (
                            :run_id, :source_system, :entity_type, :started_at, :status
                        )
                    """)

                    conn.execute(
                        insert_query,
                        {
                            "run_id": run_id,
                            "source_system": "silver_transformation",
                            "entity_type": "tdx_department",
                            "started_at": datetime.now(timezone.utc),
                            "status": "running",
                        },
                    )

            logger.info(f"🚀 Created transformation run: {run_id}")
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
        error_message: Optional[str] = None,
    ):
        """
        Mark a transformation run as completed or failed.

        Args:
            run_id: The run ID to update
            records_processed: Total records processed
            records_created: Number of new records created
            records_updated: Number of existing records updated
            error_message: Error message if run failed
        """
        status = "failed" if error_message else "completed"

        try:
            with self.db_adapter.engine.connect() as conn:
                with conn.begin():
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

            if error_message:
                logger.error(f"❌ Transformation run {run_id} failed: {error_message}")
            else:
                logger.info(f"✅ Transformation run {run_id} completed successfully")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete transformation run: {e}")

    def transform_incremental(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Perform incremental transformation of TDX departments.

        Args:
            full_sync: Force full transformation instead of incremental
            dry_run: Preview changes without committing

        Returns:
            Dictionary with transformation statistics
        """
        run_id = self.create_transformation_run() if not dry_run else "dry-run-id"

        try:
            # Get last transformation timestamp
            last_timestamp = (
                None if full_sync else self._get_last_transformation_timestamp()
            )

            # Fetch bronze records
            bronze_records = self._fetch_bronze_records(
                since_timestamp=last_timestamp, full_sync=full_sync
            )

            if not bronze_records:
                self.complete_transformation_run(
                    run_id, 0, 0, 0
                ) if not dry_run else None
                return {
                    "records_processed": 0,
                    "records_created": 0,
                    "records_updated": 0,
                    "run_id": run_id,
                }

            # Transform each bronze record
            records_processed = 0
            records_created = 0
            records_updated = 0
            enriched_count = 0
            not_enriched_count = 0

            for bronze_record in bronze_records:
                try:
                    # Transform bronze to silver
                    silver_record = self._transform_bronze_to_silver(bronze_record)

                    # Calculate quality
                    quality_score, quality_flags = self._calculate_data_quality(
                        silver_record
                    )
                    silver_record["data_quality_score"] = quality_score
                    silver_record["quality_flags"] = quality_flags

                    # Calculate hash
                    silver_record["entity_hash"] = self._calculate_content_hash(
                        silver_record
                    )

                    # Track enrichment stats
                    if silver_record["is_enriched"]:
                        enriched_count += 1
                    else:
                        not_enriched_count += 1

                    # Upsert record
                    self._upsert_silver_record(silver_record, run_id, dry_run)

                    records_processed += 1

                    # Log progress every 100 records
                    if records_processed % 100 == 0:
                        logger.info(
                            f"📊 Processed {records_processed}/{len(bronze_records)} TDX departments..."
                        )

                except Exception as e:
                    logger.error(
                        f"❌ Failed to transform bronze record {bronze_record.get('raw_id')}: {e}"
                    )
                    continue

            # Complete run
            if not dry_run:
                self.complete_transformation_run(
                    run_id, records_processed, records_created, records_updated
                )

            # Log summary
            logger.info("=" * 80)
            logger.info("🎉 TDX DEPARTMENT TRANSFORMATION COMPLETE")
            logger.info("=" * 80)
            logger.info(f"📊 Records processed: {records_processed}")
            logger.info(f"✅ Enriched records: {enriched_count}")
            logger.info(f"⚠️  Not enriched: {not_enriched_count}")
            logger.info(f"🔑 Run ID: {run_id}")
            logger.info("=" * 80)

            return {
                "records_processed": records_processed,
                "records_created": records_created,
                "records_updated": records_updated,
                "enriched_count": enriched_count,
                "not_enriched_count": not_enriched_count,
                "run_id": run_id,
            }

        except Exception as e:
            error_msg = f"Transformation failed: {str(e)}"
            if not dry_run:
                self.complete_transformation_run(run_id, 0, 0, 0, error_msg)
            raise

    def close(self):
        """Close database connection."""
        self.db_adapter.close()
        logger.info("🔌 Database connection closed")


def main():
    """Main entry point for TDX department transformation."""
    parser = argparse.ArgumentParser(
        description="Transform bronze TDX departments to silver.tdx_departments table"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Force full transformation instead of incremental",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )
    parser.add_argument(
        "--log",
        type=str,
        default="logs/002_transform_tdx_departments.log",
        help="Log file path",
    )

    args = parser.parse_args()

    # Reconfigure logging with custom log file
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(args.log),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Load environment
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    # Create service and run transformation
    service = TdxDepartmentTransformationService(database_url)

    try:
        if args.dry_run:
            logger.info("🔍 DRY RUN MODE - No changes will be committed")

        result = service.transform_incremental(
            full_sync=args.full_sync, dry_run=args.dry_run
        )

        logger.info("✨ Transformation completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}", exc_info=True)
        sys.exit(1)

    finally:
        service.close()


if __name__ == "__main__":
    main()
