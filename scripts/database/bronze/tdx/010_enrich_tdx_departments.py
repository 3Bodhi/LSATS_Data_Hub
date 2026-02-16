#!/usr/bin/env python3
"""
Progressive Bronze Layer Enrichment Service

This service implements a two-stage bronze ingestion pattern with hash-based change detection:
1. Rapid ingestion using get_accounts() for basic change detection (001_ingest_tdx_departments.py)
2. Detailed enrichment using get_account(ID) for complete data with enriched hash tracking (this script)

Uses separate basic and enriched content hashes to prevent redundant API calls.

Medallion Architecture: Bronze Layer Enrichment
Entity: Departments (from TeamDynamix Accounts API)
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# Core Python imports for PostgreSQL operations
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.pool import QueuePool

# Add your LSATS project to Python path (adjust path as needed)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter, create_postgres_adapter
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

# Determine log directory based on script location
script_name = os.path.basename(__file__).replace(".py", "")
log_dir = "logs/bronze"
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


class ProgressiveBronzeEnrichmentService:
    """
    Service that implements progressive bronze layer enrichment for TeamDynamix departments.

    This service handles both the rapid initial ingestion using get_accounts() and the
    detailed enrichment using individual get_account(ID) calls to complete the data.
    """

    def __init__(
        self,
        database_url: str,
        tdx_base_url: str,
        tdx_api_token: str = None,
        tdx_username: str = None,
        tdx_password: str = None,
        tdx_beid: str = None,
        tdx_web_services_key: str = None,
        tdx_app_id: str = None,
        api_rate_limit_delay: float = 1.0,
    ):
        """
        Initialize the progressive enrichment service.

        Args:
            database_url: PostgreSQL connection string
            tdx_base_url: TeamDynamix API base URL
            tdx_api_token: TeamDynamix API authentication token (optional if using other auth)
            tdx_username: TDX username for JWT auth (optional)
            tdx_password: TDX password for JWT auth (optional)
            tdx_beid: TDX BEID for admin auth (optional)
            tdx_web_services_key: TDX web services key for admin auth (optional)
            tdx_app_id: TeamDynamix application ID
            api_rate_limit_delay: Delay between individual API calls (seconds)
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        self.tdx_facade = TeamDynamixFacade(
            base_url=tdx_base_url,
            app_id=tdx_app_id,
            api_token=tdx_api_token,
            username=tdx_username,
            password=tdx_password,
            beid=tdx_beid,
            web_services_key=tdx_web_services_key,
        )

        # Rate limiting for individual get_account() calls
        self.api_rate_limit_delay = api_rate_limit_delay

        logger.info(
            f"🔌 Progressive bronze enrichment service initialized "
            f"(API delay: {api_rate_limit_delay}s between calls)"
        )

    def _get_last_enrichment_timestamp(self) -> Optional[datetime]:
        """
        Get the timestamp of the most recent successful enrichment run.

        This allows incremental enrichment - only processing records ingested
        since the last enrichment run.

        Returns:
            datetime of last successful enrichment, or None if this is the first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'tdx'
            AND entity_type = 'department'
            AND status = 'completed'
            AND metadata->>'ingestion_type' = 'progressive_enrichment'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"🔍 Last successful enrichment was at: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "🆕 No previous enrichment found - will enrich all timestamp_based records"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last enrichment timestamp: {e}")
            return None

    def _calculate_basic_content_hash(self, dept_data: Dict[str, Any]) -> str:
        """
        Calculate basic content hash (matching ingest script).

        Only includes fields from get_accounts API to match 001_ingest_tdx_departments.py hash.

        Args:
            dept_data: Department data (from any source)

        Returns:
            SHA-256 hash of basic fields only
        """

        # Helper to safely strip strings
        def safe_strip(value):
            return value.strip() if value is not None else ""

        # Match the hash calculation in 001_ingest_tdx_departments.py
        significant_fields = {
            "ID": dept_data.get("ID"),
            "Name": safe_strip(dept_data.get("Name")),
            "Code": safe_strip(dept_data.get("Code")),
            "Notes": safe_strip(dept_data.get("Notes")),
            "IsActive": dept_data.get("IsActive"),
            "ParentID": dept_data.get("ParentID"),
            "ManagerUID": dept_data.get("ManagerUID"),
        }

        # Normalize and hash
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _calculate_enriched_content_hash(self, dept_data: Dict[str, Any]) -> str:
        """
        Calculate content hash for enriched department data.

        Includes all significant fields including Attributes from get_account(ID).

        Args:
            dept_data: Complete department data from get_account(ID)

        Returns:
            SHA-256 hash of all significant fields
        """

        # Helper to safely strip strings
        def safe_strip(value):
            return value.strip() if value is not None else ""

        # Include all significant fields
        significant_fields = {
            "ID": dept_data.get("ID"),
            "Name": safe_strip(dept_data.get("Name")),
            "Code": safe_strip(dept_data.get("Code")),
            "Notes": safe_strip(dept_data.get("Notes")),
            "IsActive": dept_data.get("IsActive"),
            "ParentID": dept_data.get("ParentID"),
            "ManagerUID": dept_data.get("ManagerUID"),
            "Attributes": dept_data.get("Attributes", []),
            "Address1": safe_strip(dept_data.get("Address1")),
            "Address2": safe_strip(dept_data.get("Address2")),
            "City": safe_strip(dept_data.get("City")),
            "StateAbbr": safe_strip(dept_data.get("StateAbbr")),
            "PostalCode": safe_strip(dept_data.get("PostalCode")),
            "Country": safe_strip(dept_data.get("Country")),
        }

        # Normalize and hash
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def get_departments_needing_enrichment(
        self, since_timestamp: Optional[datetime] = None, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Find bronze layer records that need detailed enrichment.

        This queries for records where _ingestion_method is 'hash_based'
        and have not yet been enriched (no _content_hash_enriched field).

        Args:
            since_timestamp: Only return records ingested after this time (for incremental enrichment)
            limit: Maximum number of records to return for processing

        Returns:
            DataFrame with department records needing enrichment
        """
        try:
            # Build query for records needing enrichment
            # Look for records that need enriched hash added:
            # 1. New 'hash_based' records without enriched hash
            # 2. Legacy 'timestamp_based' records without enriched hash
            # 3. Legacy 'timestamp_based_complete' records without enriched hash (need migration)
            query = """
            SELECT
                raw_id,
                external_id,
                raw_data,
                ingested_at,
                ingestion_run_id,
                raw_data->>'Name' as department_name,
                raw_data->>'ID' as tdx_id,
                raw_data->>'_ingestion_method' as current_method,
                raw_data->>'_content_hash_basic' as basic_hash,
                raw_data->>'_content_hash_enriched' as enriched_hash
            FROM bronze.raw_entities
            WHERE entity_type = 'department'
            AND source_system = 'tdx'
            AND raw_data->>'_content_hash_enriched' IS NULL
            """

            # Add incremental timestamp filter if provided
            params = {}
            if since_timestamp:
                query += " AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query += " ORDER BY ingested_at ASC"

            # Add limit if specified
            if limit:
                query += f" LIMIT {limit}"

            results_df = self.db_adapter.query_to_dataframe(query, params)

            logger.info(f"🔍 Found {len(results_df)} departments needing enrichment")

            # Log some sample data for visibility
            if not results_df.empty:
                logger.info("📋 Sample departments to enrich:")
                for _, row in results_df.head(3).iterrows():
                    logger.info(
                        f"   - {row['department_name']} (TDX ID: {row['tdx_id']})"
                    )

            return results_df

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to query departments needing enrichment: {e}")
            raise

    def enrich_department_record(
        self,
        raw_id: str,
        external_id: str,
        original_raw_data: Dict[str, Any],
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Enrich a single department record by calling get_account(ID) for complete data.

        Uses hash-based change detection to skip enrichment if data hasn't changed.

        Args:
            raw_id: The raw_id of the bronze record to enrich
            external_id: The TeamDynamix department ID
            original_raw_data: The current raw_data from the bronze record
            dry_run: If True, preview changes without committing

        Returns:
            Dictionary with enrichment results and statistics
        """
        enrichment_result = {
            "raw_id": raw_id,
            "external_id": external_id,
            "success": False,
            "action": None,  # 'enriched', 'skipped', or 'error'
            "error_message": None,
            "fields_added": [],
            "attributes_count": 0,
        }

        try:
            # Call TeamDynamix get_account(ID) for complete department data
            logger.debug(f"🔬 Calling get_account({external_id}) for complete data...")

            # Add rate limiting to respect TeamDynamix API limits
            time.sleep(self.api_rate_limit_delay)

            complete_data = self.tdx_facade.accounts.get_account(int(external_id))

            if not complete_data:
                raise ValueError(f"get_account({external_id}) returned empty response")

            # Calculate enriched content hash
            enriched_hash = self._calculate_enriched_content_hash(complete_data)

            # Check if enriched data has changed
            existing_enriched_hash = original_raw_data.get("_content_hash_enriched")
            if existing_enriched_hash == enriched_hash:
                enrichment_result["success"] = True
                enrichment_result["action"] = "skipped"
                logger.debug(
                    f"⏭️  Department {external_id} enriched data unchanged, skipping"
                )
                return enrichment_result

            # Analyze what new fields we're getting
            original_fields = set(original_raw_data.keys())
            complete_fields = set(complete_data.keys())
            new_fields = complete_fields - original_fields

            # Special attention to Attributes field since that's what we're primarily after
            attributes = complete_data.get("Attributes", [])
            attributes_count = len(attributes) if attributes else 0

            enrichment_result["fields_added"] = list(new_fields)
            enrichment_result["attributes_count"] = attributes_count

            # Prepare the enriched raw data
            enriched_raw_data = complete_data.copy()
            enriched_raw_data["_ingestion_method"] = "hash_based_enriched"
            enriched_raw_data["_ingestion_source"] = "get_account"
            enriched_raw_data["_enrichment_timestamp"] = datetime.now(
                timezone.utc
            ).isoformat()
            enriched_raw_data["_original_ingestion_method"] = original_raw_data.get(
                "_ingestion_method", "unknown"
            )
            # Store both basic and enriched hashes
            enriched_raw_data["_content_hash_basic"] = (
                self._calculate_basic_content_hash(complete_data)
            )
            enriched_raw_data["_content_hash_enriched"] = enriched_hash

            # Update the bronze record with the enriched data (unless dry run)
            if dry_run:
                logger.debug(
                    f"[DRY RUN] Would update raw_id {raw_id} with {len(new_fields)} new fields, "
                    f"{attributes_count} attributes"
                )
            else:
                with self.db_adapter.engine.connect() as conn:
                    # Update the raw_data field and ingestion_metadata
                    update_query = text("""
                        UPDATE bronze.raw_entities
                        SET raw_data = :enriched_raw_data,
                            ingestion_metadata = jsonb_set(
                                COALESCE(ingestion_metadata, '{}'::jsonb),
                                '{full_data}',
                                'true'::jsonb
                            )
                        WHERE raw_id = :raw_id
                    """)

                    conn.execute(
                        update_query,
                        {
                            "raw_id": raw_id,
                            "enriched_raw_data": json.dumps(enriched_raw_data),
                        },
                    )

                    conn.commit()

            enrichment_result["success"] = True
            enrichment_result["action"] = "enriched"

            logger.debug(
                f"✅ Successfully enriched department {external_id} - added {len(new_fields)} fields, "
                f"{attributes_count} attributes"
            )

            return enrichment_result

        except Exception as e:
            error_msg = f"Failed to enrich department {external_id}: {str(e)}"
            logger.warning(f"⚠️  {error_msg}")

            enrichment_result["error_message"] = error_msg
            enrichment_result["action"] = "error"
            return enrichment_result

    def update_original_records_metadata(
        self, processed_raw_ids: List[str], dry_run: bool = False
    ) -> int:
        """
        Update the ingestion_metadata for original records to mark them as having basic data only.

        This retroactively adds full_data=false to records that were processed with basic ingestion.

        Args:
            processed_raw_ids: List of raw_id values that were processed for enrichment
            dry_run: If True, preview changes without committing

        Returns:
            Number of records updated
        """
        try:
            if not processed_raw_ids:
                return 0

            if dry_run:
                logger.info(
                    f"[DRY RUN] Would update ingestion_metadata for existing records"
                )
                return 0

            # Create a query to update ingestion_metadata for all processed records
            # We'll update any records that don't already have full_data metadata
            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE bronze.raw_entities
                    SET ingestion_metadata = jsonb_set(
                        COALESCE(ingestion_metadata, '{}'::jsonb),
                        '{full_data}',
                        CASE
                            WHEN raw_data->>'_ingestion_method' = 'timestamp_based_complete' THEN 'true'::jsonb
                            ELSE 'false'::jsonb
                        END
                    )
                    WHERE entity_type = 'department'
                    AND source_system = 'tdx'
                    AND (ingestion_metadata->>'full_data' IS NULL)
                """)

                result = conn.execute(update_query)
                updated_count = result.rowcount
                conn.commit()

            logger.info(
                f"📝 Updated ingestion_metadata for {updated_count} existing records"
            )
            return updated_count

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to update original records metadata: {e}")
            raise

    def create_enrichment_run(
        self, departments_to_process: int, incremental_since: Optional[datetime] = None
    ) -> str:
        """Create an ingestion run record specifically for the enrichment process."""
        try:
            run_id = str(uuid.uuid4())

            metadata = {
                "ingestion_type": "progressive_enrichment",
                "stage": "detail_enrichment",
                "departments_to_process": departments_to_process,
                "api_rate_limit_delay": self.api_rate_limit_delay,
                "incremental_since": incremental_since.isoformat()
                if incremental_since
                else None,
            }

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
                        "source_system": "tdx",
                        "entity_type": "department",
                        "started_at": datetime.now(timezone.utc),
                        "metadata": json.dumps(metadata),
                    },
                )

                conn.commit()

            logger.info(
                f"🚀 Created enrichment run {run_id} for {departments_to_process} departments"
            )
            return run_id

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create enrichment run: {e}")
            raise

    def complete_enrichment_run(
        self,
        run_id: str,
        departments_processed: int,
        departments_enriched: int,
        departments_failed: int,
        error_message: Optional[str] = None,
    ):
        """Mark an enrichment run as completed with detailed statistics."""
        try:
            status = "failed" if error_message else "completed"

            with self.db_adapter.engine.connect() as conn:
                update_query = text("""
                    UPDATE meta.ingestion_runs
                    SET completed_at = :completed_at,
                        status = :status,
                        records_processed = :records_processed,
                        records_created = :records_enriched,
                        records_updated = :records_failed,
                        error_message = :error_message
                    WHERE run_id = :run_id
                """)

                conn.execute(
                    update_query,
                    {
                        "run_id": run_id,
                        "completed_at": datetime.now(timezone.utc),
                        "status": status,
                        "records_processed": departments_processed,
                        "records_enriched": departments_enriched,
                        "records_failed": departments_failed,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"✅ Completed enrichment run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete enrichment run: {e}")

    def run_progressive_enrichment(
        self,
        full_sync: bool = False,
        dry_run: bool = False,
        batch_size: Optional[int] = None,
        stop_on_errors: bool = False,
    ) -> Dict[str, Any]:
        """
        Run the progressive enrichment process to complete department data.

        This method:
        1. Finds bronze records with _ingestion_method = 'hash_based' and no enriched hash
        2. Calls get_account(ID) for each to get complete data including Attributes
        3. Calculates enriched content hash and compares to existing hash
        4. Only updates records where enriched data has changed
        5. Updates _ingestion_method to 'hash_based_enriched' and stores enriched hash

        Args:
            full_sync: If True, re-enrich ALL records (ignore last enrichment timestamp)
            dry_run: If True, preview changes without committing to database
            batch_size: Maximum number of departments to process in this run
            stop_on_errors: Whether to stop processing if individual enrichments fail

        Returns:
            Dictionary with detailed enrichment statistics
        """
        # Determine last enrichment timestamp (for incremental processing)
        last_enrichment_timestamp = (
            None if full_sync else self._get_last_enrichment_timestamp()
        )

        if full_sync:
            logger.info("🔄 Full sync mode: Re-enriching ALL timestamp_based records")
        elif last_enrichment_timestamp:
            logger.info(
                f"⚡ Incremental mode: Enriching records ingested since {last_enrichment_timestamp}"
            )
        else:
            logger.info(
                "🆕 First enrichment run: Processing ALL timestamp_based records"
            )

        # Find departments needing enrichment
        departments_to_enrich = self.get_departments_needing_enrichment(
            since_timestamp=last_enrichment_timestamp, limit=batch_size
        )

        if departments_to_enrich.empty:
            logger.info("✨ No departments found needing enrichment")
            return {
                "run_id": None,
                "full_sync": full_sync,
                "dry_run": dry_run,
                "incremental_since": last_enrichment_timestamp,
                "departments_processed": 0,
                "departments_enriched": 0,
                "departments_skipped": 0,
                "departments_failed": 0,
                "total_attributes_added": 0,
                "total_new_fields": 0,
                "errors": [],
                "started_at": datetime.now(timezone.utc),
                "completed_at": datetime.now(timezone.utc),
            }

        # Create enrichment run for tracking
        run_id = self.create_enrichment_run(
            len(departments_to_enrich), incremental_since=last_enrichment_timestamp
        )

        enrichment_stats = {
            "run_id": run_id,
            "full_sync": full_sync,
            "dry_run": dry_run,
            "incremental_since": last_enrichment_timestamp,
            "departments_processed": 0,
            "departments_enriched": 0,
            "departments_skipped": 0,
            "departments_failed": 0,
            "total_attributes_added": 0,
            "total_new_fields": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info(
                f"🚀 Starting progressive enrichment for {len(departments_to_enrich)} departments..."
            )

            # Process each department for enrichment
            for index, row in departments_to_enrich.iterrows():
                try:
                    raw_id = row["raw_id"]
                    external_id = row["external_id"]
                    department_name = row["department_name"]
                    original_raw_data = row["raw_data"]  # This comes as dict from JSONB

                    logger.info(
                        f"📝 Enriching department {enrichment_stats['departments_processed'] + 1}/{len(departments_to_enrich)}: "
                        f"{department_name} (ID: {external_id})"
                    )

                    # Enrich this specific department
                    enrichment_result = self.enrich_department_record(
                        raw_id=raw_id,
                        external_id=external_id,
                        original_raw_data=original_raw_data,
                        dry_run=dry_run,
                    )

                    if enrichment_result["action"] == "enriched":
                        enrichment_stats["departments_enriched"] += 1
                        enrichment_stats["total_attributes_added"] += enrichment_result[
                            "attributes_count"
                        ]
                        enrichment_stats["total_new_fields"] += len(
                            enrichment_result["fields_added"]
                        )

                        logger.info(
                            f"   ✅ Enriched: +{len(enrichment_result['fields_added'])} fields, "
                            f"+{enrichment_result['attributes_count']} attributes"
                        )
                    elif enrichment_result["action"] == "skipped":
                        enrichment_stats["departments_skipped"] += 1
                        logger.debug(f"   ⏭️  Skipped: enriched data unchanged")
                    else:  # error
                        enrichment_stats["departments_failed"] += 1
                        enrichment_stats["errors"].append(
                            enrichment_result["error_message"]
                        )

                        logger.warning(
                            f"   ❌ Failed: {enrichment_result['error_message']}"
                        )

                        if stop_on_errors:
                            logger.error(
                                "🛑 Stopping enrichment due to error and stop_on_errors=True"
                            )
                            break

                    enrichment_stats["departments_processed"] += 1

                    # Progress logging
                    if enrichment_stats["departments_processed"] % 25 == 0:
                        logger.info(
                            f"📈 Progress: {enrichment_stats['departments_processed']}/{len(departments_to_enrich)} departments processed"
                        )

                except Exception as dept_error:
                    error_msg = f"Unexpected error processing department {row.get('external_id', 'unknown')}: {dept_error}"
                    logger.error(f"❌ {error_msg}")
                    enrichment_stats["errors"].append(error_msg)
                    enrichment_stats["departments_failed"] += 1
                    enrichment_stats["departments_processed"] += 1

                    if stop_on_errors:
                        logger.error(
                            "🛑 Stopping enrichment due to unexpected error and stop_on_errors=True"
                        )
                        break

            # Complete the enrichment run (unless dry run)
            if not dry_run:
                error_summary = None
                if enrichment_stats["errors"]:
                    error_summary = (
                        f"{len(enrichment_stats['errors'])} enrichment errors occurred"
                    )

                self.complete_enrichment_run(
                    run_id=run_id,
                    departments_processed=enrichment_stats["departments_processed"],
                    departments_enriched=enrichment_stats["departments_enriched"],
                    departments_failed=enrichment_stats["departments_failed"],
                    error_message=error_summary,
                )

            enrichment_stats["completed_at"] = datetime.now(timezone.utc)
            duration = (
                enrichment_stats["completed_at"] - enrichment_stats["started_at"]
            ).total_seconds()

            # Log comprehensive results
            logger.info("=" * 80)
            logger.info("🎉 PROGRESSIVE ENRICHMENT COMPLETED")
            logger.info("=" * 80)
            logger.info(f"📊 Enrichment Results Summary:")
            logger.info(
                f"   Departments Processed: {enrichment_stats['departments_processed']:>6,}"
            )
            logger.info(
                f"   ├─ Successfully Enriched: {enrichment_stats['departments_enriched']:>6,}"
            )
            logger.info(
                f"   ├─ Skipped (unchanged): {enrichment_stats['departments_skipped']:>6,}"
            )
            logger.info(
                f"   └─ Failed:             {enrichment_stats['departments_failed']:>6,}"
            )
            logger.info(f"")
            logger.info(
                f"   📄 Total New Fields:   {enrichment_stats['total_new_fields']:>6,}"
            )
            logger.info(
                f"   🏷️  Total Attributes:    {enrichment_stats['total_attributes_added']:>6,}"
            )
            logger.info(
                f"   Errors:                {len(enrichment_stats['errors']):>6,}"
            )
            logger.info(f"   Duration:              {duration:.2f}s")

            if dry_run:
                logger.info(f"")
                logger.info(f"   ⚠️  DRY RUN MODE - No changes committed to database")

            logger.info("=" * 80)

            return enrichment_stats

        except Exception as e:
            error_msg = f"Progressive enrichment failed: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)

            if not dry_run:
                self.complete_enrichment_run(
                    run_id=run_id,
                    departments_processed=enrichment_stats["departments_processed"],
                    departments_enriched=enrichment_stats["departments_enriched"],
                    departments_failed=enrichment_stats["departments_failed"],
                    error_message=error_msg,
                )

            raise

    def get_enrichment_status_summary(self) -> pd.DataFrame:
        """
        Get a summary of enrichment status across all department records.

        Returns:
            DataFrame with counts of different enrichment states
        """
        try:
            query = """
            SELECT
                raw_data->>'_ingestion_method' as ingestion_method,
                ingestion_metadata->>'full_data' as full_data_flag,
                CASE
                    WHEN raw_data->>'_content_hash_enriched' IS NOT NULL THEN 'enriched'
                    WHEN raw_data->>'_content_hash_basic' IS NOT NULL THEN 'basic'
                    ELSE 'no_hash'
                END as hash_status,
                COUNT(*) as record_count,
                MIN(ingested_at) as earliest_record,
                MAX(ingested_at) as latest_record
            FROM bronze.raw_entities
            WHERE entity_type = 'department'
            AND source_system = 'tdx'
            GROUP BY raw_data->>'_ingestion_method', ingestion_metadata->>'full_data', hash_status
            ORDER BY ingestion_method, hash_status
            """

            status_df = self.db_adapter.query_to_dataframe(query)

            logger.info("📊 Current enrichment status summary:")
            for _, row in status_df.iterrows():
                logger.info(
                    f"   {row['ingestion_method']} [{row['hash_status']}] (full_data={row['full_data_flag']}): "
                    f"{row['record_count']} records"
                )

            return status_df

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get enrichment status summary: {e}")
            raise

    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()
        logger.info("🔌 Progressive bronze enrichment service closed")


def main():
    """
    Main function to run progressive bronze enrichment from command line.
    """
    try:
        # Load environment variables
        load_dotenv()

        # Parse command-line arguments
        parser = argparse.ArgumentParser(
            description="Enrich TeamDynamix departments in bronze layer (progressive enrichment)"
        )
        parser.add_argument(
            "--full-sync",
            action="store_true",
            help="Re-enrich all records (ignore last enrichment timestamp)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without committing to database",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            metavar="N",
            help="Maximum number of departments to process in this run",
        )
        parser.add_argument(
            "--api-delay",
            type=float,
            default=1.0,
            metavar="SECONDS",
            help="Delay between API calls (default: 1.0 seconds)",
        )
        parser.add_argument(
            "--stop-on-errors",
            action="store_true",
            help="Stop processing if enrichment errors occur",
        )
        parser.add_argument(
            "--show-status",
            action="store_true",
            help="Show enrichment status summary and exit",
        )
        args = parser.parse_args()

        # Get required configuration from environment
        database_url = os.getenv("DATABASE_URL")
        tdx_base_url = os.getenv("TDX_BASE_URL")
        tdx_api_token = os.getenv("TDX_API_TOKEN")
        tdx_username = os.getenv("TDX_USERNAME")
        tdx_password = os.getenv("TDX_PASSWORD")
        tdx_beid = os.getenv("TDX_BEID")
        tdx_web_services_key = os.getenv("TDX_WEB_SERVICES_KEY")
        tdx_app_id = os.getenv("TDX_APP_ID")

        # Validate configuration
        required_vars = {
            "DATABASE_URL": database_url,
            "TDX_BASE_URL": tdx_base_url,
            "TDX_APP_ID": tdx_app_id,
        }

        missing_vars = [name for name, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

        has_credentials = (
            (tdx_beid and tdx_web_services_key)
            or (tdx_username and tdx_password)
            or tdx_api_token
        )
        if not has_credentials:
            raise ValueError("Missing TDX credentials: provide BEID+WebServicesKey, Username+Password, or API_TOKEN")

        # Create enrichment service
        enrichment_service = ProgressiveBronzeEnrichmentService(
            database_url=database_url,
            tdx_base_url=tdx_base_url,
            tdx_api_token=tdx_api_token,
            tdx_username=tdx_username,
            tdx_password=tdx_password,
            tdx_beid=tdx_beid,
            tdx_web_services_key=tdx_web_services_key,
            tdx_app_id=tdx_app_id,
            api_rate_limit_delay=args.api_delay,
        )

        # Handle --show-status
        if args.show_status:
            print("\n📊 Current enrichment status:")
            enrichment_service.get_enrichment_status_summary()
            enrichment_service.close()
            return

        # Run the progressive enrichment process
        print("=" * 80)
        print("🔄 STARTING PROGRESSIVE BRONZE ENRICHMENT")
        print("=" * 80)
        print(
            f"   Mode:                {'FULL SYNC' if args.full_sync else 'INCREMENTAL'}"
        )
        print(f"   Dry Run:             {args.dry_run}")
        print(
            f"   Batch Size:          {args.batch_size if args.batch_size else 'Unlimited'}"
        )
        print(f"   API Delay:           {args.api_delay}s")
        print(f"   Stop on Errors:      {args.stop_on_errors}")
        print("=" * 80)

        results = enrichment_service.run_progressive_enrichment(
            full_sync=args.full_sync,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
            stop_on_errors=args.stop_on_errors,
        )

        # Display comprehensive summary
        print(f"\n{'=' * 80}")
        print(f"📊 ENRICHMENT SUMMARY")
        print(f"{'=' * 80}")
        print(f"   Run ID:               {results['run_id']}")
        print(
            f"   Mode:                 {'Full Sync' if results['full_sync'] else 'Incremental'}"
        )
        print(f"   Incremental Since:    {results['incremental_since'] or 'First Run'}")
        print(f"   Departments Processed: {results['departments_processed']:>6,}")
        print(f"   ├─ Successfully Enriched: {results['departments_enriched']:>6,}")
        print(f"   ├─ Skipped (unchanged): {results['departments_skipped']:>6,}")
        print(f"   └─ Failed:            {results['departments_failed']:>6,}")
        print(f"")
        print(f"   📄 Total New Fields:  {results['total_new_fields']:>6,}")
        print(f"   🏷️  Total Attributes:   {results['total_attributes_added']:>6,}")
        print(f"   Errors:               {len(results['errors']):>6,}")

        duration = (results["completed_at"] - results["started_at"]).total_seconds()
        print(f"   Duration:             {duration:.2f}s")
        print(f"{'=' * 80}")

        if args.dry_run:
            print("\n⚠️  DRY RUN MODE - No changes committed to database")
        else:
            print("\n✅ Progressive bronze enrichment completed successfully!")

        # Show updated status after processing
        if not args.dry_run and results["departments_enriched"] > 0:
            print(f"\n📊 Updated enrichment status:")
            enrichment_service.get_enrichment_status_summary()

        # Clean up
        enrichment_service.close()

    except Exception as e:
        logger.error(f"❌ Progressive enrichment failed: {e}", exc_info=True)
        print(f"\n❌ Enrichment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
