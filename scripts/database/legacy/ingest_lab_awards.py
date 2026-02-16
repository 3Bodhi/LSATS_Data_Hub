#!/usr/bin/env python3
"""
Lab Awards Ingestion Service

This service ingests lab award data from CSV exports into the bronze layer
for cross-referencing and analysis.

Lab awards provide information about:
- Award identifiers (Award Id, Project/Grant)
- Award details (Award Title, Award Class)
- People (Person Role, Person Name, Person Uniqname)
- Departments and organizations (Award Admin Department, Person Appt Department)
- Financial information (Award Direct/Indirect/Total Dollars)
- Dates (Award Project Start/End Date, Award Publish Date)
- Sponsor information (Direct/Prime Sponsor Name, Category, Subcategory)

The script automatically finds the newest lab_awards*.csv file in the
configured data folder. A composite external_id is constructed from Award ID,
Person Uniqname, and Person Appt Department Id in the format
<Award ID>-<Person Uniqname>-<Person Appt Department Id> since Award ID alone
is not unique (multiple people can be associated with the same award, and the
same person can have records across different departments).
"""

import glob
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Core Python imports for PostgreSQL operations
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add your LSATS project to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/lab_awards_ingestion.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Configuration for data folder (easily changeable)
DATA_FOLDER = "data"
FILE_PATTERN = "lab_awards*.csv"


class LabAwardsIngestionService:
    """
    Lab awards ingestion service for CSV exports.

    Uses content hashing for change detection. This approach:

    1. Finds the newest lab_awards*.csv file in the data folder
    2. Reads award data from the CSV file
    3. Calculates content hashes for each award record
    4. Compares against stored hashes from previous ingestions
    5. Only creates new bronze records when award content has actually changed
    6. Preserves complete change history for award analysis

    Key Features:
    - Automatic discovery of latest CSV export file
    - Efficient change detection without requiring timestamps
    - Personnel and financial data tracking
    - Sponsor and department tracking
    - Comprehensive audit trail for award changes
    - Detailed ingestion statistics and monitoring
    """

    def __init__(self, database_url: str, data_folder: str = DATA_FOLDER):
        """
        Initialize the lab awards ingestion service.

        Args:
            database_url: PostgreSQL connection string
            data_folder: Path to folder containing CSV files (default: 'data')
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )
        self.data_folder = data_folder

        logger.info(
            f"Lab awards ingestion service initialized with data folder: {data_folder}"
        )

    def _find_latest_lab_awards_file(self) -> Optional[str]:
        """
        Find the newest lab_awards*.csv file in the data folder.

        Returns:
            Path to the newest file, or None if no files found
        """
        search_pattern = os.path.join(self.data_folder, FILE_PATTERN)
        files = glob.glob(search_pattern)

        # Filter out temporary files
        files = [f for f in files if not os.path.basename(f).startswith("~$")]

        if not files:
            logger.warning(
                f"No files matching pattern '{FILE_PATTERN}' found in {self.data_folder}"
            )
            return None

        # Get the newest file by modification time
        newest_file = max(files, key=os.path.getmtime)
        logger.info(f"Found latest lab awards file: {newest_file}")

        return newest_file

    def _normalize_value(self, value: Any) -> Any:
        """
        Normalize values for consistent hashing and JSON serialization.

        Handles NaN, NaT, timestamps, and other pandas data types.

        Args:
            value: Raw value from CSV/pandas

        Returns:
            Normalized value suitable for JSON serialization
        """
        if pd.isna(value):
            return None
        elif isinstance(value, pd.Timestamp):
            return value.isoformat()
        elif isinstance(value, (int, float)):
            return value
        elif isinstance(value, bool):
            return value
        else:
            return str(value).strip()

    def _normalize_award_data(self, award_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize all values in award data dictionary.

        Args:
            award_data: Raw award data from CSV

        Returns:
            Dictionary with all values normalized
        """
        normalized = {}
        for key, value in award_data.items():
            normalized[key] = self._normalize_value(value)
        return normalized

    def _calculate_award_content_hash(self, award_data: Dict[str, Any]) -> str:
        """
        Calculate a content hash for lab award data to detect meaningful changes.

        This hash represents the "content fingerprint" of the award record.
        We include all fields that would represent meaningful award changes.

        IMPORTANT: Metadata fields (starting with '_') are explicitly EXCLUDED from
        the hash calculation to ensure that changes in source file names or ingestion
        timestamps don't affect change detection.

        Args:
            award_data: Raw award data from CSV or database

        Returns:
            SHA-256 hash of the normalized award content
        """

        def get_value(key: str) -> Any:
            """Get value, excluding metadata fields."""
            return self._normalize_value(award_data.get(key))

        # Extract significant fields for change detection
        significant_fields = {
            "Award Id": get_value("Award Id"),
            "Project/Grant": get_value("Project/Grant"),
            "Award Title": get_value("Award Title"),
            "Person Role": get_value("Person Role"),
            "Person Last Name": get_value("Person Last Name"),
            "Person First Name": get_value("Person First Name"),
            "Person Uniqname": get_value("Person Uniqname"),
            "Person Appt Department Id": get_value("Person Appt Department Id"),
            "Person Appt Department": get_value("Person Appt Department"),
            "Person Appt School/College": get_value("Person Appt School/College"),
            "Award Admin Department": get_value("Award Admin Department"),
            "Award Admin School/College": get_value("Award Admin School/College"),
            "Award Project Start Date": get_value("Award Project Start Date"),
            "Award Project End Date": get_value("Award Project End Date"),
            "Pre NCE Project End Date": get_value("Pre NCE Project End Date"),
            "Award Direct Dollars": get_value("Award Direct Dollars"),
            "Award Indirect Dollars": get_value("Award Indirect Dollars"),
            "Award Total Dollars": get_value("Award Total Dollars"),
            "Facilities & Admin Rate (%)": get_value("Facilities & Admin Rate (%)"),
            "Direct Sponsor Name": get_value("Direct Sponsor Name"),
            "Direct Sponsor Award Reference Number\n(Current Budget Period)": get_value(
                "Direct Sponsor Award Reference Number\n(Current Budget Period)"
            ),
            "Direct Sponsor Category": get_value("Direct Sponsor Category"),
            "Direct Sponsor Subcategory": get_value("Direct Sponsor Subcategory"),
            "Prime Sponsor Name": get_value("Prime Sponsor Name"),
            "Prime Sponsor Award Reference Number": get_value(
                "Prime Sponsor Award Reference Number"
            ),
            "Prime Sponsor Category": get_value("Prime Sponsor Category"),
            "Prime Sponsor Subcategory": get_value("Prime Sponsor Subcategory"),
            "Award Publish Date": get_value("Award Publish Date"),
            "Award Class": get_value("Award Class"),
        }

        # Create normalized JSON for consistent hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )

        # Generate SHA-256 hash
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        award_id = award_data.get("Award Id") or "unknown"
        logger.debug(f"Content hash for award {award_id}: {content_hash}")
        logger.debug(f"  Hash input (first 200 chars): {normalized_json[:200]}")

        return content_hash

    def _get_existing_award_hashes(self) -> Dict[str, str]:
        """
        Retrieve the latest content hash for each lab award from the bronze layer.

        This uses a window function to get only the most recent record for each
        award, allowing efficient comparison with new data.

        Returns:
            Dictionary mapping composite ID (Award ID-Person Uniqname-Person Appt Department Id) -> latest_content_hash
        """
        try:
            # Query to get the most recent record for each award
            query = """
            WITH latest_awards AS (
                SELECT
                    external_id,
                    raw_data,
                    ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'lab_award'
                AND source_system = 'lab_awards'
            )
            SELECT
                external_id,
                raw_data
            FROM latest_awards
            WHERE row_num = 1
            """

            results_df = self.db_adapter.query_to_dataframe(query)

            # Calculate content hashes for existing records
            existing_hashes = {}
            for _, row in results_df.iterrows():
                composite_id = row[
                    "external_id"
                ]  # Already in Award ID-Person Uniqname-Dept ID format
                raw_data = row["raw_data"]  # JSONB comes back as dict
                content_hash = self._calculate_award_content_hash(raw_data)
                existing_hashes[composite_id] = content_hash

            logger.info(
                f"Retrieved content hashes for {len(existing_hashes)} existing lab award records"
            )
            return existing_hashes

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve existing award hashes: {e}")
            raise

    def create_ingestion_run(
        self, source_system: str, entity_type: str, source_file: str
    ) -> str:
        """
        Create a new ingestion run record for tracking purposes.

        Args:
            source_system: Source system identifier
            entity_type: Entity type being ingested
            source_file: Path to the source CSV file

        Returns:
            The ingestion run ID (UUID string)
        """
        try:
            run_id = str(uuid.uuid4())

            # Metadata specific to lab awards CSV ingestion
            metadata = {
                "ingestion_type": "content_hash_based",
                "source_api": "lab_awards_csv",
                "source_file": source_file,
                "file_pattern": FILE_PATTERN,
                "data_folder": self.data_folder,
                "change_detection_method": "sha256_content_hash",
                "includes_personnel_data": True,
                "includes_financial_data": True,
                "includes_sponsor_data": True,
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
                        "source_system": source_system,
                        "entity_type": entity_type,
                        "started_at": datetime.now(timezone.utc),
                        "metadata": json.dumps(metadata),
                    },
                )

                conn.commit()

            logger.info(
                f"Created lab awards ingestion run {run_id} for {source_system}/{entity_type}"
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
        """
        Mark an ingestion run as completed with comprehensive statistics.

        Args:
            run_id: The ingestion run ID
            records_processed: Total number of records processed
            records_created: Number of new records created
            records_skipped: Number of records skipped (unchanged)
            error_message: Error message if the run failed
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

            logger.info(f"Completed lab awards ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete ingestion run: {e}")

    def ingest_lab_awards_with_change_detection(self) -> Dict[str, Any]:
        """
        Ingest lab awards using intelligent content hashing.

        This method:
        1. Finds the latest lab_awards*.csv file
        2. Reads award data from the CSV file
        3. Calculates content hashes for each award
        4. Compares against existing bronze records
        5. Only creates new records when content has actually changed
        6. Provides detailed statistics about award changes detected

        Returns:
            Dictionary with comprehensive ingestion statistics
        """
        ingestion_stats = {
            "run_id": None,
            "records_read_from_file": 0,
            "records_processed": 0,
            "records_created": 0,
            "records_skipped_unchanged": 0,
            "new_awards": 0,
            "changed_awards": 0,
            "award_classes": {},
            "sponsors": {},
            "departments": {},
            "total_award_dollars": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
            "source_file": None,
        }

        try:
            logger.info(
                "Starting lab awards ingestion with content hash change detection..."
            )

            # Step 1: Find the latest CSV file
            source_file = self._find_latest_lab_awards_file()
            if not source_file:
                error_msg = f"No lab awards files found matching pattern '{FILE_PATTERN}' in {self.data_folder}"
                logger.error(error_msg)
                ingestion_stats["errors"].append(error_msg)
                return ingestion_stats

            ingestion_stats["source_file"] = source_file

            # Create ingestion run for tracking
            run_id = self.create_ingestion_run("lab_awards", "lab_award", source_file)
            ingestion_stats["run_id"] = run_id

            # Step 2: Get existing award content hashes from bronze layer
            existing_hashes = self._get_existing_award_hashes()

            # Step 3: Read data from CSV file
            logger.info(f"Reading award data from {source_file}...")
            df = pd.read_csv(source_file)

            if df.empty:
                logger.warning("No awards found in CSV file")
                return ingestion_stats

            logger.info(f"Retrieved {len(df)} award records from CSV file")
            ingestion_stats["records_read_from_file"] = len(df)

            # Step 4: Process each award with content hash change detection
            for idx, row in df.iterrows():
                try:
                    award_data = row.to_dict()

                    # Extract award identifiers
                    award_id = self._normalize_value(award_data.get("Award Id"))
                    person_uniqname = self._normalize_value(
                        award_data.get("Person Uniqname")
                    )
                    person_dept_id = self._normalize_value(
                        award_data.get("Person Appt Department Id")
                    )
                    award_title = self._normalize_value(award_data.get("Award Title"))

                    # Skip if missing required fields for composite external_id
                    if not award_id:
                        logger.warning(
                            f"Skipping row {idx} - missing Award ID for award: {award_title}"
                        )
                        continue

                    if not person_uniqname:
                        logger.warning(
                            f"Skipping row {idx} - missing Person Uniqname for award: {award_id}"
                        )
                        continue

                    if not person_dept_id:
                        logger.warning(
                            f"Skipping row {idx} - missing Person Appt Department Id for award: {award_id}, person: {person_uniqname}"
                        )
                        continue

                    # Construct composite external_id: Award ID-Person Uniqname-Person Appt Department Id
                    composite_id = f"{award_id}-{person_uniqname}-{person_dept_id}"

                    # Track analytics for reporting
                    award_class = self._normalize_value(award_data.get("Award Class"))
                    if award_class:
                        ingestion_stats["award_classes"][award_class] = (
                            ingestion_stats["award_classes"].get(award_class, 0) + 1
                        )

                    sponsor = self._normalize_value(
                        award_data.get("Direct Sponsor Name")
                    )
                    if sponsor:
                        ingestion_stats["sponsors"][sponsor] = (
                            ingestion_stats["sponsors"].get(sponsor, 0) + 1
                        )

                    department = self._normalize_value(
                        award_data.get("Award Admin Department")
                    )
                    if department:
                        ingestion_stats["departments"][department] = (
                            ingestion_stats["departments"].get(department, 0) + 1
                        )

                    # Track financial stats
                    total_dollars_str = award_data.get("Award Total Dollars")
                    if pd.notna(total_dollars_str):
                        # Remove $ and , from dollar amounts
                        total_dollars_clean = (
                            str(total_dollars_str).replace("$", "").replace(",", "")
                        )
                        try:
                            total_dollars = float(total_dollars_clean)
                            ingestion_stats["total_award_dollars"] += total_dollars
                        except ValueError:
                            logger.debug(
                                f"Could not parse Award Total Dollars: {total_dollars_str}"
                            )

                    # Calculate content hash for this award
                    current_hash = self._calculate_award_content_hash(award_data)

                    # Check if this award is new or has changed using composite ID
                    existing_hash = existing_hashes.get(composite_id)

                    logger.debug(f"Checking award: {composite_id}")
                    logger.debug(f"  Current hash:  {current_hash}")
                    logger.debug(f"  Existing hash: {existing_hash}")

                    if existing_hash is None:
                        # This is a completely new award record
                        logger.info(
                            f"New award record detected: {composite_id} - {award_title}"
                        )
                        should_insert = True
                        ingestion_stats["new_awards"] += 1

                    elif existing_hash != current_hash:
                        # This award record exists but has changed
                        logger.info(
                            f"Award record changed: {composite_id} - {award_title}"
                        )
                        logger.info(f"   Old hash: {existing_hash}")
                        logger.info(f"   New hash: {current_hash}")
                        should_insert = True
                        ingestion_stats["changed_awards"] += 1

                    else:
                        # This award record exists and hasn't changed - skip it
                        logger.debug(
                            f"Award record unchanged, skipping: {composite_id}"
                        )
                        should_insert = False
                        ingestion_stats["records_skipped_unchanged"] += 1

                    # Only insert if the award is new or changed
                    if should_insert:
                        # Normalize all raw data for JSON serialization
                        normalized_data = self._normalize_award_data(award_data)

                        # Enhance with metadata for future reference
                        normalized_data["_content_hash"] = current_hash
                        normalized_data["_change_detection"] = "content_hash_based"
                        normalized_data["_composite_id"] = composite_id
                        normalized_data["_source_file"] = source_file
                        normalized_data["_ingestion_timestamp"] = datetime.now(
                            timezone.utc
                        ).isoformat()

                        # Insert into bronze layer using composite ID as external_id
                        entity_id = self.db_adapter.insert_raw_entity(
                            entity_type="lab_award",
                            source_system="lab_awards",
                            external_id=composite_id,
                            raw_data=normalized_data,
                            ingestion_run_id=run_id,
                        )

                        ingestion_stats["records_created"] += 1

                    # Log progress periodically
                    if (
                        ingestion_stats["records_processed"] % 100 == 0
                        and ingestion_stats["records_processed"] > 0
                    ):
                        logger.info(
                            f"Progress: {ingestion_stats['records_processed']} awards processed "
                            f"({ingestion_stats['records_created']} new/changed, "
                            f"{ingestion_stats['records_skipped_unchanged']} unchanged)"
                        )

                except Exception as record_error:
                    award_id_safe = (
                        award_data.get("Award Id", "unknown")
                        if "Award Id" in award_data
                        else "unknown"
                    )
                    error_msg = (
                        f"Failed to process award {award_id_safe}: {record_error}"
                    )
                    logger.error(error_msg)
                    ingestion_stats["errors"].append(error_msg)

                ingestion_stats["records_processed"] += 1

            # Complete the ingestion run
            error_summary = None
            if ingestion_stats["errors"]:
                error_summary = f"{len(ingestion_stats['errors'])} individual record errors occurred"

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
            logger.info(f"Lab awards ingestion completed in {duration:.2f} seconds")
            logger.info(f"Results Summary:")
            logger.info(f"   Source File: {source_file}")
            logger.info(
                f"   Records Read from File: {ingestion_stats['records_read_from_file']}"
            )
            logger.info(f"   Total Processed: {ingestion_stats['records_processed']}")
            logger.info(f"   New Records Created: {ingestion_stats['records_created']}")
            logger.info(f"   ├─ New Awards: {ingestion_stats['new_awards']}")
            logger.info(f"   └─ Changed Awards: {ingestion_stats['changed_awards']}")
            logger.info(
                f"   Skipped (Unchanged): {ingestion_stats['records_skipped_unchanged']}"
            )
            logger.info(f"   Award Analytics:")
            logger.info(
                f"   ├─ Total Award Dollars: ${ingestion_stats['total_award_dollars']:,.2f}"
            )
            logger.info(f"   ├─ Award Classes: {len(ingestion_stats['award_classes'])}")
            logger.info(f"   ├─ Sponsors: {len(ingestion_stats['sponsors'])}")
            logger.info(f"   └─ Departments: {len(ingestion_stats['departments'])}")
            logger.info(f"   Errors: {len(ingestion_stats['errors'])}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"Lab awards ingestion failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            if ingestion_stats["run_id"]:
                self.complete_ingestion_run(
                    run_id=ingestion_stats["run_id"],
                    records_processed=ingestion_stats["records_processed"],
                    records_created=ingestion_stats["records_created"],
                    records_skipped=ingestion_stats["records_skipped_unchanged"],
                    error_message=error_msg,
                )

            raise

    def get_award_analytics(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze lab award data from bronze layer.

        This provides insights into award distribution, financial data,
        and can help identify patterns or anomalies in the award data.

        Returns:
            Dictionary containing DataFrames for different award analyses
        """
        try:
            # Query for award analytics
            analytics_query = """
            WITH latest_awards AS (
                SELECT
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'lab_award'
                AND source_system = 'lab_awards'
            )
            SELECT
                raw_data->>'Award Id' as award_id,
                raw_data->>'Award Title' as award_title,
                raw_data->>'Award Class' as award_class,
                raw_data->>'Person Uniqname' as person_uniqname,
                raw_data->>'Person Role' as person_role,
                raw_data->>'Award Admin Department' as admin_department,
                raw_data->>'Award Admin School/College' as admin_school,
                raw_data->>'Direct Sponsor Name' as direct_sponsor,
                raw_data->>'Award Total Dollars' as total_dollars,
                raw_data->>'Award Project Start Date' as start_date,
                raw_data->>'Award Project End Date' as end_date
            FROM latest_awards
            WHERE row_num = 1
            ORDER BY award_id
            """

            analytics_df = self.db_adapter.query_to_dataframe(analytics_query)

            # Create summary analyses
            analyses = {}

            # Award class distribution
            if not analytics_df.empty and "award_class" in analytics_df.columns:
                class_summary = (
                    analytics_df.groupby("award_class")
                    .size()
                    .reset_index(name="award_count")
                )
                analyses["class_summary"] = class_summary.sort_values(
                    "award_count", ascending=False
                )

            # Sponsor distribution
            if not analytics_df.empty and "direct_sponsor" in analytics_df.columns:
                sponsor_summary = (
                    analytics_df.groupby("direct_sponsor")
                    .size()
                    .reset_index(name="award_count")
                )
                analyses["sponsor_summary"] = sponsor_summary.sort_values(
                    "award_count", ascending=False
                )

            # Department distribution
            if not analytics_df.empty and "admin_department" in analytics_df.columns:
                dept_summary = (
                    analytics_df.groupby("admin_department")
                    .size()
                    .reset_index(name="award_count")
                )
                analyses["department_summary"] = dept_summary.sort_values(
                    "award_count", ascending=False
                )

            # Summary statistics
            if not analytics_df.empty:
                summary = {
                    "total_awards": len(analytics_df),
                    "unique_sponsors": analytics_df["direct_sponsor"].nunique()
                    if "direct_sponsor" in analytics_df.columns
                    else 0,
                    "unique_departments": analytics_df["admin_department"].nunique()
                    if "admin_department" in analytics_df.columns
                    else 0,
                    "unique_people": analytics_df["person_uniqname"].nunique()
                    if "person_uniqname" in analytics_df.columns
                    else 0,
                }
                analyses["summary"] = pd.DataFrame([summary])

            # Full award list
            analyses["full_award_list"] = analytics_df

            logger.info(
                f"Generated award analytics with {len(analytics_df)} awards from lab_awards"
            )
            return analyses

        except SQLAlchemyError as e:
            logger.error(f"Failed to generate award analytics: {e}")
            raise

    def get_award_change_history(self, composite_id: str) -> pd.DataFrame:
        """
        Get the complete change history for a specific lab award record.

        Args:
            composite_id: The composite ID in format "Award ID-Person Uniqname-Person Appt Department Id"

        Returns:
            DataFrame with all historical versions of the award record
        """
        try:
            query = """
            SELECT
                raw_id,
                external_id,
                raw_data->>'Award Id' as award_id,
                raw_data->>'Award Title' as award_title,
                raw_data->>'Person Uniqname' as person_uniqname,
                raw_data->>'Person Role' as person_role,
                raw_data->>'_content_hash' as content_hash,
                ingested_at,
                ingestion_run_id
            FROM bronze.raw_entities
            WHERE entity_type = 'lab_award'
            AND source_system = 'lab_awards'
            AND external_id = :composite_id
            ORDER BY ingested_at DESC
            """

            history_df = self.db_adapter.query_to_dataframe(
                query, {"composite_id": composite_id}
            )

            logger.info(
                f"Retrieved {len(history_df)} historical records for award record {composite_id}"
            )
            return history_df

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve award history: {e}")
            raise

    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()
        logger.info("Lab awards ingestion service closed")


def main():
    """
    Main function to run lab awards ingestion from command line.
    """
    try:
        # Ensure logs directory exists
        os.makedirs("logs", exist_ok=True)

        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv("DATABASE_URL")

        # Validate configuration
        if not database_url:
            raise ValueError("Missing required environment variable: DATABASE_URL")

        # Create and run lab awards ingestion service
        ingestion_service = LabAwardsIngestionService(
            database_url=database_url, data_folder=DATA_FOLDER
        )

        # Run the content hash-based ingestion process
        print("🏆 Starting lab awards ingestion with content hashing...")
        results = ingestion_service.ingest_lab_awards_with_change_detection()

        # Display comprehensive summary
        print(f"\n📊 Lab Awards Ingestion Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Source File: {results['source_file']}")
        print(f"   Records Read from File: {results['records_read_from_file']}")
        print(f"   Total Awards Processed: {results['records_processed']}")
        print(f"   New Records Created: {results['records_created']}")
        print(f"     ├─ Brand New Awards: {results['new_awards']}")
        print(f"     └─ Awards with Changes: {results['changed_awards']}")
        print(f"   Skipped (No Changes): {results['records_skipped_unchanged']}")
        print(f"   Award Analytics:")
        print(f"     ├─ Total Award Dollars: ${results['total_award_dollars']:,.2f}")
        print(f"     ├─ Award Classes: {len(results['award_classes'])}")
        print(f"     ├─ Sponsors: {len(results['sponsors'])}")
        print(f"     └─ Departments: {len(results['departments'])}")
        print(f"   Errors: {len(results['errors'])}")

        if (
            results["records_skipped_unchanged"] > 0
            and results["records_processed"] > 0
        ):
            efficiency_percentage = (
                results["records_skipped_unchanged"] / results["records_processed"]
            ) * 100
            print(
                f"\n⚡ Efficiency: {efficiency_percentage:.1f}% of awards were unchanged and skipped"
            )

        # Show award analytics
        print("\n🏗️  Analyzing award data...")
        award_analyses = ingestion_service.get_award_analytics()

        # Award class distribution
        if "class_summary" in award_analyses:
            print("\n📋 Award Class Distribution:")
            class_summary = award_analyses["class_summary"]
            for _, row in class_summary.head(10).iterrows():
                print(f"   - {row['award_class']}: {row['award_count']} awards")

        # Sponsor distribution
        if "sponsor_summary" in award_analyses:
            print("\n🏢 Top 10 Sponsors:")
            sponsor_summary = award_analyses["sponsor_summary"]
            for _, row in sponsor_summary.head(10).iterrows():
                print(f"   - {row['direct_sponsor']}: {row['award_count']} awards")

        # Department distribution
        if "department_summary" in award_analyses:
            print("\n🏛️  Top 10 Departments:")
            dept_summary = award_analyses["department_summary"]
            for _, row in dept_summary.head(10).iterrows():
                print(f"   - {row['admin_department']}: {row['award_count']} awards")

        # Summary statistics
        if "summary" in award_analyses:
            print("\n📈 Overall Award Statistics:")
            summary = award_analyses["summary"].iloc[0]
            print(f"   - Total Awards: {summary['total_awards']}")
            print(f"   - Unique Sponsors: {summary['unique_sponsors']}")
            print(f"   - Unique Departments: {summary['unique_departments']}")
            print(f"   - Unique People: {summary['unique_people']}")

        # Clean up
        ingestion_service.close()

        print("\n✅ Lab awards ingestion completed successfully!")

    except Exception as e:
        logger.error(f"Lab awards ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
