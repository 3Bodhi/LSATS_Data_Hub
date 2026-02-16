#!/usr/bin/env python3
"""
MCommunity LDAP User Ingestion Service

This service ingests user data from the University of Michigan MCommunity LDAP
directory (ldap.umich.edu) into the bronze layer for cross-referencing and analysis.

MCommunity provides authoritative directory information for all U-M people including:
- Contact information (email, phone, address)
- Department and organizational affiliation (ou field)
- Job titles (umichTitle)
- Name variations (cn field)

All user records are stored in ou=People,dc=umich,dc=edu with uidNumber as the
unique external identifier.

IMPORTANT: Many LDAP attributes (cn, ou, umichPostalAddress, etc.) can be either
strings or lists of strings depending on the user record. The normalization functions
handle this appropriately.
"""

import hashlib
import json
import logging
import os
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
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# LSATS Data Hub imports
from dotenv import load_dotenv

from database.adapters.postgres_adapter import PostgresAdapter, create_postgres_adapter
from ldap.adapters.ldap_adapter import LDAPAdapter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/mcommunity_user_ingestion.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class MCommunityUserIngestionService:
    """
    User ingestion service for University of Michigan MCommunity LDAP directory.

    Uses content hashing for change detection since LDAP doesn't provide
    modification timestamps in a consistent way. This approach:

    1. Fetches current user data from MCommunity LDAP (ou=People)
    2. Calculates content hashes for each user
    3. Compares against stored hashes from previous ingestions
    4. Only creates new bronze records when user content has actually changed
    5. Preserves complete change history for user analysis

    Key Features:
    - Efficient change detection without requiring timestamps
    - Department and organizational affiliation tracking (ou field)
    - Handles multi-value LDAP attributes (cn, ou can be strings or lists)
    - Comprehensive audit trail for user changes
    - Detailed ingestion statistics and monitoring
    """

    def __init__(self, database_url: str, ldap_config: Dict[str, Any]):
        """
        Initialize the MCommunity user ingestion service.

        Args:
            database_url: PostgreSQL connection string
            ldap_config: LDAP connection configuration dictionary
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        # Initialize LDAP adapter for MCommunity
        self.ldap_adapter = LDAPAdapter(ldap_config)

        # Test LDAP connection
        if not self.ldap_adapter.test_connection():
            raise Exception("Failed to connect to MCommunity LDAP")

        logger.info(
            "MCommunity user ingestion service initialized with content hashing"
        )

    def _normalize_ldap_attribute(self, value: Any) -> Any:
        """
        Normalize LDAP attribute values for consistent hashing.

        LDAP attributes can be single values, lists, or None. Many MCommunity
        attributes like cn, ou, umichPostalAddress can be either strings or
        lists depending on the user.

        This normalizes them for consistent JSON serialization.

        Args:
            value: Raw LDAP attribute value

        Returns:
            Normalized value suitable for JSON serialization
        """
        if value is None:
            return ""
        elif isinstance(value, list):
            if len(value) == 0:
                return ""
            elif len(value) == 1:
                return str(value[0]).strip()
            else:
                # Sort multi-value attributes for consistent hashing
                return sorted([str(v).strip() for v in value])
        else:
            return str(value).strip()

    def _calculate_user_content_hash(self, user_data: Dict[str, Any]) -> str:
        """
        Calculate a content hash for MCommunity user data to detect meaningful changes.

        This hash represents the "content fingerprint" of the user record.
        We include all fields that would represent meaningful user changes.

        Args:
            user_data: Raw user data from MCommunity LDAP

        Returns:
            SHA-256 hash of the normalized user content
        """
        # Extract significant fields for change detection
        # Based on actual MCommunity LDAP schema from sample data
        significant_fields = {
            "uid": self._normalize_ldap_attribute(user_data.get("uid")),
            "uidNumber": self._normalize_ldap_attribute(user_data.get("uidNumber")),
            "cn": self._normalize_ldap_attribute(user_data.get("cn")),
            "displayName": self._normalize_ldap_attribute(user_data.get("displayName")),
            "givenName": self._normalize_ldap_attribute(user_data.get("givenName")),
            "sn": self._normalize_ldap_attribute(user_data.get("sn")),
            "mail": self._normalize_ldap_attribute(user_data.get("mail")),
            "ou": self._normalize_ldap_attribute(user_data.get("ou")),
            "umichTitle": self._normalize_ldap_attribute(user_data.get("umichTitle")),
            "telephoneNumber": self._normalize_ldap_attribute(
                user_data.get("telephoneNumber")
            ),
            "homeDirectory": self._normalize_ldap_attribute(
                user_data.get("homeDirectory")
            ),
            "loginShell": self._normalize_ldap_attribute(user_data.get("loginShell")),
            "gidNumber": self._normalize_ldap_attribute(user_data.get("gidNumber")),
            "umichPostalAddress": self._normalize_ldap_attribute(
                user_data.get("umichPostalAddress")
            ),
            "umichPostalAddressData": self._normalize_ldap_attribute(
                user_data.get("umichPostalAddressData")
            ),
        }

        # Create normalized JSON for consistent hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )

        # Generate SHA-256 hash
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        uid = user_data.get("uid", "unknown")
        display_name = self._normalize_ldap_attribute(user_data.get("displayName"))
        logger.debug(f"Content hash for user {uid} ({display_name}): {content_hash}")

        return content_hash

    def _get_existing_user_hashes(self) -> Dict[str, str]:
        """
        Retrieve the latest content hash for each MCommunity user from the bronze layer.

        This uses a window function to get only the most recent record for each
        user, allowing efficient comparison with new data.

        Returns:
            Dictionary mapping uidNumber -> latest_content_hash
        """
        try:
            # Query to get the most recent record for each user
            query = """
            WITH latest_users AS (
                SELECT
                    external_id,
                    raw_data,
                    ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'user'
                AND source_system = 'mcommunity_ldap'
            )
            SELECT
                external_id,
                raw_data
            FROM latest_users
            WHERE row_num = 1
            """

            results_df = self.db_adapter.query_to_dataframe(query)

            # Calculate content hashes for existing records
            existing_hashes = {}
            for _, row in results_df.iterrows():
                uid_number = row["external_id"]
                raw_data = row["raw_data"]  # JSONB comes back as dict
                content_hash = self._calculate_user_content_hash(raw_data)
                existing_hashes[uid_number] = content_hash

            logger.info(
                f"Retrieved content hashes for {len(existing_hashes)} existing MCommunity users"
            )
            return existing_hashes

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve existing user hashes: {e}")
            raise

    def create_ingestion_run(self, source_system: str, entity_type: str) -> str:
        """Create a new ingestion run record for tracking purposes."""
        try:
            run_id = str(uuid.uuid4())

            # Metadata specific to MCommunity LDAP content hashing approach
            metadata = {
                "ingestion_type": "content_hash_based",
                "source_api": "mcommunity_ldap",
                "ldap_server": "ldap.umich.edu",
                "search_base": "ou=People,dc=umich,dc=edu",
                "change_detection_method": "sha256_content_hash",
                "includes_department_relationships": True,
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
                f"Created MCommunity ingestion run {run_id} for {source_system}/{entity_type}"
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

            logger.info(f"Completed MCommunity ingestion run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"Failed to complete ingestion run: {e}")

    def ingest_mcommunity_users_with_change_detection(self) -> Dict[str, Any]:
        """
        Ingest University of Michigan MCommunity users using intelligent content hashing.

        This method:
        1. Fetches all user data from the MCommunity LDAP (ou=People)
        2. Calculates content hashes for each user
        3. Compares against existing bronze records
        4. Only creates new records when content has actually changed
        5. Provides detailed statistics about user changes detected

        Returns:
            Dictionary with comprehensive ingestion statistics
        """
        # Create ingestion run for tracking
        run_id = self.create_ingestion_run("mcommunity_ldap", "user")

        ingestion_stats = {
            "run_id": run_id,
            "records_processed": 0,
            "records_created": 0,
            "records_skipped_unchanged": 0,
            "new_users": 0,
            "changed_users": 0,
            "unique_departments": set(),
            "unique_job_titles": set(),
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            logger.info(
                "Starting MCommunity user ingestion with content hash change detection..."
            )

            # Step 1: Get existing user content hashes from bronze layer
            existing_hashes = self._get_existing_user_hashes()

            # Step 2: Fetch current data from MCommunity LDAP
            logger.info(
                "Fetching user data from MCommunity LDAP (ou=People,dc=umich,dc=edu)..."
            )

            # Define comprehensive attribute list for MCommunity users
            # Based on actual LDAP schema from sample data
            user_attributes = [
                "uid",
                "uidNumber",
                "cn",
                "displayName",
                "givenName",
                "sn",
                "mail",
                "ou",
                "umichTitle",
                "telephoneNumber",
                "homeDirectory",
                "loginShell",
                "gidNumber",
                "umichPostalAddress",
                "umichPostalAddressData",
                "objectClass",
                "dn",
            ]

            # Search for all users in ou=People
            # Using objectClass=umichPerson for U-M specific person records
            raw_users = self.ldap_adapter.search_as_dicts(
                search_filter="(uid=*)",
                search_base="ou=People,dc=umich,dc=edu",
                scope="subtree",
                attributes=user_attributes,
                use_pagination=True,
            )

            if not raw_users:
                logger.warning("No users found in MCommunity LDAP")
                return ingestion_stats

            logger.info(f"Retrieved {len(raw_users)} users from MCommunity LDAP")

            # Step 3: Process each user with content hash change detection
            for user_data in raw_users:
                try:
                    # Extract user identifiers
                    uid = self._normalize_ldap_attribute(user_data.get("uid"))
                    uid_number = self._normalize_ldap_attribute(
                        user_data.get("uidNumber")
                    )
                    display_name = self._normalize_ldap_attribute(
                        user_data.get("displayName", "Unknown User")
                    )

                    # Skip if no uidNumber (required as external_id)
                    if not uid_number:
                        logger.warning(
                            f"Skipping user {uid} - missing uidNumber attribute"
                        )
                        continue

                    # Track analytics for reporting
                    # ou field contains department affiliations (can be string or list)
                    ou = user_data.get("ou")
                    if ou:
                        if isinstance(ou, list):
                            ingestion_stats["unique_departments"].update(ou)
                        else:
                            ingestion_stats["unique_departments"].add(ou)

                    # Track job titles
                    job_title = self._normalize_ldap_attribute(
                        user_data.get("umichTitle")
                    )
                    if job_title:
                        ingestion_stats["unique_job_titles"].add(job_title)

                    # Calculate content hash for this user
                    current_hash = self._calculate_user_content_hash(user_data)

                    # Check if this user is new or has changed
                    existing_hash = existing_hashes.get(uid_number)

                    if existing_hash is None:
                        # This is a completely new user
                        logger.info(
                            f"🆕 New user detected: {display_name} ({uid}, uidNumber: {uid_number})"
                        )
                        should_insert = True
                        ingestion_stats["new_users"] += 1

                    elif existing_hash != current_hash:
                        # This user exists but has changed
                        logger.info(
                            f"📝 User changed: {display_name} ({uid}, uidNumber: {uid_number})"
                        )
                        logger.debug(f"   Old hash: {existing_hash}")
                        logger.debug(f"   New hash: {current_hash}")
                        should_insert = True
                        ingestion_stats["changed_users"] += 1

                    else:
                        # This user exists and hasn't changed - skip it
                        logger.debug(
                            f"⏭️  User unchanged, skipping: {display_name} ({uid}, uidNumber: {uid_number})"
                        )
                        should_insert = False
                        ingestion_stats["records_skipped_unchanged"] += 1

                    # Only insert if the user is new or changed
                    if should_insert:
                        # Enhance raw data with metadata for future reference
                        enhanced_raw_data = user_data.copy()
                        enhanced_raw_data["_content_hash"] = current_hash
                        enhanced_raw_data["_change_detection"] = "content_hash_based"
                        enhanced_raw_data["_ldap_server"] = "ldap.umich.edu"
                        enhanced_raw_data["_search_base"] = "ou=People,dc=umich,dc=edu"

                        # Insert into bronze layer using uidNumber as external_id
                        entity_id = self.db_adapter.insert_raw_entity(
                            entity_type="user",
                            source_system="mcommunity_ldap",
                            external_id=uid_number,
                            raw_data=enhanced_raw_data,
                            ingestion_run_id=run_id,
                        )

                        ingestion_stats["records_created"] += 1

                    # Log progress periodically
                    if (
                        ingestion_stats["records_processed"] % 100 == 0
                        and ingestion_stats["records_processed"] > 0
                    ):
                        logger.info(
                            f"Progress: {ingestion_stats['records_processed']} users processed "
                            f"({ingestion_stats['records_created']} new/changed, "
                            f"{ingestion_stats['records_skipped_unchanged']} unchanged)"
                        )

                except Exception as record_error:
                    error_msg = f"Failed to process user {uid_number}: {record_error}"
                    logger.error(error_msg)
                    ingestion_stats["errors"].append(error_msg)

                ingestion_stats["records_processed"] += 1

            # Convert sets to counts for final reporting
            analytics_counts = {
                "departments": len(ingestion_stats["unique_departments"]),
                "job_titles": len(ingestion_stats["unique_job_titles"]),
            }
            ingestion_stats["analytics_summary"] = analytics_counts

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
            logger.info(
                f"🎉 MCommunity user ingestion completed in {duration:.2f} seconds"
            )
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Total Processed: {ingestion_stats['records_processed']}")
            logger.info(f"   New Records Created: {ingestion_stats['records_created']}")
            logger.info(f"   ├─ New Users: {ingestion_stats['new_users']}")
            logger.info(f"   └─ Changed Users: {ingestion_stats['changed_users']}")
            logger.info(
                f"   Skipped (Unchanged): {ingestion_stats['records_skipped_unchanged']}"
            )
            logger.info(f"   User Analytics:")
            logger.info(
                f"   ├─ Unique Departments/OUs: {analytics_counts['departments']}"
            )
            logger.info(f"   └─ Unique Job Titles: {analytics_counts['job_titles']}")
            logger.info(f"   Errors: {len(ingestion_stats['errors'])}")

            return ingestion_stats

        except Exception as e:
            error_msg = f"MCommunity user ingestion failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            self.complete_ingestion_run(
                run_id=run_id,
                records_processed=ingestion_stats["records_processed"],
                records_created=ingestion_stats["records_created"],
                records_skipped=ingestion_stats["records_skipped_unchanged"],
                error_message=error_msg,
            )

            raise

    def get_user_analytics(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze MCommunity user data from bronze layer.

        This provides insights into the user structure and can help
        identify patterns or anomalies in the user data.

        Returns:
            Dictionary containing DataFrames for different user analyses
        """
        try:
            # Query for user analytics using actual MCommunity LDAP fields
            analytics_query = """
            WITH latest_users AS (
                SELECT
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY external_id
                        ORDER BY ingested_at DESC
                    ) as row_num
                FROM bronze.raw_entities
                WHERE entity_type = 'user'
                AND source_system = 'mcommunity_ldap'
            )
            SELECT
                raw_data->>'uid' as uid,
                raw_data->>'uidNumber' as uid_number,
                raw_data->>'displayName' as display_name,
                raw_data->>'mail' as email,
                raw_data->>'ou' as organizational_units,
                raw_data->>'umichTitle' as job_title,
                raw_data->>'telephoneNumber' as phone,
                raw_data->>'cn' as common_names,
                raw_data->>'givenName' as given_name,
                raw_data->>'sn' as surname
            FROM latest_users
            WHERE row_num = 1
            ORDER BY uid
            """

            analytics_df = self.db_adapter.query_to_dataframe(analytics_query)

            # Create summary analyses
            analyses = {}

            # Job title summary
            job_title_summary = (
                analytics_df.groupby("job_title").size().reset_index(name="user_count")
            )
            analyses["job_title_summary"] = job_title_summary.sort_values(
                "user_count", ascending=False
            )

            # Full user list
            analyses["full_user_list"] = analytics_df

            logger.info(
                f"Generated user analytics with {len(analytics_df)} users from MCommunity"
            )
            return analyses

        except SQLAlchemyError as e:
            logger.error(f"Failed to generate user analytics: {e}")
            raise

    def get_user_change_history(self, uid_number: str) -> pd.DataFrame:
        """
        Get the complete change history for a specific MCommunity user.

        Args:
            uid_number: The MCommunity uidNumber

        Returns:
            DataFrame with all historical versions of the user
        """
        try:
            query = """
            SELECT
                raw_id,
                raw_data->>'uid' as uid,
                raw_data->>'displayName' as display_name,
                raw_data->>'mail' as email,
                raw_data->>'ou' as organizational_units,
                raw_data->>'umichTitle' as job_title,
                raw_data->>'_content_hash' as content_hash,
                ingested_at,
                ingestion_run_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
            AND source_system = 'mcommunity_ldap'
            AND external_id = :uid_number
            ORDER BY ingested_at DESC
            """

            history_df = self.db_adapter.query_to_dataframe(
                query, {"uid_number": uid_number}
            )

            logger.info(
                f"Retrieved {len(history_df)} historical records for MCommunity user {uid_number}"
            )
            return history_df

        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve user history: {e}")
            raise

    def close(self):
        """Clean up database and LDAP connections."""
        if self.db_adapter:
            self.db_adapter.close()
        if self.ldap_adapter:
            # LDAPAdapter doesn't have explicit close, connection is managed internally
            pass
        logger.info("MCommunity user ingestion service closed")


def main():
    """
    Main function to run MCommunity user ingestion from command line.
    """
    try:
        # Ensure logs directory exists
        os.makedirs("logs", exist_ok=True)

        # Load environment variables
        load_dotenv()

        # Get required configuration from environment
        database_url = os.getenv("DATABASE_URL")

        # MCommunity LDAP configuration
        ldap_config = {
            "server": os.getenv("MCOMMUNITY_LDAP_SERVER", "ldap.umich.edu"),
            "search_base": os.getenv("MCOMMUNITY_LDAP_BASE", "dc=umich,dc=edu"),
            "user": os.getenv("MCOMMUNITY_LDAP_USER"),
            "keyring_service": os.getenv("MCOMMUNITY_KEYRING_SERVICE", "Mcom_umich"),
            "port": int(os.getenv("MCOMMUNITY_LDAP_PORT", "636")),
            "use_ssl": os.getenv("MCOMMUNITY_LDAP_USE_SSL", "true").lower() == "true",
        }

        # Validate configuration
        if not database_url:
            raise ValueError("Missing required environment variable: DATABASE_URL")

        if not ldap_config["user"]:
            raise ValueError(
                "Missing required environment variable: MCOMMUNITY_LDAP_USER"
            )

        # Create and run MCommunity ingestion service
        ingestion_service = MCommunityUserIngestionService(
            database_url=database_url, ldap_config=ldap_config
        )

        # Run the content hash-based ingestion process
        print("👥 Starting MCommunity user ingestion with content hashing...")
        results = ingestion_service.ingest_mcommunity_users_with_change_detection()

        # Display comprehensive summary
        print(f"\n📊 MCommunity User Ingestion Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Total Users Processed: {results['records_processed']}")
        print(f"   New Records Created: {results['records_created']}")
        print(f"     ├─ Brand New Users: {results['new_users']}")
        print(f"     └─ Users with Changes: {results['changed_users']}")
        print(f"   Skipped (No Changes): {results['records_skipped_unchanged']}")
        print(f"   User Analytics:")
        print(
            f"     ├─ Unique Departments/OUs: {results['analytics_summary']['departments']}"
        )
        print(
            f"     └─ Unique Job Titles: {results['analytics_summary']['job_titles']}"
        )
        print(f"   Errors: {len(results['errors'])}")

        if results["records_skipped_unchanged"] > 0:
            efficiency_percentage = (
                results["records_skipped_unchanged"] / results["records_processed"]
            ) * 100
            print(
                f"\n⚡ Efficiency: {efficiency_percentage:.1f}% of users were unchanged and skipped"
            )

        # Show user analytics
        print("\n🏗️  Analyzing user data...")
        user_analyses = ingestion_service.get_user_analytics()

        # Job title distribution
        print("\n💼 Top 20 Job Titles by User Count:")
        job_title_summary = user_analyses["job_title_summary"]
        for _, row in job_title_summary.head(20).iterrows():
            job_title = (
                str(row["job_title"])[:60] if row["job_title"] else "No Title Listed"
            )
            print(f"   - {job_title}: {row['user_count']} users")

        if len(job_title_summary) > 20:
            remaining_title_count = len(job_title_summary) - 20
            remaining_user_count = job_title_summary.iloc[20:]["user_count"].sum()
            print(
                f"   - ... and {remaining_title_count} more job titles with {remaining_user_count} additional users"
            )

        # Overall statistics
        total_stats = {
            "Total Users": len(user_analyses["full_user_list"]),
            "Unique Job Titles": len(job_title_summary),
        }

        print(f"\n📈 Overall User Statistics:")
        for stat, count in total_stats.items():
            print(f"   - {stat}: {count}")

        # Clean up
        ingestion_service.close()

        print("✅ MCommunity user ingestion completed successfully!")

    except Exception as e:
        logger.error(f"MCommunity user ingestion failed: {e}", exc_info=True)
        print(f"❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
