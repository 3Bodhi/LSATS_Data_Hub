#!/usr/bin/env python3
"""
User Silver Layer Transformation Service

Merges bronze user data from four sources (active_directory, mcommunity_ldap, tdx, umich_api)
into unified silver records.

Key features:
- Matches users by uniqname (normalized to lowercase)
- Handles multiple employment records from umich_api (combines into arrays)
- Maps MCommunity OU data to silver.departments via fuzzy matching
- Stores complete Active Directory group memberships
- Prioritizes TDX for operational IDs, UMAPI for job data, LDAP for contact info
- Incremental processing (only transforms users with new bronze records)
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
        logging.FileHandler("logs/silver_user_transformation.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class UserSilverTransformationService:
    """
    Service for transforming bronze user records into silver layer.

    This service merges data from four bronze sources:
    1. tdx: Provides operational UIDs, contact info, and active status
    2. umich_api: Provides employment records, job codes, department IDs
    3. mcommunity_ldap: Provides LDAP UIDs, OU affiliations, contact info
    4. active_directory: Provides AD GUIDs, group memberships, account status

    The merge creates a unified silver record with uniqname (lowercase) as the primary key.
    """

    @staticmethod
    def _json_dumps_or_none(value):
        """
        Helper to serialize JSON values, returning None instead of 'null' string.

        Args:
            value: Value to serialize

        Returns:
            JSON string if value is not None, else None
        """
        return json.dumps(value) if value is not None else None

    def __init__(self, database_url: str):
        """
        Initialize the transformation service.

        Args:
            database_url: PostgreSQL connection string
        """
        self.db_adapter = PostgresAdapter(
            database_url=database_url, pool_size=5, max_overflow=10
        )

        # Cache for department matching
        self._dept_cache: Optional[pd.DataFrame] = None

        logger.info("User silver transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful silver transformation run.

        Returns:
            datetime of last successful transformation, or None for first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
            AND entity_type = 'user'
            AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"Last successful transformation was at: {last_timestamp}")
                return last_timestamp
            else:
                logger.info("No previous transformation found - processing all users")
                return None

        except SQLAlchemyError as e:
            logger.warning(f"Could not determine last transformation timestamp: {e}")
            return None

    def _get_users_needing_transformation(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Set[str]:
        """
        Find uniqnames that have new/updated bronze records.

        OPTIMIZATION: Only process users that have a TDX record (operational requirement).
        This dramatically reduces processing time by excluding MCommunity-only users.

        Args:
            since_timestamp: Only include users with bronze records after this time (ignored if full_sync=True)
            full_sync: If True, return ALL TDX users regardless of timestamp

        Returns:
            Set of uniqnames (lowercase) that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            # OPTIMIZATION: Only get users that exist in TDX (critical for operations)
            # This excludes MCommunity-only users (alumni, etc.) which don't need silver records
            query = f"""
            SELECT DISTINCT
                LOWER(raw_data->>'AlternateID') as uniqname
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
            AND source_system = 'tdx'
            {time_filter}
            AND raw_data->>'AlternateID' IS NOT NULL
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            uniqnames = set(result_df["uniqname"].tolist())

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"Found {len(uniqnames)} users needing transformation (TDX users only, {sync_mode} mode)"
            )
            return uniqnames

        except SQLAlchemyError as e:
            logger.error(f"Failed to get users needing transformation: {e}")
            raise

    def _fetch_existing_silver_hashes(self, uniqnames: Set[str]) -> Dict[str, str]:
        """
        Fetch existing entity hashes from silver records for hash-based change detection.

        Args:
            uniqnames: Set of uniqnames to fetch hashes for

        Returns:
            Dictionary mapping uniqname -> entity_hash
        """
        try:
            if not uniqnames:
                return {}

            uniqname_list = list(uniqnames)

            query = """
            SELECT uniqname, entity_hash
            FROM silver.users
            WHERE uniqname = ANY(:uniqnames)
            """

            result_df = self.db_adapter.query_to_dataframe(
                query, {"uniqnames": uniqname_list}
            )

            hash_map = dict(zip(result_df["uniqname"], result_df["entity_hash"]))
            logger.info(f"Fetched {len(hash_map)} existing silver record hashes")
            return hash_map

        except SQLAlchemyError as e:
            logger.warning(f"Failed to fetch existing silver hashes: {e}")
            return {}

    def _fetch_all_bronze_records_batch(
        self, uniqnames: Set[str]
    ) -> Dict[str, Tuple[Optional[Dict], Optional[Dict], List[Dict], Optional[Dict]]]:
        """
        Fetch bronze records for ALL users at once (batch operation).

        OPTIMIZATION: Single query per source instead of N queries per user.
        This is 10-100x faster for large user sets.

        Args:
            uniqnames: Set of all uniqnames to fetch

        Returns:
            Dictionary mapping uniqname -> (tdx_record, mcom_record, umapi_records_list, ad_record)
        """
        try:
            result_map = {uniqname: (None, None, [], None) for uniqname in uniqnames}

            # Convert to list for SQL ANY clause
            uniqname_list = list(uniqnames)

            # Fetch ALL TDX records in one query
            logger.info("Fetching TDX records (batch)...")
            tdx_query = """
            WITH ranked_records AS (
                SELECT
                    LOWER(raw_data->>'AlternateID') as uniqname,
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY LOWER(raw_data->>'AlternateID')
                        ORDER BY ingested_at DESC
                    ) as rn
                FROM bronze.raw_entities
                WHERE entity_type = 'user'
                AND source_system = 'tdx'
                AND LOWER(raw_data->>'AlternateID') = ANY(:uniqnames)
            )
            SELECT uniqname, raw_data
            FROM ranked_records
            WHERE rn = 1
            """
            tdx_df = self.db_adapter.query_to_dataframe(
                tdx_query, {"uniqnames": uniqname_list}
            )
            for _, row in tdx_df.iterrows():
                uniqname = row["uniqname"]
                if uniqname in result_map:
                    current = result_map[uniqname]
                    result_map[uniqname] = (
                        row["raw_data"],
                        current[1],
                        current[2],
                        current[3],
                    )

            logger.info(f"Fetched {len(tdx_df)} TDX records")

            # Fetch ALL MCommunity records in one query
            logger.info("Fetching MCommunity records (batch)...")
            mcom_query = """
            WITH ranked_records AS (
                SELECT
                    LOWER(raw_data->>'uid') as uniqname,
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY LOWER(raw_data->>'uid')
                        ORDER BY ingested_at DESC
                    ) as rn
                FROM bronze.raw_entities
                WHERE entity_type = 'user'
                AND source_system = 'mcommunity_ldap'
                AND LOWER(raw_data->>'uid') = ANY(:uniqnames)
            )
            SELECT uniqname, raw_data
            FROM ranked_records
            WHERE rn = 1
            """
            mcom_df = self.db_adapter.query_to_dataframe(
                mcom_query, {"uniqnames": uniqname_list}
            )
            for _, row in mcom_df.iterrows():
                uniqname = row["uniqname"]
                if uniqname in result_map:
                    current = result_map[uniqname]
                    result_map[uniqname] = (
                        current[0],
                        row["raw_data"],
                        current[2],
                        current[3],
                    )

            logger.info(f"Fetched {len(mcom_df)} MCommunity records")

            # Fetch ALL UMAPI records (including multiple employment records per user)
            logger.info("Fetching UMAPI records (batch)...")
            umapi_query = """
            WITH latest_ingestion AS (
                SELECT DISTINCT ON (LOWER(raw_data->>'UniqName'))
                    LOWER(raw_data->>'UniqName') as uniqname,
                    ingestion_run_id as latest_run_id
                FROM bronze.raw_entities
                WHERE entity_type = 'user'
                AND source_system = 'umich_api'
                AND LOWER(raw_data->>'UniqName') = ANY(:uniqnames)
                ORDER BY LOWER(raw_data->>'UniqName'), ingested_at DESC
            )
            SELECT
                LOWER(e.raw_data->>'UniqName') as uniqname,
                e.raw_data,
                CAST(e.raw_data->>'EmplRcd' AS INTEGER) as empl_rcd
            FROM bronze.raw_entities e
            INNER JOIN latest_ingestion l ON LOWER(e.raw_data->>'UniqName') = l.uniqname
            WHERE e.entity_type = 'user'
            AND e.source_system = 'umich_api'
            AND e.ingestion_run_id = l.latest_run_id
            ORDER BY uniqname, empl_rcd
            """
            umapi_df = self.db_adapter.query_to_dataframe(
                umapi_query, {"uniqnames": uniqname_list}
            )

            # Group UMAPI records by uniqname (multiple records per user)
            for uniqname, group in umapi_df.groupby("uniqname"):
                if uniqname in result_map:
                    umapi_records = group["raw_data"].tolist()
                    current = result_map[uniqname]
                    result_map[uniqname] = (
                        current[0],
                        current[1],
                        umapi_records,
                        current[3],
                    )

            logger.info(f"Fetched {len(umapi_df)} UMAPI records")

            # Fetch ALL Active Directory records in one query
            logger.info("Fetching Active Directory records (batch)...")
            ad_query = """
            WITH ranked_records AS (
                SELECT
                    LOWER(raw_data->>'uid') as uniqname,
                    raw_data,
                    ROW_NUMBER() OVER (
                        PARTITION BY LOWER(raw_data->>'uid')
                        ORDER BY ingested_at DESC
                    ) as rn
                FROM bronze.raw_entities
                WHERE entity_type = 'user'
                AND source_system = 'active_directory'
                AND LOWER(raw_data->>'uid') = ANY(:uniqnames)
            )
            SELECT uniqname, raw_data
            FROM ranked_records
            WHERE rn = 1
            """
            ad_df = self.db_adapter.query_to_dataframe(
                ad_query, {"uniqnames": uniqname_list}
            )
            for _, row in ad_df.iterrows():
                uniqname = row["uniqname"]
                if uniqname in result_map:
                    current = result_map[uniqname]
                    result_map[uniqname] = (
                        current[0],
                        current[1],
                        current[2],
                        row["raw_data"],
                    )

            logger.info(f"Fetched {len(ad_df)} Active Directory records")

            return result_map

        except SQLAlchemyError as e:
            logger.error(f"Failed to fetch bronze records in batch: {e}")
            raise

    def _fetch_latest_bronze_records(
        self, uniqname: str
    ) -> Tuple[Optional[Dict], Optional[Dict], List[Dict], Optional[Dict]]:
        """
        Fetch the latest bronze records for a user from all sources.

        Args:
            uniqname: The uniqname identifier (lowercase)

        Returns:
            Tuple of (tdx_record, mcommunity_record, umich_api_records_list, ad_record)
        """
        try:
            # Fetch latest TDX record
            tdx_query = """
            SELECT raw_data, ingested_at, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
            AND source_system = 'tdx'
            AND LOWER(raw_data->>'AlternateID') = :uniqname
            ORDER BY ingested_at DESC
            LIMIT 1
            """
            tdx_df = self.db_adapter.query_to_dataframe(
                tdx_query, {"uniqname": uniqname}
            )
            tdx_record = tdx_df.iloc[0]["raw_data"] if not tdx_df.empty else None

            # Fetch latest MCommunity LDAP record
            mcom_query = """
            SELECT raw_data, ingested_at, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
            AND source_system = 'mcommunity_ldap'
            AND LOWER(raw_data->>'uid') = :uniqname
            ORDER BY ingested_at DESC
            LIMIT 1
            """
            mcom_df = self.db_adapter.query_to_dataframe(
                mcom_query, {"uniqname": uniqname}
            )
            mcom_record = mcom_df.iloc[0]["raw_data"] if not mcom_df.empty else None

            # Fetch ALL umich_api records (multiple employment records)
            # Get records from the latest ingestion run only
            umapi_query = """
            WITH latest_ingestion AS (
                SELECT ingestion_run_id as latest_run_id
                FROM bronze.raw_entities
                WHERE entity_type = 'user'
                AND source_system = 'umich_api'
                AND UPPER(raw_data->>'UniqName') = UPPER(:uniqname)
                ORDER BY ingested_at DESC
                LIMIT 1
            )
            SELECT raw_data, ingested_at, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
            AND source_system = 'umich_api'
            AND UPPER(raw_data->>'UniqName') = UPPER(:uniqname)
            AND ingestion_run_id = (SELECT latest_run_id FROM latest_ingestion)
            ORDER BY CAST(raw_data->>'EmplRcd' AS INTEGER)
            """
            umapi_df = self.db_adapter.query_to_dataframe(
                umapi_query, {"uniqname": uniqname}
            )
            umapi_records = umapi_df["raw_data"].tolist() if not umapi_df.empty else []

            # Fetch latest Active Directory record
            ad_query = """
            SELECT raw_data, ingested_at, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'user'
            AND source_system = 'active_directory'
            AND LOWER(raw_data->>'uid') = :uniqname
            ORDER BY ingested_at DESC
            LIMIT 1
            """
            ad_df = self.db_adapter.query_to_dataframe(ad_query, {"uniqname": uniqname})
            ad_record = ad_df.iloc[0]["raw_data"] if not ad_df.empty else None

            return tdx_record, mcom_record, umapi_records, ad_record

        except SQLAlchemyError as e:
            logger.error(f"Failed to fetch bronze records for user {uniqname}: {e}")
            raise

    def _load_department_cache(self):
        """Load department data for OU matching."""
        if self._dept_cache is not None:
            return

        try:
            query = """
            SELECT
                dept_id,
                department_name,
                department_code,
                campus_name,
                college_group
            FROM silver.departments
            WHERE is_active = true
            """
            self._dept_cache = self.db_adapter.query_to_dataframe(query)
            logger.info(f"Loaded {len(self._dept_cache)} departments for matching")
        except SQLAlchemyError as e:
            logger.warning(f"Failed to load department cache: {e}")
            self._dept_cache = pd.DataFrame()

    def _match_ou_to_departments(self, ou_list: List[str]) -> List[str]:
        """
        Match MCommunity OU strings to department IDs.

        Uses regex pattern matching against department names.
        Example OU: "LSA Psychology - Faculty and Staff" -> matches dept "LSA Psychology"

        Args:
            ou_list: List of OU strings from MCommunity

        Returns:
            List of matched dept_ids
        """
        if not ou_list or self._dept_cache is None or self._dept_cache.empty:
            return []

        matched_dept_ids = []

        for ou_string in ou_list:
            # Remove common suffixes like " - Faculty and Staff"
            cleaned_ou = re.sub(
                r"\s*-\s*(Faculty and Staff|Staff|Students?|All)$",
                "",
                ou_string,
                flags=re.IGNORECASE,
            ).strip()

            # Try exact match first
            exact_matches = self._dept_cache[
                self._dept_cache["department_name"].str.lower() == cleaned_ou.lower()
            ]

            if not exact_matches.empty:
                matched_dept_ids.append(exact_matches.iloc[0]["dept_id"])
                continue

            # Try partial match (department name is contained in OU string)
            for _, dept_row in self._dept_cache.iterrows():
                dept_name = dept_row["department_name"]
                if dept_name and dept_name.lower() in cleaned_ou.lower():
                    matched_dept_ids.append(dept_row["dept_id"])
                    break

        return list(set(matched_dept_ids))  # Remove duplicates

    def _normalize_phone(self, phone: Optional[str]) -> Optional[str]:
        """Normalize phone numbers to consistent format."""
        if not phone:
            return None

        # Remove common separators and spaces
        digits_only = re.sub(r"[^\d]", "", phone)

        # Format as (XXX) XXX-XXXX if 10 digits
        if len(digits_only) == 10:
            return f"({digits_only[:3]}) {digits_only[3:6]}-{digits_only[6:]}"

        return phone  # Return as-is if not standard format

    def _merge_bronze_to_silver(
        self,
        uniqname: str,
        tdx_data: Optional[Dict],
        mcom_data: Optional[Dict],
        umapi_data_list: List[Dict],
        ad_data: Optional[Dict],
    ) -> Dict[str, Any]:
        """
        Merge bronze records from all sources into a unified silver record.

        Field Priority Rules:
        - uniqname: Primary key (normalized to lowercase)
        - umich_empl_id: From UMAPI EmplId or TDX ExternalID
        - tdx_user_uid: From TDX UID
        - Names: Prioritize TDX/UMAPI, fallback to LDAP
        - Email: Prioritize TDX PrimaryEmail, fallback to LDAP mail
        - Phone: Prioritize TDX WorkPhone, then UMAPI, then LDAP
        - Job info: Arrays from multiple UMAPI records
        - Department: From UMAPI DepartmentId (array if multiple jobs)
        - OU affiliations: From MCommunity, mapped to dept_ids
        - AD groups: Full array from Active Directory

        Args:
            uniqname: The user identifier (lowercase)
            tdx_data: Bronze record from tdx
            mcom_data: Bronze record from mcommunity_ldap
            umapi_data_list: List of bronze records from umich_api
            ad_data: Bronze record from active_directory

        Returns:
            Dictionary representing silver record
        """
        silver_record = {
            "uniqname": uniqname.lower(),
        }

        # Determine source system
        sources = []
        if tdx_data:
            sources.append("tdx")
        if mcom_data:
            sources.append("mcommunity_ldap")
        if umapi_data_list:
            sources.append("umich_api")
        if ad_data:
            sources.append("active_directory")

        if not sources:
            raise ValueError(f"No bronze data found for user {uniqname}")

        silver_record["source_system"] = "+".join(sources)

        # Extract TDX fields (operational IDs and status)
        if tdx_data:
            silver_record["tdx_user_uid"] = tdx_data.get("UID")
            silver_record["is_active"] = tdx_data.get("IsActive", True)

            # Names from TDX
            silver_record["first_name"] = tdx_data.get("FirstName")
            silver_record["last_name"] = tdx_data.get("LastName")
            silver_record["full_name"] = tdx_data.get("FullName")

            # Contact from TDX
            silver_record["primary_email"] = tdx_data.get("PrimaryEmail")
            silver_record["work_phone"] = self._normalize_phone(
                tdx_data.get("WorkPhone")
            )

            # TDX has single job title
            silver_record["tdx_job_title"] = tdx_data.get("Title")

            # Work location from TDX
            silver_record["work_city"] = tdx_data.get("WorkCity")
            silver_record["work_state"] = tdx_data.get("WorkState")
            silver_record["work_postal_code"] = tdx_data.get("WorkZip")
            silver_record["work_country"] = tdx_data.get("WorkCountry")
            silver_record["work_address_line1"] = tdx_data.get("WorkAddress")
            silver_record["work_address_line2"] = None  # TDX doesn't have line 2
        else:
            silver_record["tdx_user_uid"] = None
            silver_record["is_active"] = True  # Assume active if unknown
            silver_record["tdx_job_title"] = None

        # Extract UMAPI fields (employment records, job codes, departments)
        if umapi_data_list:
            # Use first record for core fields, then aggregate multi-value fields
            primary_record = umapi_data_list[0]

            silver_record["umich_empl_id"] = primary_record.get("EmplId")

            # Fill in names if not from TDX
            if not silver_record.get("first_name"):
                silver_record["first_name"] = primary_record.get("FirstName")
            if not silver_record.get("last_name"):
                silver_record["last_name"] = primary_record.get("LastName")
            if not silver_record.get("full_name"):
                silver_record["full_name"] = primary_record.get("Name")

            # Work location from UMAPI if not from TDX
            if not silver_record.get("work_city"):
                silver_record["work_city"] = primary_record.get("Work_City")
            if not silver_record.get("work_state"):
                silver_record["work_state"] = primary_record.get("Work_State")
            if not silver_record.get("work_postal_code"):
                silver_record["work_postal_code"] = primary_record.get("Work_Postal")
            if not silver_record.get("work_country"):
                silver_record["work_country"] = primary_record.get("Work_Country")
            if not silver_record.get("work_address_line1"):
                silver_record["work_address_line1"] = primary_record.get(
                    "Work_Address1"
                )
            silver_record["work_address_line2"] = primary_record.get("Work_Address2")

            # Phone from UMAPI if not from TDX
            if not silver_record.get("work_phone"):
                silver_record["work_phone"] = self._normalize_phone(
                    primary_record.get("Work_Phone")
                )

            # Aggregate arrays for multiple employment records
            job_titles = []
            dept_job_titles = []
            dept_ids = []
            job_codes = []
            supervisor_ids = []

            for record in umapi_data_list:
                # University job title (usually same across records)
                univ_title = record.get("UniversityJobTitle")
                if univ_title and univ_title not in job_titles:
                    job_titles.append(univ_title)

                # Department job title (varies by position)
                dept_title = record.get("DepartmentJobTitle")
                if dept_title:
                    dept_job_titles.append(dept_title)

                # Department ID
                dept_id = record.get("DepartmentId")
                if dept_id:
                    dept_ids.append(dept_id)

                # Job code
                job_code = record.get("Jobcode")
                if job_code:
                    job_codes.append(job_code)

                # Supervisor ID (note: field name is 'SupervisorId' not 'SupervisorID')
                supervisor_id = record.get("SupervisorId")
                if supervisor_id:
                    supervisor_ids.append(supervisor_id)

            silver_record["job_title"] = job_titles[0] if job_titles else None
            silver_record["department_job_titles"] = (
                dept_job_titles if dept_job_titles else None
            )
            silver_record["department_ids"] = dept_ids if dept_ids else None
            silver_record["job_codes"] = job_codes if job_codes else None
            silver_record["supervisor_ids"] = supervisor_ids if supervisor_ids else None

            # Primary department is first one (EmplRcd 0)
            silver_record["department_id"] = dept_ids[0] if dept_ids else None
        else:
            silver_record["umich_empl_id"] = None

            # Try to get EmplId from TDX ExternalID if UMAPI missing
            if tdx_data and tdx_data.get("ExternalID"):
                silver_record["umich_empl_id"] = tdx_data.get("ExternalID")

            silver_record["job_title"] = None
            silver_record["department_job_titles"] = None
            silver_record["department_ids"] = None
            silver_record["job_codes"] = None
            silver_record["supervisor_ids"] = None
            silver_record["department_id"] = None

        # Extract MCommunity LDAP fields (OU affiliations, alternate contact)
        if mcom_data:
            # Fill in names if still missing
            if not silver_record.get("first_name"):
                silver_record["first_name"] = mcom_data.get("givenName")
            if not silver_record.get("last_name"):
                silver_record["last_name"] = mcom_data.get("sn")
            if not silver_record.get("full_name"):
                silver_record["full_name"] = mcom_data.get("displayName")

            # Email fallback
            if not silver_record.get("primary_email"):
                silver_record["primary_email"] = mcom_data.get("mail")

            # Phone fallback
            if not silver_record.get("work_phone"):
                phone_data = mcom_data.get("telephoneNumber")
                if isinstance(phone_data, list) and phone_data:
                    silver_record["work_phone"] = self._normalize_phone(phone_data[0])
                elif phone_data:
                    silver_record["work_phone"] = self._normalize_phone(phone_data)

            # Job title from MCommunity if still missing
            if not silver_record.get("job_title"):
                silver_record["job_title"] = mcom_data.get("umichTitle")

            # OU affiliations (stored as array)
            ou_data = mcom_data.get("ou")
            if ou_data:
                if isinstance(ou_data, list):
                    silver_record["mcommunity_ou_affiliations"] = ou_data
                else:
                    silver_record["mcommunity_ou_affiliations"] = [ou_data]

                # Match OUs to department IDs
                self._load_department_cache()
                matched_dept_ids = self._match_ou_to_departments(
                    silver_record["mcommunity_ou_affiliations"]
                )
                silver_record["ou_department_ids"] = (
                    matched_dept_ids if matched_dept_ids else None
                )
            else:
                silver_record["mcommunity_ou_affiliations"] = None
                silver_record["ou_department_ids"] = None

            # LDAP identifiers
            silver_record["ldap_uid_number"] = mcom_data.get("uidNumber")
        else:
            silver_record["mcommunity_ou_affiliations"] = None
            silver_record["ou_department_ids"] = None
            silver_record["ldap_uid_number"] = None

        # Extract Active Directory fields (groups, account status)
        if ad_data:
            # AD identifiers
            silver_record["ad_object_guid"] = ad_data.get("objectGUID")
            silver_record["ad_sam_account_name"] = ad_data.get("sAMAccountName")

            # Group memberships (full array)
            member_of = ad_data.get("memberOf")
            if member_of:
                if isinstance(member_of, list):
                    silver_record["ad_group_memberships"] = member_of
                else:
                    silver_record["ad_group_memberships"] = [member_of]
            else:
                silver_record["ad_group_memberships"] = None

            # Account control status
            user_account_control = ad_data.get("userAccountControl")
            if user_account_control:
                # Bit 2 (0x2) = Account disabled
                try:
                    uac_int = (
                        int(user_account_control)
                        if isinstance(user_account_control, str)
                        else user_account_control
                    )
                    silver_record["ad_account_disabled"] = bool(uac_int & 0x2)
                except (ValueError, TypeError):
                    silver_record["ad_account_disabled"] = None
            else:
                silver_record["ad_account_disabled"] = None

            # Last logon timestamp
            silver_record["ad_last_logon"] = ad_data.get("lastLogonTimestamp")
        else:
            silver_record["ad_object_guid"] = None
            silver_record["ad_sam_account_name"] = None
            silver_record["ad_group_memberships"] = None
            silver_record["ad_account_disabled"] = None
            silver_record["ad_last_logon"] = None

        # Set source entity ID (use most specific ID available)
        silver_record["source_entity_id"] = (
            silver_record.get("umich_empl_id")
            or silver_record.get("tdx_user_uid")
            or silver_record.get("ldap_uid_number")
            or uniqname
        )

        return silver_record

    def _calculate_data_quality(
        self,
        silver_record: Dict,
        tdx_data: Optional[Dict],
        mcom_data: Optional[Dict],
        umapi_data_list: List[Dict],
        ad_data: Optional[Dict],
    ) -> Tuple[float, List[str]]:
        """
        Calculate data quality score and identify quality flags.

        Scoring criteria:
        - Start at 1.0 (perfect)
        - Deduct 0.2 if missing TDX UID (critical for operations)
        - Deduct 0.1 if missing uniqname
        - Deduct 0.1 if missing UMAPI employment data
        - Deduct 0.1 if missing department information
        - Deduct 0.05 if missing contact email
        - Deduct 0.05 if missing job title
        - Deduct 0.05 if missing AD group memberships

        Args:
            silver_record: The merged silver record
            tdx_data: TDX bronze record
            mcom_data: MCommunity bronze record
            umapi_data_list: UMAPI bronze records
            ad_data: Active Directory bronze record

        Returns:
            Tuple of (quality_score, quality_flags_list)
        """
        score = 1.0
        flags = []

        # Critical: TDX UID is essential for operations
        if not silver_record.get("tdx_user_uid"):
            score -= 0.2
            flags.append("missing_tdx_uid")

        # Critical: Uniqname is the primary key
        if not silver_record.get("uniqname"):
            score -= 0.1
            flags.append("missing_uniqname")

        # Important: Employment data
        if not umapi_data_list:
            score -= 0.1
            flags.append("missing_umapi_data")

        # Important: Department affiliation
        if not silver_record.get("department_id") and not silver_record.get(
            "ou_department_ids"
        ):
            score -= 0.1
            flags.append("missing_department_info")

        # Contact information
        if not silver_record.get("primary_email"):
            score -= 0.05
            flags.append("missing_email")

        # Job title
        if not silver_record.get("job_title") and not silver_record.get(
            "tdx_job_title"
        ):
            score -= 0.05
            flags.append("missing_job_title")

        # AD group memberships
        if not silver_record.get("ad_group_memberships"):
            score -= 0.05
            flags.append("missing_ad_groups")

        # Check for disabled AD account
        if silver_record.get("ad_account_disabled"):
            flags.append("ad_account_disabled")

        # Check for inactive TDX status
        if not silver_record.get("is_active"):
            flags.append("tdx_inactive")

        # Check for name mismatches between sources
        names_to_compare = []
        if tdx_data and tdx_data.get("FullName"):
            names_to_compare.append(tdx_data["FullName"].lower())
        if umapi_data_list and umapi_data_list[0].get("Name"):
            names_to_compare.append(umapi_data_list[0]["Name"].lower())
        if mcom_data and mcom_data.get("displayName"):
            names_to_compare.append(mcom_data["displayName"].lower())

        if len(names_to_compare) > 1 and len(set(names_to_compare)) > 1:
            flags.append("name_mismatch_across_sources")

        # Ensure score doesn't go below 0
        score = max(0.0, score)

        return round(score, 2), flags

    def _calculate_entity_hash(self, silver_record: Dict) -> str:
        """
        Calculate content hash for the merged silver record.

        Args:
            silver_record: The silver record dictionary

        Returns:
            SHA-256 hash string
        """
        # Include significant fields in hash
        significant_fields = {
            "uniqname": silver_record.get("uniqname"),
            "umich_empl_id": silver_record.get("umich_empl_id"),
            "tdx_user_uid": silver_record.get("tdx_user_uid"),
            "first_name": silver_record.get("first_name"),
            "last_name": silver_record.get("last_name"),
            "full_name": silver_record.get("full_name"),
            "primary_email": silver_record.get("primary_email"),
            "job_title": silver_record.get("job_title"),
            "department_job_titles": silver_record.get("department_job_titles"),
            "department_id": silver_record.get("department_id"),
            "department_ids": silver_record.get("department_ids"),
            "job_codes": silver_record.get("job_codes"),
            "supervisor_ids": silver_record.get("supervisor_ids"),
            "work_phone": silver_record.get("work_phone"),
            "is_active": silver_record.get("is_active"),
            "ad_group_count": len(silver_record.get("ad_group_memberships") or []),
            "ou_affiliations_count": len(
                silver_record.get("mcommunity_ou_affiliations") or []
            ),
        }

        # Create normalized JSON for hashing
        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        content_hash = hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

        return content_hash

    def _bulk_upsert_silver_records(
        self, silver_records: List[Dict], run_id: str
    ) -> Tuple[int, int]:
        """
        Bulk insert or update silver user records using executemany().

        OPTIMIZATION: Batch upsert multiple users in single transaction.
        This is 10-50x faster than individual upserts.

        Args:
            silver_records: List of silver record dictionaries
            run_id: The current transformation run ID

        Returns:
            Tuple of (records_created, records_updated)
        """
        if not silver_records:
            return 0, 0

        try:
            # Prepare data for bulk insert
            insert_data = []
            for silver_record in silver_records:
                insert_data.append(
                    {
                        "uniqname": silver_record["uniqname"],
                        "umich_empl_id": silver_record.get("umich_empl_id"),
                        "tdx_user_uid": silver_record.get("tdx_user_uid"),
                        "first_name": silver_record.get("first_name"),
                        "last_name": silver_record.get("last_name"),
                        "full_name": silver_record.get("full_name"),
                        "primary_email": silver_record.get("primary_email"),
                        "job_title": silver_record.get("job_title"),
                        "department_job_titles": self._json_dumps_or_none(
                            silver_record.get("department_job_titles")
                        ),
                        "department_id": silver_record.get("department_id"),
                        "department_ids": self._json_dumps_or_none(
                            silver_record.get("department_ids")
                        ),
                        "job_codes": self._json_dumps_or_none(
                            silver_record.get("job_codes")
                        ),
                        "supervisor_ids": self._json_dumps_or_none(
                            silver_record.get("supervisor_ids")
                        ),
                        "work_phone": silver_record.get("work_phone"),
                        "work_city": silver_record.get("work_city"),
                        "work_state": silver_record.get("work_state"),
                        "work_postal_code": silver_record.get("work_postal_code"),
                        "work_country": silver_record.get("work_country"),
                        "work_address_line1": silver_record.get("work_address_line1"),
                        "work_address_line2": silver_record.get("work_address_line2"),
                        "is_active": silver_record.get("is_active", True),
                        "tdx_job_title": silver_record.get("tdx_job_title"),
                        "mcommunity_ou_affiliations": self._json_dumps_or_none(
                            silver_record.get("mcommunity_ou_affiliations")
                        ),
                        "ou_department_ids": self._json_dumps_or_none(
                            silver_record.get("ou_department_ids")
                        ),
                        "ldap_uid_number": silver_record.get("ldap_uid_number"),
                        "ad_object_guid": silver_record.get("ad_object_guid"),
                        "ad_sam_account_name": silver_record.get("ad_sam_account_name"),
                        "ad_group_memberships": self._json_dumps_or_none(
                            silver_record.get("ad_group_memberships")
                        ),
                        "ad_account_disabled": silver_record.get("ad_account_disabled"),
                        "ad_last_logon": silver_record.get("ad_last_logon"),
                        "data_quality_score": silver_record.get("data_quality_score"),
                        "quality_flags": self._json_dumps_or_none(
                            silver_record.get("quality_flags", [])
                        ),
                        "source_system": silver_record["source_system"],
                        "source_entity_id": silver_record["source_entity_id"],
                        "entity_hash": silver_record["entity_hash"],
                        "ingestion_run_id": run_id,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    }
                )

            # Count existing records for statistics
            uniqnames = [r["uniqname"] for r in silver_records]
            existing_query = """
            SELECT uniqname FROM silver.users WHERE uniqname = ANY(:uniqnames)
            """
            existing_df = self.db_adapter.query_to_dataframe(
                existing_query, {"uniqnames": uniqnames}
            )
            existing_uniqnames = set(existing_df["uniqname"].tolist())

            records_created = len([u for u in uniqnames if u not in existing_uniqnames])
            records_updated = len([u for u in uniqnames if u in existing_uniqnames])

            # Bulk upsert using executemany
            upsert_query = text("""
                INSERT INTO silver.users (
                    uniqname, umich_empl_id, tdx_user_uid, first_name, last_name, full_name,
                    primary_email, job_title, department_job_titles, department_id, department_ids,
                    job_codes, supervisor_ids, work_phone, work_city, work_state, work_postal_code, work_country,
                    work_address_line1, work_address_line2, is_active, tdx_job_title,
                    mcommunity_ou_affiliations, ou_department_ids, ldap_uid_number,
                    ad_object_guid, ad_sam_account_name, ad_group_memberships, ad_account_disabled,
                    ad_last_logon, data_quality_score, quality_flags, source_system, source_entity_id,
                    entity_hash, ingestion_run_id, created_at, updated_at
                ) VALUES (
                    :uniqname, :umich_empl_id, :tdx_user_uid, :first_name, :last_name, :full_name,
                    :primary_email, :job_title, CAST(:department_job_titles AS jsonb), :department_id,
                    CAST(:department_ids AS jsonb), CAST(:job_codes AS jsonb), CAST(:supervisor_ids AS jsonb), :work_phone,
                    :work_city, :work_state, :work_postal_code, :work_country,
                    :work_address_line1, :work_address_line2, :is_active, :tdx_job_title,
                    CAST(:mcommunity_ou_affiliations AS jsonb), CAST(:ou_department_ids AS jsonb),
                    :ldap_uid_number, :ad_object_guid, :ad_sam_account_name,
                    CAST(:ad_group_memberships AS jsonb), :ad_account_disabled, :ad_last_logon,
                    :data_quality_score, CAST(:quality_flags AS jsonb), :source_system, :source_entity_id,
                    :entity_hash, :ingestion_run_id, :created_at, :updated_at
                )
                ON CONFLICT (uniqname) DO UPDATE SET
                    umich_empl_id = EXCLUDED.umich_empl_id,
                    tdx_user_uid = EXCLUDED.tdx_user_uid,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    full_name = EXCLUDED.full_name,
                    primary_email = EXCLUDED.primary_email,
                    job_title = EXCLUDED.job_title,
                    department_job_titles = EXCLUDED.department_job_titles,
                    department_id = EXCLUDED.department_id,
                    department_ids = EXCLUDED.department_ids,
                    job_codes = EXCLUDED.job_codes,
                    supervisor_ids = EXCLUDED.supervisor_ids,
                    work_phone = EXCLUDED.work_phone,
                    work_city = EXCLUDED.work_city,
                    work_state = EXCLUDED.work_state,
                    work_postal_code = EXCLUDED.work_postal_code,
                    work_country = EXCLUDED.work_country,
                    work_address_line1 = EXCLUDED.work_address_line1,
                    work_address_line2 = EXCLUDED.work_address_line2,
                    is_active = EXCLUDED.is_active,
                    tdx_job_title = EXCLUDED.tdx_job_title,
                    mcommunity_ou_affiliations = EXCLUDED.mcommunity_ou_affiliations,
                    ou_department_ids = EXCLUDED.ou_department_ids,
                    ldap_uid_number = EXCLUDED.ldap_uid_number,
                    ad_object_guid = EXCLUDED.ad_object_guid,
                    ad_sam_account_name = EXCLUDED.ad_sam_account_name,
                    ad_group_memberships = EXCLUDED.ad_group_memberships,
                    ad_account_disabled = EXCLUDED.ad_account_disabled,
                    ad_last_logon = EXCLUDED.ad_last_logon,
                    data_quality_score = EXCLUDED.data_quality_score,
                    quality_flags = EXCLUDED.quality_flags,
                    source_system = EXCLUDED.source_system,
                    source_entity_id = EXCLUDED.source_entity_id,
                    entity_hash = EXCLUDED.entity_hash,
                    ingestion_run_id = EXCLUDED.ingestion_run_id,
                    updated_at = EXCLUDED.updated_at
            """)

            with self.db_adapter.engine.connect() as conn:
                # Execute bulk insert
                conn.execute(upsert_query, insert_data)
                conn.commit()

            logger.info(
                f"Bulk upserted {len(silver_records)} users ({records_created} created, {records_updated} updated)"
            )
            return records_created, records_updated

        except SQLAlchemyError as e:
            logger.error(
                f"Failed to bulk upsert {len(silver_records)} silver records: {e}"
            )
            raise

    def _upsert_silver_record(self, silver_record: Dict, run_id: str):
        """
        Insert or update a silver user record (single record - kept for compatibility).

        Uses PostgreSQL UPSERT (INSERT ... ON CONFLICT) to handle both
        new users and updates to existing ones.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
        """
        try:
            with self.db_adapter.engine.connect() as conn:
                upsert_query = text("""
                    INSERT INTO silver.users (
                        uniqname, umich_empl_id, tdx_user_uid, first_name, last_name, full_name,
                        primary_email, job_title, department_job_titles, department_id, department_ids,
                        job_codes, supervisor_ids, work_phone, work_city, work_state, work_postal_code, work_country,
                        work_address_line1, work_address_line2, is_active, tdx_job_title,
                        mcommunity_ou_affiliations, ou_department_ids, ldap_uid_number,
                        ad_object_guid, ad_sam_account_name, ad_group_memberships, ad_account_disabled,
                        ad_last_logon, data_quality_score, quality_flags, source_system, source_entity_id,
                        entity_hash, ingestion_run_id, created_at, updated_at
                    ) VALUES (
                        :uniqname, :umich_empl_id, :tdx_user_uid, :first_name, :last_name, :full_name,
                        :primary_email, :job_title, CAST(:department_job_titles AS jsonb), :department_id,
                        CAST(:department_ids AS jsonb), CAST(:job_codes AS jsonb), CAST(:supervisor_ids AS jsonb), :work_phone,
                        :work_city, :work_state, :work_postal_code, :work_country,
                        :work_address_line1, :work_address_line2, :is_active, :tdx_job_title,
                        CAST(:mcommunity_ou_affiliations AS jsonb), CAST(:ou_department_ids AS jsonb),
                        :ldap_uid_number, :ad_object_guid, :ad_sam_account_name,
                        CAST(:ad_group_memberships AS jsonb), :ad_account_disabled, :ad_last_logon,
                        :data_quality_score, CAST(:quality_flags AS jsonb), :source_system, :source_entity_id,
                        :entity_hash, :ingestion_run_id, :created_at, :updated_at
                    )
                    ON CONFLICT (uniqname) DO UPDATE SET
                        umich_empl_id = EXCLUDED.umich_empl_id,
                        tdx_user_uid = EXCLUDED.tdx_user_uid,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        full_name = EXCLUDED.full_name,
                        primary_email = EXCLUDED.primary_email,
                        job_title = EXCLUDED.job_title,
                        department_job_titles = EXCLUDED.department_job_titles,
                        department_id = EXCLUDED.department_id,
                        department_ids = EXCLUDED.department_ids,
                        job_codes = EXCLUDED.job_codes,
                        supervisor_ids = EXCLUDED.supervisor_ids,
                        work_phone = EXCLUDED.work_phone,
                        work_city = EXCLUDED.work_city,
                        work_state = EXCLUDED.work_state,
                        work_postal_code = EXCLUDED.work_postal_code,
                        work_country = EXCLUDED.work_country,
                        work_address_line1 = EXCLUDED.work_address_line1,
                        work_address_line2 = EXCLUDED.work_address_line2,
                        is_active = EXCLUDED.is_active,
                        tdx_job_title = EXCLUDED.tdx_job_title,
                        mcommunity_ou_affiliations = EXCLUDED.mcommunity_ou_affiliations,
                        ou_department_ids = EXCLUDED.ou_department_ids,
                        ldap_uid_number = EXCLUDED.ldap_uid_number,
                        ad_object_guid = EXCLUDED.ad_object_guid,
                        ad_sam_account_name = EXCLUDED.ad_sam_account_name,
                        ad_group_memberships = EXCLUDED.ad_group_memberships,
                        ad_account_disabled = EXCLUDED.ad_account_disabled,
                        ad_last_logon = EXCLUDED.ad_last_logon,
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
                        "uniqname": silver_record["uniqname"],
                        "umich_empl_id": silver_record.get("umich_empl_id"),
                        "tdx_user_uid": silver_record.get("tdx_user_uid"),
                        "first_name": silver_record.get("first_name"),
                        "last_name": silver_record.get("last_name"),
                        "full_name": silver_record.get("full_name"),
                        "primary_email": silver_record.get("primary_email"),
                        "job_title": silver_record.get("job_title"),
                        "department_job_titles": self._json_dumps_or_none(
                            silver_record.get("department_job_titles")
                        ),
                        "department_id": silver_record.get("department_id"),
                        "department_ids": self._json_dumps_or_none(
                            silver_record.get("department_ids")
                        ),
                        "job_codes": self._json_dumps_or_none(
                            silver_record.get("job_codes")
                        ),
                        "supervisor_ids": self._json_dumps_or_none(
                            silver_record.get("supervisor_ids")
                        ),
                        "work_phone": silver_record.get("work_phone"),
                        "work_city": silver_record.get("work_city"),
                        "work_state": silver_record.get("work_state"),
                        "work_postal_code": silver_record.get("work_postal_code"),
                        "work_country": silver_record.get("work_country"),
                        "work_address_line1": silver_record.get("work_address_line1"),
                        "work_address_line2": silver_record.get("work_address_line2"),
                        "is_active": silver_record.get("is_active", True),
                        "tdx_job_title": silver_record.get("tdx_job_title"),
                        "mcommunity_ou_affiliations": self._json_dumps_or_none(
                            silver_record.get("mcommunity_ou_affiliations")
                        ),
                        "ou_department_ids": self._json_dumps_or_none(
                            silver_record.get("ou_department_ids")
                        ),
                        "ldap_uid_number": silver_record.get("ldap_uid_number"),
                        "ad_object_guid": silver_record.get("ad_object_guid"),
                        "ad_sam_account_name": silver_record.get("ad_sam_account_name"),
                        "ad_group_memberships": self._json_dumps_or_none(
                            silver_record.get("ad_group_memberships")
                        ),
                        "ad_account_disabled": silver_record.get("ad_account_disabled"),
                        "ad_last_logon": silver_record.get("ad_last_logon"),
                        "data_quality_score": silver_record.get("data_quality_score"),
                        "quality_flags": self._json_dumps_or_none(
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
                f"Failed to upsert silver record for user {silver_record['uniqname']}: {e}"
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
                "entity_type": "user",
                "incremental_since": incremental_since.isoformat()
                if incremental_since
                else None,
                "merge_sources": [
                    "active_directory",
                    "mcommunity_ldap",
                    "tdx",
                    "umich_api",
                ],
            }

            with self.db_adapter.engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status, metadata
                    ) VALUES (
                        :run_id, 'silver_transformation', 'user', :started_at, 'running', :metadata
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
            records_processed: Total users processed
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

    def transform_users_incremental(self, full_sync: bool = False) -> Dict[str, Any]:
        """
        Main entry point: Transform bronze users to silver layer incrementally or full sync.

        Process flow:
        1. Determine last successful transformation timestamp (skipped if full_sync=True)
        2. Find users with bronze records newer than that timestamp (or all TDX users if full_sync=True)
        3. For each user:
           a. Fetch latest bronze records from all sources
           b. Merge into unified silver record
           c. Calculate entity hash and compare to existing (skip if unchanged)
           d. Match OU data to departments
           e. Calculate data quality metrics
           f. Upsert to silver.users
        4. Track statistics and return results

        Args:
            full_sync: If True, process ALL TDX users and use hash comparison to detect changes

        Returns:
            Dictionary with transformation statistics
        """
        # Get timestamp of last successful transformation (ignored if full_sync)
        last_transformation = (
            None if full_sync else self._get_last_transformation_timestamp()
        )

        # Create transformation run
        run_id = self.create_transformation_run(last_transformation)

        stats = {
            "run_id": run_id,
            "incremental_since": last_transformation,
            "full_sync": full_sync,
            "users_processed": 0,
            "users_skipped_unchanged": 0,
            "records_created": 0,
            "records_updated": 0,
            "source_distribution": {
                "all_four_sources": 0,
                "three_sources": 0,
                "two_sources": 0,
                "single_source": 0,
            },
            "quality_issues": [],
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(f"Starting {sync_mode} silver transformation for users...")

            # Find users needing transformation
            uniqnames = self._get_users_needing_transformation(
                last_transformation, full_sync=full_sync
            )

            if not uniqnames:
                logger.info("No users need transformation - all up to date")
                self.complete_transformation_run(run_id, 0, 0, 0)
                return stats

            logger.info(f"Processing {len(uniqnames)} users in batches of 5000")

            # Process users in batches to balance memory vs speed
            uniqname_list = list(uniqnames)
            batch_size = 5000
            total_batches = (len(uniqname_list) + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                batch_start = batch_num * batch_size
                batch_end = min((batch_num + 1) * batch_size, len(uniqname_list))
                batch_uniqnames = set(uniqname_list[batch_start:batch_end])

                logger.info(
                    f"Processing batch {batch_num + 1}/{total_batches} "
                    f"({len(batch_uniqnames)} users)..."
                )

                # OPTIMIZATION: Fetch bronze records for this batch (4 queries per batch)
                all_bronze_records = self._fetch_all_bronze_records_batch(
                    batch_uniqnames
                )

                # OPTIMIZATION: Fetch existing silver hashes for hash-based change detection (full sync only)
                existing_hashes = {}
                if full_sync:
                    existing_hashes = self._fetch_existing_silver_hashes(
                        batch_uniqnames
                    )

                # Transform all users in this batch (in-memory)
                silver_records_batch = []

                for uniqname in batch_uniqnames:
                    try:
                        # Get pre-fetched bronze records from batch
                        tdx_data, mcom_data, umapi_data_list, ad_data = (
                            all_bronze_records.get(uniqname, (None, None, [], None))
                        )

                        # Skip if no data found at all
                        if not any([tdx_data, mcom_data, umapi_data_list, ad_data]):
                            logger.warning(f"No bronze data found for user {uniqname}")
                            stats["errors"].append(f"No bronze data for {uniqname}")
                            continue

                        # Track source distribution
                        source_count = sum(
                            [
                                bool(tdx_data),
                                bool(mcom_data),
                                bool(umapi_data_list),
                                bool(ad_data),
                            ]
                        )

                        if source_count == 4:
                            stats["source_distribution"]["all_four_sources"] += 1
                        elif source_count == 3:
                            stats["source_distribution"]["three_sources"] += 1
                        elif source_count == 2:
                            stats["source_distribution"]["two_sources"] += 1
                        else:
                            stats["source_distribution"]["single_source"] += 1

                        # Merge bronze records into silver
                        silver_record = self._merge_bronze_to_silver(
                            uniqname, tdx_data, mcom_data, umapi_data_list, ad_data
                        )

                        # Calculate data quality
                        quality_score, quality_flags = self._calculate_data_quality(
                            silver_record, tdx_data, mcom_data, umapi_data_list, ad_data
                        )
                        silver_record["data_quality_score"] = quality_score
                        silver_record["quality_flags"] = quality_flags

                        # Track quality issues
                        if quality_flags:
                            stats["quality_issues"].append(
                                {
                                    "uniqname": uniqname,
                                    "flags": quality_flags,
                                    "score": quality_score,
                                }
                            )

                        # Calculate entity hash
                        silver_record["entity_hash"] = self._calculate_entity_hash(
                            silver_record
                        )

                        # OPTIMIZATION: Skip if hash unchanged (full sync only)
                        if full_sync and uniqname in existing_hashes:
                            if (
                                existing_hashes[uniqname]
                                == silver_record["entity_hash"]
                            ):
                                stats["users_skipped_unchanged"] += 1
                                continue  # Skip this user - no changes detected

                        # Add to batch for bulk upsert
                        silver_records_batch.append(silver_record)
                        stats["users_processed"] += 1

                    except Exception as record_error:
                        error_msg = (
                            f"Failed to transform user {uniqname}: {record_error}"
                        )
                        logger.error(error_msg)
                        stats["errors"].append(error_msg)

                # OPTIMIZATION: Bulk upsert the entire batch at once
                if silver_records_batch:
                    logger.info(f"Bulk upserting {len(silver_records_batch)} users...")
                    try:
                        created, updated = self._bulk_upsert_silver_records(
                            silver_records_batch, run_id
                        )
                        stats["records_created"] += created
                        stats["records_updated"] += updated

                        logger.info(
                            f"Batch {batch_num + 1}/{total_batches} complete: "
                            f"{created} created, {updated} updated"
                        )
                    except Exception as batch_error:
                        error_msg = f"Failed to bulk upsert batch {batch_num + 1}: {batch_error}"
                        logger.error(error_msg)
                        stats["errors"].append(error_msg)

                        # Fall back to individual upserts for this batch
                        logger.warning(
                            f"Falling back to individual upserts for batch {batch_num + 1}"
                        )
                        for silver_record in silver_records_batch:
                            try:
                                self._upsert_silver_record(silver_record, run_id)
                                stats["records_created"] += 1  # Approximation
                            except Exception as individual_error:
                                logger.error(
                                    f"Failed individual upsert: {individual_error}"
                                )

            # Complete the transformation run
            error_summary = None
            if stats["errors"]:
                error_summary = f"{len(stats['errors'])} users failed to transform"

            self.complete_transformation_run(
                run_id=run_id,
                records_processed=stats["users_processed"],
                records_created=stats["records_created"],
                records_updated=stats["records_updated"],
                error_message=error_summary,
            )

            stats["completed_at"] = datetime.now(timezone.utc)
            duration = (stats["completed_at"] - stats["started_at"]).total_seconds()

            # Log comprehensive results
            logger.info(f"Silver transformation completed in {duration:.2f} seconds")
            logger.info(f"📊 Results Summary:")
            logger.info(f"   Mode: {'Full Sync' if full_sync else 'Incremental'}")
            logger.info(f"   Users Processed: {stats['users_processed']}")
            if full_sync:
                logger.info(
                    f"   Users Skipped (Unchanged): {stats['users_skipped_unchanged']}"
                )
            logger.info(f"   ├─ New Records Created: {stats['records_created']}")
            logger.info(f"   └─ Existing Records Updated: {stats['records_updated']}")
            logger.info(f"   Source Distribution:")
            logger.info(
                f"   ├─ All 4 Sources: {stats['source_distribution']['all_four_sources']}"
            )
            logger.info(
                f"   ├─ 3 Sources: {stats['source_distribution']['three_sources']}"
            )
            logger.info(
                f"   ├─ 2 Sources: {stats['source_distribution']['two_sources']}"
            )
            logger.info(
                f"   └─ Single Source: {stats['source_distribution']['single_source']}"
            )
            logger.info(f"   Quality Issues: {len(stats['quality_issues'])}")
            logger.info(f"   Errors: {len(stats['errors'])}")

            return stats

        except Exception as e:
            error_msg = f"Silver transformation failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            self.complete_transformation_run(
                run_id=run_id,
                records_processed=stats["users_processed"],
                records_created=stats["records_created"],
                records_updated=stats["records_updated"],
                error_message=error_msg,
            )

            raise

    def get_transformation_summary(self) -> pd.DataFrame:
        """
        Get a summary of silver user records with quality metrics.

        Returns:
            DataFrame with user summaries
        """
        try:
            query = """
            SELECT
                uniqname,
                full_name,
                primary_email,
                department_id,
                job_title,
                is_active,
                source_system,
                data_quality_score,
                quality_flags,
                CASE
                    WHEN ad_group_memberships IS NULL THEN 0
                    WHEN jsonb_typeof(ad_group_memberships) = 'array' THEN jsonb_array_length(ad_group_memberships)
                    ELSE 0
                END as ad_group_count,
                updated_at
            FROM silver.users
            ORDER BY uniqname
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

    Usage:
        python scripts/database/transform_silver_users.py [--full-sync]

    Options:
        --full-sync    Process ALL TDX users and use hash comparison to detect changes
                       (default: incremental mode - only process users with new bronze records)
    """
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Transform bronze user records into silver layer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Incremental sync (default - only new/updated bronze records)
  python scripts/database/transform_silver_users.py

  # Full sync (all TDX users, skip unchanged via hash comparison)
  python scripts/database/transform_silver_users.py --full-sync
        """,
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all TDX users (not just new bronze records). Uses hash comparison to skip unchanged records.",
    )

    args = parser.parse_args()

    try:
        # Load environment variables
        load_dotenv()

        database_url = os.getenv("DATABASE_URL")

        if not database_url:
            raise ValueError("DATABASE_URL environment variable is required")

        # Create transformation service
        transformation_service = UserSilverTransformationService(database_url)

        # Run transformation (incremental or full sync)
        sync_mode = "full sync" if args.full_sync else "incremental"
        print(f"🔄 Starting user silver transformation ({sync_mode} mode)...")
        results = transformation_service.transform_users_incremental(
            full_sync=args.full_sync
        )

        # Display results
        print(f"\n📊 Silver Transformation Summary:")
        print(f"   Run ID: {results['run_id']}")
        print(f"   Mode: {'Full Sync' if results['full_sync'] else 'Incremental'}")
        if not results["full_sync"]:
            print(
                f"   Incremental Since: {results['incremental_since'] or 'First Run'}"
            )
        print(f"   Users Processed: {results['users_processed']}")
        if results["full_sync"]:
            print(f"   Users Skipped (Unchanged): {results['users_skipped_unchanged']}")
        print(f"   ├─ New Records Created: {results['records_created']}")
        print(f"   └─ Existing Records Updated: {results['records_updated']}")
        print(f"\n   Source Distribution:")
        print(
            f"   ├─ All 4 Sources: {results['source_distribution']['all_four_sources']}"
        )
        print(f"   ├─ 3 Sources: {results['source_distribution']['three_sources']}")
        print(f"   ├─ 2 Sources: {results['source_distribution']['two_sources']}")
        print(f"   └─ Single Source: {results['source_distribution']['single_source']}")
        print(f"\n   Data Quality:")
        print(f"   ├─ Users with Issues: {len(results['quality_issues'])}")
        print(f"   └─ Errors: {len(results['errors'])}")

        # Show quality issues if any
        if results["quality_issues"]:
            print(f"\n⚠️  Quality Issues Detected (showing first 10):")
            for issue in results["quality_issues"][:10]:
                print(
                    f"   - {issue['uniqname']}: {', '.join(issue['flags'])} (score: {issue['score']})"
                )

        # Show summary of silver records
        print("\n📋 Getting summary of silver records...")
        summary_df = transformation_service.get_transformation_summary()
        print(f"   Total silver user records: {len(summary_df)}")

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
                    print(f"   ├─ {label} ({low}-{high}): {count} users")

        # Clean up
        transformation_service.close()

        print("\n✅ Silver transformation completed successfully!")

    except Exception as e:
        logger.error(f"Silver transformation failed: {e}", exc_info=True)
        print(f"❌ Transformation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
