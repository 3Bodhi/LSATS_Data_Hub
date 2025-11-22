#!/usr/bin/env python3
"""
TDX Assets Source-Specific Silver Layer Transformation Service

This service transforms bronze TeamDynamix asset records into the source-specific
silver.tdx_assets table. This is TIER 1 of the two-tier silver architecture.

Key features:
- Extracts all TDX asset fields from JSONB to typed columns
- Extracts critical Attributes (MAC, IP, OS, etc.) to typed columns for cross-system matching
- Preserves complete Attributes array in JSONB for audit and flexibility
- Content hash-based change detection
- Incremental processing (only transform assets with new bronze data)
- Comprehensive logging with emoji standards
- Dry-run mode for validation
- Standard service class pattern following medallion architecture

The extracted attribute columns enable efficient joins with KeyClient, AD, and other
computer sources during consolidated silver.computers transformation.
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

import dateutil.parser
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
log_dir = "logs"
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


class TDXAssetTransformationService:
    """
    Service for transforming bronze TDX asset records into source-specific silver layer.

    This service creates silver.tdx_assets records from bronze.raw_entities where:
    - entity_type = 'asset'
    - source_system = 'tdx'

    Transformation Logic:
    - Extract all TDX asset fields from JSONB to typed columns
    - Extract critical Attributes to typed columns for cross-system matching:
      * MAC Address, IP Address, Operating System (for computer matching)
      * Last Inventoried Date, Function, Support Groups (for compliance)
      * Memory, Storage, Processors (for hardware specs)
    - Preserve complete Attributes array in JSONB
    - Calculate entity_hash for change detection
    - Track raw_id for traceability back to bronze

    This is TIER 1 (source-specific). Future consolidated tier 2 will merge
    tdx_assets + key_client_computers + ad_computers into silver.computers.
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
        logger.info("🔌 TDX assets silver transformation service initialized")

    def _get_last_transformation_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of last successful TDX assets transformation.

        Returns:
            Timestamp of last completed run, or None if this is the first run
        """
        try:
            query = """
            SELECT MAX(completed_at) as last_completed
            FROM meta.ingestion_runs
            WHERE source_system = 'silver_transformation'
              AND entity_type = 'tdx_asset'
              AND status = 'completed'
            """

            result_df = self.db_adapter.query_to_dataframe(query)

            if not result_df.empty and result_df.iloc[0]["last_completed"] is not None:
                last_timestamp = result_df.iloc[0]["last_completed"]
                logger.info(f"📅 Last successful transformation: {last_timestamp}")
                return last_timestamp
            else:
                logger.info(
                    "🆕 No previous transformation found - processing all assets"
                )
                return None

        except SQLAlchemyError as e:
            logger.warning(f"⚠️  Could not determine last transformation timestamp: {e}")
            return None

    def _get_assets_needing_transformation(
        self, since_timestamp: Optional[datetime] = None, full_sync: bool = False
    ) -> Set[int]:
        """
        Find TDX asset IDs that have new/updated bronze records.

        Args:
            since_timestamp: Only include assets with bronze records after this time
            full_sync: If True, return ALL TDX assets regardless of timestamp

        Returns:
            Set of TDX asset IDs (integers) that need transformation
        """
        try:
            time_filter = ""
            params = {}

            if not full_sync and since_timestamp:
                time_filter = "AND ingested_at > :since_timestamp"
                params["since_timestamp"] = since_timestamp

            query = f"""
            SELECT DISTINCT
                (raw_data->>'ID')::int as tdx_asset_id
            FROM bronze.raw_entities
            WHERE entity_type = 'asset'
              AND source_system = 'tdx'
              {time_filter}
              AND raw_data->>'ID' IS NOT NULL
            """

            result_df = self.db_adapter.query_to_dataframe(query, params)
            asset_ids = set(result_df["tdx_asset_id"].tolist())

            sync_mode = "full sync" if full_sync else "incremental"
            logger.info(
                f"🔍 Found {len(asset_ids)} TDX assets needing transformation ({sync_mode} mode)"
            )
            return asset_ids

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get assets needing transformation: {e}")
            raise

    def _fetch_latest_bronze_record(
        self, tdx_asset_id: int
    ) -> Optional[Tuple[Dict, str]]:
        """
        Fetch the latest bronze record for a TDX asset.

        Args:
            tdx_asset_id: The TDX asset ID (integer)

        Returns:
            Tuple of (raw_data dict, raw_id UUID) or None if not found
        """
        try:
            query = """
            SELECT raw_data, raw_id
            FROM bronze.raw_entities
            WHERE entity_type = 'asset'
              AND source_system = 'tdx'
              AND (raw_data->>'ID')::int = :tdx_asset_id
            ORDER BY ingested_at DESC
            LIMIT 1
            """

            result_df = self.db_adapter.query_to_dataframe(
                query, {"tdx_asset_id": tdx_asset_id}
            )

            if result_df.empty:
                return None

            return result_df.iloc[0]["raw_data"], result_df.iloc[0]["raw_id"]

        except SQLAlchemyError as e:
            logger.error(
                f"❌ Failed to fetch bronze record for asset ID {tdx_asset_id}: {e}"
            )
            raise

    def _calculate_content_hash(self, raw_data: Dict[str, Any]) -> str:
        """
        Calculate SHA-256 content hash for change detection.

        Only includes significant fields (not metadata like timestamps).

        Args:
            raw_data: Raw asset data from bronze layer

        Returns:
            SHA-256 hash string
        """
        # Include only significant fields for change detection
        significant_fields = {
            "ID": raw_data.get("ID"),
            "Tag": raw_data.get("Tag"),
            "Name": raw_data.get("Name"),
            "SerialNumber": raw_data.get("SerialNumber"),
            "StatusID": raw_data.get("StatusID"),
            "FormID": raw_data.get("FormID"),
            "LocationID": raw_data.get("LocationID"),
            "OwningCustomerID": raw_data.get("OwningCustomerID"),
            "OwningDepartmentID": raw_data.get("OwningDepartmentID"),
            "ConfigurationItemID": raw_data.get("ConfigurationItemID"),
            "Attributes": raw_data.get("Attributes", []),
            "ManufacturerID": raw_data.get("ManufacturerID"),
            "ProductModelID": raw_data.get("ProductModelID"),
            "PurchaseCost": raw_data.get("PurchaseCost"),
        }

        normalized_json = json.dumps(
            significant_fields, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(normalized_json.encode("utf-8")).hexdigest()

    def _parse_tdx_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Parse TeamDynamix timestamp strings into Python datetime objects.

        Args:
            timestamp_str: ISO format timestamp (e.g., "2023-11-14T17:30:05.623Z")

        Returns:
            datetime object with timezone, or None if parsing fails
        """
        if not timestamp_str:
            return None

        try:
            # Handle special case for default/zero dates
            if timestamp_str.startswith("0001-01-01"):
                return None

            parsed_dt = dateutil.parser.isoparse(timestamp_str)
            if parsed_dt.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
            return parsed_dt
        except (ValueError, TypeError) as e:
            logger.debug(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None

    def _extract_attribute_value(
        self, attributes: List[Dict], attribute_name: str, field: str = "Value"
    ) -> Optional[str]:
        """
        Extract a specific attribute value from the TDX Attributes array.

        Args:
            attributes: List of attribute dictionaries from TDX
            attribute_name: Name of the attribute to find (e.g., "MAC Address(es)")
            field: Which field to extract ("Value", "ValueText", "ID", etc.)

        Returns:
            The attribute value as string, or None if not found
        """
        if not attributes:
            return None

        for attr in attributes:
            if attr.get("Name") == attribute_name:
                value = attr.get(field)
                # Return None for empty strings
                if value == "" or value == "None":
                    return None
                return str(value) if value is not None else None

        return None

    def _extract_multiselect_attribute(
        self, attributes: List[Dict], attribute_name: str
    ) -> Tuple[Optional[List], Optional[str]]:
        """
        Extract a multiselect attribute (returns both ID array and text value).

        Args:
            attributes: List of attribute dictionaries from TDX
            attribute_name: Name of the multiselect attribute (e.g., "Support Group(s)")

        Returns:
            Tuple of (list of choice IDs, comma-separated text values)
        """
        if not attributes:
            return None, None

        for attr in attributes:
            if attr.get("Name") == attribute_name:
                # For multiselect, Value could be a single ID or comma-separated IDs
                value = attr.get("Value")
                value_text = attr.get("ValueText")

                if not value:
                    return None, None

                # Parse Value field (could be "2364" or "2364,5176")
                try:
                    if isinstance(value, str) and "," in value:
                        ids = [int(x.strip()) for x in value.split(",") if x.strip()]
                    else:
                        ids = [int(value)]
                except (ValueError, TypeError):
                    ids = None

                return ids, value_text if value_text else None

        return None, None

    def _extract_tdx_fields(
        self, raw_data: Dict[str, Any], raw_id: str
    ) -> Dict[str, Any]:
        """
        Extract and type-cast TDX asset fields from bronze JSONB to silver columns.

        This includes:
        1. All standard TDX asset fields
        2. Critical attributes extracted to typed columns
        3. Complete Attributes array preserved in JSONB

        Args:
            raw_data: Raw JSONB data from bronze.raw_entities
            raw_id: UUID of the bronze record

        Returns:
            Dictionary with all silver.tdx_assets columns
        """

        # Helper to safely convert to UUID or return None
        def to_uuid(val):
            if val is None or val == "00000000-0000-0000-0000-000000000000":
                return None
            try:
                return str(uuid.UUID(str(val)))
            except (ValueError, AttributeError):
                return None

        # Helper to safely convert to int
        def to_int(val):
            if val is None or val == 0:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        # Helper to safely convert to decimal
        def to_decimal(val):
            if val is None or val == 0.0:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        # Extract Attributes array
        attributes = raw_data.get("Attributes", [])

        # Extract multiselect attributes
        support_groups_ids, support_groups_text = self._extract_multiselect_attribute(
            attributes, "Support Group(s)"
        )

        silver_record = {
            # Primary identifier
            "tdx_asset_id": raw_data.get("ID"),
            # Core identity fields
            "tag": raw_data.get("Tag"),
            "name": raw_data.get("Name"),
            "uri": raw_data.get("Uri"),
            "external_id": raw_data.get("ExternalID") or None,
            "serial_number": raw_data.get("SerialNumber")
            if raw_data.get("SerialNumber") not in ["", "None"]
            else None,
            # Form/Type classification
            "app_id": raw_data.get("AppID"),
            "app_name": raw_data.get("AppName"),
            "form_id": raw_data.get("FormID"),
            "form_name": raw_data.get("FormName"),
            "status_id": raw_data.get("StatusID"),
            "status_name": raw_data.get("StatusName"),
            # Hierarchy (rare - only 1.6% populated)
            "parent_id": to_int(raw_data.get("ParentID")),
            "parent_tag": raw_data.get("ParentTag")
            if raw_data.get("ParentTag") not in ["", "None"]
            else None,
            "parent_name": raw_data.get("ParentName")
            if raw_data.get("ParentName") not in ["", "None"]
            else None,
            "parent_serial_number": raw_data.get("ParentSerialNumber")
            if raw_data.get("ParentSerialNumber") not in ["", "None"]
            else None,
            # Configuration Item relationship
            "configuration_item_id": raw_data.get("ConfigurationItemID"),
            # Location information
            "location_id": to_int(raw_data.get("LocationID")),
            "location_name": raw_data.get("LocationName"),
            "location_room_id": to_int(raw_data.get("LocationRoomID")),
            "location_room_name": raw_data.get("LocationRoomName")
            if raw_data.get("LocationRoomName") not in ["", "None"]
            else None,
            # Ownership and responsibility
            "owning_customer_id": to_uuid(raw_data.get("OwningCustomerID")),
            "owning_customer_name": raw_data.get("OwningCustomerName"),
            "owning_department_id": to_int(raw_data.get("OwningDepartmentID")),
            "owning_department_name": raw_data.get("OwningDepartmentName"),
            # Requesting party
            "requesting_customer_id": to_uuid(raw_data.get("RequestingCustomerID")),
            "requesting_customer_name": raw_data.get("RequestingCustomerName")
            if raw_data.get("RequestingCustomerName") not in ["", "None"]
            else None,
            "requesting_department_id": to_int(raw_data.get("RequestingDepartmentID")),
            "requesting_department_name": raw_data.get("RequestingDepartmentName")
            if raw_data.get("RequestingDepartmentName") not in ["", "None"]
            else None,
            # Financial information
            "purchase_cost": to_decimal(raw_data.get("PurchaseCost")),
            "acquisition_date": self._parse_tdx_timestamp(
                raw_data.get("AcquisitionDate")
            ),
            "expected_replacement_date": self._parse_tdx_timestamp(
                raw_data.get("ExpectedReplacementDate")
            ),
            # Manufacturer and model
            "manufacturer_id": to_int(raw_data.get("ManufacturerID")),
            "manufacturer_name": raw_data.get("ManufacturerName")
            if raw_data.get("ManufacturerName") not in ["", "None"]
            else None,
            "product_model_id": to_int(raw_data.get("ProductModelID")),
            "product_model_name": raw_data.get("ProductModelName")
            if raw_data.get("ProductModelName") not in ["", "None"]
            else None,
            "supplier_id": to_int(raw_data.get("SupplierID")),
            "supplier_name": raw_data.get("SupplierName")
            if raw_data.get("SupplierName") not in ["", "None"]
            else None,
            # Maintenance
            "maintenance_schedule_id": to_int(raw_data.get("MaintenanceScheduleID")),
            "maintenance_schedule_name": raw_data.get("MaintenanceScheduleName")
            if raw_data.get("MaintenanceScheduleName") not in ["", "None"]
            else None,
            # External source integration
            "external_source_id": to_int(raw_data.get("ExternalSourceID")),
            "external_source_name": raw_data.get("ExternalSourceName")
            if raw_data.get("ExternalSourceName") not in ["", "None"]
            else None,
            # Audit fields
            "created_uid": to_uuid(raw_data.get("CreatedUid")),
            "created_full_name": raw_data.get("CreatedFullName"),
            "created_date": self._parse_tdx_timestamp(raw_data.get("CreatedDate")),
            "modified_uid": to_uuid(raw_data.get("ModifiedUid")),
            "modified_full_name": raw_data.get("ModifiedFullName"),
            "modified_date": self._parse_tdx_timestamp(raw_data.get("ModifiedDate")),
            # ============================================
            # EXTRACTED ATTRIBUTES (for matching & queries)
            # ============================================
            # TIER 1: Critical for cross-system matching
            "attr_mac_address": self._extract_attribute_value(
                attributes, "MAC Address(es)"
            ),
            "attr_ip_address": self._extract_attribute_value(
                attributes, "Reserved IP Address(es)"
            ),
            "attr_operating_system_id": to_int(
                self._extract_attribute_value(attributes, "Operating System", "Value")
            ),
            "attr_operating_system_name": self._extract_attribute_value(
                attributes, "Operating System", "ValueText"
            ),
            # TIER 2: High usage & compliance/reporting
            "attr_last_inventoried_date": self._parse_tdx_timestamp(
                self._extract_attribute_value(attributes, "Last Inventoried Date")
            ),
            "attr_purchase_shortcode": self._extract_attribute_value(
                attributes, "Purchase Shortcode"
            ),
            "attr_function_id": to_int(
                self._extract_attribute_value(attributes, "Function", "Value")
            ),
            "attr_function_name": self._extract_attribute_value(
                attributes, "Function", "ValueText"
            ),
            "attr_financial_owner_uid": to_uuid(
                self._extract_attribute_value(
                    attributes, "Financial Owner/Responsible", "Value"
                )
            ),
            "attr_financial_owner_name": self._extract_attribute_value(
                attributes, "Financial Owner/Responsible", "ValueText"
            ),
            "attr_support_groups_ids": support_groups_ids,
            "attr_support_groups_text": support_groups_text,
            "attr_memory": self._extract_attribute_value(attributes, "Memory"),
            "attr_storage": self._extract_attribute_value(attributes, "Storage"),
            "attr_processor_count": self._extract_attribute_value(
                attributes, "Processor(s)"
            ),
            # Complete attributes array (includes ALL attributes)
            "attributes": attributes,
            "attachments": raw_data.get("Attachments", []),
            # Traceability
            "raw_id": raw_id,
            "raw_data_snapshot": None,  # Optional: set to raw_data for full audit
            # Standard metadata
            "source_system": "tdx",
            "entity_hash": self._calculate_content_hash(raw_data),
        }

        return silver_record

    def _upsert_silver_record(
        self, silver_record: Dict[str, Any], run_id: str, dry_run: bool = False
    ) -> str:
        """
        Insert or update a silver.tdx_assets record.

        Uses PostgreSQL UPSERT (INSERT ... ON CONFLICT) to handle both
        new assets and updates to existing ones.

        Args:
            silver_record: The silver record to upsert
            run_id: The current transformation run ID
            dry_run: If True, log what would be done but don't commit

        Returns:
            Action taken: 'created', 'updated', or 'skipped'
        """
        tdx_asset_id = silver_record["tdx_asset_id"]

        if dry_run:
            logger.info(
                f"[DRY RUN] Would upsert asset: ID={tdx_asset_id}, "
                f"name={silver_record.get('name')}, tag={silver_record.get('tag')}"
            )
            return "dry_run"

        try:
            # Check if exists and compare hash
            check_query = """
            SELECT entity_hash
            FROM silver.tdx_assets
            WHERE tdx_asset_id = :tdx_asset_id
            """
            existing_df = self.db_adapter.query_to_dataframe(
                check_query, {"tdx_asset_id": tdx_asset_id}
            )

            is_new = existing_df.empty
            existing_hash = None if is_new else existing_df.iloc[0]["entity_hash"]

            # Skip if unchanged
            if not is_new and existing_hash == silver_record["entity_hash"]:
                logger.debug(f"⏭️  Asset unchanged, skipping: {tdx_asset_id}")
                return "skipped"

            with self.db_adapter.engine.connect() as conn:
                upsert_query = text("""
                    INSERT INTO silver.tdx_assets (
                        tdx_asset_id, tag, name, uri, external_id, serial_number,
                        app_id, app_name, form_id, form_name, status_id, status_name,
                        parent_id, parent_tag, parent_name, parent_serial_number,
                        configuration_item_id,
                        location_id, location_name, location_room_id, location_room_name,
                        owning_customer_id, owning_customer_name,
                        owning_department_id, owning_department_name,
                        requesting_customer_id, requesting_customer_name,
                        requesting_department_id, requesting_department_name,
                        purchase_cost, acquisition_date, expected_replacement_date,
                        manufacturer_id, manufacturer_name,
                        product_model_id, product_model_name,
                        supplier_id, supplier_name,
                        maintenance_schedule_id, maintenance_schedule_name,
                        external_source_id, external_source_name,
                        created_uid, created_full_name, created_date,
                        modified_uid, modified_full_name, modified_date,
                        attr_mac_address, attr_ip_address,
                        attr_operating_system_id, attr_operating_system_name,
                        attr_last_inventoried_date, attr_purchase_shortcode,
                        attr_function_id, attr_function_name,
                        attr_financial_owner_uid, attr_financial_owner_name,
                        attr_support_groups_ids, attr_support_groups_text,
                        attr_memory, attr_storage, attr_processor_count,
                        attributes, attachments,
                        raw_id, raw_data_snapshot, source_system, entity_hash,
                        ingestion_run_id, created_at, updated_at
                    ) VALUES (
                        :tdx_asset_id, :tag, :name, :uri, :external_id, :serial_number,
                        :app_id, :app_name, :form_id, :form_name, :status_id, :status_name,
                        :parent_id, :parent_tag, :parent_name, :parent_serial_number,
                        :configuration_item_id,
                        :location_id, :location_name, :location_room_id, :location_room_name,
                        :owning_customer_id, :owning_customer_name,
                        :owning_department_id, :owning_department_name,
                        :requesting_customer_id, :requesting_customer_name,
                        :requesting_department_id, :requesting_department_name,
                        :purchase_cost, :acquisition_date, :expected_replacement_date,
                        :manufacturer_id, :manufacturer_name,
                        :product_model_id, :product_model_name,
                        :supplier_id, :supplier_name,
                        :maintenance_schedule_id, :maintenance_schedule_name,
                        :external_source_id, :external_source_name,
                        :created_uid, :created_full_name, :created_date,
                        :modified_uid, :modified_full_name, :modified_date,
                        :attr_mac_address, :attr_ip_address,
                        :attr_operating_system_id, :attr_operating_system_name,
                        :attr_last_inventoried_date, :attr_purchase_shortcode,
                        :attr_function_id, :attr_function_name,
                        :attr_financial_owner_uid, :attr_financial_owner_name,
                        CAST(:attr_support_groups_ids AS jsonb), :attr_support_groups_text,
                        :attr_memory, :attr_storage, :attr_processor_count,
                        CAST(:attributes AS jsonb), CAST(:attachments AS jsonb),
                        :raw_id, CAST(:raw_data_snapshot AS jsonb),
                        :source_system, :entity_hash,
                        :ingestion_run_id, :created_at, :updated_at
                    )
                    ON CONFLICT (tdx_asset_id) DO UPDATE SET
                        tag = EXCLUDED.tag,
                        name = EXCLUDED.name,
                        uri = EXCLUDED.uri,
                        external_id = EXCLUDED.external_id,
                        serial_number = EXCLUDED.serial_number,
                        app_id = EXCLUDED.app_id,
                        app_name = EXCLUDED.app_name,
                        form_id = EXCLUDED.form_id,
                        form_name = EXCLUDED.form_name,
                        status_id = EXCLUDED.status_id,
                        status_name = EXCLUDED.status_name,
                        parent_id = EXCLUDED.parent_id,
                        parent_tag = EXCLUDED.parent_tag,
                        parent_name = EXCLUDED.parent_name,
                        parent_serial_number = EXCLUDED.parent_serial_number,
                        configuration_item_id = EXCLUDED.configuration_item_id,
                        location_id = EXCLUDED.location_id,
                        location_name = EXCLUDED.location_name,
                        location_room_id = EXCLUDED.location_room_id,
                        location_room_name = EXCLUDED.location_room_name,
                        owning_customer_id = EXCLUDED.owning_customer_id,
                        owning_customer_name = EXCLUDED.owning_customer_name,
                        owning_department_id = EXCLUDED.owning_department_id,
                        owning_department_name = EXCLUDED.owning_department_name,
                        requesting_customer_id = EXCLUDED.requesting_customer_id,
                        requesting_customer_name = EXCLUDED.requesting_customer_name,
                        requesting_department_id = EXCLUDED.requesting_department_id,
                        requesting_department_name = EXCLUDED.requesting_department_name,
                        purchase_cost = EXCLUDED.purchase_cost,
                        acquisition_date = EXCLUDED.acquisition_date,
                        expected_replacement_date = EXCLUDED.expected_replacement_date,
                        manufacturer_id = EXCLUDED.manufacturer_id,
                        manufacturer_name = EXCLUDED.manufacturer_name,
                        product_model_id = EXCLUDED.product_model_id,
                        product_model_name = EXCLUDED.product_model_name,
                        supplier_id = EXCLUDED.supplier_id,
                        supplier_name = EXCLUDED.supplier_name,
                        maintenance_schedule_id = EXCLUDED.maintenance_schedule_id,
                        maintenance_schedule_name = EXCLUDED.maintenance_schedule_name,
                        external_source_id = EXCLUDED.external_source_id,
                        external_source_name = EXCLUDED.external_source_name,
                        created_uid = EXCLUDED.created_uid,
                        created_full_name = EXCLUDED.created_full_name,
                        created_date = EXCLUDED.created_date,
                        modified_uid = EXCLUDED.modified_uid,
                        modified_full_name = EXCLUDED.modified_full_name,
                        modified_date = EXCLUDED.modified_date,
                        attr_mac_address = EXCLUDED.attr_mac_address,
                        attr_ip_address = EXCLUDED.attr_ip_address,
                        attr_operating_system_id = EXCLUDED.attr_operating_system_id,
                        attr_operating_system_name = EXCLUDED.attr_operating_system_name,
                        attr_last_inventoried_date = EXCLUDED.attr_last_inventoried_date,
                        attr_purchase_shortcode = EXCLUDED.attr_purchase_shortcode,
                        attr_function_id = EXCLUDED.attr_function_id,
                        attr_function_name = EXCLUDED.attr_function_name,
                        attr_financial_owner_uid = EXCLUDED.attr_financial_owner_uid,
                        attr_financial_owner_name = EXCLUDED.attr_financial_owner_name,
                        attr_support_groups_ids = EXCLUDED.attr_support_groups_ids,
                        attr_support_groups_text = EXCLUDED.attr_support_groups_text,
                        attr_memory = EXCLUDED.attr_memory,
                        attr_storage = EXCLUDED.attr_storage,
                        attr_processor_count = EXCLUDED.attr_processor_count,
                        attributes = EXCLUDED.attributes,
                        attachments = EXCLUDED.attachments,
                        raw_id = EXCLUDED.raw_id,
                        raw_data_snapshot = EXCLUDED.raw_data_snapshot,
                        entity_hash = EXCLUDED.entity_hash,
                        ingestion_run_id = EXCLUDED.ingestion_run_id,
                        updated_at = EXCLUDED.updated_at
                    WHERE silver.tdx_assets.entity_hash != EXCLUDED.entity_hash
                """)

                conn.execute(
                    upsert_query,
                    {
                        **silver_record,
                        # Convert JSONB fields to JSON strings
                        "attributes": json.dumps(silver_record.get("attributes", [])),
                        "attachments": json.dumps(silver_record.get("attachments", [])),
                        "attr_support_groups_ids": json.dumps(
                            silver_record.get("attr_support_groups_ids")
                        )
                        if silver_record.get("attr_support_groups_ids")
                        else None,
                        "raw_data_snapshot": json.dumps(
                            silver_record.get("raw_data_snapshot")
                        )
                        if silver_record.get("raw_data_snapshot")
                        else None,
                        "ingestion_run_id": run_id,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )

                conn.commit()

            action = "created" if is_new else "updated"
            logger.debug(
                f"✅ {action.capitalize()} asset: {tdx_asset_id} "
                f"(name: {silver_record.get('name')}, tag: {silver_record.get('tag')})"
            )
            return action

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to upsert asset {tdx_asset_id}: {e}")
            raise

    def create_transformation_run(
        self, incremental_since: Optional[datetime] = None, full_sync: bool = False
    ) -> str:
        """
        Create a transformation run record for tracking.

        Args:
            incremental_since: Timestamp for incremental processing
            full_sync: Whether this is a full sync

        Returns:
            Run ID (UUID string)
        """
        try:
            run_id = str(uuid.uuid4())

            metadata = {
                "transformation_type": "bronze_to_silver_tdx_assets",
                "entity_type": "tdx_asset",
                "source_table": "bronze.raw_entities",
                "target_table": "silver.tdx_assets",
                "tier": "source_specific",
                "full_sync": full_sync,
                "incremental_since": incremental_since.isoformat()
                if incremental_since
                else None,
                "extracted_attributes": [
                    "MAC Address(es)",
                    "Reserved IP Address(es)",
                    "Operating System",
                    "Last Inventoried Date",
                    "Purchase Shortcode",
                    "Function",
                    "Financial Owner/Responsible",
                    "Support Group(s)",
                    "Memory",
                    "Storage",
                    "Processor(s)",
                ],
            }

            with self.db_adapter.engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO meta.ingestion_runs (
                        run_id, source_system, entity_type, started_at, status, metadata
                    ) VALUES (
                        :run_id, 'silver_transformation', 'tdx_asset', :started_at, 'running', :metadata
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

            mode = "FULL SYNC" if full_sync else "INCREMENTAL"
            logger.info(f"📝 Created transformation run {run_id} ({mode})")
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
        records_skipped: int,
        error_message: Optional[str] = None,
    ):
        """
        Mark a transformation run as completed.

        Args:
            run_id: The run ID to complete
            records_processed: Total assets processed
            records_created: New silver records created
            records_updated: Existing silver records updated
            records_skipped: Records skipped (unchanged)
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
                        error_message = :error_message,
                        metadata = jsonb_set(
                            metadata,
                            '{records_skipped}',
                            to_jsonb(:records_skipped::int)
                        )
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
                        "records_skipped": records_skipped,
                        "error_message": error_message,
                    },
                )

                conn.commit()

            logger.info(f"✅ Completed transformation run {run_id}: {status}")

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to complete transformation run: {e}")

    def transform_incremental(
        self, full_sync: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Main entry point: Transform bronze TDX assets to silver.tdx_assets incrementally.

        Process flow:
        1. Determine last successful transformation timestamp (unless full_sync)
        2. Find TDX assets with bronze records newer than that timestamp
        3. For each asset:
           a. Fetch latest bronze record
           b. Extract fields to silver columns
           c. Extract critical Attributes to typed columns
           d. Calculate entity hash
           e. Upsert to silver.tdx_assets
        4. Track statistics and return results

        Args:
            full_sync: If True, process all assets regardless of timestamp
            dry_run: If True, preview changes without committing to database

        Returns:
            Dictionary with transformation statistics
        """
        # Get timestamp of last successful transformation
        last_transformation = (
            None if full_sync else self._get_last_transformation_timestamp()
        )

        # Create transformation run
        run_id = self.create_transformation_run(last_transformation, full_sync)

        stats = {
            "run_id": run_id,
            "incremental_since": last_transformation,
            "full_sync": full_sync,
            "dry_run": dry_run,
            "assets_processed": 0,
            "records_created": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc),
        }

        try:
            if dry_run:
                logger.info("⚠️  DRY RUN MODE - No changes will be committed")

            if full_sync:
                logger.info("🔄 Full sync mode: Processing ALL TDX assets")
            elif last_transformation:
                logger.info(
                    f"⚡ Incremental mode: Processing assets since {last_transformation}"
                )
            else:
                logger.info("🆕 First run: Processing ALL TDX assets")

            logger.info("🚀 Starting TDX assets silver transformation...")

            # Find assets needing transformation
            asset_ids = self._get_assets_needing_transformation(
                last_transformation, full_sync
            )

            if not asset_ids:
                logger.info("✨ All records up to date - no transformation needed")
                self.complete_transformation_run(run_id, 0, 0, 0, 0)
                return stats

            logger.info(f"📊 Processing {len(asset_ids)} TDX assets")

            # Process each asset
            for idx, tdx_asset_id in enumerate(asset_ids, 1):
                try:
                    # Fetch latest bronze record
                    bronze_result = self._fetch_latest_bronze_record(tdx_asset_id)

                    if not bronze_result:
                        logger.warning(
                            f"⚠️  No bronze data found for asset ID {tdx_asset_id}"
                        )
                        stats["errors"].append(f"No bronze data for {tdx_asset_id}")
                        continue

                    raw_data, raw_id = bronze_result

                    # Extract TDX fields to silver columns (including extracted attributes)
                    silver_record = self._extract_tdx_fields(raw_data, raw_id)

                    # Upsert to silver layer
                    action = self._upsert_silver_record(silver_record, run_id, dry_run)

                    if action == "created":
                        stats["records_created"] += 1
                    elif action == "updated":
                        stats["records_updated"] += 1
                    elif action == "skipped":
                        stats["records_skipped"] += 1

                    stats["assets_processed"] += 1

                    # Log progress periodically
                    if idx % 1000 == 0:
                        logger.info(
                            f"📈 Progress: {idx}/{len(asset_ids)} assets processed "
                            f"({stats['records_created']} created, {stats['records_updated']} updated, "
                            f"{stats['records_skipped']} skipped)"
                        )

                except Exception as record_error:
                    error_msg = (
                        f"Error processing asset {tdx_asset_id}: {str(record_error)}"
                    )
                    logger.error(f"❌ {error_msg}")
                    stats["errors"].append(error_msg)
                    # Continue processing other assets

            # Calculate duration
            stats["completed_at"] = datetime.now(timezone.utc)
            stats["duration_seconds"] = (
                stats["completed_at"] - stats["started_at"]
            ).total_seconds()

            # Complete the run
            if not dry_run:
                self.complete_transformation_run(
                    run_id,
                    stats["assets_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                )

            # Log summary
            logger.info("=" * 80)
            logger.info("🎉 Transformation Complete!")
            logger.info(f"📊 Assets processed: {stats['assets_processed']}")
            logger.info(f"🆕 Records created: {stats['records_created']}")
            logger.info(f"📝 Records updated: {stats['records_updated']}")
            logger.info(f"⏭️  Records skipped (unchanged): {stats['records_skipped']}")
            if stats["errors"]:
                logger.warning(f"⚠️  Errors encountered: {len(stats['errors'])}")
            logger.info(f"⏱️  Duration: {stats['duration_seconds']:.2f} seconds")
            logger.info("=" * 80)

            return stats

        except Exception as e:
            error_msg = f"Fatal error during transformation: {str(e)}"
            logger.error(f"❌ {error_msg}")
            stats["errors"].append(error_msg)

            if not dry_run:
                self.complete_transformation_run(
                    run_id,
                    stats["assets_processed"],
                    stats["records_created"],
                    stats["records_updated"],
                    stats["records_skipped"],
                    error_msg,
                )

            raise

    def close(self):
        """Clean up database connections."""
        if self.db_adapter:
            self.db_adapter.close()


def main():
    """Command-line entry point with argument parsing."""
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Transform TDX assets from bronze to source-specific silver layer"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Process all records (ignore last transformation timestamp)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing to database",
    )
    args = parser.parse_args()

    # Get database URL from environment
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        sys.exit(1)

    # Initialize service
    service = TDXAssetTransformationService(database_url)

    try:
        # Run transformation
        stats = service.transform_incremental(
            full_sync=args.full_sync, dry_run=args.dry_run
        )

        # Exit with appropriate code
        if stats["errors"]:
            sys.exit(1)
        else:
            sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}")
        sys.exit(1)
    finally:
        service.close()


if __name__ == "__main__":
    main()
