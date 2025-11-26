#!/usr/bin/env python3
"""
Script to create TeamDynamix Configuration Items (CIs) for research labs.
Reads from silver.v_labs_monitored and related views to populate CI fields.
"""

import argparse
import logging
import os
import sys
from typing import Dict, List, Any, Optional
from collections import defaultdict
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Load environment variables
load_dotenv()

from database.adapters.postgres_adapter import PostgresAdapter
from teamdynamix.facade.teamdynamix_facade import TeamDynamixFacade

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Constants
FORM_ID_RESEARCH_LAB = 3830
ATTR_ID_LAB_MANAGER_1 = 20550
ATTR_ID_LAB_MANAGER_2 = 20551
ATTR_ID_LAB_MANAGER_3 = 20552
ATTR_ID_LOCATION_2 = 20553
ATTR_ID_LOCATION_3 = 20555
ATTR_ID_HAS_INSTRUMENTATION = 20557  # Assuming this from user example, though not explicitly in requirements to set dynamically yet

def parse_args():
    parser = argparse.ArgumentParser(description="Create TDX Lab Configuration Items")
    parser.add_argument("--lab-id", help="Specific Lab ID to process (e.g., 'aabol')")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Dry run mode (default: True)")
    parser.add_argument("--no-dry-run", action="store_false", dest="dry_run", help="Execute actual API calls")
    parser.add_argument("--full-sync", action="store_true", help="Update existing CIs if found")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    return parser.parse_args()

def fetch_lab_data(db: PostgresAdapter, lab_id: Optional[str] = None) -> Dict[str, Any]:
    """Fetch all necessary lab data from Postgres."""
    
    # Base queries
    labs_query = "SELECT * FROM silver.v_labs_monitored"
    managers_query = "SELECT * FROM silver.v_lab_managers_tdx_reference"
    locations_query = "SELECT * FROM silver.v_lab_locations_tdx_reference"
    
    params = {}
    if lab_id:
        labs_query += " WHERE lab_id = :lab_id"
        managers_query += " WHERE lab_id = :lab_id"
        locations_query += " WHERE lab_id = :lab_id"
        params = {"lab_id": lab_id}
        
    logger.info(f"Fetching data from database (Lab ID filter: {lab_id})...")
    
    labs_df = db.query_to_dataframe(labs_query, params)
    managers_df = db.query_to_dataframe(managers_query, params)
    locations_df = db.query_to_dataframe(locations_query, params)
    
    return {
        "labs": labs_df.to_dict("records"),
        "managers": managers_df.to_dict("records"),
        "locations": locations_df.to_dict("records")
    }

def process_labs(tdx: TeamDynamixFacade, data: Dict[str, Any], args):
    """Process labs and create/update CIs."""
    
    labs = data["labs"]
    managers_by_lab = defaultdict(list)
    for m in data["managers"]:
        managers_by_lab[m["lab_id"]].append(m)
        
    locations_by_lab = defaultdict(list)
    for l in data["locations"]:
        locations_by_lab[l["lab_id"]].append(l)
        
    # Sort locations by computer count (descending) to pick top ones
    for lab_id in locations_by_lab:
        locations_by_lab[lab_id].sort(key=lambda x: x.get("computers_with_location_description", 0) or 0, reverse=True)

    logger.info(f"Found {len(labs)} labs to process.")
    
    for lab in labs:
        lab_id = lab["lab_id"]
        ci_name = f"{lab_id} Lab"
        
        logger.info(f"Processing lab: {lab_id} ({ci_name})")
        
        # Get PI info
        # The v_labs_monitored view has pi_uniqname, but we need TDX UID.
        # We can get it from the managers view where manager_tdx_uid is present, 
        # but we need to know which manager is the PI.
        # The user said: "OwnerUID with the pi's tdx_uid".
        # Let's look at v_lab_managers_tdx_reference. It has pi_tdx_uid column?
        # Checking the user provided SQL output in Step 22:
        # lab_id | pi_tdx_uid | manager_tdx_uid ...
        # So every row in managers view has pi_tdx_uid. We can just take the first one.
        
        lab_managers = managers_by_lab.get(lab_id, [])
        if not lab_managers:
            logger.warning(f"No managers/PI info found for lab {lab_id}. Skipping.")
            continue
            
        pi_tdx_uid = lab_managers[0].get("pi_tdx_uid")
        if not pi_tdx_uid:
             logger.warning(f"No PI TDX UID found for lab {lab_id}. Skipping.")
             continue

        # Get Locations
        lab_locations = locations_by_lab.get(lab_id, [])
        primary_location = lab_locations[0] if lab_locations else None
        
        # Construct Attributes
        attributes = []
        
        # Managers (up to 3)
        # We should exclude the PI from the manager list if they are listed as a manager?
        # The user said: "fill in the lab managers fields from v_lab_managers_tdx_reference, manager_tdx_uid"
        # The view seems to list all managers.
        
        # Let's collect unique manager UIDs
        manager_uids = []
        for m in lab_managers:
            uid = m.get("manager_tdx_uid")
            if uid and uid not in manager_uids:
                manager_uids.append(uid)
        
        if len(manager_uids) > 0:
            attributes.append({"ID": ATTR_ID_LAB_MANAGER_1, "Value": str(manager_uids[0])})
        if len(manager_uids) > 1:
            attributes.append({"ID": ATTR_ID_LAB_MANAGER_2, "Value": str(manager_uids[1])})
        if len(manager_uids) > 2:
            attributes.append({"ID": ATTR_ID_LAB_MANAGER_3, "Value": str(manager_uids[2])})
            
        # Secondary Locations (up to 2 more)
        if len(lab_locations) > 1:
             # Location 2
             loc = lab_locations[1]
             # FieldType: locationroom. Value should be ID?
             # User example: "Value": "105179", "ValueText": "WEST QUADRANGLE - B128 0B"
             # The view has room_id and location_id. 
             # If room_id is present, use it? Or location_id?
             # User example shows "LocationRoomID": 8440 for main location.
             # For attributes, it seems to be room ID if available.
             val = loc.get("room_id") or loc.get("location_id")
             if val:
                 attributes.append({"ID": ATTR_ID_LOCATION_2, "Value": str(int(float(val)))})
                 
        if len(lab_locations) > 2:
             # Location 3
             loc = lab_locations[2]
             val = loc.get("room_id") or loc.get("location_id")
             if val:
                 attributes.append({"ID": ATTR_ID_LOCATION_3, "Value": str(int(float(val)))})

        # Construct Payload
        payload = {
            "FormID": FORM_ID_RESEARCH_LAB,
            "Name": ci_name,
            "OwnerUID": str(pi_tdx_uid),
            "TypeName": "Lab", # Optional, but good for clarity
            "Attributes": attributes
        }
        
        if primary_location:
            if primary_location.get("location_id"):
                payload["LocationID"] = int(float(primary_location["location_id"]))
            if primary_location.get("room_id"):
                payload["LocationRoomID"] = int(float(primary_location["room_id"]))

        if args.verbose:
            logger.info(f"Payload for {ci_name}: {payload}")

        if args.dry_run:
            logger.info(f"[DRY RUN] Would create/update CI '{ci_name}' with payload: {payload}")
            continue

        # Check existence
        existing_ci = tdx.configuration_items.get_ci(ci_name)
        
        if existing_ci:
            if args.full_sync:
                logger.info(f"CI '{ci_name}' exists (ID: {existing_ci['ID']}). Updating...")
                # Update logic
                # edit_ci takes fields and identifier
                # We need to be careful with Attributes. edit_ci might need special handling for attributes if the API wrapper doesn't handle it deep merge style.
                # The wrapper's edit_ci just does data.update(fields) and PUT.
                # TDX API usually replaces the list of attributes if provided.
                
                try:
                    result = tdx.configuration_items.edit_ci(payload, existing_ci['ID'])
                    logger.info(f"Successfully updated CI '{ci_name}'.")
                except Exception as e:
                    logger.error(f"Failed to update CI '{ci_name}': {e}")
            else:
                logger.info(f"CI '{ci_name}' exists (ID: {existing_ci['ID']}). Skipping (use --full-sync to update).")
        else:
            logger.info(f"Creating new CI '{ci_name}'...")
            try:
                result = tdx.configuration_items.create_ci(payload)
                logger.info(f"Successfully created CI '{ci_name}' (ID: {result.get('ID')}).")
            except Exception as e:
                logger.error(f"Failed to create CI '{ci_name}': {e}")

def main():
    args = parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Initialize adapters
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        # Fallback for local dev if env var not set, though it should be
        db_url = "postgresql://lsats_user:password@localhost:5432/lsats_db"
        
    db = PostgresAdapter(db_url)
    
    tdx_base_url = os.getenv("TDX_BASE_URL")
    tdx_token = os.getenv("TDX_API_TOKEN")
    tdx_app_id = 48 # Asset/CI App ID
    
    if not tdx_base_url or not tdx_token:
        logger.error("TDX_BASE_URL and TDX_API_TOKEN environment variables must be set.")
        sys.exit(1)
        
    tdx = TeamDynamixFacade(tdx_base_url, tdx_app_id, tdx_token)

    try:
        data = fetch_lab_data(db, args.lab_id)
        process_labs(tdx, data, args)
    finally:
        db.close()

if __name__ == "__main__":
    main()
