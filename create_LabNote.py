import dotenv
import argparse
from google_drive import GoogleSheetsAdapter, Sheet
from teamdynamix import TeamDynamixFacade
import os
from dotenv import load_dotenv

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Create a lab note for a PI in TeamDynamix')
    parser.add_argument('uniqname', help='Uniqname of the PI')
    parser.add_argument('--env-file', default='.env', help='Path to the .env file')
    args = parser.parse_args()

    # Load environment variables
    load_dotenv(args.env_file)

    TDX_BASE_URL = os.getenv('TDX_BASE_URL')
    TDX_APP_ID = os.getenv('TDX_APP_ID')
    API_TOKEN = os.getenv('TDX_API_TOKEN')

    # Initialize TeamDynamix service
    tdx_service = TeamDynamixFacade(TDX_BASE_URL, TDX_APP_ID, API_TOKEN)

    # Create the lab
    lab = tdx_service.create_lab(args.uniqname)
    print(f"Created lab for {args.uniqname}: {lab['Name']} (ID: {lab['ID']})")

if __name__ == '__main__':
    main()
