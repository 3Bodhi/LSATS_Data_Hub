import pandas as pd
import re
from teamdynamix import TeamDynamixFacade
from dotenv import load_dotenv
import os
import json

load_dotenv()
TDX_BASE_URL = os.getenv('TDX_BASE_URL')
path_replacements = {
    '/SBTDWebApi/api': '/SBTDNext/',
    '/TDWebApi/api': '/TDNext/'
}
TDX_TICKET_DOMAIN = TDX_BASE_URL
for old_path, new_path in path_replacements.items():
    TDX_TICKET_DOMAIN = TDX_TICKET_DOMAIN.replace(old_path, new_path)
TDX_APP_ID = os.getenv('TDX_APP_ID')
API_TOKEN = os.getenv('TDX_API_TOKEN')

CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
SHEET_NAME = os.getenv('SHEET_NAME')

tdx_service = TeamDynamixFacade(TDX_BASE_URL, TDX_APP_ID, API_TOKEN)

def is_valid_umich_email(email):
    """Check if email is a valid @umich.edu email"""
    if pd.isna(email) or not isinstance(email, str):
        return False
    return bool(re.match(r'^[^@]+@umich\.edu$', email.strip()))

def search_and_update_email(name, row_index):
    """Search for user and return email, with error reporting"""
    try:
        # Prepare search data
        data = {
            "SearchText": name,
        }

        # Call the search function
        people_list = tdx_service.users.search_user(data)
        #print(json.dumps(people_list, indent=4))

        if not people_list:
            print(f"Row {row_index}: No users found for '{name}'")
            return None
        elif len(people_list) == 1:
            # Single match - use this user
            email = people_list[0].get('PrimaryEmail')
            if email:
                print(f"Row {row_index}: Found email for '{name}': {email}")
                return email
            else:
                print(f"Row {row_index}: User found for '{name}' but no PrimaryEmail field")
                return None
        else:
            # Multiple matches - look for exact FullName match
            for person in people_list:
                if person.get('FullName') == name:
                    email = person.get('PrimaryEmail')
                    if email:
                        print(f"Row {row_index}: Found email for '{name}' via FullName match: {email}")
                        return email
                    else:
                        print(f"Row {row_index}: FullName match found for '{name}' but no PrimaryEmail field")
                        return None

            # No exact FullName match found
            print(f"Row {row_index}: Multiple users found for '{name}' but no exact FullName match")
            full_names = [person.get('FullName', 'Unknown') for person in people_list]
            print(f"  Available FullNames: {full_names}")
            return None

    except Exception as e:
        print(f"Row {row_index}: Error searching for '{name}': {str(e)}")
        return None

import os
import keyring
import getpass
from ldap3 import Server, Connection, ALL, SUBTREE
import json

def get_ad_password():
    """Get AD password from keyring or prompt user"""
    try:
        # Try to get password from keyring first
        password = keyring.get_password("ldap_umich", "myodhes1")
        if password:
            print("Using password from keyring")
            return password
    except Exception as e:
        print(f"Could not retrieve password from keyring: {e}")

    # If keyring fails or no password stored, prompt user
    password = getpass.getpass("Enter AD password for umroot\\myodhes1: ")

    # Optionally store in keyring for future use
    try:
        save_password = input("Save password to keyring? (y/n): ").lower().strip()
        if save_password == 'y':
            keyring.set_password("ldap_umich", "myodhes1", password)
            print("Password saved to keyring")
    except Exception as e:
        print(f"Could not save password to keyring: {e}")

    return password

def test_ldap_search(name):
    """Test LDAP search and return raw results for analysis"""

    # Environment variables
    AD_SERVER = os.getenv('AD_SERVER', 'adsroot.itcs.umich.edu')
    AD_SEARCH_BASE = os.getenv('AD_SEARCH_BASE', 'OU=People,OU=UMICH,DC=adsroot,DC=itcs,DC=umich,DC=edu')
    AD_USER = os.getenv('AD_USER', 'umroot\\myodhes1')

    print(f"Connecting to: {AD_SERVER}")
    print(f"Search base: {AD_SEARCH_BASE}")
    print(f"User: {AD_USER}")
    print(f"Searching for: {name}")
    print("-" * 50)

    try:
        # Get password
        password = get_ad_password()
        # Create server and connection
        server = Server(AD_SERVER,use_ssl=True, get_info=ALL,port=636)
        with Connection(server, user=AD_USER, password=password, auto_bind=True) as conn:
            print(f"Successfully connected to {AD_SERVER}")

            # Build search filter - search multiple name fields
            # This searches for the name in common name, display name, given name, and surname
            #search_filter = f'(|(cn={name}*)(displayName={name}*)(givenName={name}*)(sn={name}*))'
            search_filter = f'(&(objectClass=user)(sn=Blankenship*)(GivenName=ben*))'
            # Attributes to retrieve

            attributes = [
                'cn',                    # Common Name
                'displayName',           # Display Name
                'givenName',            # First Name
                'sn',                   # Last Name (Surname)
                'userPrincipalName',    # Email (UPN)
                'sAMAccountName',       # Username
                'department',           # Department
                'description'
            ]
            # Perform search
            print(f"Search filter: {search_filter}")
            print(f"Searching...")

            success = conn.search(
                search_base=AD_SEARCH_BASE,
                search_filter=search_filter,
                search_scope=SUBTREE,
                attributes=attributes
            )
            if success:
                print(f"Search successful! Found {len(conn.entries)} entries")

                if conn.entries:
                    print("\nSearch Results:")
                    print("=" * 50)

                    for i, entry in enumerate(conn.entries):
                        if entry['sn'].value.lower():
                            print(f"\nEntry {i + 1}:")
                            print(f"DN: {entry.entry_dn}")

                            # Print all attributes
                            for attr in attributes:
                                if hasattr(entry, attr) and getattr(entry, attr):
                                    value = getattr(entry, attr).value
                                    print(f"  {attr}: {value}")

                            print("-" * 30)

                    # Also return raw JSON for analysis
                    print("\nRaw JSON representation of first entry:")
                    if conn.entries:
                        first_entry_dict = {}
                        for attr in attributes:
                            if hasattr(conn.entries[0], attr):
                                first_entry_dict[attr] = getattr(conn.entries[0], attr).value
                        print(json.dumps(first_entry_dict, indent=2, default=str))

                    return conn.entries
                else:
                    print("No entries found")
                    return []
            else:
                print(f"Search failed: {conn.result}")
                print(conn.entries)
                return None

    except Exception as e:
        print(f"LDAP search error: {str(e)}")
        return None
    finally:
        try:
            conn.unbind()
            print("\nConnection closed")
        except:
            pass
def main():
    # Read the CSV file
    try:
        df = pd.read_csv('lab_personnel.csv')
        print(f"Loaded {len(df)} rows from lab_personnel.csv")
    except FileNotFoundError:
        print("Error: lab_personnel.csv file not found")
        return
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        return

    # Verify required columns exist
    if 'name' not in df.columns:
        print("Error: 'name' column not found in CSV")
        return
    if 'email' not in df.columns:
        print("Error: 'email' column not found in CSV")
        return

    # Track statistics
    total_rows = len(df)
    skipped_valid_emails = 0
    updated_emails = 0
    failed_updates = 0

    # Iterate through each row
    for index, row in df.iterrows():
        name = row['name']
        current_email = row['email']

        # Skip if name is empty
        if pd.isna(name) or not isinstance(name, str) or not name.strip():
            print(f"Row {index}: Skipping empty name")
            continue

        # Skip if already has valid @umich.edu email
        if is_valid_umich_email(current_email):
            print(f"Row {index}: Skipping '{name}' - already has valid @umich.edu email: {current_email}")
            skipped_valid_emails += 1
            continue

        # Search for user and update email
        new_email = search_and_update_email(name.strip(), index)

        if new_email:
            df.at[index, 'email'] = new_email
            updated_emails += 1
        else:
            failed_updates += 1

    # Save the updated CSV
    try:
        df.to_csv('lab_personnel.csv', index=False)
        print(f"\nUpdated CSV saved successfully!")
    except Exception as e:
        print(f"\nError saving CSV file: {str(e)}")
        return

    # Print summary statistics
    print(f"\nSummary:")
    print(f"Total rows processed: {total_rows}")
    print(f"Rows with valid @umich.edu emails (skipped): {skipped_valid_emails}")
    print(f"Emails successfully updated: {updated_emails}")
    print(f"Failed to find/update emails: {failed_updates}")

if __name__ == "__main__":
    test_name = input("Enter a name to search for: ").strip()
    if test_name:
        results = test_ldap_search(test_name)
    else:
        print("No name provided")
#main()
