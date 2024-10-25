import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

class GoogleSheetsAdapter:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    def __init__(self, credentials_file, token_file='token.json'):
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.creds = self._get_credentials()
        self.service = self._initialize_service()

    def _get_credentials(self):
        creds = None
        if os.path.exists(self.token_file):
            creds = Credentials.from_authorized_user_file(self.token_file, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, SCOPES
                )
                creds = flow.run_local_server(port=8080)
            with open(self.token_file, 'w') as token:
                token.write(creds.to_json())
        return creds

    def _initialize_service(self):
        return build('sheets', 'v4', credentials=self.creds)

    def fetch_data(self, spreadsheet_id, range_name):
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=range_name).execute()
            return result.get('values', [])
        except HttpError as err:
            print(f"HttpError occurred: {err}")
            return None

    def update_data(self, spreadsheet_id, range_name, values):
        try:
            body = {
                'values': values
            }
            result = self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=range_name,
                valueInputOption="RAW", body=body).execute()
            return result
        except HttpError as err:
            print(f"HttpError occurred: {err}")
            return None

    def fetch_metadata(self, spreadsheet_id):

        try:
            sheet_metadata = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            return sheet_metadata
        except HttpError as err:
            print(f"HttpError occurred: {err}")
            return None
    def update_metadata(self, spreadsheet_id, properties):
        try:
            body = {
                'properties': properties
            }
            result = self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body).execute()
            return result
        except HttpError as err:
            print(f"HttpError occurred: {err}")
            return None
class Sheet:
    def __init__(self, adapter, spreadsheet_id, sheet_name=None, header_row=0):
        self.adapter = adapter
        self.spreadsheet_id = spreadsheet_id
        self.metadata = self.adapter.fetch_metadata(spreadsheet_id)
        self.spreadsheet_name = self.metadata.get('properties', {}).get('title', 'Untitled Spreadsheet')
        self.sheet_name = sheet_name
        self.sheet_id = self._get_sheet_id() if sheet_name else None
        self.data = self._fetch_all_sheet_data() if not sheet_name else self.adapter.fetch_data(self.spreadsheet_id, self.sheet_name)
        self.columns = self._get_columns(header_row)

    def _get_sheet_id(self):
        sheets = self.metadata.get('sheets', [])
        for sheet in sheets:
            if sheet.get("properties", {}).get("title") == self.sheet_name:
                return sheet.get("properties", {}).get("sheetId")
        return None

    def _fetch_all_sheet_data(self):
            all_data = []
            self.sheet_name = []
            sheets = self.metadata.get('sheets', [])
            for sheet in sheets:
                title = sheet.get("properties", {}).get("title")
                self.sheet_name.append(title)
                data = self.adapter.fetch_data(self.spreadsheet_id, title)
                if data:
                    all_data.extend(data)
            return all_data

    def _get_columns(self, column_row):
        if self.sheet_name and isinstance(self.sheet_name, list):
            columns = []
            for sheet in self.sheet_name:
                data = self.adapter.fetch_data(self.spreadsheet_id, sheet)
                if data:
                    columns.extend(data[column_row])
            return columns
        return self.data[column_row] if self.data else []
    def get_column_names(self):
        return self.columns

    def search_columns(self, query, columns=None):
        results = []
        columns = columns if columns else self.columns
        indices = [self.columns.index(col) for col in columns if col in self.columns]
        cell_location = []
        for row_index, row in enumerate(self.data[1:], start=2):  # start=2 to account for header row
            if any(i < len(row) and query.lower() in str(row[i]).lower() for i in indices):
                cell_location.append(f"A{row_index}")
                results.append(row)
        results.append(cell_location)
        return results

    def search_multiple_columns(self, queries):
            # Ensure all query columns exist in the sheet's columns
            for column in queries.keys():
                if column not in self.columns:
                    print(f"Column '{column}' not found in the sheet's columns.")
                    return []
            results = []
            for row_index, row in enumerate(self.data[1:], start=2):  # start=2 to account for header row
                match = True
                for column, query in queries.items():
                    if column in self.columns:
                        column_index = self.columns.index(column)
                        if column_index >= len(row) or str(query).lower() not in str(row[column_index]).lower():
                            match = False
                            break
                if match:
                    cell_location = f"{self.sheet_name}!A{row_index}"
                    results.append((row, cell_location))
            return results

    def write_data(self, range_name, values):
        return self.adapter.update_data(self.spreadsheet_id, f"{self.sheet_name}!{range_name}", values)

    def generate_url(self, cell_location):
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/edit#gid={self.sheet_id}&range={cell_location}"

if __name__ == "__main__":
    CREDENTIALS_FILE = 'credentials.json'
    SPREADSHEET_ID = '1VvNpXEvyA7NS4JXpJT6A-IPzf0fIV_S_HvfLz-JfnxI'
    SHEET_NAME = 'updated_links.csv'

    adapter = GoogleSheetsAdapter(CREDENTIALS_FILE)
    sheet = Sheet(adapter, SPREADSHEET_ID, SHEET_NAME)

    # Fetch column names
    columns = sheet.get_column_names()
    print("Columns:", columns)

    # Search for a value in all columns
    search_results = sheet.search_columns("Ellen")
    print("Search results for 'Ellen':", search_results)

    # Search for values in specific columns
    search_results = sheet.search_columns("Math", columns=["Major"])
    print("Search results for 'Physics' in 'Major':", search_results)

    # Search for multiple values across different columns
    query = {"Class Level": "1. Freshman", "Home State": "ZZ"}
    search_results = sheet.search_multiple_columns(query)
    print("Search results for 'John' in 'Name' and 'Engineering' in 'Major':", search_results)

    # Write data to the sheet
    range_name = "A10:B11"
    values = [
        ["New Student", "Biology"],
        ["Another Student", "Math"]
    ]
    update_result = sheet.write_data(range_name, values)
    print("Update result:", update_result)
