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

    def update_data(self, spreadsheet_id, range_name, values, value_InputOption='RAW'):
        try:
            body = {
                'values': values
            }
            result = self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=range_name,
                valueInputOption=value_InputOption, body=body).execute()
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


def index_to_column_letter(index):
    """
    Convert a zero-based column index to an Excel-style column letter.
    Examples:
    - 0 -> 'A'
    - 25 -> 'Z'
    - 26 -> 'AA'
    - 701 -> 'ZZ'
    - 702 -> 'AAA'

    This works for columns beyond 'ZZ', supporting the full Excel column naming pattern:
    A-Z, AA-AZ, BA-BZ, ..., ZA-ZZ, AAA-AAZ, etc.
    """
    result = ""
    while index >= 0:
        remainder = index % 26
        result = chr(65 + remainder) + result
        index = index // 26 - 1
    return result


def column_letter_to_index(column_letter):
    """
    Convert an Excel-style column letter to a zero-based column index.
    Examples:
    - 'A' -> 0
    - 'Z' -> 25
    - 'AA' -> 26
    - 'ZZ' -> 701
    - 'AAA' -> 702
    """
    result = 0
    for char in column_letter:
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result - 1


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
        self.column_indices = {col: idx for idx, col in enumerate(self.columns)} if self.columns else {}

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

    def get_column_letter(self, column_name):
        """
        Get the spreadsheet column letter (A, B, C, etc.) for a given column name.

        Args:
            column_name (str): The name of the column.

        Returns:
            str: The column letter (A, B, C, etc.) or None if the column name is not found.
        """
        if column_name not in self.column_indices:
            return None

        return index_to_column_letter(self.column_indices[column_name])

    def get_cell_reference(self, column_name, row_number):
        """
        Get the spreadsheet cell reference (A1, B2, etc.) for a given column name and row number.

        Args:
            column_name (str): The name of the column.
            row_number (int): The row number (1-based).

        Returns:
            str: The cell reference (A1, B2, etc.) or None if the column name is not found.
        """
        column_letter = self.get_column_letter(column_name)
        if not column_letter:
            return None

        return f"{column_letter}{row_number}"

    def write_column_data(self, column_name, row_number, values, value_InputOption="RAW"):
        """
        Write data to a cell or range of cells in a specific column.

        Args:
            column_name (str): The name of the column.
            row_number (int): The starting row number (1-based).
            values (list): The values to write, e.g., [["value"]] for a single cell.
            value_InputOption (str): Input option for the write operation.

        Returns:
            dict: The result of the write operation or None if there was an error.
        """
        cell_reference = self.get_cell_reference(column_name, row_number)
        if not cell_reference:
            print(f"Column '{column_name}' not found.")
            return None

        return self.write_data(cell_reference, values, value_InputOption)

    def search_columns(self, query, columns=None):
        results = []
        columns = columns if columns else self.columns
        indices = [self.column_indices[col] for col in columns if col in self.column_indices]
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
                    column_index = self.column_indices[column]
                    if column_index >= len(row) or str(query).lower() not in str(row[column_index]).lower():
                        match = False
                        break
            if match:
                cell_location = f"{self.sheet_name}!A{row_index}"
                results.append((row, cell_location))
        return results

    def write_data(self, range_name, values, value_InputOption="RAW"):
        return self.adapter.update_data(self.spreadsheet_id, f"{self.sheet_name}!{range_name}", values, value_InputOption=value_InputOption)

    def generate_url(self, cell_location):
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/edit#gid={self.sheet_id}&range={cell_location}"


if __name__ == "__main__":
    import datetime
    CREDENTIALS_FILE = 'credentials.json'
    SPREADSHEET_ID = '1AYqjkAPYFUoo62CK7MG-g-E9P906aJBtEyXbmeL2OUQ'
    # original: https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit?gid=0#gid=0
    # testing: https://docs.google.com/spreadsheets/d/1AYqjkAPYFUoo62CK7MG-g-E9P906aJBtEyXbmeL2OUQ/edit?usp=sharing
    SHEET_NAME = 'Class Data'

    adapter = GoogleSheetsAdapter(CREDENTIALS_FILE)
    sheet = Sheet(adapter, SPREADSHEET_ID, SHEET_NAME)

    # Fetch column names
    columns = sheet.get_column_names()
    print("Columns:", columns)

    # Example of using the new column name features
    for col_name in columns[:5]:  # First 5 columns
        col_letter = sheet.get_column_letter(col_name)
        print(f"Column '{col_name}' is column letter {col_letter}")

    # Write to a cell using column name
    if "Student Name" in sheet.column_indices:
        now = datetime.datetime.now()
        result = sheet.write_column_data("Student Name", 32, [[str(now)]])
        print("Write result:", result)

    # Search for a value in all columns
    search_results = sheet.search_columns("Ellen")
    print("Search results for 'Ellen':", search_results)

    # Search for values in specific columns
    search_results = sheet.search_columns("Math", columns=["Major"])
    print("Search results for 'Math' in 'Major':", search_results)

    # Search for multiple values across different columns
    query = {"Class Level": "1. Freshman", "Home State": "ZZ"}
    search_results = sheet.search_multiple_columns(query)
    print("Search results for multi-column query:", search_results)
