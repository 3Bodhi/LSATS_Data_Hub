import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from typing import List, Dict, Any, Optional, Union, Tuple

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

        # Try to load existing credentials
        if os.path.exists(self.token_file):
            try:
                creds = Credentials.from_authorized_user_file(self.token_file, self.SCOPES)
            except ValueError as e:
                # Handle the case where token.json is malformed or missing refresh_token
                print(f"Token file is invalid: {e}")
                print("Removing invalid token file and starting fresh authorization...")
                os.remove(self.token_file)
                creds = None

        # Check if credentials are valid and have refresh capability
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                # We have a valid refresh token, so we can renew the access token
                try:
                    creds.refresh(Request())
                except Exception as e:
                    print(f"Failed to refresh token: {e}")
                    creds = None

            # If we still don't have valid credentials, start the full flow
            if not creds:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, self.SCOPES
                )
                # Explicitly request offline access to ensure we get a refresh token
                creds = flow.run_local_server(
                    port=8081,
                    access_type='offline',  # This ensures we get a refresh token
                    prompt='consent'        # This forces the consent screen even for returning users
                )

            # Save the new credentials
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

    def search_columns_raw(self, query, columns=None):
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

    def search_multiple_columns_raw(self, queries):
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

    def get_column_as_list(self, column_name: str, include_header: bool = False, skip_empty: bool = True) -> List[Any]:
        """
        Get all values from a specific column as a list.

        Args:
            column_name (str): The name of the column to extract.
            include_header (bool): Whether to include the header row in the result.
            skip_empty (bool): Whether to skip empty/None values.

        Returns:
            List[Any]: List of values from the specified column.

        Raises:
            ValueError: If the column name doesn't exist.
        """
        if column_name not in self.column_indices:
            raise ValueError(f"Column '{column_name}' not found. Available columns: {list(self.column_indices.keys())}")

        column_index = self.column_indices[column_name]
        column_values = []

        # Start from row 1 to skip header, or row 0 to include it
        start_row = 0 if include_header else 1

        for row in self.data[start_row:]:
            # Handle cases where row might be shorter than expected
            if column_index < len(row):
                value = row[column_index]
                if not skip_empty or (value is not None and str(value).strip() != ''):
                    column_values.append(value)
            elif not skip_empty:
                column_values.append(None)

        return column_values

    def get_columns_as_dict(self, key_column: str, value_column: str,
                           include_header_row: bool = False,
                           skip_empty_keys: bool = True) -> Dict[Any, Any]:
        """
        Create a dictionary mapping values from one column to values from another column.

        Args:
            key_column (str): The name of the column to use as dictionary keys.
            value_column (str): The name of the column to use as dictionary values.
            include_header_row (bool): Whether to include the header row in the mapping.
            skip_empty_keys (bool): Whether to skip rows where the key column is empty.

        Returns:
            Dict[Any, Any]: Dictionary mapping key_column values to value_column values.

        Raises:
            ValueError: If either column name doesn't exist.
        """
        if key_column not in self.column_indices:
            raise ValueError(f"Key column '{key_column}' not found. Available columns: {list(self.column_indices.keys())}")

        if value_column not in self.column_indices:
            raise ValueError(f"Value column '{value_column}' not found. Available columns: {list(self.column_indices.keys())}")

        key_index = self.column_indices[key_column]
        value_index = self.column_indices[value_column]

        result_dict = {}

        # Start from row 1 to skip header, or row 0 to include it
        start_row = 0 if include_header_row else 1

        for row in self.data[start_row:]:
            # Get key and value, handling cases where row might be shorter
            key = row[key_index] if key_index < len(row) else None
            value = row[value_index] if value_index < len(row) else None

            # Skip empty keys if requested
            if skip_empty_keys and (key is None or str(key).strip() == ''):
                continue

            result_dict[key] = value

        return result_dict

    def get_dataframe_subset(self, column_names: List[str], include_header: bool = True) -> pd.DataFrame:
        """
        Create a pandas DataFrame with only the specified columns.

        Args:
            column_names (List[str]): List of column names to include in the DataFrame.
            include_header (bool): Whether to use the first row as column headers.

        Returns:
            pd.DataFrame: DataFrame containing only the specified columns.

        Raises:
            ValueError: If any column name doesn't exist.
            ImportError: If pandas is not available.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for this method. Install it with: pip install pandas")

        # Validate all column names exist
        missing_columns = [col for col in column_names if col not in self.column_indices]
        if missing_columns:
            raise ValueError(f"Column(s) not found: {missing_columns}. Available columns: {list(self.column_indices.keys())}")

        # Get column indices for the requested columns
        column_indices = [self.column_indices[col_name] for col_name in column_names]

        # Extract data for the specified columns
        subset_data = []
        start_row = 1 if include_header else 0  # Skip header row if using it as column names

        for row in self.data[start_row:]:
            subset_row = []
            for col_index in column_indices:
                # Handle cases where row might be shorter than expected
                if col_index < len(row):
                    subset_row.append(row[col_index])
                else:
                    subset_row.append(None)
            subset_data.append(subset_row)

        # Create DataFrame
        if include_header:
            # Use the specified column names as headers
            df = pd.DataFrame(subset_data, columns=column_names)
        else:
            # Use default column names (0, 1, 2, etc.)
            df = pd.DataFrame(subset_data)

        return df

    def get_column_statistics(self, column_name: str, numeric_only: bool = True) -> Dict[str, Any]:
        """
        Get basic statistics for a column (bonus method).

        Args:
            column_name (str): The name of the column to analyze.
            numeric_only (bool): Whether to only include numeric values in calculations.

        Returns:
            Dict[str, Any]: Dictionary containing statistics like count, mean, min, max, etc.
        """
        column_data = self.get_column_as_list(column_name, include_header=False, skip_empty=True)

        if not column_data:
            return {"count": 0, "error": "No data found in column"}

        stats = {
            "count": len(column_data),
            "unique_count": len(set(column_data)),
        }

        if numeric_only:
            # Try to convert to numeric values
            numeric_data = []
            for value in column_data:
                try:
                    if isinstance(value, (int, float)):
                        numeric_data.append(value)
                    elif isinstance(value, str) and value.replace('.', '', 1).replace('-', '', 1).isdigit():
                        numeric_data.append(float(value))
                except (ValueError, TypeError):
                    continue

            if numeric_data:
                stats.update({
                    "numeric_count": len(numeric_data),
                    "mean": sum(numeric_data) / len(numeric_data),
                    "min": min(numeric_data),
                    "max": max(numeric_data),
                    "sum": sum(numeric_data)
                })
            else:
                stats["error"] = "No numeric data found in column"

        return stats

    def search_columns_as_dicts(self, query: str, columns: Optional[List[str]] = None) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Search for a query in specified columns and return results as dictionaries with column name access.

        Args:
            query (str): The search term to look for.
            columns (Optional[List[str]]): List of column names to search in. If None, searches all columns.

        Returns:
            Tuple[List[Dict[str, Any]], List[str]]:
                - List of dictionaries where each dict represents a matching row with column names as keys
                - List of cell locations for the matching rows
        """
        results = []
        columns = columns if columns else self.columns
        indices = [self.column_indices[col] for col in columns if col in self.column_indices]
        row_locations = []

        for row_index, row in enumerate(self.data[1:], start=2):  # start=2 to account for header row
            if any(i < len(row) and query.lower() in str(row[i]).lower() for i in indices):
                # Create dictionary with column names as keys
                row_dict = {}
                for col_name, col_index in self.column_indices.items():
                    if col_index < len(row):
                        row_dict[col_name] = row[col_index]
                    else:
                        row_dict[col_name] = None

                results.append(row_dict)
                row_locations.append(f"{row_index}")

        return results, row_locations

    def search_columns_as_dataframe(self, query: str, columns: Optional[List[str]] = None) -> Tuple[pd.DataFrame, List[str]]:
        """
        Search for a query in specified columns and return results as a pandas DataFrame.

        Args:
            query (str): The search term to look for.
            columns (Optional[List[str]]): List of column names to search in. If None, searches all columns.

        Returns:
            Tuple[pd.DataFrame, List[str]]:
                - DataFrame containing matching rows with proper column names
                - List of cell locations for the matching rows
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for this method. Install it with: pip install pandas")

        results, cell_locations = self.search_columns_as_dicts(query, columns)

        if results:
            df = pd.DataFrame(results)
            # Add row locations as a column for reference
            df['_cell_location'] = cell_locations
            return df, cell_locations
        else:
            # Return empty DataFrame with proper columns
            empty_df = pd.DataFrame(columns=self.columns + ['_cell_location'])
            return empty_df, []

    def search_multiple_columns(self, queries: Dict[str, str]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Enhanced version of search_multiple_columns that returns dictionaries instead of tuples.

        Args:
            queries (Dict[str, str]): Dictionary mapping column names to search terms.

        Returns:
            Tuple[List[Dict[str, Any]], List[str]]:
                - List of dictionaries representing matching rows
                - List of cell locations for the matching rows
        """
        # Ensure all query columns exist in the sheet's columns
        for column in queries.keys():
            if column not in self.columns:
                raise ValueError(f"Column '{column}' not found in the sheet's columns.")

        results = []
        cell_locations = []

        for row_index, row in enumerate(self.data[1:], start=2):  # start=2 to account for header row
            match = True
            for column, query in queries.items():
                if column in self.columns:
                    column_index = self.column_indices[column]
                    if column_index >= len(row) or str(query).lower() not in str(row[column_index]).lower():
                        match = False
                        break

            if match:
                # Create dictionary with column names as keys
                row_dict = {}
                for col_name, col_index in self.column_indices.items():
                    if col_index < len(row):
                        row_dict[col_name] = row[col_index]
                    else:
                        row_dict[col_name] = None

                results.append(row_dict)
                cell_locations.append(f"{self.sheet_name}!A{row_index}")

        return results, cell_locations

    # Enhanced wrapper that provides both old and new functionality
    def search_columns(self, query: str, columns: Optional[List[str]] = None,
                              return_format: str = 'dict') -> Union[List[Any], Tuple[List[Dict[str, Any]], List[str]], Tuple[pd.DataFrame, List[str]]]:
        """
        Enhanced search method that can return results in multiple formats.

        Args:
            query (str): The search term to look for.
            columns (Optional[List[str]]): List of column names to search in. If None, searches all columns.
            return_format (str): Format for returned data:
                - 'legacy': Returns the original format (list of lists + cell locations)
                - 'dict': Returns list of dictionaries + cell locations
                - 'dataframe': Returns pandas DataFrame + cell locations

        Returns:
            Depends on return_format parameter.
        """
        if return_format == 'legacy':
            # Return original format for backward compatibility
            return self.search_columns_raw(query, columns)
        elif return_format == 'dict':
            return self.search_columns_as_dicts(query, columns)
        elif return_format == 'dataframe':
            return self.search_columns_as_dataframe(query, columns)
        else:
            raise ValueError("return_format must be 'legacy', 'dict', or 'dataframe'")


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

    # Get a column as a list
    names = sheet.get_column_as_list("Student Name")
    print("Names:", names[:5])  # First 5 names

    # Create a dictionary mapping student names to majors
    name_to_major = sheet.get_columns_as_dict("Student Name", "Major")
    print("Name to Major mapping:", dict(list(name_to_major.items())[:3]))  # First 3 items

    # Get a subset DataFrame with only specific columns
    subset_df = sheet.get_dataframe_subset(["Student Name", "Major", "Class Level"])
    print("Subset DataFrame:")
    print(subset_df.head())

    # Get statistics for a numeric column (if you have one)
    if "GPA" in sheet.column_indices:
        gpa_stats = sheet.get_column_statistics("GPA")
        print("GPA Statistics:", gpa_stats)
