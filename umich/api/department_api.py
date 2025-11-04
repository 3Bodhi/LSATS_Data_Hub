from .um_api import UMichAPI, create_headers
from typing import Dict, List, Union, Any, Optional
from urllib.parse import urlencode


class DepartmentAPI(UMichAPI):
    """
    API adapter for interacting with UM Department API endpoints.

    This class provides methods for retrieving department and department employee data
    from the University of Michigan Business & Finance Department API.
    """

    def _build_query_string(self, params: Dict[str, Any]) -> str:
        """
        Build a query string from parameters, filtering out None values.

        Args:
            params: Dictionary of query parameters.

        Returns:
            str: URL-encoded query string.
        """
        # Filter out None values
        filtered_params = {k: v for k, v in params.items() if v is not None}
        return urlencode(filtered_params) if filtered_params else ""

    def get_department_data(
        self,
        dept_id: Optional[str] = None,
        dept_description: Optional[str] = None,
        dept_group: Optional[str] = None,
        dept_group_description: Optional[str] = None,
        pagination: Optional[Dict[str, int]] = None,
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Gets department data from the UM Department API.

        Args:
            dept_id: Department Identification Code for filtering.
            dept_description: Department Name for filtering.
            dept_group: Department Group Code for filtering.
            dept_group_description: Department Group Description for filtering.
            pagination: Dictionary containing pagination parameters with keys:
                       - count: Number of records to return (max 1,000)
                       - start_index: 0-based index for pagination

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: Department data
            from the API if successful, None otherwise.

        Note:
            Pagination parameters will be ignored when set beyond 1,000 records.
            Use start_index for 0-based pagination through large result sets.
        """
        # Build query parameters
        params = {
            "DeptId": dept_id,
            "DeptDescription": dept_description,
            "DeptGroup": dept_group,
            "DeptGroupDescription": dept_group_description,
        }

        # Add pagination if provided
        if pagination:
            if "count" in pagination:
                params["$count"] = pagination["count"]
            if "start_index" in pagination:
                params["$start_index"] = pagination["start_index"]

        query_string = self._build_query_string(params)
        endpoint = f"Department/v2/DeptData"

        if query_string:
            endpoint += f"?{query_string}"

        return self.get(endpoint)

    def get_department_employee_data(
        self,
        empl_id: Optional[str] = None,
        uniqname: Optional[str] = None,
        department_id: Optional[str] = None,
        dept_description: Optional[str] = None,
        pagination: Optional[Dict[str, int]] = None,
    ) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Gets department employee data from the UM Department API.

        Args:
            empl_id: Employee Identification Code for filtering.
            uniqname: Uniqname of the Person for filtering.
            department_id: Appointing Department Identification Code for filtering.
            dept_description: UniqName of the Person for filtering (appears to be dept description based on API docs).
            pagination: Dictionary containing pagination parameters with keys:
                       - count: Number of records to return (max 1,000)
                       - start_index: 0-based index for pagination

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: Department employee data
            from the API if successful, None otherwise.

        Note:
            Pagination parameters will be ignored when set beyond 1,000 records.
            Use start_index for 0-based pagination through large result sets.
        """
        # Build query parameters
        params = {
            "EmplId": empl_id,
            "Uniqname": uniqname,
            "DepartmentId": department_id,
            "Dept_Description": dept_description,
        }

        # Add pagination if provided
        if pagination:
            if "count" in pagination:
                params["$count"] = pagination["count"]
            if "start_index" in pagination:
                params["$start_index"] = pagination["start_index"]

        query_string = self._build_query_string(params)
        endpoint = f"Department/v2/DeptEmpData"

        if query_string:
            endpoint += f"?{query_string}"

        return self.get(endpoint)

    def get_all_departments(
        self, max_records: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all department data by automatically handling pagination.

        Args:
            max_records: Maximum number of records to retrieve. If None, retrieves all records.

        Returns:
            List[Dict[str, Any]]: List of all department records.

        Note:
            This method automatically handles pagination by making multiple API calls if necessary.
            Use with caution for large datasets as it may result in many API requests.
        """
        all_departments = []
        start_index = 0
        page_size = 1000  # Maximum allowed by the API

        while True:
            # Get current page
            pagination = {"count": page_size, "start_index": start_index}
            result = self.get_department_data(pagination=pagination)
            result = result["DepartmentList"]["DeptData"]
            if not result or (isinstance(result, list) and len(result) == 0):
                break

            # Add results to our collection
            if isinstance(result, list):
                all_departments.extend(result)
                # If we got fewer results than requested, we've reached the end
                if len(result) < page_size:
                    break
            else:
                # Single result
                all_departments.append(result)
                print(type(result))
                break

            # Check if we've hit our max_records limit
            if max_records and len(all_departments) >= max_records:
                all_departments = all_departments[:max_records]
                break

            start_index += page_size

        return all_departments

    def get_all_department_employees(
        self, max_records: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all department employee data by automatically handling pagination.

        Args:
            max_records: Maximum number of records to retrieve. If None, retrieves all records.

        Returns:
            List[Dict[str, Any]]: List of all department employee records.

        Note:
            This method automatically handles pagination by making multiple API calls if necessary.
            Use with caution for large datasets as it may result in many API requests.
        """
        all_employees = []
        start_index = 0
        page_size = 1000  # Maximum allowed by the API

        while True:
            # Get current page
            pagination = {"count": page_size, "start_index": start_index}
            result = self.get_department_employee_data(pagination=pagination)

            if not result or (isinstance(result, list) and len(result) == 0):
                break

            # Add results to our collection
            if isinstance(result, list):
                all_employees.extend(result)
                # If we got fewer results than requested, we've reached the end
                if len(result) < page_size:
                    break
            else:
                # Single result
                all_employees.append(result)
                break

            # Check if we've hit our max_records limit
            if max_records and len(all_employees) >= max_records:
                all_employees = all_employees[:max_records]
                break

            start_index += page_size

        return all_employees

    def get_all_employees_in_department(
        self, department_id: str, max_records: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all employees in a specific department by automatically handling pagination.

        Args:
            department_id: The Department Identification Code to filter employees by.
            max_records: Maximum number of records to retrieve. If None, retrieves all records.

        Returns:
            List[Dict[str, Any]]: List of all employee records for the specified department.

        Note:
            This method automatically handles pagination by making multiple API calls if necessary.
            Useful for departments with more than 1,000 employees where pagination is required.
            Use with caution for large departments as it may result in many API requests.
        """
        all_employees = []
        start_index = 0
        page_size = 1000  # Maximum allowed by the API

        while True:
            # Get current page for the specific department
            pagination = {"count": page_size, "start_index": start_index}
            result = self.get_department_employee_data(
                department_id=department_id, pagination=pagination
            )

            if not result or (isinstance(result, list) and len(result) == 0):
                break

            # Add results to our collection
            if isinstance(result, list):
                all_employees.extend(result)
                # If we got fewer results than requested, we've reached the end
                if len(result) < page_size:
                    break
            else:
                # Single result
                all_employees.append(result)
                break

            # Check if we've hit our max_records limit
            if max_records and len(all_employees) >= max_records:
                all_employees = all_employees[:max_records]
                break

            start_index += page_size

        logging.debug(
            f"Retrieved {len(all_employees)} employees for department {department_id}"
        )
        return all_employees


if __name__ == "__main__":
    # Use absolute imports when running as script
    from umich.api.um_api import UMichAPI, create_headers
    from dotenv import load_dotenv
    import os
    import json
    import logging

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Load environment variables
    load_dotenv()
    UM_BASE_URL = os.getenv("UM_BASE_URL")
    UM_CATEGORY_ID = os.getenv("UM_CATEGORY_ID")
    UM_CLIENT_KEY = os.getenv("UM_CLIENT_KEY")
    UM_CLIENT_SECRET = os.getenv("UM_CLIENT_SECRET")
    SCOPE = "department"

    # Initialize API
    headers = create_headers(UM_CLIENT_KEY, UM_CLIENT_SECRET, SCOPE)
    department_api = DepartmentAPI(UM_BASE_URL, UM_CATEGORY_ID, headers)

    print("=" * 80)
    print("DepartmentAPI Test Suite")
    print("=" * 80)

    # Test 1: Get department data without filters (with pagination)
    print("\n1. Testing get_department_data() with pagination (first 5 records):")
    print("-" * 80)
    dept_data = department_api.get_department_data(
        pagination={"count": 5, "start_index": 0}
    )
    if dept_data:
        dept_list = dept_data.get("DepartmentList", {}).get("DeptData", [])
        print(
            f"Retrieved {len(dept_list) if isinstance(dept_list, list) else 1} department(s)"
        )
        if dept_list:
            first_dept = dept_list[0] if isinstance(dept_list, list) else dept_list
            print("\nSample department record:")
            print(json.dumps(first_dept, indent=2))
    else:
        print("No data returned")

    # Test 2: Get department data with filter
    print("\n\n2. Testing get_department_data() with dept_group filter:")
    print("-" * 80)
    lsa_depts = department_api.get_department_data(dept_group="COLLEGE_OF_LSA")
    if lsa_depts:
        lsa_list = lsa_depts.get("DepartmentList", {}).get("DeptData", [])
        count = len(lsa_list) if isinstance(lsa_list, list) else 1
        print(f"Retrieved {count} LSA department(s)")
        if lsa_list:
            sample = lsa_list[0] if isinstance(lsa_list, list) else lsa_list
            print("\nSample LSA department:")
            print(json.dumps(sample, indent=2))
    else:
        print("No LSA departments found")

    # Test 3: Get all departments with limit (testing pagination logic)
    print("\n\n3. Testing get_all_departments() with max_records limit:")
    print("-" * 80)
    all_depts = department_api.get_all_departments(max_records=10)
    print(f"Retrieved {len(all_depts)} departments (limited to 10)")
    if all_depts:
        print("\nFirst department from get_all_departments():")
        print(json.dumps(all_depts[0], indent=2))

    # Test 4: Get department employee data by department_id
    print("\n\n4. Testing get_department_employee_data() by department_id:")
    print("-" * 80)
    # Use a specific department ID (185500 from your original code)
    emp_data = department_api.get_department_employee_data(
        department_id="185500", pagination={"count": 5, "start_index": 0}
    )
    if emp_data:
        emp_list = emp_data.get("DeptEmpList", {}).get("DeptEmpData", [])
        if not emp_list:
            # Try alternative structure
            emp_list = emp_data if isinstance(emp_data, list) else [emp_data]

        count = len(emp_list) if isinstance(emp_list, list) else 1
        print(f"Retrieved {count} employee(s) for department 185500")
        if emp_list:
            sample = emp_list[0] if isinstance(emp_list, list) else emp_list
            print("\nSample employee record:")
            print(json.dumps(sample, indent=2))
    else:
        print("No employee data found for department 185500")

    # Test 5: Get department employee data by uniqname
    print("\n\n5. Testing get_department_employee_data() by uniqname:")
    print("-" * 80)
    # Use a sample uniqname (you may want to replace with a valid one)
    uniqname_data = department_api.get_department_employee_data(
        uniqname="myodhes", pagination={"count": 1, "start_index": 0}
    )
    if uniqname_data:
        print("Retrieved employee data by uniqname")
        print(json.dumps(uniqname_data, indent=2))
    else:
        print("No employee data found for the specified uniqname")

    # Test 6: Get all employees in a department (testing pagination)
    print("\n\n6. Testing get_all_employees_in_department() with max_records:")
    print("-" * 80)
    dept_employees = department_api.get_all_employees_in_department(
        department_id="185500", max_records=5
    )
    print(f"Retrieved {len(dept_employees)} employee(s) (limited to 5)")
    if dept_employees:
        print("\nFirst employee from get_all_employees_in_department():")
        print(json.dumps(dept_employees[0], indent=2))

    # Test 7: Get all department employees (testing pagination across all departments)
    print("\n\n7. Testing get_all_department_employees() with max_records:")
    print("-" * 80)
    all_employees = department_api.get_all_department_employees(max_records=10)
    print(
        f"Retrieved {len(all_employees)} employee(s) across all departments (limited to 10)"
    )
    if all_employees:
        print("\nFirst employee from get_all_department_employees():")
        print(json.dumps(all_employees[0], indent=2))
        print(len(all_employees[0]["DeptEmpInfo"]["DeptEmpData"]))

    # Test 8: Test pagination with second page
    print("\n\n8. Testing pagination - comparing first and second page:")
    print("-" * 80)
    page1 = department_api.get_department_data(
        pagination={"count": 2, "start_index": 0}
    )
    page2 = department_api.get_department_data(
        pagination={"count": 2, "start_index": 2}
    )

    if page1 and page2:
        page1_data = page1.get("DepartmentList", {}).get("DeptData", [])
        page2_data = page2.get("DepartmentList", {}).get("DeptData", [])

        if isinstance(page1_data, list) and isinstance(page2_data, list):
            print(f"Page 1 (start_index=0): {len(page1_data)} records")
            print(f"Page 2 (start_index=2): {len(page2_data)} records")

            # Compare first record from each page to verify they're different
            if page1_data and page2_data:
                page1_id = page1_data[0].get("DeptId", "N/A")
                page2_id = page2_data[0].get("DeptId", "N/A")
                print(f"\nFirst record on page 1: DeptId = {page1_id}")
                print(f"First record on page 2: DeptId = {page2_id}")
                print(f"Pagination working correctly: {page1_id != page2_id}")
        else:
            print("Pagination returned single records instead of lists")
    else:
        print("Pagination test failed - no data returned")

    print("\n" + "=" * 80)
    print("Test Suite Complete")
    print("=" * 80)
