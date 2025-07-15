from um_api import UMichAPI, create_headers
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

    def get_department_data(self,
                          dept_id: Optional[str] = None,
                          dept_description: Optional[str] = None,
                          dept_group: Optional[str] = None,
                          dept_group_description: Optional[str] = None,
                          pagination: Optional[Dict[str, int]] = None) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
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
            'DeptId': dept_id,
            'DeptDescription': dept_description,
            'DeptGroup': dept_group,
            'DeptGroupDescription': dept_group_description
        }

        # Add pagination if provided
        if pagination:
            if 'count' in pagination:
                params['$count'] = pagination['count']
            if 'start_index' in pagination:
                params['$start_index'] = pagination['start_index']

        query_string = self._build_query_string(params)
        endpoint = f"Department/v2/DeptData"

        if query_string:
            endpoint += f"?{query_string}"

        return self.get(endpoint)

    def get_department_employee_data(self,
                                   empl_id: Optional[str] = None,
                                   uniq_name: Optional[str] = None,
                                   department_id: Optional[str] = None,
                                   dept_description: Optional[str] = None,
                                   pagination: Optional[Dict[str, int]] = None) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Gets department employee data from the UM Department API.

        Args:
            empl_id: Employee Identification Code for filtering.
            uniq_name: UniqName of the Person for filtering.
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
            'EmplId': empl_id,
            'UniqName': uniq_name,
            'DepartmentId': department_id,
            'Dept_Description': dept_description
        }

        # Add pagination if provided
        if pagination:
            if 'count' in pagination:
                params['$count'] = pagination['count']
            if 'start_index' in pagination:
                params['$start_index'] = pagination['start_index']

        query_string = self._build_query_string(params)
        endpoint = f"Department/v2/DeptEmpData"

        if query_string:
            endpoint += f"?{query_string}"

        return self.get(endpoint)

    def get_all_departments(self, max_records: Optional[int] = None) -> List[Dict[str, Any]]:
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
            pagination = {'count': page_size, 'start_index': start_index}
            result = self.get_department_data(pagination=pagination)

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
                break

            # Check if we've hit our max_records limit
            if max_records and len(all_departments) >= max_records:
                all_departments = all_departments[:max_records]
                break

            start_index += page_size

        return all_departments

    def get_all_department_employees(self, max_records: Optional[int] = None) -> List[Dict[str, Any]]:
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
            pagination = {'count': page_size, 'start_index': start_index}
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

if __name__ == "__main__":
    UM_BASE_URL = "https://gw.api.it.umich.edu/um"
    UM_CATEGORY_ID = "bf"
    UM_CLIENT_KEY = "g1ZP399VzZgMpDAPJf9bC1fiRoJU8qANpMmioizIpUOLTofQ"
    UM_CLIENT_SECRET = "8S8dRlYUv8dAOz6Fx180YXohOM4LU8QPgK7FPyx1qLnt5DKNRayKedYKFGypA9Cg"
    SCOPE = "department"
    headers = create_headers(UM_CLIENT_KEY,UM_CLIENT_SECRET,SCOPE)
    department = DepartmentAPI(UM_BASE_URL,UM_CATEGORY_ID,headers)

    print(department.get_department_employee_data(uniq_name="myodhes"))
