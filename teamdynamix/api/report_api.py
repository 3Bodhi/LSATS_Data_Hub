from .teamdynamix_api import TeamDynamixAPI
from typing import Dict, List, Union, Any, Optional

class ReportAPI(TeamDynamixAPI):
    def get_reports(self) -> List[Dict[str, Any]]:
        """
        Gets a list of all Report Builder reports visible to the user.

        Returns:
            A list of Report Builder reports visible to the current user.

        Note:
            This API is rate-limited to 45 calls per user every 60 seconds.
        """
        return self.get('reports')

    def get_report(self,
                  id: int,
                  withData: bool = False,
                  dataSortExpression: str = '') -> Dict[str, Any]:
        """
        Gets information about a report, optionally including data.

        Args:
            id: The report ID.
            withData: If true, will populate the returned report's collection of rows.
            dataSortExpression: The sorting expression to use for the report's data.
                               Only applicable when data is being retrieved. When not provided,
                               will fall back to the default used for the report.

        Returns:
            A dictionary containing the report information and optionally its data.

        Note:
            This API is rate-limited to 30 calls per user every 60 seconds.
        """
        return self.get(f'reports/{id}?withData={withData}&dataSortExpression={dataSortExpression}')

    def search_reports(self, search_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Gets a list of Report Builder reports visible to the user that match the provided search criteria.

        Args:
            search_data: The searching parameters to use. Should conform to TeamDynamix.Api.Reporting.ReportSearch structure.

        Returns:
            A list of Report Builder reports matching the search criteria and visible to the current user.

        Note:
            This API is rate-limited to 45 calls per user every 60 seconds.
        """
        return self.post('reports/search', search_data)
