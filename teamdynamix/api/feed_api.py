from .teamdynamix_api import TeamDynamixAPI
from typing import Dict, List, Union, Any, Optional

class FeedAPI(TeamDynamixAPI):
    def get_feed_entry(self, id: int) -> Dict[str, Any]:
        """
        Gets a feed entry, including its replies and likes.

        Args:
            id: The ID of the feed entry to retrieve.

        Returns:
            A dictionary containing the feed entry with replies and likes.

        Note:
            This API is rate-limited to 60 calls per IP address every 60 seconds.
        """
        return self.get(f'feed/{id}')
