from ..api.teamdynamix_api import TeamDynamixAPI, create_headers
from ..api.asset_api import AssetAPI
from ..api.user_api import UserAPI
from ..api.account_api import AccountAPI
from ..api.configuration_item_api import ConfigurationItemAPI
from ..api.ticket_api import TicketAPI
from ..api.feed_api import FeedAPI
from ..api.group_api import GroupAPI
from ..api.kb_api import KnowledgeBaseAPI
from ..api.report_api import ReportAPI
import datetime

class TeamDynamixFacade:
    def __init__(self, base_url, app_id, api_token):
        headers = create_headers(api_token)
        self.users = UserAPI(base_url, "", headers)
        self.assets = AssetAPI(base_url, app_id, headers)
        self.accounts = AccountAPI(base_url, "", headers)
        self.configuration_items = ConfigurationItemAPI(base_url, app_id, headers)
        self.tickets = TicketAPI(base_url, 46, headers)
        self.feed = FeedAPI(base_url, "", headers)
        self.groups = GroupAPI(base_url, "", headers)
        self.knowledge_base = KnowledgeBaseAPI(base_url, app_id, headers)
        self.reports = ReportAPI(base_url, "", headers)

    def get_user_assets_by_uniqname(self, uniqname):
        user_id = self.users.get_user_attribute(uniqname,'UID')
        if user_id:
            return self.assets.get_assets([user_id])
        else:
            return None

    def get_user_tickets_by_uniqname(self, uniqname):
        user_id = self.users.get_user_attribute(uniqname,'UID')
        if user_id:
            return self.tickets.get_tickets([user_id])
        else:
            return None

    def get_dept_users(self, dept_id):
        data = {"AccountIDs": dept_id }
        self.users.search_user(data)

    def create_lab(self, pi):
        def create_lab_CI(assets):
            lab = self.configuration_items.create_ci({
                'Name':f"{(pi).title()} Lab",
                'OwnerUID': assets[0]['OwningCustomerID'],
                'OwningDepartmentID': assets[0]['OwningDepartmentID'],
                'LocationID': assets[0]['LocationID']
            })
            print(f"{lab['Name']} created with ID {lab['ID']}" )
            return lab

        def add_assets(ci, assets):
            configurationIDs = [asset['ConfigurationItemID'] for asset in assets]
            for id in configurationIDs:
                self.configuration_items.add_asset(ci['ID'], id)
                print(f"Added asset {id} to {ci['Name']}")
            return ci

        def add_tickets(ci, tickets):
            if tickets:
                for ticket in tickets:
                    self.tickets.add_ticket_configuration_item(ticket['ID'], ci['ID'])
                    print(f"Added ticket '{ticket['Title']}' to {ci['Name']}")

        assets = self.get_user_assets_by_uniqname(pi)
        tickets = self.get_user_tickets_by_uniqname(pi)
        lab = create_lab_CI(assets)
        add_assets(lab, assets)
        add_tickets(lab, tickets)

    def get_ticket_last_activity(self, ticket_id: str) -> datetime.datetime:
        """
        Gets the timestamp of the most recent activity on a ticket from any user.

        Args:
            ticket_id: The TeamDynamix ticket ID

        Returns:
            datetime: The datetime of the most recent activity, or None if no activity found
        """

        # First, get basic ticket info which includes ModifiedDate
        ticket = self.tickets.get_ticket(ticket_id)
        if not ticket:
            return None

        # Get the complete feed history
        feed = self.tickets.get_ticket_feed(ticket_id)
        if not feed:
            # If feed can't be retrieved, fall back to the ticket's modified date
            modified_date = ticket.get('ModifiedDate')
            if modified_date:
                return datetime.datetime.fromisoformat(modified_date.replace('Z', '+00:00'))
            return None

        # Find the most recent feed entry timestamp
        if feed:
            # Ensure we're sorting by the correct date field (may vary based on API)
            latest_entry = max(feed, key=lambda x: x.get('CreatedDate', ''))
            created_date = latest_entry.get('CreatedDate')
            if created_date:
                return datetime.datetime.fromisoformat(created_date.replace('Z', '+00:00'))

        return None

    def get_last_requestor_response(self, ticket_id: str, requestor_name: str = None) -> datetime.datetime:
        """
        Gets the timestamp of the most recent response from the ticket requestor.
        Also checks replies to feed entries, not just top-level entries.

        Args:
            ticket_id: The TeamDynamix ticket ID
            requestor_name: Optional requestor name to filter responses by. If None, uses ticket requestor.

        Returns:
            datetime: The datetime of the most recent requestor response, or None if no response found
        """

        # First, get basic ticket info to identify the requestor if name not provided
        ticket = self.tickets.get_ticket(ticket_id)
        if not ticket:
            return None

        # If requestor_name not provided, get it from the ticket
        if not requestor_name:
            requestor_name = ticket.get('RequestorName')

        if not requestor_name:
            return None

        # Quick check: if the last person to modify the ticket was the requestor,
        # we can just use the ModifiedDate without needing to get the feed
        if ticket.get('ModifiedFullName') == requestor_name:
            modified_date = ticket.get('ModifiedDate')
            if modified_date:
                return datetime.datetime.fromisoformat(modified_date.replace('Z', '+00:00'))

        # Get the complete feed history
        feed = self.tickets.get_ticket_feed(ticket_id)
        if not feed:
            return None

        # Filter entries to include comments from the requestor (both top-level and replies)
        requestor_entries = []

        for entry in feed:
            # Check if this entry is from the requestor
            is_from_requestor = entry.get('CreatedFullName') == requestor_name

            if is_from_requestor:
                requestor_entries.append(entry)

            # Check if the entry has replies by comparing LastUpdatedDate with CreatedDate
            last_updated = entry.get('LastUpdatedDate')
            created_date = entry.get('CreatedDate')

            if last_updated and created_date and last_updated != created_date:
                # This entry might have replies, so get the full entry with replies
                entry_id = entry.get('ID')
                if entry_id:
                    try:
                        # Get the full feed entry with replies
                        detailed_entry = self.feed.get_feed_entry(entry_id)

                        if detailed_entry and 'Replies' in detailed_entry:
                            replies = detailed_entry.get('Replies', [])

                            # Check each reply to see if it's from the requestor
                            for reply in replies:
                                if reply.get('CreatedFullName') == requestor_name:
                                    # Create a dictionary with necessary fields for the max() function later
                                    requestor_reply = {
                                        'CreatedDate': reply.get('CreatedDate'),
                                        'CreatedFullName': reply.get('CreatedFullName')
                                    }
                                    requestor_entries.append(requestor_reply)
                    except Exception as e:
                        # If we can't get replies, continue with what we have
                        print(f"Error getting replies for entry {entry_id}: {str(e)}")
                        continue

        # Get most recent entry timestamp
        if requestor_entries:
            latest_response = max(requestor_entries, key=lambda x: x.get('CreatedDate', ''))
            created_date = latest_response.get('CreatedDate')
            if created_date:
                return datetime.datetime.fromisoformat(created_date.replace('Z', '+00:00'))

        return None

    def days_since_requestor_response(self, ticket_id: str, requestor_name: str = None) -> int:
        """
        Calculates the number of days since the last response from the requestor or specific user.
        Uses date-only comparison (ignores hours, minutes, seconds).

        Args:
            ticket_id: The TeamDynamix ticket ID
            requestor_name: Optional requestor name to filter responses by. If None, uses ticket requestor.

        Returns:
            int: Number of days since last requestor response (date-only comparison)
            If the requestor has never responded, returns float('inf')
        """

        last_response = self.get_last_requestor_response(ticket_id, requestor_name)

        if last_response is None:
            # No response found
            return float('inf')

        # Convert to date objects (removing time components) for day-level comparison
        last_response_date = last_response.date()
        today_date = datetime.datetime.now(datetime.timezone.utc).date()

        # Calculate days between dates
        days_since = (today_date - last_response_date).days
        return days_since

    def days_since_any_activity(self, ticket_id: str) -> int:
        """
        Calculates the number of days since any activity on the ticket.
        Uses date-only comparison (ignores hours, minutes, seconds).

        Args:
            ticket_id: The TeamDynamix ticket ID

        Returns:
            int: Number of days since last activity (date-only comparison), or None if no activity found
        """

        last_activity = self.get_ticket_last_activity(ticket_id)

        if last_activity is None:
            return None

        # Convert to date objects (removing time components) for day-level comparison
        last_activity_date = last_activity.date()
        today_date = datetime.datetime.now(datetime.timezone.utc).date()

        # Calculate days between dates
        days_since = (today_date - last_activity_date).days
        return days_since

    def get_ticket_feed_by_user(self, ticket_id: str, user_name: str = None) -> list:
        """
        Gets all feed entries from a specific user, or all entries if no name provided.

        Args:
            ticket_id: The TeamDynamix ticket ID
            user_name: Optional full name to filter entries by

        Returns:
            list: Feed entries from the specified user, or all entries if no name provided
        """
        # Get the complete feed history
        feed = self.tickets.get_ticket_feed(ticket_id)
        if not feed or not user_name:
            return feed

        # Filter entries by the provided name
        user_entries = []

        for entry in feed:
            # Check if this entry is from the specified user
            is_from_user = entry.get('CreatedFullName') == user_name

            if is_from_user:
                user_entries.append(entry)

        return user_entries

    def get_full_feed(self, ticket_id: str) -> list:
        """
        Gets the full feed entries for a ticket, including detailed information for each entry.

        This function first retrieves the ticket feed, then extracts the URI from each entry,
        strips the digit values from each URI, and uses those digits to fetch the complete
        feed entry details including replies and other detailed information.

        Args:
            ticket_id: The TeamDynamix ticket ID

        Returns:
            list: List of full feed entry objects with detailed information,
            or empty list if no feed entries found
        """
        # Get the ticket feed
        feed = self.tickets.get_ticket_feed(ticket_id)
        if not feed:
            return []

        full_feed_entries = []

        for entry in feed:
            # Extract the URI and get digits from it
            uri = entry.get('Uri', '')
            if uri:
                # Extract digits from the URI
                digits = ''.join(c for c in uri if c.isdigit())
                if digits:
                    try:
                        # Get the full feed entry using the extracted digits
                        feed_id = int(digits)
                        full_entry = self.feed.get_feed_entry(feed_id)
                        if full_entry:
                            full_feed_entries.append(full_entry)
                    except (ValueError, Exception) as e:
                        # Continue processing other entries if one fails
                        print(f"Error getting feed entry for URI {uri}: {str(e)}")
                        continue

        return full_feed_entries
