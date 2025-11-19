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
import re
from html import unescape
from typing import Optional

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

    def get_conversation(self, ticket_id: str,
                                    exclude_system: bool = True,
                                    max_messages: Optional[int] = None,
                                    recent_days: Optional[int] = None,
                                    merge_consecutive: bool = False) -> list:
        """
        Gets a flattened conversation optimized for LLM consumption.

        Args:
            ticket_id: The TeamDynamix ticket ID
            exclude_system: Remove system-generated messages (default: True)
            max_messages: Limit to most recent N messages
            recent_days: Only include messages from last N days
            merge_consecutive: Merge consecutive messages from same sender

        Returns:
            list: Optimized conversation for LLM processing
        """
        full_feed = self.get_full_feed(ticket_id)
        if not full_feed:
            return []

        conversation = []

        for entry in full_feed:
            # Process main entry
            if not (exclude_system and self._is_system_message(entry)):
                main_entry = {
                    "sender": entry.get('CreatedFullName', 'Unknown'),
                    "timestamp": entry.get('CreatedDate', ''),
                    "message": self._clean_html_message(entry.get('Body', ''))
                }
                conversation.append(main_entry)

            # Process replies
            replies = entry.get('Replies', [])
            for reply in replies:
                if not (exclude_system and self._is_system_message(reply)):
                    reply_entry = {
                        "sender": reply.get('CreatedFullName', 'Unknown'),
                        "timestamp": reply.get('CreatedDate', ''),
                        "message": self._clean_html_message(reply.get('Body', ''))
                    }
                    conversation.append(reply_entry)

        # Sort chronologically
        conversation.sort(key=lambda x: x['timestamp'])

        # Apply filters
        if recent_days:
            conversation = self._filter_by_recent_days(conversation, recent_days)

        if merge_consecutive:
            conversation = self._merge_consecutive_messages(conversation)

        if max_messages:
            conversation = conversation[-max_messages:]  # Keep most recent

        return conversation

    def get_conversation_text(self, ticket_id: str,
                                include_timestamps: bool = False,
                                exclude_system: bool = True,
                                max_messages: Optional[int] = None) -> str:
        """
        Gets conversation as clean text format optimized for LLM processing.

        This format is most natural for LLMs to process and uses fewer tokens
        than structured JSON format.

        Returns:
            str: Clean conversation text like "User: message\n\nStaff: response\n\n..."
        """
        conversation = self.get_conversation(
            ticket_id=ticket_id,
            exclude_system=exclude_system,
            max_messages=max_messages,
            merge_consecutive=True
        )

        if not conversation:
            return ""

        text_parts = []
        for msg in conversation:
            timestamp_part = f" ({msg['timestamp']})" if include_timestamps else ""
            text_parts.append(f"{msg['sender']}{timestamp_part}: {msg['message']}")

        return "\n\n".join(text_parts)

    def get_contextual_summary(self, ticket_id: str, recent_count: int = 15) -> dict:
        """
        Gets conversation with summary of older messages for context efficiency.

        Perfect for long tickets where you need context but want to save tokens.
        """
        full_conversation = self.get_conversation(
            ticket_id=ticket_id,
            exclude_system=True
        )

        if len(full_conversation) <= recent_count:
            return {
                "summary": None,
                "recent_messages": full_conversation,
                "total_messages": len(full_conversation)
            }

        older_messages = full_conversation[:-recent_count]
        recent_messages = full_conversation[-recent_count:]

        # Create a simple summary of older messages
        participants = set(msg['sender'] for msg in older_messages)
        date_range = f"{older_messages[0]['timestamp'][:10]} to {older_messages[-1]['timestamp'][:10]}"

        summary = (f"Earlier conversation ({len(older_messages)} messages from "
                    f"{date_range}) involved {', '.join(participants)}. "
                    f"Key topics discussed in the initial messages.")

        return {
            "summary": summary,
            "recent_messages": recent_messages,
            "total_messages": len(full_conversation)
        }

    def _is_system_message(self, entry: dict) -> bool:
        """Identify system-generated messages."""
        sender = entry.get('CreatedFullName', '').lower()
        message = entry.get('Body', '').lower()

        # Common system message patterns
        # need to verify these to ensure they do not cut too much
        system_indicators = [
            'system',
            'teamdynamix',
            'automated',
            'changed status from',
            'assigned to',
            'priority changed',
            'due date',
            'automatically took this ticket'
        ]

        return (sender == 'system' or
                any(indicator in message for indicator in system_indicators))

    def _filter_by_recent_days(self, conversation: list, days: int) -> list:
        """Filter messages to only recent days."""
        import datetime
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        cutoff_str = cutoff.isoformat()

        return [msg for msg in conversation if msg['timestamp'] >= cutoff_str]

    def _merge_consecutive_messages(self, conversation: list) -> list:
        """Merge consecutive messages from the same sender."""
        if not conversation:
            return conversation

        merged = []
        current_msg = conversation[0].copy()

        for next_msg in conversation[1:]:
            if (next_msg['sender'] == current_msg['sender'] and
                self._messages_within_timeframe(current_msg['timestamp'],
                                                next_msg['timestamp'],
                                                minutes=30)):
                # Merge messages
                current_msg['message'] += f"\n\n{next_msg['message']}"
                current_msg['timestamp'] = next_msg['timestamp']  # Use latest timestamp
            else:
                merged.append(current_msg)
                current_msg = next_msg.copy()

        merged.append(current_msg)
        return merged

    def _messages_within_timeframe(self, timestamp1: str, timestamp2: str, minutes: int = 30) -> bool:
        """Check if two timestamps are within specified minutes of each other."""
        try:
            import datetime
            dt1 = datetime.datetime.fromisoformat(timestamp1.replace('Z', '+00:00'))
            dt2 = datetime.datetime.fromisoformat(timestamp2.replace('Z', '+00:00'))
            return abs((dt2 - dt1).total_seconds()) <= (minutes * 60)
        except:
            return False

        def _clean_html_message(self, html_content: str) -> str:
            """
            Clean HTML content and convert it to readable plain text.

            Args:
                html_content: HTML content string

            Returns:
                str: Cleaned plain text message
            """
            if not html_content:
                return ""

            # Remove HTML tags
            clean_text = re.sub(r'<[^>]+>', '', html_content)

            # Convert HTML entities to regular characters
            clean_text = unescape(clean_text)

            # Replace multiple whitespace/newlines with single spaces
            clean_text = re.sub(r'\s+', ' ', clean_text)

            # Remove leading/trailing whitespace
            clean_text = clean_text.strip()

            # Convert common patterns back to readable format
            clean_text = clean_text.replace('\\n', '\n')

            return clean_text

    def _clean_html_message(self, html_content: str) -> str:
        """
        Clean HTML content and convert it to readable plain text.

        Args:
            html_content: HTML content string

        Returns:
            str: Cleaned plain text message
        """
        if not html_content:
            return ""

        # Remove HTML tags
        clean_text = re.sub(r'<[^>]+>', '', html_content)

        # Convert HTML entities to regular characters
        clean_text = unescape(clean_text)

        # Replace multiple whitespace/newlines with single spaces
        clean_text = re.sub(r'\s+', ' ', clean_text)

        # Remove leading/trailing whitespace
        clean_text = clean_text.strip()

        # Convert common patterns back to readable format
        clean_text = clean_text.replace('\\n', '\n')

        return clean_text
