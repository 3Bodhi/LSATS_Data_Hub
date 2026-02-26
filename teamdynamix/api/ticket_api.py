from .teamdynamix_api import TeamDynamixAPI
from typing import Dict, List, Union, Any, Optional, BinaryIO

class TicketAPI(TeamDynamixAPI):
    def get_ticket(self, id: int) -> Dict[str, Any]:
        """
        Gets a ticket by ID.

        Args:
            id: The ticket ID.
        """
        return self.get(f'tickets/{id}')

    def get_tickets(self, search_item: List[str]) -> List[Dict[str, Any]]:
        """
        Returns a list of tickets for specified requestors. Will not include full ticket information.

        Args:
            search_item: List of requestor UIDs to search for.
        """
        data = {"RequestorUids": search_item}
        return self.post(f'tickets/search', data)

    def get_active_tickets(self, search_item: List[str]) -> List[Dict[str, Any]]:
        """
        Returns a list of active tickets for specified requestors. Will not include full ticket information.

        Args:
            search_item: List of requestor UIDs to search for.
        """
        data = {"RequestorUids": search_item,
                "StatusIDs": [115, 117, 121, 619, 620, 622]
        }
        return self.post(f'tickets/search', data)

    def move_ticket(self, id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Moves a ticket to a different application.

        Args:
            id: The ticket ID.
            data: The ticket move options.
        """
        return self.post(f'tickets/{id}/application', data)

    def get_ticket_assets(self, id: int) -> List[Dict[str, Any]]:
        """
        Gets the collection of configuration items (assets) associated with a ticket.

        Args:
            id: The ticket ID.
        """
        return self.get(f'tickets/{id}/assets')

    def remove_ticket_asset(self, id: int, asset_id: int) -> Any:
        """
        Removes an asset from a ticket.

        Args:
            id: The ticket ID.
            asset_id: The asset ID to remove.
        """
        return self.delete(f'tickets/{id}/assets/{asset_id}')

    def add_ticket_asset(self, id: int, asset_id: int) -> Any:
        """
        Adds an asset to a ticket.

        Args:
            id: The ticket ID.
            asset_id: The asset ID to add.
        """
        return self.post(f'tickets/{id}/assets/{asset_id}', data={})

    def upload_ticket_attachment(self, id: int, file: BinaryIO, show_view_link: bool = False) -> Dict[str, Any]:
        """
        Uploads an attachment to a ticket.

        Args:
            id: The ticket ID.
            file: The file to upload.
            show_view_link: If true, the View link will be shown for HTML files.
        """
        url = f'tickets/{id}/attachments?showViewLink={show_view_link}'
        files = {'file': file}
        return self.post(url, files=files)

    def add_ticket_children(self, id: int, data: List[int]) -> Any:
        """
        Adds all of the specified tickets as children to the specified parent ticket.

        Args:
            id: The parent ticket ID.
            data: A collection of ticket IDs to be set as children of the specified parent ticket.
        """
        return self.post(f'tickets/{id}/children', data)

    def change_ticket_classification(self, id: int, new_classification_id: int) -> Dict[str, Any]:
        """
        Changes a ticket's classification.

        Args:
            id: The ticket ID.
            new_classification_id: The new classification ID for the ticket.
        """
        return self.put(f'tickets/{id}/classification?newClassificationId={new_classification_id}', data={})

    def get_ticket_configuration_items(self, id: int) -> List[Dict[str, Any]]:
        """
        Gets the collection of configuration items associated with a ticket.

        Args:
            id: The ticket ID.
        """
        return self.get(f'tickets/{id}/configurationItems')

    def remove_ticket_configuration_item(self, id: int, configuration_item_id: int) -> Any:
        """
        Removes a configuration item from a ticket.

        Args:
            id: The ticket ID.
            configuration_item_id: The configuration item ID to remove.
        """
        return self.delete(f'tickets/{id}/configurationItems/{configuration_item_id}')

    def add_ticket_configuration_item(self, id: int, configuration_item_id: int) -> Any:
        """
        Adds a configuration item to a ticket.

        Args:
            id: The ticket ID.
            configuration_item_id: The configuration item ID to add.
        """
        data = {
            'configurationItemId': configuration_item_id
        }
        return self.post(f'tickets/{id}/configurationItems/{configuration_item_id}', data=data)

    def get_ticket_contacts(self, id: int) -> List[Dict[str, Any]]:
        """
        Gets the ticket contacts.

        Args:
            id: The ticket ID.
        """
        return self.get(f'tickets/{id}/contacts')

    def remove_ticket_contact(self, id: int, contact_uid: str) -> Any:
        """
        Removes a contact from a ticket.

        Args:
            id: The ticket ID.
            contact_uid: The UID of the contact to remove.
        """
        return self.delete(f'tickets/{id}/contacts/{contact_uid}')

    def add_ticket_contact(self, id: int, contact_uid: str) -> Any:
        """
        Adds a contact to a ticket.

        Args:
            id: The ticket ID.
            contact_uid: The UID of the contact to add.
        """
        return self.post(f'tickets/{id}/contacts/{contact_uid}', data={})

    def get_ticket_feed(self, id: int) -> List[Dict[str, Any]]:
        """
        Gets the feed entries for a ticket.

        Args:
            id: The ticket ID.
        """
        return self.get(f'tickets/{id}/feed')

    def update_ticket(self, id: int, comments: str, private: bool, commrecord: bool, status: int = 0,
                     cascade: bool = False, notify: List[str] = ['null'], rich: bool = True) -> Dict[str, Any]:
        """
        Updates a ticket by adding a new feed entry.

        Args:
            id: The ticket ID.
            comments: The comments to add to the ticket feed.
            private: If true, the comments will be marked as private.
            commrecord: If true, the update will be marked as a communication record.
            status: The new status ID for the ticket.
            cascade: If true, the status change will cascade to child tickets.
            notify: Recipients to notify about the update.
            rich: If true, the comments will be treated as rich HTML content.
        """
        data = {
            "NewStatusID": status,
            "CascadeStatus": cascade,
            "Comments": comments,
            "Notify": notify,
            "IsPrivate": private,
            "IsRichHTML": rich,
            "IsCommunication": commrecord
        }
        return self.post(f'tickets/{id}/feed', data)

    def set_sla(self, id: int, sla_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sets or changes the ticket's current service level agreement (SLA).

        Args:
            id: The ticket ID.
            sla_data: The service level agreement (SLA) assignment options.
        """
        return self.put(f'tickets/{id}/sla', sla_data)

    def remove_sla(self, id: int) -> Dict[str, Any]:
        """
        Removes the ticket's current service level agreement (SLA).

        Args:
            id: The ticket ID.
        """
        return self.put(f'tickets/{id}/sla/delete', data={})

    def get_ticket_workflow(self, id: int) -> Dict[str, Any]:
        """
        Gets the currently assigned workflow details for a ticket.

        Args:
            id: The ticket ID.
        """
        return self.get(f'tickets/{id}/workflow')

    def get_ticket_workflow_actions(self, id: int, step_id: str) -> List[Dict[str, Any]]:
        """
        Gets the currently assigned workflow's actions for a ticket for the authenticated user.

        Args:
            id: The ticket ID.
            step_id: The workflow step ID.
        """
        return self.get(f'tickets/{id}/workflow/actions?stepId={step_id}')

    def approve_workflow_step(self, id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Performs the provided action on the provided workflow step.

        Args:
            id: The ticket ID.
            data: The workflow step approval request data.
        """
        return self.post(f'tickets/{id}/workflow/approve', data)

    def reassign_workflow_step(self, id: int, data: Dict[str, Any]) -> Any:
        """
        Reassigns the provided step to the provided resource(s).

        Args:
            id: The ticket ID.
            data: The workflow step reassignment request data.
        """
        return self.post(f'tickets/{id}/workflow/reassign', data)

    def assign_or_reassign_workflow(self, id: int, new_workflow_id: int, allow_remove_existing: bool) -> Dict[str, Any]:
        """
        Assigns or reassigns the workflow to the ticket.

        Args:
            id: The ticket ID.
            new_workflow_id: The workflow ID to assign.
            allow_remove_existing: If true, will remove existing workflow if assigned and assign the specified workflow.
        """
        return self.put(f'tickets/{id}/workflow?newWorkflowId={new_workflow_id}&allowRemoveExisting={allow_remove_existing}', data={})

    def patch_ticket(self, id: int, patch_data: Dict[str, Any], notify_new_responsible: bool = False) -> Dict[str, Any]:
        """
        Patches an existing ticket. This only supports patching the ticket itself and custom attributes.

        Args:
            id: The ticket ID.
            patch_data: The patch document containing changes to apply to the ticket.
            notify_new_responsible: If true, will notify the newly-responsible resource(s) if responsibility is changed.
        """
        return self.patch(f'tickets/{id}?notifyNewResponsible={notify_new_responsible}', patch_data)

    def edit_ticket(self, id: int, edit_data: Dict[str, Any], notify_new_responsible: bool = False) -> Dict[str, Any]:
        """
        Edits an existing ticket.

        Args:
            id: The ticket ID.
            edit_data: The ticket with updated values.
            notify_new_responsible: If true, will notify the newly-responsible resource(s) if responsibility is changed.
        """
        return self.post(f'tickets/{id}?notifyNewResponsible={notify_new_responsible}', edit_data)

    def search_tickets_feed(self, date_from: str, date_to: str, reply_count: int, return_count: int) -> Dict[str, Any]:
        """
        Gets the feed items from the Tickets app matching the specified search.

        Args:
            date_from: The start date for the search.
            date_to: The end date for the search.
            reply_count: The number of replies to include.
            return_count: The maximum number of feed items to return.
        """
        return self.get(f'tickets/feed?DateFrom={date_from}&DateTo={date_to}&ReplyCount={reply_count}&ReturnCount={return_count}')

    def get_ticket_forms(self) -> List[Dict[str, Any]]:
        """
        Gets all active ticket forms for the specified application.
        """
        return self.get(f'tickets/forms')

    def get_ticket_resources(self, search_text: str) -> List[Dict[str, Any]]:
        """
        Gets a list of eligible assignments for the ticketing application.

        Args:
            search_text: The search text to use for finding resources.
        """
        return self.get(f'tickets/resources?searchText={search_text}')

    def search_tickets(self, search_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Gets a list of tickets matching the specified search criteria.
        Will not include full ticket information.

        Args:
            search_data: The search parameters to use.
        """
        return self.post(f'tickets/search', search_data)

    def create_ticket(self, ticket_data: Dict[str, Any], notify_requestor: bool,
                     notify_responsible: bool, allow_requestor_creation: bool,
                     enable_notify_reviewer: bool = False, apply_defaults: bool = True) -> Dict[str, Any]:
        """
        Creates a ticket.

        Args:
            ticket_data: The ticket data to create.
            notify_requestor: If true, will notify the requestor about the ticket creation.
            notify_responsible: If true, will notify the responsible resource(s) about the ticket creation.
            allow_requestor_creation: If true, allows the requestor to create the ticket.
            enable_notify_reviewer: If true, will notify the reviewer.
            apply_defaults: If true, will apply default values for properties that are not specified.
        """
        return self.post(f'tickets?EnableNotifyReviewer={enable_notify_reviewer}&NotifyRequestor={notify_requestor}&NotifyResponsible={notify_responsible}&AllowRequestorCreation={allow_requestor_creation}&applyDefaults={apply_defaults}', ticket_data)
