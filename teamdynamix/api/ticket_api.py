from .teamdynamix_api import TeamDynamixAPI
class TicketAPI(TeamDynamixAPI):
    def get_ticket(self, id):
        """Gets a ticket."""
        return self.get(f'tickets/{id}')

    def get_tickets(self, search_item):
        """Returns a list of active tickets. Will not include full ticket information."""
        data = {"RequestorUids": search_item
        }
        return self.post(f'tickets/search', data)

    def get_active_tickets(self, search_item):
        """Returns a list of active tickets. Will not include full ticket information."""
        data = {"RequestorUids": search_item,
                "StatusIDs": [115,117,121,619,620,622]
        }
        return self.post(f'tickets/search', data)

    def move_ticket(self, id, data):
        """Moves a ticket to a different application."""
        return self.post(f'tickets/{id}/application', data)

    def get_ticket_assets(self, id):
        """Gets the collection of configuration items associated with a ticket."""
        return self.get(f'tickets/{id}/assets')

    def remove_ticket_asset(self, id, asset_id):
        """Removes an asset from a ticket."""
        return self.delete(f'tickets/{id}/assets/{asset_id}')

    def add_ticket_asset(self, id, asset_id):
        """Adds an asset to a ticket."""
        return self.post(f'tickets/{id}/assets/{asset_id}', data={})

    def upload_ticket_attachment(self, id, file, show_view_link=False):
        """Uploads an attachment to a ticket."""
        url = f'tickets/{id}/attachments?showViewLink={show_view_link}'
        files = {'file': file}
        return self.post(url, files=files)

    def add_ticket_children(self, id, data):
        """Adds all of the specified tickets as children to the specified parent ticket."""
        return self.post(f'tickets/{id}/children', data)

    def change_ticket_classification(self, id, new_classification_id):
        """Changes a ticket's classification."""
        return self.put(f'tickets/{id}/classification?newClassificationId={new_classification_id}', data={})

    def get_ticket_configuration_items(self, id):
        """Gets the collection of configuration items associated with a ticket."""
        return self.get(f'tickets/{id}/configurationItems')

    def remove_ticket_configuration_item(self, id, configuration_item_id):
        """Removes a configuration item from a ticket."""
        return self.delete(f'tickets/{id}/configurationItems/{configuration_item_id}')

    def add_ticket_configuration_item(self, id, configuration_item_id):
        """Adds a configuration item to a ticket."""
        data = {
            'configurationItemId': configuration_item_id
        }
        return self.post(f'tickets/{id}/configurationItems/{configuration_item_id}', data=data)

    def get_ticket_contacts(self, id):
        """Gets the ticket contacts."""
        return self.get(f'tickets/{id}/contacts')

    def remove_ticket_contact(self, id, contact_uid):
        """Removes a contact from a ticket."""
        return self.delete(f'tickets/{id}/contacts/{contact_uid}')

    def add_ticket_contact(self, id, contact_uid):
        """Adds a contact to a ticket."""
        return self.post(f'tickets/{id}/contacts/{contact_uid}', data={})

    def get_ticket_feed(self, id):
        """Gets the feed entries for a ticket."""
        return self.get(f'tickets/{id}/feed')

    def update_ticket(self, id, comments, private, commrecord, status=0, cascade=False, notify='null',rich=True):
        """Updates a ticket."""
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

    def set_sla(self, id, sla_data):
        """Sets or changes the ticket's current service level agreement (SLA)."""
        return self.put(f'tickets/{id}/sla', sla_data)

    def remove_sla(self, id):
        """Removes the ticket's current service level agreement (SLA)."""
        return self.put(f'tickets/{id}/sla/delete', data={})

    def get_ticket_workflow(self, id):
        """Gets the currently assigned workflow details for a ticket."""
        return self.get(f'tickets/{id}/workflow')

    def get_ticket_workflow_actions(self, id, step_id):
        """Gets the currently assigned workflow's actions for a ticket for the authenticated user."""
        return self.get(f'tickets/{id}/workflow/actions?stepId={step_id}')

    def approve_workflow_step(self, id, data):
        """Performs the provided action on the provided workflow step."""
        return self.post(f'tickets/{id}/workflow/approve', data)

    def reassign_workflow_step(self, id, data):
        """Reassigns the provided step to the provided resource(s)."""
        return self.post(f'tickets/{id}/workflow/reassign', data)

    def assign_or_reassign_workflow(self, id, new_workflow_id, allow_remove_existing):
        """Assigns or reassigns the workflow to the ticket."""
        return self.put(f'tickets/{id}/workflow?newWorkflowId={new_workflow_id}&allowRemoveExisting={allow_remove_existing}', data={})

    def patch_ticket(self, id, patch_data, notify_new_responsible=False):
        """Patches an existing ticket. This only supports patching the ticket itself and custom attributes."""
        return self.patch(f'tickets/{id}?notifyNewResponsible={notify_new_responsible}', patch_data)

    def edit_ticket(self, id, edit_data, notify_new_responsible=False):
        """Edits an existing ticket."""
        return self.post(f'tickets/{id}?notifyNewResponsible={notify_new_responsible}', edit_data)

    def search_tickets_feed(self, date_from, date_to, reply_count, return_count):
        """Gets the feed items from the Tickets app matching the specified search."""
        return self.get(f'tickets/feed?DateFrom={date_from}&DateTo={date_to}&ReplyCount={reply_count}&ReturnCount={return_count}')

    def get_ticket_forms(self):
        """Gets all active ticket forms for the specified application."""
        return self.get(f'tickets/forms')

    def get_ticket_resources(self, search_text):
        """Gets a list of eligible assignments for the ticketing application."""
        return self.get(f'tickets/resources?searchText={search_text}')

    def search_tickets(self, search_data):
        """Gets a list of tickets. Will not include full ticket information."""
        return self.post(f'tickets/search', search_data)

    def create_ticket(self, ticket_data, notify_requestor, notify_responsible, allow_requestor_creation, enable_notify_reviewer=False, apply_defaults=True):
        """Creates a ticket."""
        return self.post(f'tickets?EnableNotifyReviewer={enable_notify_reviewer}&NotifyRequestor={notify_requestor}&NotifyResponsible={notify_responsible}&AllowRequestorCreation={allow_requestor_creation}&applyDefaults={apply_defaults}', ticket_data)
