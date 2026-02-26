from .teamdynamix_api import TeamDynamixAPI
from typing import Dict, List, Union, Any, Optional, BinaryIO

class AssetAPI(TeamDynamixAPI):
    def get_asset(self, asset_id: int) -> Dict[str, Any]:
        """
        Gets an asset by ID.

        Args:
            asset_id: The asset ID.
        """
        return self.get(f'assets/{asset_id}')

    def get_assets(self, search_item: List[str], search_by: str = 'owner') -> List[Dict[str, Any]]:
        """
        Gets a list of assets based on search criteria.

        Args:
            search_item: List of identifiers to search for.
            search_by: Type of search to perform ('owner' or 'shortcode').
        """
        if search_by == 'shortcode':
            attribute_id = 3513  # custom attribute # for shortcode
            data = {
                "CustomAttributes": [{
                   "ID": attribute_id,  # shortcode
                   "Value": search_item
                   }]
            }
        else:  # If Owner
            data = {"OwningCustomerIDs": search_item}
        return self.post(f'assets/search', data)

    def get_asset_attribute(self, asset_id: int, attribute: str, custom: bool = False) -> Any:
        """
        Gets a specific attribute of an asset.

        Args:
            asset_id: The asset ID.
            attribute: The name of the attribute to retrieve.
            custom: Whether the attribute is a custom attribute.
        """
        asset = self.get_asset
        return NotImplemented

    def search_asset(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Gets a list of assets based on search criteria. Will not return full asset information.

        Args:
            data: The searching parameters to use.
        """
        return self.post('assets/search', data)

    def add_asset(self, asset_id: int, ticket_id: int) -> Dict[str, Any]:
        """
        Adds an asset to a ticket.

        Args:
            asset_id: The asset ID.
            ticket_id: The ticket ID.
        """
        return self.post(f'/assets/{asset_id}/tickets/{ticket_id}')

    def create_asset(self, asset_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a new asset.

        Args:
            asset_data: The asset data.
        """
        return self.post('assets', asset_data)

    def delete_asset(self, asset_id: int) -> Dict[str, Any]:
        """
        Deletes the specified asset. This cannot be undone.

        Args:
            asset_id: The asset ID.
        """
        return self.delete(f'assets/{asset_id}')

    def update_asset(self, asset_id: int, asset_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Edits an existing asset.

        Args:
            asset_id: The asset ID.
            asset_data: The asset with updated values.
        """
        return self.post(f'assets/{asset_id}', asset_data)

    def patch_asset(self, asset_id: int, patch_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Patches an existing asset. This only supports patching the asset itself and custom attributes.

        Args:
            asset_id: The asset ID.
            patch_data: The patch document containing changes to apply to the asset.
        """
        return self.patch(f'assets/{asset_id}', patch_data)

    def get_asset_articles(self, asset_id: int) -> List[Dict[str, Any]]:
        """
        Gets a list of the knowledge base articles associated with the specified asset.

        Args:
            asset_id: The asset ID.
        """
        return self.get(f'assets/{asset_id}/articles')

    def add_article_to_asset(self, asset_id: int, article_id: int) -> Dict[str, Any]:
        """
        Adds an article relationship to the specified asset.

        Args:
            asset_id: The asset ID.
            article_id: The ID of the article to associate.
        """
        return self.post(f'assets/{asset_id}/articles/{article_id}', data={})

    def remove_article_from_asset(self, asset_id: int, article_id: int) -> Dict[str, Any]:
        """
        Removes a relationship between an asset and a knowledge base article.

        Args:
            asset_id: The asset ID.
            article_id: The ID of the related article to remove.
        """
        return self.delete(f'assets/{asset_id}/articles/{article_id}')

    def get_asset_contracts(self, asset_id: int) -> List[Dict[str, Any]]:
        """
        Gets a list of contracts associated to an asset.

        Args:
            asset_id: The asset ID.
        """
        return self.get(f'assets/{asset_id}/associatedcontracts')

    def add_contract_to_asset(self, asset_id: int, contract_id: int) -> Dict[str, Any]:
        """
        Associates a contract to an asset.

        Args:
            asset_id: The asset ID.
            contract_id: The ID of the contract to associate to this asset.
        """
        return self.post(f'assets/{asset_id}/associatedcontracts/{contract_id}', data={})

    def remove_contract_from_asset(self, asset_id: int, contract_id: int) -> Dict[str, Any]:
        """
        Removes a contract from an asset.

        Args:
            asset_id: The asset ID.
            contract_id: The ID of the contract to remove from this asset.
        """
        return self.delete(f'assets/{asset_id}/associatedcontracts/{contract_id}')

    def update_asset_contract(self, asset_id: int, contract_id: int, contract_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Edits an asset's contract dates for the specified asset-contract association.

        Args:
            asset_id: The asset ID.
            contract_id: The ID of the contract to edit asset-contract dates for.
            contract_data: The asset-contract object with edited dates for a Sliding date model contract.
        """
        return self.put(f'assets/{asset_id}/associatedcontracts/{contract_id}', contract_data)

    def upload_asset_attachment(self, asset_id: int, file: BinaryIO, show_view_link: bool = False) -> Dict[str, Any]:
        """
        Uploads an attachment to an asset.

        Args:
            asset_id: The asset ID.
            file: The file to upload.
            show_view_link: Whether to show the View link for HTML files.
        """
        url = f'assets/{asset_id}/attachments?showViewLink={show_view_link}'
        files = {'file': file}
        return self.post(url, files=files)

    def get_asset_feed(self, asset_id: int) -> List[Dict[str, Any]]:
        """
        Gets the feed entries for an asset.

        Args:
            asset_id: The asset ID.
        """
        return self.get(f'assets/{asset_id}/feed')

    def add_comment_to_asset(self, asset_id: int, comment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add a comment to an asset.

        Args:
            asset_id: The asset ID.
            comment_data: The item update containing the comment.
        """
        return self.post(f'assets/{asset_id}/feed', comment_data)

    def remove_ticket_from_asset(self, asset_id: int, ticket_id: int) -> Dict[str, Any]:
        """
        Removes a ticket from an asset.

        Args:
            asset_id: The asset ID.
            ticket_id: The ticket ID.
        """
        return self.delete(f'assets/{asset_id}/tickets/{ticket_id}')

    def get_asset_users(self, asset_id: int) -> List[Dict[str, Any]]:
        """
        Gets the asset resources (users).

        Args:
            asset_id: The asset ID.
        """
        return self.get(f'assets/{asset_id}/users')

    def add_user_to_asset(self, asset_id: int, resource_id: str) -> Dict[str, Any]:
        """
        Adds a resource (user) to asset.

        Args:
            asset_id: The asset ID.
            resource_id: The resource ID.
        """
        return self.post(f'assets/{asset_id}/users/{resource_id}', data={})

    def remove_user_from_asset(self, asset_id: int, resource_id: str) -> Dict[str, Any]:
        """
        Removes a resource (user) from an asset.

        Args:
            asset_id: The asset ID.
            resource_id: The resource ID.
        """
        return self.delete(f'assets/{asset_id}/users/{resource_id}')

    def get_asset_feed_items(self, date_from: str, date_to: str, reply_count: int, return_count: int) -> Dict[str, Any]:
        """
        Gets feed items for an asset application feed matching the specified search.

        Args:
            date_from: The start date for the feed search.
            date_to: The end date for the feed search.
            reply_count: The number of replies to return.
            return_count: The number of feed items to return.
        """
        return self.get(f'assets/feed?DateFrom={date_from}&DateTo={date_to}&ReplyCount={reply_count}&ReturnCount={return_count}')

    def get_asset_forms(self) -> List[Dict[str, Any]]:
        """
        Gets all active asset forms for the specified application.
        """
        return self.get('assets/forms')

    def import_assets(self, import_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Performs a bulk insert/update of assets in the system.

        Args:
            import_data: The collection of items that are being imported and the corresponding import settings.
        """
        return self.post('assets/import', import_data)
