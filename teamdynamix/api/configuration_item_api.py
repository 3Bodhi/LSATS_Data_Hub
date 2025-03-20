import json
from .teamdynamix_api import TeamDynamixAPI
import copy
from typing import Dict, List, Union, Any, Optional, BinaryIO

class ConfigurationItemAPI(TeamDynamixAPI):
    with open('teamdynamix/api/ci_defaults.json', 'r') as file:
        default_config = json.load(file)

    def search_ci(self, ci_name: str) -> List[Dict[str, Any]]:
        """
        Gets a list of configuration items matching the name.

        Args:
            ci_name: The configuration item name to search for.
        """
        data = {"NameLike": ci_name}
        return self.post('cmdb/search', data)  # list of ci dictionary objects

    def get_ci(self, identifier: Union[int, str]) -> Optional[Dict[str, Any]]:
        """
        Gets a configuration item by ID or name.

        Args:
            identifier: The configuration item ID or name.
        """
        if str(identifier).isdigit():
            return self.get(f"cmdb/{identifier}")  # 1 CI dictionary object
        search = self.search_ci(identifier)
        if not search:
            print(f"Bad identifier {identifier}")
            return None
        # NOTE: if multiple CIs have the same name, this will the match with the highest ID!
        def find_exact_match(search, identifier):
            return next((item for item in search if item['Name'] == identifier), None)
        def find_case_insensitive_match(search, identifier):
            return next((item for item in search if item['Name'].lower().strip() == identifier.lower().strip()), None)
        exact_match = find_exact_match(search, identifier)
        if exact_match:
            return exact_match
        case_insensitive_match = find_case_insensitive_match(search, identifier)
        if case_insensitive_match:
            return case_insensitive_match
        print(f"No exact matches to search text, returning {search[0]['Name']}.")
        return search[0]

    def edit_ci(self, fields: Dict[str, Any], identifier: Optional[Union[int, str]] = None) -> Optional[Dict[str, Any]]:
        """
        Edits the specified configuration item.

        Args:
            fields: Dictionary of fields to update.
            identifier: The configuration item ID or name.
        """
        ci = self.get_ci(identifier)
        if ci:
            data = copy.deepcopy(self.default_config)
            if fields == {key: ci[key] for key in fields.keys() if key in ci}:
               print("Configuration Item already up to date!")
               return None
            if not identifier:
                identifier = (ci['ID'])
            data.update(fields)
            return self.put(f"cmdb/{identifier}", data)
        else:
            return None

    def create_ci(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a configuration item.

        Args:
            fields: Dictionary of fields for the new configuration item.
        """
        data = copy.deepcopy(self.default_config)
        fields = fields
        data.update(fields)
        return self.post("/cmdb", data)

    def get_relationships(self, identifier: Union[int, str]) -> List[Dict[str, Any]]:
        """
        Gets a configuration item's relationships.

        Args:
            identifier: The configuration item ID or name.
        """
        id = (self.get_ci(identifier)['ID']) if not str(identifier).isdigit() else identifier
        return self.get(f'cmdb/{id}/relationships')

    def add_relationship(self, ci_id: int, type_id: int, other_item_id: int, is_parent: bool = True, remove_existing: bool = False) -> Dict[str, Any]:
        """
        Adds a relationship between the specified configuration item and another item.

        Args:
            ci_id: The configuration item ID.
            type_id: The ID of the associated relationship type.
            other_item_id: The ID of the other configuration item.
            is_parent: If true, indicates that the configuration item will be the parent.
            remove_existing: If true, will remove existing relationships that match the typeId/isParent combination.
        """
        # ConfigurationItemID is needed for assets
        return self.put(f"cmdb/{ci_id}/relationships?typeId={type_id}&otherItemId={other_item_id}&isParent={is_parent}&removeExisting={remove_existing}", data={})

    def add_asset(self, ci_id: int, asset_id: int) -> Dict[str, Any]:
        """
        Adds an asset relationship to a configuration item.

        Args:
            ci_id: The configuration item ID.
            asset_id: The asset ID.
        """
        return self.add_relationship(ci_id, type_id=10012, other_item_id=asset_id)

    def delete_ci(self, ci_id: int) -> Dict[str, Any]:
        """
        Deletes the specified configuration item. This cannot be undone.

        Args:
            ci_id: The configuration item ID.
        """
        return self.delete(f'cmdb/{ci_id}')

    def get_ci_articles(self, ci_id: int) -> List[Dict[str, Any]]:
        """
        Gets a list of the knowledge base articles associated with the specified configuration item.

        Args:
            ci_id: The configuration item ID.
        """
        return self.get(f'cmdb/{ci_id}/articles')

    def add_article_to_ci(self, ci_id: int, article_id: int) -> Dict[str, Any]:
        """
        Adds an article relationship to the specified configuration item.

        Args:
            ci_id: The configuration item ID.
            article_id: The ID of the article to associate.
        """
        return self.post(f'cmdb/{ci_id}/articles/{article_id}', data={})

    def remove_article_from_ci(self, ci_id: int, article_id: int) -> Dict[str, Any]:
        """
        Removes a relationship between a configuration item and a knowledge base article.

        Args:
            ci_id: The configuration item ID.
            article_id: The ID of the related article to remove.
        """
        return self.delete(f'cmdb/{ci_id}/articles/{article_id}')

    def upload_ci_attachment(self, ci_id: int, file: BinaryIO, show_view_link: bool = False) -> Dict[str, Any]:
        """
        Uploads an attachment to a configuration item.

        Args:
            ci_id: The configuration item ID.
            file: The file to upload.
            show_view_link: True if the View link should be shown, otherwise False.
                           This only applies to HTML files.
        """
        url = f'cmdb/{ci_id}/attachments?showViewLink={show_view_link}'
        files = {'file': file}
        return self.post(url, files=files)

    def get_ci_feed(self, ci_id: int) -> List[Dict[str, Any]]:
        """
        Gets the feed entries for a configuration item.

        Args:
            ci_id: The configuration item ID.
        """
        return self.get(f'cmdb/{ci_id}/feed')

    def add_comment_to_ci(self, ci_id: int, comment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Posts a comment to the configuration item's feed.

        Args:
            ci_id: The configuration item ID.
            comment_data: The update data containing the comment.
        """
        return self.post(f'cmdb/{ci_id}/feed', comment_data)

    def remove_relationship(self, ci_id: int, relationship_id: int) -> Dict[str, Any]:
        """
        Removes a relationship from a configuration item.

        Args:
            ci_id: The configuration item ID.
            relationship_id: The ID of the relationship to remove.
        """
        return self.delete(f'cmdb/{ci_id}/relationships/{relationship_id}')

    def get_ci_tickets(self, ci_id: int) -> List[Dict[str, Any]]:
        """
        Gets the tickets related to a configuration item.

        Args:
            ci_id: The ID of the configuration item.
        """
        return self.get(f'cmdb/{ci_id}/tickets')

    def get_ci_forms(self) -> List[Dict[str, Any]]:
        """
        Gets all active configuration item forms for the specified application.
        """
        return self.get('cmdb/forms')

    def search_ci_advanced(self, search_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Gets a list of configuration items based on advanced search criteria.
        Will not return full configuration item information.

        Args:
            search_params: The searching parameters to use.
        """
        return self.post('cmdb/search', search_params)
