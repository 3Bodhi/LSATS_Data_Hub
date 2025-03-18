from .teamdynamix_api import TeamDynamixAPI
from typing import Dict, List, Union, Any, Optional, BinaryIO

class KnowledgeBaseAPI(TeamDynamixAPI):
    def create_article(self, article_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a Knowledge Base article.

        Args:
            article_data: The article data.
        """
        return self.post('knowledgebase', article_data)

    def delete_article(self, article_id: int) -> Dict[str, Any]:
        """
        Deletes a knowledge base article. This cannot be undone.

        Args:
            article_id: The article ID.
        """
        return self.delete(f'knowledgebase/{article_id}')

    def get_article(self, article_id: int) -> Dict[str, Any]:
        """
        Gets a Knowledge Base article.

        Args:
            article_id: The article ID.
        """
        return self.get(f'knowledgebase/{article_id}')

    def update_article(self, article_id: int, article_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Edits an existing article. This will not update the article's permission options,
        but published status can be modified.

        Args:
            article_id: The article ID.
            article_data: The article with updated values.
        """
        return self.put(f'knowledgebase/{article_id}', article_data)

    def get_article_assets_cis(self, article_id: int) -> List[Dict[str, Any]]:
        """
        Gets a list of assets and configuration items associated with the specified article.

        Args:
            article_id: The article ID.
        """
        return self.get(f'knowledgebase/{article_id}/assetscis')

    def add_article_attachment(self, article_id: int, file: BinaryIO, show_view_link: bool = False) -> Dict[str, Any]:
        """
        Adds an attachment to an article.

        Args:
            article_id: The article ID.
            file: The file to upload.
            show_view_link: True if the View link should be shown, otherwise False.
                           This only applies to HTML files.
        """
        url = f'knowledgebase/{article_id}/attachments?showViewLink={show_view_link}'
        files = {'file': file}
        return self.post(url, files=files)

    def get_related_articles(self, article_id: int) -> List[Dict[str, Any]]:
        """
        Gets a list of the knowledge base articles associated with the specified article.

        Args:
            article_id: The article ID.
        """
        return self.get(f'knowledgebase/{article_id}/related')

    def remove_related_article(self, article_id: int, related_article_id: int) -> Dict[str, Any]:
        """
        Removes a relationship between two knowledge base articles.

        Args:
            article_id: The article ID.
            related_article_id: The ID of the related article to remove.
        """
        return self.delete(f'knowledgebase/{article_id}/related/{related_article_id}')

    def add_related_article(self, article_id: int, related_article_id: int) -> Dict[str, Any]:
        """
        Adds a relationship between two knowledge base articles.

        Args:
            article_id: The article ID.
            related_article_id: The ID of the article to associate.
        """
        return self.post(f'knowledgebase/{article_id}/related/{related_article_id}', data={})

    def get_categories(self) -> List[Dict[str, Any]]:
        """
        Gets the categories for the service context.
        """
        return self.get('knowledgebase/categories')

    def create_category(self, category_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a knowledge base category. The permissions will be automatically
        inherited from the parent category.

        Args:
            category_data: The category data.
        """
        return self.post('knowledgebase/categories', category_data)

    def delete_category(self, category_id: int) -> Dict[str, Any]:
        """
        Deletes the specified category. This cannot be undone.

        Args:
            category_id: The category ID.
        """
        return self.delete(f'knowledgebase/categories/{category_id}')

    def get_category(self, category_id: int) -> Dict[str, Any]:
        """
        Gets the specified category.

        Args:
            category_id: The category ID.
        """
        return self.get(f'knowledgebase/categories/{category_id}')

    def update_category(self, category_id: int, category_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Edits the specified category. This will not update the category's permission options.

        Args:
            category_id: The category ID.
            category_data: The category with updated values.
        """
        return self.put(f'knowledgebase/categories/{category_id}', category_data)

    def search_articles(self, search_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Gets a list of Knowledge Base articles. Will not return full article information.
        Certain searching parameters can only be used by individuals with the "View All Articles" permission.

        Args:
            search_params: The searching parameters to use. Note that this is in addition
                          to the standard visibility restrictions enforced for the user.
        """
        return self.post('knowledgebase/search', search_params)
