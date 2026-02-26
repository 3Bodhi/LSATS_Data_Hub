import unittest
from unittest.mock import patch, MagicMock, ANY
import json
from io import BytesIO
from teamdynamix.api.kb_api import KnowledgeBaseAPI

class TestKnowledgeBaseAPI(unittest.TestCase):
    """
    Comprehensive unit tests for the KnowledgeBaseAPI class.

    These tests verify that the KnowledgeBaseAPI methods correctly interact with
    the TeamDynamix Knowledge Base API endpoints.
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create mock for the parent class
        self.mock_team_dynamix_api = patch('teamdynamix.api.kb_api.TeamDynamixAPI').start()

        # Create an instance of KnowledgeBaseAPI
        self.base_url = "https://api.teamdynamix.com"
        self.app_id = "48"
        self.headers = {"Authorization": "Bearer test_token", "Content-Type": "application/json"}
        self.api = KnowledgeBaseAPI(self.base_url, self.app_id, self.headers)

        # Mock common API methods
        self.api.get = MagicMock()
        self.api.post = MagicMock()
        self.api.put = MagicMock()
        self.api.delete = MagicMock()

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        # Stop all patches
        patch.stopall()

    # === Article Management Tests ===

    def test_create_article(self):
        """Test creating a new article."""
        # Test data
        article_data = {
            "Title": "Test Article",
            "Body": "This is a test article.",
            "CategoryID": 123
        }
        expected_response = {
            "ID": 456,
            "Title": "Test Article",
            "Body": "This is a test article.",
            "CategoryID": 123
        }

        # Set up mock response
        self.api.post.return_value = expected_response

        # Call the method
        result = self.api.create_article(article_data)

        # Verify post was called with the correct parameters
        self.api.post.assert_called_once_with('knowledgebase', article_data)

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_delete_article(self):
        """Test deleting an article."""
        # Test data
        article_id = 123
        expected_response = {"Success": True}

        # Set up mock response
        self.api.delete.return_value = expected_response

        # Call the method
        result = self.api.delete_article(article_id)

        # Verify delete was called with the correct parameters
        self.api.delete.assert_called_once_with(f'knowledgebase/{article_id}')

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_get_article(self):
        """Test retrieving an article."""
        # Test data
        article_id = 123
        expected_response = {
            "ID": 123,
            "Title": "Test Article",
            "Body": "This is a test article."
        }

        # Set up mock response
        self.api.get.return_value = expected_response

        # Call the method
        result = self.api.get_article(article_id)

        # Verify get was called with the correct parameters
        self.api.get.assert_called_once_with(f'knowledgebase/{article_id}')

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_update_article(self):
        """Test updating an article."""
        # Test data
        article_id = 123
        article_data = {
            "Title": "Updated Article",
            "Body": "This is an updated article."
        }
        expected_response = {
            "ID": 123,
            "Title": "Updated Article",
            "Body": "This is an updated article."
        }

        # Set up mock response
        self.api.put.return_value = expected_response

        # Call the method
        result = self.api.update_article(article_id, article_data)

        # Verify put was called with the correct parameters
        self.api.put.assert_called_once_with(f'knowledgebase/{article_id}', article_data)

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_get_article_assets_cis(self):
        """Test retrieving assets and configuration items associated with an article."""
        # Test data
        article_id = 123
        expected_response = [
            {"ID": 456, "Name": "Asset 1"},
            {"ID": 789, "Name": "Asset 2"}
        ]

        # Set up mock response
        self.api.get.return_value = expected_response

        # Call the method
        result = self.api.get_article_assets_cis(article_id)

        # Verify get was called with the correct parameters
        self.api.get.assert_called_once_with(f'knowledgebase/{article_id}/assetscis')

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_add_article_attachment(self):
        """Test adding an attachment to an article."""
        # Test data
        article_id = 123
        file_content = b"Test file content"
        file = BytesIO(file_content)
        show_view_link = True
        expected_response = {"ID": 789, "FileName": "test.txt"}

        # Set up mock response
        self.api.post.return_value = expected_response

        # Call the method
        result = self.api.add_article_attachment(article_id, file, show_view_link)

        # Verify post was called with the correct parameters
        expected_url = f'knowledgebase/{article_id}/attachments?showViewLink={show_view_link}'
        expected_files = {'file': file}
        self.api.post.assert_called_once_with(expected_url, files=expected_files)

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_get_related_articles(self):
        """Test retrieving related articles."""
        # Test data
        article_id = 123
        expected_response = [
            {"ID": 456, "Title": "Related Article 1"},
            {"ID": 789, "Title": "Related Article 2"}
        ]

        # Set up mock response
        self.api.get.return_value = expected_response

        # Call the method
        result = self.api.get_related_articles(article_id)

        # Verify get was called with the correct parameters
        self.api.get.assert_called_once_with(f'knowledgebase/{article_id}/related')

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_remove_related_article(self):
        """Test removing a related article relationship."""
        # Test data
        article_id = 123
        related_article_id = 456
        expected_response = {"Success": True}

        # Set up mock response
        self.api.delete.return_value = expected_response

        # Call the method
        result = self.api.remove_related_article(article_id, related_article_id)

        # Verify delete was called with the correct parameters
        self.api.delete.assert_called_once_with(f'knowledgebase/{article_id}/related/{related_article_id}')

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_add_related_article(self):
        """Test adding a related article relationship."""
        # Test data
        article_id = 123
        related_article_id = 456
        expected_response = {"Success": True}

        # Set up mock response
        self.api.post.return_value = expected_response

        # Call the method
        result = self.api.add_related_article(article_id, related_article_id)

        # Verify post was called with the correct parameters
        self.api.post.assert_called_once_with(
            f'knowledgebase/{article_id}/related/{related_article_id}',
            data={}
        )

        # Verify the result
        self.assertEqual(result, expected_response)

    # === Category Management Tests ===

    def test_get_categories(self):
        """Test retrieving all categories."""
        # Test data
        expected_response = [
            {"ID": 123, "Name": "Category 1"},
            {"ID": 456, "Name": "Category 2"}
        ]

        # Set up mock response
        self.api.get.return_value = expected_response

        # Call the method
        result = self.api.get_categories()

        # Verify get was called with the correct parameters
        self.api.get.assert_called_once_with('knowledgebase/categories')

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_create_category(self):
        """Test creating a new category."""
        # Test data
        category_data = {
            "Name": "Test Category",
            "ParentID": 123
        }
        expected_response = {
            "ID": 456,
            "Name": "Test Category",
            "ParentID": 123
        }

        # Set up mock response
        self.api.post.return_value = expected_response

        # Call the method
        result = self.api.create_category(category_data)

        # Verify post was called with the correct parameters
        self.api.post.assert_called_once_with('knowledgebase/categories', category_data)

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_delete_category(self):
        """Test deleting a category."""
        # Test data
        category_id = 123
        expected_response = {"Success": True}

        # Set up mock response
        self.api.delete.return_value = expected_response

        # Call the method
        result = self.api.delete_category(category_id)

        # Verify delete was called with the correct parameters
        self.api.delete.assert_called_once_with(f'knowledgebase/categories/{category_id}')

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_get_category(self):
        """Test retrieving a specific category."""
        # Test data
        category_id = 123
        expected_response = {
            "ID": 123,
            "Name": "Test Category",
            "ParentID": 0
        }

        # Set up mock response
        self.api.get.return_value = expected_response

        # Call the method
        result = self.api.get_category(category_id)

        # Verify get was called with the correct parameters
        self.api.get.assert_called_once_with(f'knowledgebase/categories/{category_id}')

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_update_category(self):
        """Test updating a category."""
        # Test data
        category_id = 123
        category_data = {
            "Name": "Updated Category",
            "ParentID": 456
        }
        expected_response = {
            "ID": 123,
            "Name": "Updated Category",
            "ParentID": 456
        }

        # Set up mock response
        self.api.put.return_value = expected_response

        # Call the method
        result = self.api.update_category(category_id, category_data)

        # Verify put was called with the correct parameters
        self.api.put.assert_called_once_with(f'knowledgebase/categories/{category_id}', category_data)

        # Verify the result
        self.assertEqual(result, expected_response)

    def test_search_articles(self):
        """Test searching for articles."""
        # Test data
        search_params = {
            "SearchText": "test",
            "CategoryIDs": [123, 456],
            "IsPublished": True
        }
        expected_response = [
            {"ID": 789, "Title": "Test Article 1"},
            {"ID": 101, "Title": "Test Article 2"}
        ]

        # Set up mock response
        self.api.post.return_value = expected_response

        # Call the method
        result = self.api.search_articles(search_params)

        # Verify post was called with the correct parameters
        self.api.post.assert_called_once_with('knowledgebase/search', search_params)

        # Verify the result
        self.assertEqual(result, expected_response)

    # === Error Handling Tests ===

    def test_get_article_not_found(self):
        """Test retrieving a non-existent article."""
        # Test data
        article_id = 999

        # Set up mock response
        self.api.get.return_value = None

        # Call the method
        result = self.api.get_article(article_id)

        # Verify get was called with the correct parameters
        self.api.get.assert_called_once_with(f'knowledgebase/{article_id}')

        # Verify the result is None
        self.assertIsNone(result)

    def test_update_article_error(self):
        """Test error handling when updating an article."""
        # Test data
        article_id = 123
        article_data = {
            "Title": "Updated Article",
            "Body": "This is an updated article."
        }

        # Set up mock response
        self.api.put.return_value = None

        # Call the method
        result = self.api.update_article(article_id, article_data)

        # Verify put was called with the correct parameters
        self.api.put.assert_called_once_with(f'knowledgebase/{article_id}', article_data)

        # Verify the result is None
        self.assertIsNone(result)

    def test_create_article_with_empty_data(self):
        """Test creating an article with empty data."""
        # Test data
        article_data = {}

        # Set up mock response
        self.api.post.return_value = None

        # Call the method
        result = self.api.create_article(article_data)

        # Verify post was called with the correct parameters
        self.api.post.assert_called_once_with('knowledgebase', article_data)

        # Verify the result is None
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
