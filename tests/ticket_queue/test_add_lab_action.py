"""
Unit tests for AddLabAction.

Tests lab CI detection and addition based on requestor membership and asset ownership.
"""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from scripts.ticket_queue.actions.add_lab_action import AddLabAction


class TestAddLabAction:
    """Tests for AddLabAction."""

    def test_action_type(self):
        """Test action type identifier."""
        action = AddLabAction(database_url="postgresql://test")
        assert action.get_action_type() == "add_lab"

    def test_action_config(self):
        """Test configuration hashing."""
        action = AddLabAction(
            database_url="postgresql://test",
            add_summary_comment=True,
            skip_if_lab_exists=False,
            lab_selection_strategy="most_common",
        )

        config = action.get_action_config()
        assert config["add_summary_comment"] is True
        assert config["skip_if_lab_exists"] is False
        assert config["lab_selection_strategy"] == "most_common"
        assert config["has_database"] is True

    def test_action_id_changes_with_config(self):
        """Test that action ID changes when config changes."""
        action1 = AddLabAction(
            database_url="postgresql://test", skip_if_lab_exists=True
        )
        action2 = AddLabAction(
            database_url="postgresql://test", skip_if_lab_exists=False
        )

        assert action1.get_action_id() != action2.get_action_id()

    def test_action_id_same_for_same_config(self):
        """Test that action ID is stable for same config."""
        action1 = AddLabAction(
            database_url="postgresql://test", skip_if_lab_exists=True
        )
        action2 = AddLabAction(
            database_url="postgresql://test", skip_if_lab_exists=True
        )

        assert action1.get_action_id() == action2.get_action_id()

    def test_get_existing_lab_cis(self):
        """Test extraction of existing lab CIs from ticket assets."""
        action = AddLabAction(database_url="postgresql://test")

        ticket_assets = [
            {
                "ID": 2830935,
                "FormID": 3830,  # Lab form
                "Name": "O'Shea Lab",
            },
            {
                "ID": 150116,
                "FormID": 2448,  # Computer form
                "Name": "psyc-adae14",
            },
            {
                "ID": 2830962,
                "FormID": 3830,  # Lab form
                "Name": "Maldonado Lab",
            },
        ]

        existing = action._get_existing_lab_cis(ticket_assets)
        assert existing == {2830935, 2830962}

    def test_get_existing_lab_cis_none(self):
        """Test when ticket has no lab CIs."""
        action = AddLabAction(database_url="postgresql://test")

        ticket_assets = [
            {"ID": 150116, "FormID": 2448, "Name": "psyc-adae14"},
            {"ID": 150117, "FormID": 2448, "Name": "chem-smald1"},
        ]

        existing = action._get_existing_lab_cis(ticket_assets)
        assert existing == set()

    def test_get_requestor_labs_single(self):
        """Test getting labs for requestor in single lab."""
        action = AddLabAction(database_url="postgresql://test")

        # Mock database adapter
        mock_df = pd.DataFrame([{"lab_ci_id": 2830993, "lab_id": "joshea"}])
        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.return_value = mock_df

        labs = action._get_requestor_labs("88d1c65f-a271-ea11-a81b-000d3a8e391e")

        assert len(labs) == 1
        assert labs[0]["lab_ci_id"] == 2830993
        assert labs[0]["lab_id"] == "joshea"

    def test_get_requestor_labs_multiple(self):
        """Test getting labs for requestor in multiple labs."""
        action = AddLabAction(database_url="postgresql://test")

        mock_df = pd.DataFrame(
            [
                {"lab_ci_id": 2830993, "lab_id": "joshea"},
                {"lab_ci_id": 2830962, "lab_id": "smald"},
            ]
        )
        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.return_value = mock_df

        labs = action._get_requestor_labs("test-uid")

        assert len(labs) == 2
        assert {lab["lab_id"] for lab in labs} == {"joshea", "smald"}

    def test_get_requestor_labs_none(self):
        """Test when requestor is not in any labs."""
        action = AddLabAction(database_url="postgresql://test")

        mock_df = pd.DataFrame(columns=["lab_ci_id", "lab_id"])
        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.return_value = mock_df

        labs = action._get_requestor_labs("test-uid")

        assert len(labs) == 0

    def test_get_asset_labs_single(self):
        """Test getting labs for assets from single lab."""
        action = AddLabAction(database_url="postgresql://test")

        mock_df = pd.DataFrame(
            [
                {
                    "lab_ci_id": 2830993,
                    "lab_id": "joshea",
                    "computer_ci_id": 141792,
                }
            ]
        )
        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.return_value = mock_df

        labs = action._get_asset_labs([141792, 141528])

        assert len(labs) == 1
        assert labs[0]["lab_ci_id"] == 2830993
        assert labs[0]["computer_ci_id"] == 141792

    def test_get_asset_labs_multiple(self):
        """Test getting labs for assets from multiple labs."""
        action = AddLabAction(database_url="postgresql://test")

        mock_df = pd.DataFrame(
            [
                {"lab_ci_id": 2830993, "lab_id": "joshea", "computer_ci_id": 141792},
                {"lab_ci_id": 2830962, "lab_id": "smald", "computer_ci_id": 141528},
            ]
        )
        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.return_value = mock_df

        labs = action._get_asset_labs([141792, 141528])

        assert len(labs) == 2
        assert {lab["lab_id"] for lab in labs} == {"joshea", "smald"}

    def test_get_asset_labs_none(self):
        """Test when assets don't belong to any labs."""
        action = AddLabAction(database_url="postgresql://test")

        mock_df = pd.DataFrame(columns=["lab_ci_id", "lab_id", "computer_ci_id"])
        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.return_value = mock_df

        labs = action._get_asset_labs([999999])

        assert len(labs) == 0

    def test_get_lab_names(self):
        """Test getting display names for labs."""
        action = AddLabAction(database_url="postgresql://test")

        mock_df = pd.DataFrame(
            [
                {"tdx_ci_id": 2830993, "lab_name": "O'Shea, John Lab (AD)"},
                {"tdx_ci_id": 2830962, "lab_name": "Maldonado, Stephen Lab (AD)"},
            ]
        )
        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.return_value = mock_df

        names = action._get_lab_names([2830993, 2830962])

        assert len(names) == 2
        assert names[2830993] == "O'Shea, John Lab (AD)"
        assert names[2830962] == "Maldonado, Stephen Lab (AD)"

    def test_execute_dry_run(self):
        """Test dry run mode."""
        action = AddLabAction(database_url="postgresql://test")

        # Mock facade
        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = {
            "ID": 12345,
            "Title": "Test Ticket",
            "RequestorUid": "test-uid",
        }
        facade_mock.tickets.get_ticket_assets.return_value = []

        # Mock database queries
        mock_requestor_df = pd.DataFrame([{"lab_ci_id": 2830993, "lab_id": "joshea"}])
        mock_names_df = pd.DataFrame([{"tdx_ci_id": 2830993, "lab_name": "O'Shea Lab"}])

        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.side_effect = [
            mock_requestor_df,
            mock_names_df,
        ]

        result = action.execute_action(
            ticket_id=12345, facade=facade_mock, dry_run=True
        )

        assert result["success"] is True
        assert "Added 1 lab:" in result["message"]
        # Verify no API calls were made
        facade_mock.configuration_items.add_ticket_to_ci.assert_not_called()

    def test_execute_requestor_single_lab(self):
        """Test adding single lab via requestor (fallback when no assets)."""
        action = AddLabAction(database_url="postgresql://test")

        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = {
            "ID": 12345,
            "Title": "Test Ticket",
            "RequestorUid": "test-uid",
            "RequestorEmail": "joshea@umich.edu",
        }
        facade_mock.tickets.get_ticket_assets.return_value = []
        facade_mock.configuration_items.add_ticket_to_ci.return_value = (
            None  # API returns None on success
        )

        # Mock database queries - no assets, so only requestor query
        mock_requestor_df = pd.DataFrame([{"lab_ci_id": 2830993, "lab_id": "joshea"}])
        mock_names_df = pd.DataFrame(
            [{"tdx_ci_id": 2830993, "lab_name": "O'Shea, John Lab (AD)"}]
        )

        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.side_effect = [
            mock_requestor_df,
            mock_names_df,
        ]

        result = action.execute_action(
            ticket_id=12345, facade=facade_mock, dry_run=False
        )

        assert result["success"] is True
        assert result["details"]["labs_added"] == 1
        assert result["details"]["detection_method"] == "requestor"
        assert "O'Shea, John Lab (AD)" in result["summary"]
        assert "Requestor (joshea) is a lab member" in result["summary"]
        facade_mock.configuration_items.add_ticket_to_ci.assert_called_once_with(
            2830993, 12345
        )

    def test_execute_assets_single_lab_selected(self):
        """Test adding single lab via assets when multiple assets from same lab."""
        action = AddLabAction(database_url="postgresql://test")

        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = {
            "ID": 12345,
            "Title": "Test Ticket",
            "RequestorUid": None,  # No requestor
        }
        facade_mock.tickets.get_ticket_assets.return_value = [
            {"ID": 141792, "FormID": 2448, "Name": "UMMA-JOSHEA"},
            {"ID": 141793, "FormID": 2448, "Name": "UMMA-JOSHEA-2"},
        ]
        facade_mock.configuration_items.add_ticket_to_ci.return_value = (
            None  # API returns None on success
        )

        # Mock database queries - both assets from same lab
        mock_asset_df = pd.DataFrame(
            [
                {"lab_ci_id": 2830993, "lab_id": "joshea", "computer_ci_id": 141792},
                {"lab_ci_id": 2830993, "lab_id": "joshea", "computer_ci_id": 141793},
            ]
        )
        mock_names_df = pd.DataFrame(
            [
                {"tdx_ci_id": 2830993, "lab_name": "O'Shea, John Lab (AD)"},
            ]
        )

        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.side_effect = [
            mock_asset_df,
            mock_names_df,
        ]

        result = action.execute_action(
            ticket_id=12345, facade=facade_mock, dry_run=False
        )

        assert result["success"] is True
        assert result["details"]["labs_added"] == 1
        assert result["details"]["detection_method"] == "assets"
        assert "O'Shea, John Lab (AD)" in result["summary"]
        assert "Ticket contains lab computer(s)" in result["summary"]
        # Should only add one lab, not two
        facade_mock.configuration_items.add_ticket_to_ci.assert_called_once_with(
            2830993, 12345
        )

    def test_execute_lab_already_exists_skip(self):
        """Test skip behavior when lab already on ticket."""
        action = AddLabAction(database_url="postgresql://test", skip_if_lab_exists=True)

        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = {
            "ID": 12345,
            "Title": "Test Ticket",
            "RequestorUid": "test-uid",
        }
        facade_mock.tickets.get_ticket_assets.return_value = [
            {"ID": 2830993, "FormID": 3830, "Name": "O'Shea Lab"}  # Existing lab CI
        ]

        action.db_adapter = Mock()

        result = action.execute_action(
            ticket_id=12345, facade=facade_mock, dry_run=False
        )

        assert result["success"] is True
        assert result["details"]["labs_added"] == 0
        assert result["details"]["skipped"] is True
        assert result["summary"] == ""  # No summary for skipped action
        facade_mock.configuration_items.add_ticket_to_ci.assert_not_called()

    def test_execute_no_labs_detected(self):
        """Test when no labs are detected."""
        action = AddLabAction(database_url="postgresql://test")

        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = {
            "ID": 12345,
            "Title": "Test Ticket",
            "RequestorUid": "test-uid",
        }
        facade_mock.tickets.get_ticket_assets.return_value = []

        # Mock database queries returning empty results
        empty_df = pd.DataFrame(columns=["lab_ci_id", "lab_id"])
        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.return_value = empty_df

        result = action.execute_action(
            ticket_id=12345, facade=facade_mock, dry_run=False
        )

        assert result["success"] is True
        assert result["details"]["labs_added"] == 0
        assert result["details"]["detection_method"] == "none"
        assert result["summary"] == ""
        facade_mock.configuration_items.add_ticket_to_ci.assert_not_called()

    def test_execute_ticket_not_found(self):
        """Test behavior when ticket doesn't exist."""
        action = AddLabAction(database_url="postgresql://test")

        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = None

        result = action.execute_action(
            ticket_id=99999, facade=facade_mock, dry_run=False
        )

        assert result["success"] is False
        assert "not found" in result["message"].lower()

    def test_execute_api_error_retryable(self):
        """Test retryable error handling."""
        action = AddLabAction(database_url="postgresql://test")

        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = {
            "ID": 12345,
            "Title": "Test Ticket",
            "RequestorUid": "test-uid",
        }
        facade_mock.tickets.get_ticket_assets.return_value = []
        facade_mock.configuration_items.add_ticket_to_ci.side_effect = Exception(
            "502 Bad Gateway"
        )

        # Mock database queries
        mock_requestor_df = pd.DataFrame([{"lab_ci_id": 2830993, "lab_id": "joshea"}])
        mock_names_df = pd.DataFrame([{"tdx_ci_id": 2830993, "lab_name": "O'Shea Lab"}])

        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.side_effect = [
            mock_requestor_df,
            mock_names_df,
        ]

        result = action.execute_action(
            ticket_id=12345, facade=facade_mock, dry_run=False
        )

        assert result["success"] is False
        assert "retry" in result["message"].lower()
        assert result["details"]["retryable"] is True

    def test_execute_assets_priority_over_requestor(self):
        """Test that asset-based detection takes priority over requestor-based."""
        action = AddLabAction(database_url="postgresql://test")

        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = {
            "ID": 12345,
            "Title": "Test Ticket",
            "RequestorUid": "test-uid",
            "RequestorEmail": "joshea@umich.edu",
        }
        facade_mock.tickets.get_ticket_assets.return_value = [
            {"ID": 141792, "FormID": 2448, "Name": "UMMA-JOSHEA"}
        ]
        facade_mock.configuration_items.add_ticket_to_ci.return_value = (
            None  # API returns None on success
        )

        # Mock database queries - asset detection finds lab, so requestor query never runs
        mock_asset_df = pd.DataFrame(
            [{"lab_ci_id": 2830993, "lab_id": "joshea", "computer_ci_id": 141792}]
        )
        mock_names_df = pd.DataFrame(
            [{"tdx_ci_id": 2830993, "lab_name": "O'Shea, John Lab (AD)"}]
        )

        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.side_effect = [
            mock_asset_df,  # Asset query runs first (Phase 1)
            mock_names_df,  # Lab names query
        ]

        result = action.execute_action(
            ticket_id=12345, facade=facade_mock, dry_run=False
        )

        assert result["success"] is True
        assert result["details"]["labs_added"] == 1
        assert result["details"]["detection_method"] == "assets"  # Assets take priority
        assert "Ticket contains lab computer(s)" in result["summary"]
        # Should only call API once
        facade_mock.configuration_items.add_ticket_to_ci.assert_called_once_with(
            2830993, 12345
        )
        # Verify only 2 database queries (no requestor query since asset found lab)
        assert action.db_adapter.query_to_dataframe.call_count == 2

    def test_database_connection_error(self):
        """Test handling of database connection errors."""
        action = AddLabAction(database_url="postgresql://test")

        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = {
            "ID": 12345,
            "Title": "Test Ticket",
            "RequestorUid": "test-uid",
        }
        facade_mock.tickets.get_ticket_assets.return_value = []

        # Mock database error
        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.side_effect = Exception(
            "connection timeout"
        )

        result = action.execute_action(
            ticket_id=12345, facade=facade_mock, dry_run=False
        )

        # Should handle gracefully and return no labs detected
        assert result["success"] is True
        assert result["details"]["labs_added"] == 0

    def test_execute_assets_most_common_strategy(self):
        """Test most_common strategy selects lab with most assets."""
        action = AddLabAction(
            database_url="postgresql://test", lab_selection_strategy="most_common"
        )

        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = {
            "ID": 12345,
            "Title": "Test Ticket",
            "RequestorUid": None,
        }
        facade_mock.tickets.get_ticket_assets.return_value = [
            {"ID": 141792, "FormID": 2448, "Name": "UMMA-JOSHEA-1"},
            {"ID": 141793, "FormID": 2448, "Name": "UMMA-JOSHEA-2"},
            {"ID": 141794, "FormID": 2448, "Name": "UMMA-JOSHEA-3"},
            {"ID": 141528, "FormID": 2448, "Name": "CHEM-SMALD1"},
        ]
        facade_mock.configuration_items.add_ticket_to_ci.return_value = None

        # Mock database queries - 3 assets from joshea lab, 1 from smald lab
        mock_asset_df = pd.DataFrame(
            [
                {"lab_ci_id": 2830993, "lab_id": "joshea", "computer_ci_id": 141792},
                {"lab_ci_id": 2830993, "lab_id": "joshea", "computer_ci_id": 141793},
                {"lab_ci_id": 2830993, "lab_id": "joshea", "computer_ci_id": 141794},
                {"lab_ci_id": 2830962, "lab_id": "smald", "computer_ci_id": 141528},
            ]
        )
        mock_names_df = pd.DataFrame(
            [{"tdx_ci_id": 2830993, "lab_name": "O'Shea, John Lab (AD)"}]
        )

        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.side_effect = [
            mock_asset_df,
            mock_names_df,
        ]

        result = action.execute_action(
            ticket_id=12345, facade=facade_mock, dry_run=False
        )

        assert result["success"] is True
        assert result["details"]["labs_added"] == 1
        assert result["details"]["lab_ci_id"] == 2830993  # joshea lab (most common)
        assert "O'Shea, John Lab (AD)" in result["summary"]
        # Should only add the most common lab
        facade_mock.configuration_items.add_ticket_to_ci.assert_called_once_with(
            2830993, 12345
        )

    def test_execute_requestor_multiple_labs_selects_first(self):
        """Test that when requestor belongs to multiple labs, first is selected."""
        action = AddLabAction(database_url="postgresql://test")

        facade_mock = Mock()
        facade_mock.tickets.get_ticket.return_value = {
            "ID": 12345,
            "Title": "Test Ticket",
            "RequestorUid": "test-uid",
            "RequestorEmail": "multilab@umich.edu",
        }
        facade_mock.tickets.get_ticket_assets.return_value = []
        facade_mock.configuration_items.add_ticket_to_ci.return_value = None

        # Mock database queries - requestor in multiple labs
        mock_requestor_df = pd.DataFrame(
            [
                {"lab_ci_id": 2830993, "lab_id": "joshea"},
                {"lab_ci_id": 2830962, "lab_id": "smald"},
                {"lab_ci_id": 2830935, "lab_id": "csmonk"},
            ]
        )
        mock_names_df = pd.DataFrame(
            [{"tdx_ci_id": 2830993, "lab_name": "O'Shea, John Lab (AD)"}]
        )

        action.db_adapter = Mock()
        action.db_adapter.query_to_dataframe.side_effect = [
            mock_requestor_df,
            mock_names_df,
        ]

        result = action.execute_action(
            ticket_id=12345, facade=facade_mock, dry_run=False
        )

        assert result["success"] is True
        assert result["details"]["labs_added"] == 1
        assert result["details"]["lab_ci_id"] == 2830993  # First lab
        assert result["details"]["detection_method"] == "requestor"
        assert "O'Shea, John Lab (AD)" in result["summary"]
        # Should only add first lab
        facade_mock.configuration_items.add_ticket_to_ci.assert_called_once_with(
            2830993, 12345
        )
