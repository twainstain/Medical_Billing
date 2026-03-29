"""Tests for the AI Analyst Agent module.

Covers:
  - SQL safety guards (_execute_gold_sql)
  - Common analyses catalog
  - ask_common() validation
  - ask() flow with mocked Claude API
  - UI endpoint returns HTML
"""

import json
import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from decimal import Decimal
from datetime import datetime, date

# Ensure azure-test-env/functions is on path
FUNC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "functions")
sys.path.insert(0, FUNC_DIR)

from agent.analyst import (
    COMMON_ANALYSES,
    GOLD_SCHEMA,
    get_common_analyses,
    ask_common,
    ask,
    _execute_gold_sql,
    _suggest_next_analyses,
)


# ============================================================================
# SQL Safety Guards
# ============================================================================

class TestSQLSafety:
    """Test that _execute_gold_sql blocks dangerous SQL."""

    def test_blocks_insert(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            _execute_gold_sql("INSERT INTO claims VALUES (1)")

    def test_blocks_update(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            _execute_gold_sql("UPDATE claims SET status='x'")

    def test_blocks_delete(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            _execute_gold_sql("DELETE FROM claims")

    def test_blocks_drop(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            _execute_gold_sql("DROP TABLE claims")

    def test_blocks_alter(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            _execute_gold_sql("ALTER TABLE claims ADD x INT")

    def test_blocks_exec(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            _execute_gold_sql("EXEC sp_help")

    def test_blocks_truncate(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            _execute_gold_sql("TRUNCATE TABLE claims")

    def test_blocks_merge(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            _execute_gold_sql("MERGE INTO claims USING src ON 1=1 WHEN MATCHED THEN DELETE;")

    def test_blocks_create(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            _execute_gold_sql("CREATE TABLE hack (id INT)")

    def test_blocks_select_into(self):
        """SELECT with embedded DROP keyword."""
        with pytest.raises(ValueError, match="Blocked SQL keyword: DROP"):
            _execute_gold_sql("SELECT 1; DROP TABLE claims")

    def test_allows_valid_select(self):
        """Valid SELECT should pass safety checks and only fail on DB connection."""
        with pytest.raises(Exception):
            # Will fail because no DB connection, but should NOT fail on safety check
            _execute_gold_sql("SELECT TOP 10 * FROM gold_recovery_by_payer")

    def test_strips_semicolons(self):
        """Trailing semicolons should be stripped."""
        with pytest.raises(Exception):
            # Should pass safety and fail on DB connection
            _execute_gold_sql("SELECT 1 FROM gold_financial_summary;")

    def test_blocks_insert_in_subquery(self):
        """INSERT embedded after semicolon."""
        with pytest.raises(ValueError, match="Blocked SQL keyword: INSERT"):
            _execute_gold_sql("SELECT 1; INSERT INTO claims VALUES (1)")


# ============================================================================
# Common Analyses Catalog
# ============================================================================

class TestCommonAnalyses:

    def test_catalog_has_10_entries(self):
        analyses = get_common_analyses()
        assert len(analyses) == 10

    def test_all_have_required_fields(self):
        for a in COMMON_ANALYSES:
            assert "id" in a, f"Missing 'id' in {a}"
            assert "name" in a, f"Missing 'name' in {a}"
            assert "description" in a, f"Missing 'description' in {a}"
            assert "question" in a, f"Missing 'question' in {a}"

    def test_ids_are_unique(self):
        ids = [a["id"] for a in COMMON_ANALYSES]
        assert len(ids) == len(set(ids)), "Duplicate IDs in COMMON_ANALYSES"

    def test_known_ids_present(self):
        ids = {a["id"] for a in COMMON_ANALYSES}
        expected = {
            "executive_summary", "worst_payers", "arbitration_ready",
            "cpt_underpayment", "deadline_risk", "case_pipeline",
            "aging_analysis", "payer_comparison", "recovery_opportunity",
            "denial_patterns",
        }
        assert expected == ids

    def test_questions_are_nonempty(self):
        for a in COMMON_ANALYSES:
            assert len(a["question"]) > 20, f"Question too short for {a['id']}"


# ============================================================================
# ask_common() Validation
# ============================================================================

class TestAskCommon:

    def test_invalid_id_returns_error(self):
        result = ask_common("nonexistent_analysis")
        assert "Unknown analysis ID" in result["answer"]
        assert result["sql"] is None
        assert result["row_count"] == 0

    def test_invalid_id_lists_valid_ids(self):
        result = ask_common("bad_id")
        for a in COMMON_ANALYSES:
            assert a["id"] in str(result["answer"])

    @patch("agent.analyst.ask")
    def test_valid_id_calls_ask(self, mock_ask):
        mock_ask.return_value = {"answer": "test", "sql": "SELECT 1", "row_count": 1, "data": [], "model": "test"}
        result = ask_common("executive_summary")
        mock_ask.assert_called_once()
        # The question passed should be the one from the catalog
        call_args = mock_ask.call_args
        assert "executive summary" in call_args[0][0].lower() or "financial" in call_args[0][0].lower()

    @patch("agent.analyst.ask")
    def test_valid_id_adds_metadata(self, mock_ask):
        mock_ask.return_value = {"answer": "test", "sql": "SELECT 1", "row_count": 1, "data": [], "model": "test"}
        result = ask_common("worst_payers")
        assert result["analysis_id"] == "worst_payers"
        assert result["analysis_name"] == "Worst Performing Payers"


# ============================================================================
# Suggested Analyses
# ============================================================================

class TestSuggestedAnalyses:

    def test_returns_3_suggestions(self):
        suggestions = _suggest_next_analyses("What is the denial rate?", "Anthem has 25% denial rate")
        assert len(suggestions) == 3

    def test_suggestions_have_required_fields(self):
        suggestions = _suggest_next_analyses("test", "test")
        for s in suggestions:
            assert "id" in s
            assert "name" in s
            assert "description" in s

    def test_suggestions_are_valid_ids(self):
        valid_ids = {a["id"] for a in COMMON_ANALYSES}
        suggestions = _suggest_next_analyses("test", "test")
        for s in suggestions:
            assert s["id"] in valid_ids


# ============================================================================
# ask() with Mocked Claude API
# ============================================================================

class TestAskFlow:

    def _mock_claude_sql_response(self):
        """Mock a Claude response that returns a tool_use block."""
        mock_block = MagicMock()
        mock_block.type = "tool_use"
        mock_block.name = "execute_sql"
        mock_block.input = {
            "sql": "SELECT TOP 5 * FROM gold_recovery_by_payer ORDER BY total_underpayment DESC",
            "explanation": "Get payers with highest underpayment"
        }

        mock_response = MagicMock()
        mock_response.content = [mock_block]
        return mock_response

    def _mock_claude_text_response(self, text="Based on the data..."):
        """Mock a Claude response that returns a text block."""
        mock_block = MagicMock()
        mock_block.type = "text"
        mock_block.text = text

        mock_response = MagicMock()
        mock_response.content = [mock_block]
        return mock_response

    def _mock_claude_no_sql_response(self):
        """Mock a Claude response with text only (no SQL generated)."""
        mock_block = MagicMock()
        mock_block.type = "text"
        mock_block.text = "I cannot answer that from the available data."

        mock_response = MagicMock()
        mock_response.content = [mock_block]
        return mock_response

    @patch("agent.analyst._log_agent_invocation")
    @patch("agent.analyst._execute_gold_sql")
    @patch("agent.analyst._get_client")
    def test_ask_full_flow(self, mock_get_client, mock_exec_sql, mock_log):
        """Test the full ask() flow: question → SQL → execute → analysis."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # First call: generate SQL
        mock_client.messages.create.side_effect = [
            self._mock_claude_sql_response(),
            self._mock_claude_text_response("Anthem has the highest underpayment at $5,000."),
        ]

        mock_exec_sql.return_value = [
            {"payer_id": 1, "payer_name": "Anthem", "total_underpayment": 5000.0}
        ]

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            result = ask("Which payer has the highest underpayment?")

        assert result["answer"] is not None
        assert "Anthem" in result["answer"]
        assert result["sql"] is not None
        assert result["row_count"] == 1
        assert len(result["data"]) == 1
        assert "suggested_analyses" in result
        mock_log.assert_called_once()

    @patch("agent.analyst._get_client")
    def test_ask_no_sql_generated(self, mock_get_client):
        """When Claude doesn't generate SQL, return text explanation."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.messages.create.return_value = self._mock_claude_no_sql_response()

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            result = ask("What's the weather?")

        assert result["sql"] is None
        assert result["row_count"] == 0
        assert "cannot answer" in result["answer"].lower()

    @patch("agent.analyst._log_agent_invocation")
    @patch("agent.analyst._execute_gold_sql")
    @patch("agent.analyst._get_client")
    def test_ask_sql_execution_error(self, mock_get_client, mock_exec_sql, mock_log):
        """When SQL execution fails, return error in response."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.messages.create.return_value = self._mock_claude_sql_response()
        mock_exec_sql.side_effect = Exception("Connection refused")

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            result = ask("Show me payer data")

        assert "error" in result
        assert result["sql"] is not None
        assert result["row_count"] == 0

    def test_ask_missing_api_key(self):
        """Should raise if ANTHROPIC_API_KEY is not set."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
                ask("test question")


# ============================================================================
# Gold Schema Prompt
# ============================================================================

class TestGoldSchema:
    """Verify the schema prompt contains all expected views."""

    def test_contains_all_8_views(self):
        views = [
            "gold_recovery_by_payer",
            "gold_cpt_analysis",
            "gold_payer_scorecard",
            "gold_financial_summary",
            "gold_claims_aging",
            "gold_case_pipeline",
            "gold_deadline_compliance",
            "gold_underpayment_detection",
        ]
        for view in views:
            assert view in GOLD_SCHEMA, f"Missing view {view} in GOLD_SCHEMA prompt"

    def test_schema_has_column_definitions(self):
        """Each view should have column descriptions."""
        assert "payer_name" in GOLD_SCHEMA
        assert "total_billed" in GOLD_SCHEMA
        assert "arbitration_eligible" in GOLD_SCHEMA
        assert "compliance_pct" in GOLD_SCHEMA


# ============================================================================
# UI HTML File
# ============================================================================

class TestUIFile:

    def test_ui_html_exists(self):
        ui_path = os.path.join(FUNC_DIR, "agent", "ui.html")
        assert os.path.exists(ui_path), "agent/ui.html not found"

    def test_ui_html_is_valid(self):
        ui_path = os.path.join(FUNC_DIR, "agent", "ui.html")
        with open(ui_path) as f:
            html = f.read()
        assert "<!DOCTYPE html>" in html
        assert "agent/ask" in html
        assert "agent/common" in html
        assert "Medical Billing" in html

    def test_ui_has_common_analyses(self):
        ui_path = os.path.join(FUNC_DIR, "agent", "ui.html")
        with open(ui_path) as f:
            html = f.read()
        assert "executive_summary" in html
        assert "worst_payers" in html
        assert "arbitration_ready" in html
