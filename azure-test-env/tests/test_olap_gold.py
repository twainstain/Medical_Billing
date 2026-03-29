"""Tests for the OLAP Gold aggregation functions (olap/gold.py).

Verifies all 13 agg_* functions produce correct output given mocked Silver
layer data. Uses pandas DataFrames to simulate Silver Parquet reads.
"""

import os
import sys
import types
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime

FUNC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "functions")
sys.path.insert(0, FUNC_DIR)

# Mock azure.storage.blob before importing olap.gold (not installed locally)
azure_mock = types.ModuleType("azure")
azure_storage_mock = types.ModuleType("azure.storage")
azure_blob_mock = types.ModuleType("azure.storage.blob")
azure_mock.storage = azure_storage_mock
azure_storage_mock.blob = azure_blob_mock
azure_blob_mock.BlobServiceClient = MagicMock()
azure_blob_mock.ContainerClient = MagicMock()
sys.modules.setdefault("azure", azure_mock)
sys.modules.setdefault("azure.storage", azure_storage_mock)
sys.modules.setdefault("azure.storage.blob", azure_blob_mock)


# ============================================================================
# Mock Silver Layer Data
# ============================================================================

def mock_claim_remittance():
    return pd.DataFrame({
        "claim_id": [1, 2, 3, 4, 5, 6, 7, 8],
        "payer_id": [1, 2, 3, 1, 2, 4, 5, 6],
        "payer_name": ["Anthem", "Aetna", "UHC", "Anthem", "Aetna", "Cigna", "Humana", "Medicare"],
        "provider_npi": ["111", "222", "333", "111", "444", "111", "333", "555"],
        "provider_name": ["Dr A", "Dr B", "Dr C", "Dr A", "Dr D", "Dr A", "Dr C", "Dr E"],
        "date_of_service": ["2025-06-15", "2025-07-10", "2025-08-22", "2025-05-10",
                            "2025-09-01", "2025-04-01", "2025-03-18", "2025-10-05"],
        "total_billed": [850, 1200, 520, 6500, 4200, 450, 320, 1800],
        "total_paid": [380, 520, 210, 2100, 1800, 420, 310, 0],
        "underpayment": [470, 680, 310, 4400, 2400, 30, 10, 1800],
        "has_denial": [0, 0, 0, 0, 0, 0, 0, 1],
        "cpt_codes": ['["99285"]', '["00142"]', '["93010"]', '["27447"]',
                      '["61510"]', '["99283"]', '["93010"]', '["74177"]'],
    })


def mock_claims():
    return pd.DataFrame({
        "claim_id": [1, 2, 3, 4, 5, 6, 7, 8],
        "payer_id": [1, 2, 3, 1, 2, 4, 5, 6],
        "provider_npi": ["111", "222", "333", "111", "444", "111", "333", "555"],
        "date_of_service": ["2025-06-15", "2025-07-10", "2025-08-22", "2025-05-10",
                            "2025-09-01", "2025-04-01", "2025-03-18", "2025-10-05"],
        "total_billed": [850, 1200, 520, 6500, 4200, 450, 320, 1800],
        "cpt_codes": ['["99285"]', '["00142"]', '["93010"]', '["27447"]',
                      '["61510"]', '["99283"]', '["93010"]', '["74177"]'],
    })


def mock_cases():
    return pd.DataFrame({
        "case_id": [1, 2, 3, 4, 5, 6],
        "assigned_analyst": ["Ana", "Mark", "Ana", "Sarah", "Mark", "Ana"],
        "status": ["negotiation", "idr_initiated", "in_review", "idr_submitted", "decided", "closed"],
        "priority": ["high", "high", "medium", "critical", "high", "medium"],
        "created_date": ["2025-08-01", "2025-08-20", "2025-10-01", "2025-06-20", "2025-10-15", "2025-03-01"],
        "closed_date": [None, None, None, None, "2026-02-28", "2025-06-15"],
        "outcome": [None, None, None, None, "won", "lost"],
        "award_amount": [None, None, None, None, 3500, None],
        "total_billed": [850, 1200, 520, 6500, 4200, 850],
        "total_underpayment": [470, 680, 310, 4400, 2400, 470],
        "age_days": [230, 210, 170, 280, 160, 390],
    })


def mock_disputes():
    return pd.DataFrame({
        "dispute_id": [1, 2, 3, 4, 5, 6],
        "claim_id": [1, 2, 3, 4, 5, 1],
        "case_id": [1, 2, 3, 4, 5, 6],
        "dispute_type": ["idr", "idr", "appeal", "idr", "idr", "appeal"],
        "status": ["negotiation", "filed", "open", "evidence_submitted", "decided", "closed"],
        "payer_id": [1, 2, 3, 1, 2, 1],
        "payer_name": ["Anthem", "Aetna", "UHC", "Anthem", "Aetna", "Anthem"],
        "provider_npi": ["111", "222", "333", "111", "444", "111"],
        "date_of_service": ["2025-06-15", "2025-07-10", "2025-08-22", "2025-05-10", "2025-09-01", "2025-06-15"],
        "billed_amount": [650, 1200, 400, 6500, 4200, 200],
        "paid_amount": [380, 520, 210, 2100, 1800, 120],
        "qpa_amount": [475, 580, 365, 3350, 2200, 155],
        "underpayment_amount": [270, 680, 190, 4400, 2400, 80],
        "filed_date": ["2025-08-05", "2025-08-25", "2025-10-05", "2025-06-25", "2025-10-20", "2025-03-10"],
    })


def mock_deadlines():
    return pd.DataFrame({
        "deadline_id": list(range(1, 15)),
        "case_id": [1, 1, 1, 2, 2, 2, 3, 4, 4, 4, 4, 5, 5, 5],
        "type": ["open_negotiation", "idr_initiation", "evidence_submission",
                 "open_negotiation", "idr_initiation", "entity_selection",
                 "open_negotiation",
                 "open_negotiation", "idr_initiation", "evidence_submission", "decision",
                 "open_negotiation", "idr_initiation", "decision"],
        "status": ["met", "met", "pending",
                    "met", "met", "pending",
                    "alerted",
                    "met", "met", "met", "pending",
                    "met", "met", "met"],
        "is_at_risk": [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
    })


def mock_fee_schedule():
    return pd.DataFrame({
        "cpt_code": ["99285", "99283", "27447"],
        "payer_id": ["MEDICARE", "MEDICARE", "MEDICARE"],
        "rate": [395, 162, 2800],
        "is_current": [1, 1, 1],
    })


def _read_silver_factory(table_map):
    """Return a function that returns mock DataFrames based on table name."""
    def _read_silver(table):
        return table_map.get(table, pd.DataFrame())
    return _read_silver


# ============================================================================
# Shared mock setup
# ============================================================================

@pytest.fixture
def silver_data():
    return {
        "claim_remittance": mock_claim_remittance(),
        "claims": mock_claims(),
        "cases": mock_cases(),
        "disputes": mock_disputes(),
        "deadlines": mock_deadlines(),
        "fee_schedule": mock_fee_schedule(),
    }


@pytest.fixture
def mock_write():
    """Mock _write_gold to capture output without writing files."""
    written = {}
    def fake_write(table, df):
        written[table] = df
        return len(df)
    return written, fake_write


# ============================================================================
# Original 8 Gold Aggregation Functions
# ============================================================================

class TestRecoveryByPayer:
    def test_produces_rows(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_recovery_by_payer
            count = agg_recovery_by_payer()
        assert count > 0
        assert "recovery_by_payer" in written
        df = written["recovery_by_payer"]
        assert "payer_id" in df.columns
        assert "recovery_rate_pct" in df.columns


class TestFinancialSummary:
    def test_produces_10_metrics(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_financial_summary
            count = agg_financial_summary()
        assert count == 10
        df = written["financial_summary"]
        metric_names = set(df["metric_name"].tolist())
        assert "total_claims" in metric_names
        assert "total_billed" in metric_names
        assert "recovery_rate_pct" in metric_names


class TestCasePipeline:
    def test_all_statuses_present(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_case_pipeline
            count = agg_case_pipeline()
        assert count > 0
        df = written["case_pipeline"]
        statuses = set(df["status"].tolist())
        assert "negotiation" in statuses
        assert "decided" in statuses


class TestDeadlineCompliance:
    def test_deadline_types(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_deadline_compliance
            count = agg_deadline_compliance()
        assert count > 0
        df = written["deadline_compliance"]
        assert "compliance_pct" in df.columns
        types = set(df["deadline_type"].tolist())
        assert "open_negotiation" in types


class TestUnderpaymentDetection:
    def test_filters_positive_underpayments(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_underpayment_detection
            count = agg_underpayment_detection()
        assert count > 0
        df = written["underpayment_detection"]
        assert "arbitration_eligible" in df.columns
        assert all(df["underpayment_amount"] > 0)


# ============================================================================
# New 5 Gold Aggregation Functions — Business Success Metrics
# ============================================================================

class TestWinLossAnalysis:
    def test_only_closed_cases(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_win_loss_analysis
            count = agg_win_loss_analysis()
        assert count > 0
        df = written["win_loss_analysis"]
        assert "outcome" in df.columns
        assert "win_rate_pct" in df.columns
        assert "recovery_roi_pct" in df.columns

    def test_contains_won_and_lost(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_win_loss_analysis
            agg_win_loss_analysis()
        outcomes = set(written["win_loss_analysis"]["outcome"].tolist())
        assert "won" in outcomes
        assert "lost" in outcomes


class TestAnalystProductivity:
    def test_all_analysts(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_analyst_productivity
            count = agg_analyst_productivity()
        assert count == 3  # Ana, Mark, Sarah
        df = written["analyst_productivity"]
        assert "win_rate_pct" in df.columns
        assert "avg_resolution_days" in df.columns
        analysts = set(df["assigned_analyst"].tolist())
        assert "Ana" in analysts
        assert "Mark" in analysts

    def test_case_counts_correct(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_analyst_productivity
            agg_analyst_productivity()
        df = written["analyst_productivity"]
        ana = df[df["assigned_analyst"] == "Ana"].iloc[0]
        assert ana["total_cases"] == 3
        assert ana["active_cases"] == 2
        assert ana["resolved_cases"] == 1


class TestTimeToResolution:
    def test_produces_rows(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_time_to_resolution
            count = agg_time_to_resolution()
        assert count > 0
        df = written["time_to_resolution"]
        assert "avg_days" in df.columns
        assert "total_at_stake" in df.columns

    def test_grouped_by_status_priority(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_time_to_resolution
            agg_time_to_resolution()
        df = written["time_to_resolution"]
        assert "status" in df.columns
        assert "priority" in df.columns


class TestProviderPerformance:
    def test_produces_rows(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_provider_performance
            count = agg_provider_performance()
        assert count > 0
        df = written["provider_performance"]
        assert "payment_rate_pct" in df.columns
        assert "dispute_rate_pct" in df.columns


class TestMonthlyTrends:
    def test_produces_monthly_rows(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_monthly_trends
            count = agg_monthly_trends()
        assert count > 0
        df = written["monthly_trends"]
        assert "month" in df.columns
        assert "recovery_rate_pct" in df.columns
        assert "total_underpayment" in df.columns

    def test_months_are_formatted(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import agg_monthly_trends
            agg_monthly_trends()
        df = written["monthly_trends"]
        for month in df["month"]:
            assert len(month) == 7  # YYYY-MM format
            assert month[4] == "-"


# ============================================================================
# Full Pipeline: aggregate_all_gold()
# ============================================================================

class TestAggregateAllGold:
    def test_runs_all_13_aggregations(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import aggregate_all_gold
            summary = aggregate_all_gold()

        assert len(summary) == 13
        for name, value in summary.items():
            assert not isinstance(value, str) or not value.startswith("ERROR"), \
                f"{name} failed: {value}"

    def test_all_tables_written(self, silver_data, mock_write):
        written, fake_write = mock_write
        with patch("olap.gold._read_silver", _read_silver_factory(silver_data)), \
             patch("olap.gold._write_gold", fake_write):
            from olap.gold import aggregate_all_gold
            aggregate_all_gold()

        expected_tables = [
            "recovery_by_payer", "cpt_analysis", "payer_scorecard",
            "financial_summary", "claims_aging", "case_pipeline",
            "deadline_compliance", "underpayment_detection",
            "win_loss_analysis", "analyst_productivity",
            "time_to_resolution", "provider_performance", "monthly_trends",
        ]
        for table in expected_tables:
            assert table in written, f"Missing table: {table}"
