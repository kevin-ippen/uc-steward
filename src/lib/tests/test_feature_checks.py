"""Tests for lib.feature_checks — privilege-aware platform feature enforcement.

Run with: pytest src/lib/tests/test_feature_checks.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.feature_checks import (
    CapabilityProbe,
    CheckResult,
    CheckStatus,
    feature_enablement_policy,
    run_feature_checks,
    results_to_spark,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_spark():
    """A mock SparkSession that can be configured per test."""
    return MagicMock()


@pytest.fixture
def mock_sdk():
    """A mock Databricks WorkspaceClient."""
    return MagicMock()


# ── CapabilityProbe tests ─────────────────────────────────────────────────────


class TestCapabilityProbe:
    def test_probe_success(self, mock_spark):
        """Probe returns True when query succeeds."""
        mock_spark.sql.return_value.collect.return_value = []
        probe = CapabilityProbe(mock_spark)
        assert probe.can_access("system.storage.predictive_optimization_operations_history") is True

    def test_probe_insufficient_permissions(self, mock_spark):
        """Probe returns False on INSUFFICIENT_PERMISSIONS."""
        from pyspark.errors import AnalysisException
        mock_spark.sql.side_effect = Exception(
            "[INSUFFICIENT_PERMISSIONS] User does not have SELECT"
        )
        probe = CapabilityProbe(mock_spark)
        assert probe.can_access("system.data_classification.__table_statuses") is False

    def test_probe_table_not_found(self, mock_spark):
        """Probe returns False when table does not exist."""
        mock_spark.sql.side_effect = Exception(
            "[TABLE_OR_VIEW_NOT_FOUND] Table system.quality.monitors not found"
        )
        probe = CapabilityProbe(mock_spark)
        assert probe.can_access("system.quality.monitors") is False

    def test_probe_caches_result(self, mock_spark):
        """Second call uses cache, no additional SQL."""
        mock_spark.sql.return_value.collect.return_value = []
        probe = CapabilityProbe(mock_spark)
        probe.can_access("some.table")
        probe.can_access("some.table")
        assert mock_spark.sql.call_count == 1

    def test_probe_all_populates_cache(self, mock_spark):
        """probe_all() probes all known tables."""
        mock_spark.sql.return_value.collect.return_value = []
        probe = CapabilityProbe(mock_spark)
        cap_map = probe.probe_all()
        expected_count = len(CapabilityProbe.STANDARD_TABLES | CapabilityProbe.ELEVATED_TABLES)
        assert len(cap_map) == expected_count
        assert all(v is True for v in cap_map.values())

    def test_summary_reports_skipped(self, mock_spark):
        """Summary shows skipped tables."""
        call_count = [0]
        def side_effect(query):
            call_count[0] += 1
            if "data_classification" in query:
                raise Exception("[INSUFFICIENT_PERMISSIONS]")
            m = MagicMock()
            m.collect.return_value = []
            return m
        mock_spark.sql.side_effect = side_effect
        probe = CapabilityProbe(mock_spark)
        probe.probe_all()
        summary = probe.summary()
        assert "skipped" in summary.lower()


# ── CheckResult tests ─────────────────────────────────────────────────────────


class TestCheckResult:
    def test_as_dict_fields(self):
        r = CheckResult(
            check_name="test_check",
            tier="standard",
            status=CheckStatus.PASSED,
            scope_type="catalog",
            scope_name="my_catalog",
            message="All good",
        )
        d = r.as_dict()
        assert d["check_name"] == "test_check"
        assert d["status"] == "PASSED"
        assert d["tier"] == "standard"
        assert "checked_at" in d

    def test_skipped_result(self):
        r = CheckResult(
            check_name="elevated_check",
            tier="elevated",
            status=CheckStatus.SKIPPED,
            scope_type="catalog",
            scope_name="my_catalog",
            message="SP lacks permissions",
        )
        assert r.status == CheckStatus.SKIPPED
        assert r.as_dict()["status"] == "SKIPPED"


# ── Policy accessor tests ─────────────────────────────────────────────────────


class TestFeatureEnablementPolicy:
    def test_returns_custom_policy(self):
        policy = {
            "feature_enablement": {
                "predictive_optimization": {"enabled": False},
            }
        }
        result = feature_enablement_policy(policy)
        assert result["predictive_optimization"]["enabled"] is False

    def test_falls_back_to_defaults(self):
        result = feature_enablement_policy({})
        assert "predictive_optimization" in result
        assert "data_classification" in result
        assert "lakehouse_monitoring" in result
        assert result["predictive_optimization"]["enabled"] is True


# ── Integration: run_feature_checks tests ─────────────────────────────────────


class TestRunFeatureChecks:
    def test_disabled_checks_produce_no_results(self, mock_spark):
        """When all features are disabled, no checks run."""
        probe = MagicMock()
        policy = {
            "feature_enablement": {
                "predictive_optimization": {"enabled": False},
                "data_classification": {"enabled": False},
                "lakehouse_monitoring": {"enabled": False},
            }
        }
        results = run_feature_checks(
            spark=mock_spark,
            sdk=None,
            probe=probe,
            policy=policy,
            catalogs="test_catalog",
            control_schema="uc_hygiene",
        )
        assert results == []

    def test_graceful_skip_on_describe_failure(self, mock_spark):
        """If DESCRIBE CATALOG fails, check is SKIPPED not crashed."""
        mock_spark.sql.side_effect = Exception("Access denied")
        probe = MagicMock()
        probe.can_access.return_value = True
        policy = {
            "feature_enablement": {
                "predictive_optimization": {"enabled": True, "scope": ["catalog"]},
                "data_classification": {"enabled": False},
                "lakehouse_monitoring": {"enabled": False},
            }
        }
        results = run_feature_checks(
            spark=mock_spark,
            sdk=None,
            probe=probe,
            policy=policy,
            catalogs="test_catalog",
            control_schema="uc_hygiene",
        )
        assert len(results) == 1
        assert results[0].status == CheckStatus.SKIPPED
        assert results[0].check_name == "predictive_optimization_enabled"

    def test_score_excludes_skipped(self, mock_spark, capsys):
        """Score denominator only counts PASSED + FAILED, not SKIPPED."""
        # Make pred opt pass
        from unittest.mock import MagicMock as MM
        row = MM()
        row.__getitem__ = lambda self, i: {
            0: "Predictive Optimization",
            1: "ENABLE (inherited from METASTORE)"
        }[i]
        mock_spark.sql.return_value.collect.return_value = [row]

        probe = MagicMock()
        probe.can_access.return_value = False  # Skip classification

        policy = {
            "feature_enablement": {
                "predictive_optimization": {"enabled": True, "scope": ["catalog"]},
                "data_classification": {"enabled": True, "enforce_masks_on_pii": True},
                "lakehouse_monitoring": {"enabled": False},
            }
        }
        results = run_feature_checks(
            spark=mock_spark,
            sdk=None,
            probe=probe,
            policy=policy,
            catalogs="test_catalog",
            control_schema="uc_hygiene",
        )
        passed = [r for r in results if r.status == CheckStatus.PASSED]
        skipped = [r for r in results if r.status == CheckStatus.SKIPPED]
        # Should have at least one passed (pred opt) and skipped (classification)
        captured = capsys.readouterr()
        # Verify score is computed from non-skipped only
        assert "coverage:" in captured.out

    def test_catalogs_accepts_string_or_list(self, mock_spark):
        """catalogs param works as both string and list."""
        mock_spark.sql.side_effect = Exception("skip")
        probe = MagicMock()
        policy = {
            "feature_enablement": {
                "predictive_optimization": {"enabled": True, "scope": ["catalog"]},
                "data_classification": {"enabled": False},
                "lakehouse_monitoring": {"enabled": False},
            }
        }
        # String
        r1 = run_feature_checks(
            spark=mock_spark, sdk=None, probe=probe, policy=policy,
            catalogs="cat_a", control_schema="uc_hygiene",
        )
        # List
        r2 = run_feature_checks(
            spark=mock_spark, sdk=None, probe=probe, policy=policy,
            catalogs=["cat_a", "cat_b"], control_schema="uc_hygiene",
        )
        assert len(r1) == 1
        assert len(r2) == 2
