"""Unit tests for src/lib/policy.py pure functions.

No Spark session needed — only tests the accessors and helpers.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from lib.policy import (
    required_table_tags,
    pii_patterns,
    naming_patterns,
    anti_patterns,
)


class TestRequiredTableTags:
    def test_empty_policy_returns_default(self):
        assert required_table_tags({}) == ["owner", "domain", "quality_tier"]

    def test_policy_value_used(self):
        policy = {"required_table_tags": ["owner", "team"]}
        assert required_table_tags(policy) == ["owner", "team"]

    def test_widget_override_wins_over_policy(self):
        policy = {"required_table_tags": ["owner", "domain", "quality_tier"]}
        result = required_table_tags(policy, widget_override="owner,cost_center")
        assert result == ["owner", "cost_center"]

    def test_widget_override_trims_whitespace(self):
        result = required_table_tags({}, widget_override=" owner , domain ")
        assert result == ["owner", "domain"]

    def test_widget_override_empty_string_falls_back(self):
        policy = {"required_table_tags": ["owner"]}
        assert required_table_tags(policy, widget_override="") == ["owner"]


class TestPiiPatterns:
    def test_empty_policy_returns_default(self):
        result = pii_patterns({})
        assert "email" in result
        assert "phone" in result
        assert isinstance(result["email"], list)

    def test_policy_value_used(self):
        policy = {"pii_patterns": {"ssn": [".*ssn.*"]}}
        result = pii_patterns(policy)
        assert "ssn" in result
        assert "email" not in result  # only what's in policy


class TestNamingPatterns:
    def test_defaults(self):
        t, s = naming_patterns({})
        assert t == "^[a-z][a-z0-9_]*$"
        assert s == "^[a-z][a-z0-9_]*$"

    def test_policy_overrides(self):
        policy = {
            "table_name_pattern":  "^[a-z][a-z0-9_]{2,63}$",
            "schema_name_pattern": "^[a-z][a-z0-9_]{2,31}$",
        }
        t, s = naming_patterns(policy)
        assert t == "^[a-z][a-z0-9_]{2,63}$"
        assert s == "^[a-z][a-z0-9_]{2,31}$"


class TestAntiPatterns:
    def test_empty_policy_returns_defaults(self):
        result = anti_patterns({})
        assert len(result) > 0
        assert all(isinstance(item, tuple) and len(item) == 2 for item in result)

    def test_policy_list_of_dicts(self):
        policy = {
            "naming_anti_patterns": [
                {"pattern": ".*_backup$", "description": "Backup suffix"},
                {"pattern": ".*_old$",    "description": "Old suffix"},
            ]
        }
        result = anti_patterns(policy)
        assert len(result) == 2
        assert result[0] == (".*_backup$", "Backup suffix")
        assert result[1] == (".*_old$",    "Old suffix")

    def test_policy_list_of_tuples(self):
        policy = {"naming_anti_patterns": [(".*_tmp$", "Temp table")]}
        result = anti_patterns(policy)
        assert result == [(".*_tmp$", "Temp table")]

    def test_missing_description_defaults_empty_string(self):
        policy = {"naming_anti_patterns": [{"pattern": ".*_test$"}]}
        result = anti_patterns(policy)
        assert result[0] == (".*_test$", "")
