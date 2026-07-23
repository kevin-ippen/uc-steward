"""Unit tests for src/lib/common.py pure functions.

No Spark session needed — only tests that don't call spark/sdk.
"""
import sys
import os

# Allow running from repo root: pytest src/lib/tests/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

from lib.common import (
    build_exempt_schemas, is_exempt,
    validate_identifier, validate_model_name, validate_uuid, safe_table_ref,
)


class TestBuildExemptSchemas:
    def test_always_includes_builtins(self):
        result = build_exempt_schemas("my_control")
        assert "information_schema" in result
        assert "__databricks_internal" in result

    def test_includes_control_schema(self):
        result = build_exempt_schemas("uc_hygiene_dev")
        assert "uc_hygiene_dev" in result

    def test_returns_frozenset(self):
        assert isinstance(build_exempt_schemas("x"), frozenset)

    def test_different_control_schemas_dont_bleed(self):
        dev  = build_exempt_schemas("uc_hygiene_dev")
        prod = build_exempt_schemas("uc_hygiene")
        assert "uc_hygiene_dev" not in prod
        assert "uc_hygiene" not in dev


class TestIsExempt:
    def test_empty_exemptions_never_exempt(self):
        assert not is_exempt("cat", "sch", "tbl", [])

    def test_catalog_match(self):
        ex = [{"pattern_type": "catalog", "pattern": "sandbox_cat"}]
        assert     is_exempt("sandbox_cat", "any", "tbl", ex)
        assert not is_exempt("prod_cat",    "any", "tbl", ex)

    def test_schema_match(self):
        ex = [{"pattern_type": "schema", "pattern": "vendor_raw"}]
        assert     is_exempt("any", "vendor_raw", "tbl", ex)
        assert not is_exempt("any", "other",      "tbl", ex)

    def test_table_fqn_match(self):
        ex = [{"pattern_type": "table", "pattern": "cat.sch.special_table"}]
        assert     is_exempt("cat", "sch", "special_table", ex)
        assert not is_exempt("cat", "sch", "other_table",   ex)

    def test_table_prefix_match(self):
        ex = [{"pattern_type": "table_prefix", "pattern": "tmp_"}]
        assert     is_exempt("c", "s", "tmp_staging", ex)
        assert not is_exempt("c", "s", "production",  ex)

    def test_schema_prefix_match(self):
        ex = [{"pattern_type": "schema_prefix", "pattern": "vendor_"}]
        assert     is_exempt("c", "vendor_ingested", "t", ex)
        assert not is_exempt("c", "core",            "t", ex)

    def test_empty_pattern_skipped(self):
        ex = [{"pattern_type": "catalog", "pattern": ""}]
        assert not is_exempt("anything", "s", "t", ex)

    def test_first_matching_rule_wins(self):
        ex = [
            {"pattern_type": "table_prefix", "pattern": "archive_"},
            {"pattern_type": "catalog",      "pattern": "prod"},
        ]
        assert is_exempt("other", "s", "archive_2023", ex)  # first rule
        assert is_exempt("prod",  "s", "any",          ex)  # second rule

    def test_unknown_pattern_type_not_exempt(self):
        ex = [{"pattern_type": "unknown_type", "pattern": "anything"}]
        assert not is_exempt("c", "s", "t", ex)

# ── SQL Injection Hardening Tests ────────────────────────────────────────────


class TestValidateIdentifier:
    """Tests for validate_identifier()."""

    def test_valid_simple(self):
        assert validate_identifier("my_catalog", "catalog") == "my_catalog"

    def test_valid_underscores(self):
        assert validate_identifier("uc_hygiene", "schema") == "uc_hygiene"

    def test_valid_leading_underscore(self):
        assert validate_identifier("_internal", "schema") == "_internal"

    def test_valid_mixed_case(self):
        assert validate_identifier("MySchema", "schema") == "MySchema"

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("", "catalog")

    def test_rejects_leading_digit(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("1bad_name", "catalog")

    def test_rejects_semicolon(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("my_cat; DROP TABLE x", "catalog")

    def test_rejects_quotes(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("cat\'--", "catalog")

    def test_rejects_spaces(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("my catalog", "catalog")

    def test_rejects_dots(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("cat.schema", "catalog")


class TestValidateModelName:
    """Tests for validate_model_name()."""

    def test_valid_fmapi_model(self):
        assert validate_model_name("databricks-gemini-3-5-flash") == "databricks-gemini-3-5-flash"

    def test_valid_with_dots(self):
        assert validate_model_name("meta.llama-3.1-70b") == "meta.llama-3.1-70b"

    def test_rejects_quotes(self):
        with pytest.raises(ValueError, match="Invalid model_name"):
            validate_model_name("model\'; DROP TABLE x --")

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="Invalid model_name"):
            validate_model_name("")

    def test_rejects_semicolons(self):
        with pytest.raises(ValueError, match="Invalid model_name"):
            validate_model_name("model; SELECT 1")


class TestValidateUuid:
    """Tests for validate_uuid()."""

    def test_valid_uuid(self):
        assert validate_uuid("550e8400-e29b-41d4-a716-446655440000") == "550e8400-e29b-41d4-a716-446655440000"

    def test_rejects_injection(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_uuid("\' OR 1=1 --")

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_uuid("")


class TestSafeTableRef:
    """Tests for safe_table_ref()."""

    def test_two_part(self):
        assert safe_table_ref("my_cat", "my_schema") == "`my_cat`.`my_schema`"

    def test_three_part(self):
        assert safe_table_ref("cat", "schema", "table") == "`cat`.`schema`.`table`"

    def test_rejects_bad_catalog(self):
        with pytest.raises(ValueError):
            safe_table_ref("bad;cat", "schema", "table")

    def test_rejects_bad_table(self):
        with pytest.raises(ValueError):
            safe_table_ref("cat", "schema", "table; DROP")
