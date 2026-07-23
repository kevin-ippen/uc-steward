"""UC Steward — shared notebook helpers.

Import pattern (add to the TOP of each notebook widget cell):

    import sys as _sys
    _nb = (dbutils.notebook.entry_point.getDbutils().notebook()
           .getContext().notebookPath().get())
    _sys.path.insert(0, '/Workspace' + '/'.join(_nb.split('/')[:-2]) + '/src')
    from lib.common import (
        require_widget, uc_list_tables, uc_list_schemas,
        tables_to_spark, build_exempt_schemas,
    )
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = [
    "require_widget",
    "build_exempt_schemas",
    "uc_list_tables",
    "uc_list_schemas",
    "tables_to_spark",
    "load_exemptions",
    "is_exempt",
    "validate_identifier",
    "validate_model_name",
    "safe_table_ref",
]

# Schemas always excluded regardless of target.
# build_exempt_schemas() extends this with the runtime control_schema.
_BUILTIN_EXEMPT: frozenset[str] = frozenset(
    ["information_schema", "__databricks_internal"]
)

# ── SQL Identifier Validation ──────────────────────────────────────────────────────────────────
# Defense-in-depth: all user-supplied values interpolated into SQL identifiers
# MUST pass validation before use. This prevents injection via catalog names,
# schema names, table names, or model names supplied through widgets or data.

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,254}$")
_MODEL_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._\-]{0,254}$")
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def validate_identifier(name: str, label: str = "identifier") -> str:
    """Validate a Unity Catalog identifier (catalog, schema, or table name).

    Returns the name unchanged if valid. Raises ValueError with a clear message
    if the name contains characters that could enable SQL injection.

    Args:
        name: The identifier to validate.
        label: Human-readable label for error messages (e.g. "catalog", "schema").
    """
    if not name or not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid {label}: {name!r} — must match ^[a-zA-Z_][a-zA-Z0-9_]{{0,254}}$. "
            f"This check prevents SQL injection via interpolated identifiers."
        )
    return name


def validate_model_name(name: str) -> str:
    """Validate an FMAPI model name (e.g. 'databricks-gemini-3-5-flash').

    Model names may contain letters, digits, dots, and hyphens.
    Rejects anything that could escape a SQL string literal.
    """
    if not name or not _MODEL_NAME_RE.match(name):
        raise ValueError(
            f"Invalid model_name: {name!r} — must match ^[a-zA-Z0-9][a-zA-Z0-9._-]{{0,254}}$. "
            f"This check prevents SQL injection in ai_query() calls."
        )
    return name


def validate_uuid(value: str, label: str = "UUID") -> str:
    """Validate a UUID string (lowercase hex with dashes)."""
    if not value or not _UUID_RE.match(value.lower()):
        raise ValueError(
            f"Invalid {label}: {value!r} — must be a valid UUID. "
            f"This check prevents SQL injection in WHERE clauses."
        )
    return value


def safe_table_ref(catalog: str, schema: str, table: str | None = None) -> str:
    """Build a validated, backtick-quoted fully qualified table reference.

    Examples:
        safe_table_ref("my_cat", "uc_hygiene")
            → "`my_cat`.`uc_hygiene`"
        safe_table_ref("my_cat", "uc_hygiene", "scan_results")
            → "`my_cat`.`uc_hygiene`.`scan_results`"

    Each component is validated before quoting. Backtick-quoting provides an
    additional layer of safety even after validation passes.
    """
    validate_identifier(catalog, "catalog")
    validate_identifier(schema, "schema")
    ref = f"`{catalog}`.`{schema}`"
    if table is not None:
        validate_identifier(table, "table")
        ref += f".`{table}`"
    return ref


# ── Widgets ────────────────────────────────────────────────────────────────────────────────

def require_widget(dbutils: Any, name: str) -> str:
    """Return widget value; raise ValueError if empty/missing."""
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"Missing required widget: '{name}'")
    return value


# ── Schema exemptions ───────────────────────────────────────────────────────────────────

def build_exempt_schemas(control_schema: str) -> frozenset[str]:
    """Return the full set of schemas to skip when scanning."""
    return _BUILTIN_EXEMPT | {control_schema}


# ── Timestamp helpers ───────────────────────────────────────────────────────────────────

def _ms_to_dt(ms: int | None) -> datetime | None:
    """SDK epoch-ms int → naive UTC datetime (Spark TimestampType-compatible)."""
    return (
        datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)
        if ms else None
    )


# ── UC SDK listing ────────────────────────────────────────────────────────────────────────────

def uc_list_tables(
    sdk: Any,
    catalogs: list[str] | str,
    control_schema: str,
) -> list[dict]:
    """List MANAGED/EXTERNAL tables via UC SDK. No Spark scan."""
    from databricks.sdk.service.catalog import TableType  # lazy import

    if isinstance(catalogs, str):
        catalogs = [catalogs]
    exempt = build_exempt_schemas(control_schema)
    rows: list[dict] = []
    for cat in catalogs:
        try:
            for sc in sdk.schemas.list(catalog_name=cat):
                if sc.name in exempt:
                    continue
                for t in sdk.tables.list(catalog_name=cat, schema_name=sc.name):
                    if t.table_type in (TableType.MANAGED, TableType.EXTERNAL):
                        rows.append({
                            "catalog_name": cat,
                            "schema_name":  sc.name,
                            "table_name":   t.name,
                            "table_type":   t.table_type.value,
                            "comment":      t.comment or "",
                            "full_name":    t.full_name or f"{cat}.{sc.name}.{t.name}",
                            "created_at":   _ms_to_dt(t.created_at),
                            "updated_at":   _ms_to_dt(t.updated_at),
                        })
        except Exception as e:
            print(f"  ⚠  Could not list {cat}: {e}")
    print(f"  UC SDK: {len(rows)} tables across {len(catalogs)} catalog(s)")
    return rows


def uc_list_schemas(
    sdk: Any,
    catalogs: list[str] | str,
    control_schema: str,
) -> list[dict]:
    """List schemas via UC SDK."""
    if isinstance(catalogs, str):
        catalogs = [catalogs]
    exempt = build_exempt_schemas(control_schema)
    rows: list[dict] = []
    for cat in catalogs:
        try:
            for sc in sdk.schemas.list(catalog_name=cat):
                if sc.name in exempt:
                    continue
                rows.append({"catalog_name": cat, "schema_name": sc.name})
        except Exception as e:
            print(f"  ⚠  Could not list schemas in {cat}: {e}")
    return rows


# ── Scan exemptions ─────────────────────────────────────────────────────────────────────────────

def load_exemptions(spark: Any, catalog: str, control_schema: str) -> list[dict]:
    """Load active, non-expired exemption rules from the exemptions table.

    Returns a list of dicts (keys: pattern_type, pattern, exempt_until).
    Returns [] if the table does not exist yet (pre-bootstrap).
    """
    fqn = safe_table_ref(catalog, control_schema, "exemptions")
    try:
        rows = spark.sql(
            f"SELECT pattern_type, pattern, exempt_until "
            f"FROM {fqn} "
            f"WHERE is_active = true "
            f"  AND (exempt_until IS NULL OR exempt_until >= current_date())"
        ).collect()
        result = [row.asDict() for row in rows]
        if result:
            print(f"  Exemptions: {len(result)} active rules loaded")
        return result
    except Exception as e:
        print(f"  ⚠  Could not load exemptions ({fqn}): {e}")
        return []


def is_exempt(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    exemptions: list[dict],
) -> bool:
    """Return True if this table matches any active exemption rule.

    Supported pattern_type values:
      catalog       — entire catalog is exempt
      schema        — exact schema name (any catalog)
      table         — exact fully-qualified name catalog.schema.table
      table_prefix  — table_name starts with this string
      schema_prefix — schema_name starts with this string

    Tag-based exemption (uc_hygiene_exempt = true) is a planned complement;
    add it by checking tags in the caller before passing rows to this function.
    """
    fqn = f"{catalog_name}.{schema_name}.{table_name}"
    for ex in exemptions:
        pt = ex.get("pattern_type", "")
        p  = ex.get("pattern", "")
        if not p:
            continue
        if pt == "catalog"       and catalog_name  == p:         return True
        if pt == "schema"        and schema_name   == p:         return True
        if pt == "table"         and fqn           == p:         return True
        if pt == "table_prefix"  and table_name.startswith(p):  return True
        if pt == "schema_prefix" and schema_name.startswith(p): return True
    return False


# ── Spark helpers ─────────────────────────────────────────────────────────────────────────────

def tables_to_spark(spark: Any, rows: list[dict]) -> "DataFrame":
    """Convert uc_list_tables() rows to a Spark DataFrame."""
    from pyspark.sql import Row
    from pyspark.sql.types import (
        StringType, StructField, StructType, TimestampType,
    )

    schema = StructType([
        StructField("table_catalog", StringType()),
        StructField("table_schema",  StringType()),
        StructField("table_name",    StringType()),
        StructField("table_type",    StringType()),
        StructField("comment",       StringType()),
        StructField("created_at",   TimestampType()),
        StructField("last_altered", TimestampType()),
    ])
    data = [
        Row(
            table_catalog=r["catalog_name"],
            table_schema=r["schema_name"],
            table_name=r["table_name"],
            table_type=r["table_type"],
            comment=r["comment"],
            created_at=r.get("created_at"),
            last_altered=r.get("updated_at"),
        )
        for r in rows
    ]
    return spark.createDataFrame(data, schema)
