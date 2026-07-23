"""Schema drift detection for UC Steward.

Compares current information_schema.columns against the last captured
baseline and emits drift events (column_added, column_removed, type_changed).

Usage:
    from lib.schema_drift import detect_schema_drift, capture_baseline

    # First run (or reset): capture current state as baseline
    capture_baseline(spark, catalog, control_schema, target_catalogs)

    # Subsequent runs: detect drift + update baseline
    drift_df = detect_schema_drift(spark, catalog, control_schema, target_catalogs)
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from typing import Any

from .common import safe_table_ref

__all__ = ["detect_schema_drift", "capture_baseline"]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _classify_severity(drift_type: str) -> str:
    """Assign severity based on drift type."""
    if drift_type == "column_removed":
        return "critical"
    elif drift_type == "type_changed":
        return "warning"
    return "info"  # column_added, nullable_changed


def capture_baseline(
    spark: Any,
    catalog: str,
    control_schema: str,
    target_catalogs: list[str] | str,
) -> int:
    """Capture current column metadata as the drift baseline.

    Uses MERGE to upsert: existing columns are updated, new columns are inserted,
    and columns that no longer exist are left (detect_schema_drift handles removal).
    Returns the number of columns captured.
    """
    if isinstance(target_catalogs, str):
        target_catalogs = [target_catalogs]

    baseline_ref = safe_table_ref(catalog, control_schema, "schema_column_baseline")
    now = _now_utc()
    total = 0

    for cat in target_catalogs:
        info_schema_ref = f"`{cat}`.`information_schema`.`columns`"

        # Get current columns into a temp view
        spark.sql(f"""
            SELECT
                table_catalog AS catalog_name,
                table_schema AS schema_name,
                table_name,
                column_name,
                full_data_type AS data_type,
                CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS is_nullable,
                comment AS column_comment,
                ordinal_position,
                TIMESTAMP '{now.strftime("%Y-%m-%d %H:%M:%S")}' AS captured_at
            FROM {info_schema_ref}
            WHERE table_schema != '{control_schema}'
              AND table_schema != 'information_schema'
        """).createOrReplaceTempView("_baseline_source")

        # MERGE: upsert current columns into baseline
        spark.sql(f"""
            MERGE INTO {baseline_ref} tgt
            USING _baseline_source src
              ON tgt.catalog_name = src.catalog_name
             AND tgt.schema_name = src.schema_name
             AND tgt.table_name = src.table_name
             AND tgt.column_name = src.column_name
            WHEN MATCHED THEN UPDATE SET
              data_type = src.data_type,
              is_nullable = src.is_nullable,
              column_comment = src.column_comment,
              ordinal_position = src.ordinal_position,
              captured_at = src.captured_at
            WHEN NOT MATCHED THEN INSERT *
        """)

        cnt = spark.sql(f"""
            SELECT COUNT(*) FROM {baseline_ref} WHERE catalog_name = '{cat}'
        """).first()[0]
        total += cnt

    return total


def detect_schema_drift(
    spark: Any,
    catalog: str,
    control_schema: str,
    target_catalogs: list[str] | str,
) -> Any:
    """Detect schema drift by comparing current columns to baseline.

    Writes events to schema_drift_events and updates the baseline.
    Returns a DataFrame of new drift events (empty if no drift detected).
    """
    if isinstance(target_catalogs, str):
        target_catalogs = [target_catalogs]

    baseline_ref = safe_table_ref(catalog, control_schema, "schema_column_baseline")
    events_ref = safe_table_ref(catalog, control_schema, "schema_drift_events")
    today = date.today()

    all_events = []

    for cat in target_catalogs:
        # Check if baseline exists for this catalog
        baseline_count = spark.sql(f"""
            SELECT COUNT(*) FROM {baseline_ref} WHERE catalog_name = '{cat}'
        """).first()[0]

        if baseline_count == 0:
            # No baseline: capture it and skip drift detection
            capture_baseline(spark, catalog, control_schema, cat)
            continue

        info_schema_ref = f"`{cat}`.`information_schema`.`columns`"

        # Load current columns
        spark.sql(f"""
            SELECT
                table_catalog AS catalog_name,
                table_schema AS schema_name,
                table_name,
                column_name,
                full_data_type AS data_type,
                CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS is_nullable,
                comment AS column_comment
            FROM {info_schema_ref}
            WHERE table_schema != '{control_schema}'
              AND table_schema != 'information_schema'
        """).createOrReplaceTempView("_drift_current")

        # Load baseline columns
        spark.sql(f"""
            SELECT catalog_name, schema_name, table_name, column_name,
                   data_type, is_nullable, column_comment
            FROM {baseline_ref}
            WHERE catalog_name = '{cat}'
        """).createOrReplaceTempView("_drift_baseline")

        # --- ADDED columns (in current, not in baseline) ---
        added = spark.sql("""
            SELECT c.catalog_name, c.schema_name, c.table_name, c.column_name,
                   c.data_type, c.is_nullable, c.column_comment
            FROM _drift_current c
            LEFT ANTI JOIN _drift_baseline b
              ON c.catalog_name = b.catalog_name
             AND c.schema_name = b.schema_name
             AND c.table_name = b.table_name
             AND c.column_name = b.column_name
        """).collect()

        for row in added:
            all_events.append((
                str(uuid.uuid4()), today, row["catalog_name"],
                row["schema_name"], row["table_name"], "column_added",
                row["column_name"], None,
                json.dumps({"type": row["data_type"], "nullable": row["is_nullable"]}),
                "info", None, None,
            ))

        # --- REMOVED columns (in baseline, not in current) ---
        removed = spark.sql("""
            SELECT b.catalog_name, b.schema_name, b.table_name, b.column_name,
                   b.data_type, b.is_nullable, b.column_comment
            FROM _drift_baseline b
            LEFT ANTI JOIN _drift_current c
              ON b.catalog_name = c.catalog_name
             AND b.schema_name = c.schema_name
             AND b.table_name = c.table_name
             AND b.column_name = c.column_name
        """).collect()

        for row in removed:
            all_events.append((
                str(uuid.uuid4()), today, row["catalog_name"],
                row["schema_name"], row["table_name"], "column_removed",
                row["column_name"],
                json.dumps({"type": row["data_type"], "nullable": row["is_nullable"]}),
                None, "critical", None, None,
            ))

        # --- TYPE / NULLABLE CHANGES ---
        type_changed = spark.sql("""
            SELECT c.catalog_name, c.schema_name, c.table_name, c.column_name,
                   b.data_type AS old_type, c.data_type AS new_type,
                   b.is_nullable AS old_nullable, c.is_nullable AS new_nullable
            FROM _drift_current c
            JOIN _drift_baseline b
              ON c.catalog_name = b.catalog_name
             AND c.schema_name = b.schema_name
             AND c.table_name = b.table_name
             AND c.column_name = b.column_name
            WHERE c.data_type != b.data_type
               OR c.is_nullable != b.is_nullable
        """).collect()

        for row in type_changed:
            drift_type = "type_changed" if row["old_type"] != row["new_type"] else "nullable_changed"
            all_events.append((
                str(uuid.uuid4()), today, row["catalog_name"],
                row["schema_name"], row["table_name"], drift_type,
                row["column_name"],
                json.dumps({"type": row["old_type"], "nullable": row["old_nullable"]}),
                json.dumps({"type": row["new_type"], "nullable": row["new_nullable"]}),
                _classify_severity(drift_type), None, None,
            ))

        # Update baseline for this catalog after detecting drift
        capture_baseline(spark, catalog, control_schema, cat)

    # Write drift events to table
    if all_events:
        schema_str = (
            "event_id STRING, detected_date DATE, catalog_name STRING, "
            "schema_name STRING, table_name STRING, drift_type STRING, "
            "column_name STRING, previous_state STRING, current_state STRING, "
            "severity STRING, acknowledged_at TIMESTAMP, acknowledged_by STRING"
        )
        events_df = spark.createDataFrame(all_events, schema_str)
        events_df.createOrReplaceTempView("_new_drift_events")
        spark.sql(f"INSERT INTO {events_ref} SELECT * FROM _new_drift_events")
        return events_df

    # Return empty DataFrame with correct schema
    return spark.sql(f"SELECT * FROM {events_ref} WHERE 1=0")
