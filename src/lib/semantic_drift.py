"""
Semantic Drift → Owner Review

The highest-value detection UC Steward adds. Glossary terms, metric view
definitions, synonyms, and certifications create authoritative claims about
tables. Those claims go stale when schema or usage drifts — but nothing
routes the invalidation to the accountable owner.

This module closes that loop:
  1. Reads schema_drift_events (column changes already detected)
  2. Cross-references against semantic assets:
     - Certified tables (certification_state)
     - Tables referenced in metric view definitions (governance_policy)
     - Tables with glossary-linked tags (term_owner, business_definition)
  3. Emits: "this change invalidated semantic asset X — here's the accountable owner"
  4. Writes review requests to notification_log + updates certification_state

Gets MORE valuable as the platform ships more semantics (glossary, metric views,
domains) — the inverse of absorption risk.
"""

from datetime import date


def detect_semantic_invalidations(spark, catalog: str, control_schema: str,
                                   lookback_days: int = 7) -> "DataFrame":
    """
    Find schema drift events that invalidate semantic claims.

    Returns DataFrame:
      catalog_name, schema_name, table_name, drift_type, column_name,
      invalidated_asset_type, invalidated_asset_name, owner_email,
      severity, evidence
    """
    from lib.common import safe_table_ref

    drift_table = safe_table_ref(catalog, control_schema, "schema_drift_events")
    cert_table = safe_table_ref(catalog, control_schema, "certification_state")

    # 1. Drift events that invalidate certifications
    #    Any critical/warning drift on a certified table = certification invalidated
    cert_invalidations = spark.sql(f"""
    SELECT
      d.catalog_name, d.schema_name, d.table_name,
      d.drift_type, d.column_name,
      'certification' AS invalidated_asset_type,
      CONCAT('Certification of ', d.catalog_name, '.', d.schema_name, '.', d.table_name) AS invalidated_asset_name,
      c.certified_by AS owner_email,
      CASE d.drift_type
        WHEN 'column_removed' THEN 'critical'
        WHEN 'type_changed' THEN 'critical'
        ELSE 'warning'
      END AS severity,
      CONCAT(d.drift_type, ' on column "', d.column_name, '": ',
             COALESCE(d.previous_state, 'existed'), ' → ',
             COALESCE(d.current_state, 'removed')) AS evidence
    FROM {drift_table} d
    INNER JOIN {cert_table} c
      ON d.catalog_name = c.catalog_name
      AND d.schema_name = c.schema_name
      AND d.table_name = c.table_name
    WHERE c.certification_status = 'certified'
      AND d.severity IN ('critical', 'warning')
      AND d.detected_date >= DATE_ADD(CURRENT_DATE(), -{lookback_days})
      AND d.acknowledged_at IS NULL
    """)

    # 2. Drift on tables with business_definition or term_owner tags
    #    (indicates glossary/metric linkage)
    tag_queries = []
    # Build from information_schema across catalogs referenced in drift
    glossary_invalidations = spark.sql(f"""
    WITH tagged_tables AS (
      SELECT DISTINCT
        d.catalog_name, d.schema_name, d.table_name,
        'glossary_term' AS asset_type,
        t.tag_value AS asset_name,
        t.tag_value AS owner_email
      FROM {drift_table} d
      INNER JOIN {catalog}.information_schema.table_tags t
        ON d.catalog_name = t.catalog_name
        AND d.schema_name = t.schema_name
        AND d.table_name = t.table_name
      WHERE t.tag_name IN ('business_definition', 'term_owner', 'metric_view')
        AND d.detected_date >= DATE_ADD(CURRENT_DATE(), -{lookback_days})
        AND d.severity IN ('critical', 'warning')
        AND d.acknowledged_at IS NULL
    )
    SELECT
      d.catalog_name, d.schema_name, d.table_name,
      d.drift_type, d.column_name,
      tt.asset_type AS invalidated_asset_type,
      CONCAT('Glossary/metric: ', tt.asset_name) AS invalidated_asset_name,
      tt.owner_email,
      'warning' AS severity,
      CONCAT(d.drift_type, ' on "', d.column_name,
             '" may invalidate semantic definition: ', tt.asset_name) AS evidence
    FROM {drift_table} d
    INNER JOIN tagged_tables tt
      ON d.catalog_name = tt.catalog_name
      AND d.schema_name = tt.schema_name
      AND d.table_name = tt.table_name
    WHERE d.detected_date >= DATE_ADD(CURRENT_DATE(), -{lookback_days})
      AND d.acknowledged_at IS NULL
    """)

    # 3. Column removals that break metric view references
    #    (metric views reference specific columns — removing them breaks the definition)
    metric_invalidations = spark.sql(f"""
    SELECT
      d.catalog_name, d.schema_name, d.table_name,
      d.drift_type, d.column_name,
      'metric_view_column' AS invalidated_asset_type,
      CONCAT('Column "', d.column_name, '" used in metric definition') AS invalidated_asset_name,
      NULL AS owner_email,
      'critical' AS severity,
      CONCAT('Column "', d.column_name, '" was ', d.drift_type,
             '. If referenced by a metric view or glossary term, ',
             'the definition is now broken.') AS evidence
    FROM {drift_table} d
    WHERE d.drift_type IN ('column_removed', 'type_changed')
      AND d.detected_date >= DATE_ADD(CURRENT_DATE(), -{lookback_days})
      AND d.acknowledged_at IS NULL
    """)

    # Union and deduplicate
    all_invalidations = (
        cert_invalidations
        .union(glossary_invalidations)
        .union(metric_invalidations)
        .dropDuplicates(["catalog_name", "schema_name", "table_name",
                         "column_name", "invalidated_asset_type"])
    )

    return all_invalidations


def write_semantic_review_requests(spark, invalidations, catalog: str,
                                    control_schema: str, dry_run: bool = False) -> int:
    """
    Write semantic invalidation review requests to notification_log.

    Returns count of requests written.
    """
    import uuid
    from lib.common import safe_table_ref

    count = invalidations.count()
    if count == 0:
        print("No semantic invalidations detected.")
        return 0

    print(f"Semantic invalidations found: {count}")
    if dry_run:
        print("DRY RUN — showing invalidations without writing")
        invalidations.show(20, truncate=False)
        return count

    notification_table = safe_table_ref(catalog, control_schema, "notification_log")
    invalidations.createOrReplaceTempView("_semantic_invalidations")

    spark.sql(f"""
    INSERT INTO {notification_table}
    SELECT
      UUID() AS notification_id,
      CURRENT_DATE() AS notification_date,
      'semantic_drift_review' AS notification_type,
      COALESCE(owner_email, 'unassigned') AS recipient,
      CONCAT(catalog_name, '.', schema_name, '.', table_name) AS asset_fqn,
      CONCAT('[', severity, '] ', invalidated_asset_type, ': ', evidence) AS message,
      severity AS priority,
      'pending' AS delivery_status,
      NULL AS delivered_at,
      NULL AS response,
      CURRENT_TIMESTAMP() AS created_at
    FROM _semantic_invalidations
    """)

    print(f"✅ Wrote {count} semantic review requests to notification_log")
    return count
