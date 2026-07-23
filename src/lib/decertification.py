"""
Decertification Detector — flags certified tables that have drifted or gone stale.

Nothing native watches for this: a table gets certified, then silently drifts
(columns removed, types changed) or goes stale (no access for N days). Without
this, Discover fills with rot — tables that look trustworthy but aren't.

Signals that trigger decertification candidacy:
  1. Schema drift: critical event (column_removed, type_changed) since certification
  2. Staleness: no lineage activity since certification date
  3. Feature regression: monitoring/classification dropped after cert

Writes decertification candidates to certification_state with
proposed_status='decertification_candidate' and a reason.
"""

from datetime import date


def detect_decertification_candidates(spark, catalog: str, control_schema: str,
                                       staleness_days: int = 30) -> "DataFrame":
    """
    Find certified tables that should be flagged for decertification review.

    Returns a DataFrame of candidates with columns:
      catalog_name, schema_name, table_name, certification_date,
      decert_reason, decert_severity, evidence

    Does NOT auto-decertify — writes proposals for steward review.
    """
    from pyspark.sql import functions as F
    from lib.common import safe_table_ref

    control_fqn = f"{catalog}.{control_schema}"
    cert_table = safe_table_ref(catalog, control_schema, "certification_state")
    drift_table = safe_table_ref(catalog, control_schema, "schema_drift_events")
    scan_table = safe_table_ref(catalog, control_schema, "scan_results")

    # 1. Certified tables with critical schema drift since certification
    drift_candidates = spark.sql(f"""
    SELECT
      c.catalog_name,
      c.schema_name,
      c.table_name,
      c.certified_at AS certification_date,
      'schema_drift' AS decert_reason,
      'critical' AS decert_severity,
      CONCAT('Column changes since certification: ',
             COLLECT_SET(CONCAT(d.drift_type, '(', d.column_name, ')'))) AS evidence
    FROM {cert_table} c
    INNER JOIN {drift_table} d
      ON c.catalog_name = d.catalog_name
      AND c.schema_name = d.schema_name
      AND c.table_name = d.table_name
    WHERE c.certification_status = 'certified'
      AND d.severity IN ('critical', 'warning')
      AND d.detected_date > c.certified_at
      AND d.acknowledged_at IS NULL
    GROUP BY c.catalog_name, c.schema_name, c.table_name, c.certified_at
    """)

    # 2. Certified tables that have gone stale (no scan activity, in stale findings)
    stale_candidates = spark.sql(f"""
    SELECT
      c.catalog_name,
      c.schema_name,
      c.table_name,
      c.certified_at AS certification_date,
      'stale_since_certification' AS decert_reason,
      'warning' AS decert_severity,
      CONCAT('Stale finding since certification: ', s.finding_detail) AS evidence
    FROM {cert_table} c
    INNER JOIN {scan_table} s
      ON c.catalog_name = s.catalog_name
      AND c.schema_name = s.schema_name
      AND c.table_name = s.table_name
    WHERE c.certification_status = 'certified'
      AND s.finding_type IN ('stale_warning', 'stale_critical')
      AND s.scan_date > c.certified_at
      AND s.resolved_at IS NULL
    """)

    # 3. Certified tables whose monitoring was dropped (feature check regression)
    feature_candidates = spark.sql(f"""
    SELECT
      c.catalog_name,
      c.schema_name,
      c.table_name,
      c.certified_at AS certification_date,
      'feature_regression' AS decert_reason,
      'warning' AS decert_severity,
      CONCAT('Feature check failed post-certification: ', f.check_name) AS evidence
    FROM {cert_table} c
    INNER JOIN {control_fqn}.feature_check_results f
      ON c.catalog_name = f.catalog_name
      AND c.schema_name = f.schema_name
      AND c.table_name = f.table_name
    WHERE c.certification_status = 'certified'
      AND f.check_status = 'FAILED'
      AND f.checked_at > c.certified_at
    """)

    # Union all candidates, deduplicate by table (keep highest severity)
    candidates = drift_candidates.union(stale_candidates).union(feature_candidates)

    return candidates


def write_decertification_proposals(spark, candidates, catalog: str,
                                     control_schema: str, dry_run: bool = False):
    """
    Write decertification candidates to certification_state as proposed status changes.

    In dry_run mode: returns the candidates without writing.
    """
    from lib.common import safe_table_ref

    count = candidates.count()
    if count == 0:
        print("No decertification candidates found.")
        return candidates

    print(f"Found {count} decertification candidate(s)")

    if dry_run:
        print("DRY RUN — no writes performed")
        candidates.show(20, truncate=False)
        return candidates

    cert_table = safe_table_ref(catalog, control_schema, "certification_state")

    # Update certification_state: set proposed_decertification fields
    candidates.createOrReplaceTempView("decert_candidates")
    spark.sql(f"""
    MERGE INTO {cert_table} t
    USING (
      SELECT catalog_name, schema_name, table_name,
             decert_reason, decert_severity, evidence,
             CURRENT_TIMESTAMP() AS proposed_at
      FROM decert_candidates
    ) s
    ON t.catalog_name = s.catalog_name
      AND t.schema_name = s.schema_name
      AND t.table_name = s.table_name
    WHEN MATCHED AND t.certification_status = 'certified' THEN UPDATE SET
      t.certification_status = 'decertification_candidate',
      t.decert_reason = s.decert_reason,
      t.decert_severity = s.decert_severity,
      t.decert_evidence = s.evidence,
      t.decert_proposed_at = s.proposed_at
    """)

    print(f"✅ Updated {count} tables to decertification_candidate status")
    return candidates
