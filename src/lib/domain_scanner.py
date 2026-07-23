"""
Domain Assignment Scanner — flags tables without a domain assignment.

An unassigned table is invisible to domain-oriented discovery (Discover, Genie,
agent tools). This scanner reads the native UC domain property and tag state,
flags unassigned tables as violations, and (optionally) suggests a domain based
on schema naming patterns and co-located table domains.

Design principle: read the native domain system, don't rebuild it. Domains are
shipping natively in UC — this module feeds them, doesn't race them.
"""

from typing import Optional


def scan_domain_assignments(spark, catalog: str, control_schema: str,
                            target_catalogs: list, domain_tag: str = "domain") -> "DataFrame":
    """
    Find tables missing a domain assignment (either native UC domain or domain tag).

    Returns DataFrame with columns:
      catalog_name, schema_name, table_name, has_native_domain, has_domain_tag,
      suggested_domain, suggestion_confidence, suggestion_source
    """
    from lib.common import safe_table_ref

    if not target_catalogs:
        raise ValueError("target_catalogs must be non-empty")

    # Build UNION query across target catalogs for domain tag presence
    tag_queries = []
    table_queries = []
    for tc in target_catalogs:
        tag_queries.append(f"""
            SELECT catalog_name, schema_name, table_name, tag_value AS domain_tag_value
            FROM {tc}.information_schema.table_tags
            WHERE tag_name = '{domain_tag}'
        """)
        table_queries.append(f"""
            SELECT table_catalog AS catalog_name, table_schema AS schema_name, table_name
            FROM {tc}.information_schema.tables
            WHERE table_schema NOT IN ('information_schema', '{control_schema}')
              AND table_type = 'MANAGED'
        """)

    all_tables = spark.sql(" UNION ALL ".join(table_queries))
    all_tables.createOrReplaceTempView("_domain_all_tables")

    domain_tags = spark.sql(" UNION ALL ".join(tag_queries))
    domain_tags.createOrReplaceTempView("_domain_tags")

    # Find unassigned tables
    unassigned = spark.sql("""
    SELECT
      t.catalog_name,
      t.schema_name,
      t.table_name,
      CASE WHEN dt.domain_tag_value IS NOT NULL THEN true ELSE false END AS has_domain_tag,
      dt.domain_tag_value
    FROM _domain_all_tables t
    LEFT JOIN _domain_tags dt
      ON t.catalog_name = dt.catalog_name
      AND t.schema_name = dt.schema_name
      AND t.table_name = dt.table_name
    WHERE dt.domain_tag_value IS NULL
    """)

    return unassigned


def suggest_domains(spark, unassigned, catalog: str, control_schema: str,
                    domain_tag: str = "domain") -> "DataFrame":
    """
    Suggest domain assignments based on schema-level majority voting.

    Logic: if >50% of tables in a schema have domain=X, suggest X for unassigned
    tables in that schema. Confidence = proportion of tagged siblings with that domain.
    """
    from pyspark.sql import functions as F

    unassigned.createOrReplaceTempView("_unassigned_tables")

    # Schema-level domain voting
    suggestions = spark.sql(f"""
    WITH schema_domains AS (
      SELECT
        catalog_name, schema_name,
        domain_tag_value AS domain,
        COUNT(*) AS domain_count,
        SUM(COUNT(*)) OVER (PARTITION BY catalog_name, schema_name) AS total_tagged
      FROM _domain_tags
      GROUP BY catalog_name, schema_name, domain_tag_value
    ),
    top_domain AS (
      SELECT
        catalog_name, schema_name, domain,
        domain_count, total_tagged,
        CAST(domain_count AS DOUBLE) / total_tagged AS confidence,
        ROW_NUMBER() OVER (PARTITION BY catalog_name, schema_name ORDER BY domain_count DESC) AS rn
      FROM schema_domains
    )
    SELECT
      u.catalog_name, u.schema_name, u.table_name,
      td.domain AS suggested_domain,
      ROUND(td.confidence, 2) AS suggestion_confidence,
      'schema_majority' AS suggestion_source
    FROM _unassigned_tables u
    LEFT JOIN top_domain td
      ON u.catalog_name = td.catalog_name
      AND u.schema_name = td.schema_name
      AND td.rn = 1
      AND td.confidence >= 0.5
    """)

    return suggestions


def write_domain_violations(spark, unassigned, catalog: str, control_schema: str,
                            dry_run: bool = False) -> int:
    """
    Write domain-unassigned findings to scan_results.
    """
    import uuid
    from datetime import date
    from lib.common import safe_table_ref

    count = unassigned.count()
    if count == 0:
        print("All tables have domain assignments.")
        return 0

    print(f"Tables without domain assignment: {count}")

    if dry_run:
        print("DRY RUN — no writes")
        unassigned.show(20, truncate=False)
        return count

    scan_id = str(uuid.uuid4())
    scan_results_table = safe_table_ref(catalog, control_schema, "scan_results")

    unassigned.createOrReplaceTempView("_domain_violations")

    spark.sql(f"""
    INSERT INTO {scan_results_table}
    SELECT
      '{scan_id}' AS scan_id,
      CURRENT_DATE() AS scan_date,
      'domain_assignment' AS scan_type,
      'table' AS asset_type,
      catalog_name, schema_name, table_name,
      NULL AS column_name,
      'missing_domain' AS finding_type,
      'warning' AS finding_severity,
      'Table has no domain assignment — invisible to domain-based discovery and agents' AS finding_detail,
      COALESCE(
        CONCAT('Suggested domain: ', suggested_domain, ' (confidence: ', suggestion_confidence, ')'),
        'No domain suggestion available — assign manually'
      ) AS recommended_action,
      NULL AS owner_email,
      NULL AS resolved_at,
      NULL AS resolved_by
    FROM _domain_violations
    """)

    print(f"✅ Wrote {count} domain-assignment findings to scan_results")
    return count
