"""
Stale Asset Cost Attribution — joins staleness findings to billing data.

Produces the one number that funds the program: annualized spend on tables
nobody queries. One SQL join turns "you have 400 stale tables" into
"you have 400 stale tables costing $18k/quarter in storage and refresh compute."
"""


def compute_stale_waste(spark, catalog: str, control_schema: str) -> "DataFrame":
    """
    Join stale scan findings to cost_attribution_daily to produce per-table
    annualized waste cost.

    Returns DataFrame:
      catalog_name, schema_name, table_name, days_stale, finding_severity,
      daily_cost_avg, annualized_waste, cost_currency, top_sku

    Uses the last 30 days of cost_attribution_daily averaged and projected to 365 days.
    """
    from lib.common import safe_table_ref

    scan_table = safe_table_ref(catalog, control_schema, "scan_results")
    cost_table = safe_table_ref(catalog, control_schema, "cost_attribution_daily")

    result = spark.sql(f"""
    WITH stale_findings AS (
      SELECT
        catalog_name, schema_name, table_name,
        finding_severity,
        CAST(REGEXP_EXTRACT(finding_detail, '(\\d+) days', 1) AS INT) AS days_stale
      FROM {scan_table}
      WHERE scan_type = 'staleness'
        AND resolved_at IS NULL
        AND finding_type IN ('stale_warning', 'stale_critical')
    ),
    recent_costs AS (
      SELECT
        catalog_name, schema_name, table_name,
        AVG(daily_dbu_cost) AS daily_cost_avg,
        MAX(sku_name) AS top_sku,
        MAX(currency_code) AS cost_currency
      FROM {cost_table}
      WHERE attribution_date >= DATE_ADD(CURRENT_DATE(), -30)
      GROUP BY catalog_name, schema_name, table_name
    )
    SELECT
      s.catalog_name,
      s.schema_name,
      s.table_name,
      s.days_stale,
      s.finding_severity,
      ROUND(COALESCE(c.daily_cost_avg, 0), 2) AS daily_cost_avg,
      ROUND(COALESCE(c.daily_cost_avg, 0) * 365, 2) AS annualized_waste,
      COALESCE(c.cost_currency, 'USD') AS cost_currency,
      c.top_sku
    FROM stale_findings s
    LEFT JOIN recent_costs c
      ON s.catalog_name = c.catalog_name
      AND s.schema_name = c.schema_name
      AND s.table_name = c.table_name
    ORDER BY annualized_waste DESC
    """)

    return result


def summarize_waste(spark, catalog: str, control_schema: str) -> dict:
    """
    Produce the headline waste numbers for dashboards and notifications.

    Returns dict:
      total_stale_tables, total_annualized_waste, top_10_wasters (list),
      waste_by_severity (dict)
    """
    waste_df = compute_stale_waste(spark, catalog, control_schema)
    waste_df.createOrReplaceTempView("_stale_waste")

    summary = spark.sql("""
    SELECT
      COUNT(*) AS total_stale_tables,
      ROUND(SUM(annualized_waste), 2) AS total_annualized_waste,
      ROUND(SUM(CASE WHEN finding_severity = 'critical' THEN annualized_waste ELSE 0 END), 2) AS critical_waste,
      ROUND(SUM(CASE WHEN finding_severity = 'warning' THEN annualized_waste ELSE 0 END), 2) AS warning_waste
    FROM _stale_waste
    """).first()

    top_10 = spark.sql("""
    SELECT catalog_name, schema_name, table_name, annualized_waste, days_stale
    FROM _stale_waste
    WHERE annualized_waste > 0
    ORDER BY annualized_waste DESC
    LIMIT 10
    """).collect()

    return {
        "total_stale_tables": summary.total_stale_tables,
        "total_annualized_waste": summary.total_annualized_waste,
        "waste_by_severity": {
            "critical": summary.critical_waste,
            "warning": summary.warning_waste,
        },
        "top_10_wasters": [
            {"table": f"{r.catalog_name}.{r.schema_name}.{r.table_name}",
             "annualized_waste": r.annualized_waste,
             "days_stale": r.days_stale}
            for r in top_10
        ],
    }
