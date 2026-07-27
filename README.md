# UC Steward

A grab-bag of cleanup utilities for Unity Catalog. Most teams come out of a migration (EDW, HMS → UC, workspace consolidation, etc.) with some combination of stale tables nobody owns, naming that made sense to one person three years ago, missing tags, and zero documentation. This gives you a starting point for getting it under control, and a maturity curve for tightening things up over time.

<img width="2816" height="1536" alt="ucsteward-updated" src="https://github.com/user-attachments/assets/bcfb69d1-1251-418f-b4c3-de7f6c9a8725" />

It's a scheduled Databricks job (deployed via DAB) that reads your UC metadata and system tables, writes findings to a set of control-plane Delta tables, and optionally notifies owners. It proposes changes — it doesn't execute them without review.

## What's in the box

Pick what's useful, ignore what isn't. Roughly ordered from "start here" to "add later":

| Module | What it does |
| --- | --- |
| **Staleness detector** | Finds tables that haven't been written to or queried in N days (configurable). Joins to cost data so you can see what you're spending on dead weight. |
| **Tag compliance scanner** | Checks tables against your required tags (owner, PII classification, domain, whatever you define in `policies/policy.yml`). |
| **Naming convention enforcer** | Flags tables/schemas that don't match your naming rules. Suggests fixes. |
| **Metadata enricher** | Uses an LLM (any FMAPI endpoint) to generate missing column descriptions and table comments from schema + sample data. Writes proposals, not direct updates. |
| **Certification workflow** | Tracks which tables meet your bar for "certified" (has owner, has docs, passes tag checks, no staleness). Maintains the certification tag in UC. |
| **Schema drift detector** | Snapshots INFORMATION_SCHEMA daily and diffs it. Flags column additions, removals, type changes. |
| **Reconciliation planner** | The big one. When you need to rename/move tables, it generates migration plans with bridge views (a view at the old location pointing to the new one) so downstream consumers don't break. TTL-based expiry. |
| **Cost attribution** | Rolls up system.billing.usage to a per-table daily cost estimate. Feeds the staleness detector and the dashboard. |
| **Owner notifier** | Sends findings to table owners via Slack, email, or Jira (configure what you use). |
| **Domain assignment scanner** | Finds tables with no UC domain assigned. Suggests domains based on schema-level majority voting. |
| **Decertification detector** | Watches certified tables for drift, staleness, or coverage loss that should revoke certification. |
| **Semantic drift review** | When schema drift invalidates a glossary term, metric view, or certification, routes a review request to the accountable owner. |
| **Monthly housekeeping** | Expires old bridge views past their TTL, archives completed findings, compacts control-plane tables. |

Plus a Jira sync module (optional) and access proposal generator (suggests GRANT changes based on query patterns).

## How it works

```
┌─────────────────────────┐
│  UC System Tables        │  system.information_schema.*
│  + Billing + Lineage     │  system.billing.usage
│  + Query History         │  system.query.history
└───────────┬─────────────┘
            │ read
            ▼
┌─────────────────────────┐
│  UC Steward Job          │  Scheduled daily/weekly
│  (notebooks + library)   │  Serverless compute
└───────────┬─────────────┘
            │ write
            ▼
┌─────────────────────────┐
│  Control Plane Tables    │  findings, proposals,
│  (your_catalog.steward)  │  notification_log,
│                          │  bridge_views, scores
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  Notifications + Dashboard │
│  (Slack / Email / Jira)    │
└─────────────────────────┘
```

Everything flows through those control-plane tables. The dashboard reads from them. Notifications are triggered by them. If you don't like how a module works, you can ignore its findings without breaking anything else.

## Design philosophy

- **Propose, don't execute.** Findings go to a table. Humans approve. Bridge views are the only thing that get created automatically (because they're backward-compatible by definition).
- **UC is the system of record.** We read metadata from UC and write trust signals (tags, comments, certifications) back to UC. No shadow catalog.
- **Policy-driven.** Thresholds, required tags, naming patterns, staleness windows — all in `policies/policy.yml`. Edit it to match your standards.
- **Cherry-pick friendly.** Every module is independent. Start with staleness + cost (instant wins), add the rest when you're ready.

## Quickstart

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- System tables access (`system.billing.usage`, `system.information_schema.*`, `system.query.history`)
- A catalog + schema for control-plane tables (e.g., `your_catalog.steward`)
- Service principal with `USE CATALOG`, `USE SCHEMA`, `SELECT` on target catalogs + `CREATE TABLE` on the steward schema
- (Optional) FMAPI endpoint access for the metadata enricher

### Setup

1. Clone this repo and configure your DAB target:

```bash
git clone https://github.com/kevin-ippen/uc-steward.git
cd uc-steward
```

2. Edit `databricks.yml` — set your catalog, schema, and target workspace.

3. Edit `policies/policy.yml` — define your naming conventions, required tags, staleness thresholds.

4. Deploy:

```bash
databricks bundle deploy --target dev
```

5. Run the bootstrap to create control-plane tables:

```bash
databricks bundle run steward_bootstrap --target dev
```

6. Run a scan:

```bash
databricks bundle run steward_daily --target dev
```

### Configuration

The interactive setup notebook (`00_config.ipynb`) walks you through configuration if you prefer that to editing YAML directly.

Key settings in `policies/policy.yml`:

```yaml
staleness:
  warning_days: 30
  critical_days: 90

tags:
  required:
    - owner
    - pii_classification
    - domain

naming:
  table_pattern: "^[a-z][a-z0-9_]*$"
  schema_pattern: "^[a-z][a-z0-9_]*$"

bridge_views:
  default_ttl_days: 90
  critical_ttl_days: 180
```

## Safety model

- **Nothing destructive runs by default.** The job scans and writes findings. That's it.
- **Bridge views are the only writes to production schemas**, and they're additive (a view at an old name pointing to a new location). They have a TTL and get cleaned up by the housekeeping module.
- **The enricher and reconciliation planner write proposals** to control-plane tables. A human reviews and approves before anything touches production metadata.
- **Dry-run mode** is available for every module (`--dry-run` or set in config).

## Maturity curve

You don't need to adopt everything at once. A reasonable progression:

1. **Week 1:** Deploy staleness detector + cost attribution. See what's dead and what it costs you.
2. **Week 2:** Add tag compliance scanner. Define your required tags. See the gap.
3. **Week 3:** Turn on owner notifications. People start fixing things.
4. **Month 2:** Add naming enforcer + metadata enricher. Start filling in docs.
5. **Month 3:** Enable certification workflow. Set your bar. Track progress on the dashboard.
6. **Ongoing:** Add schema drift, domain assignment, reconciliation planner as needs arise.

## Project structure

```
uc-steward/
├── src/
│   ├── 00_control_plane_bootstrap.py   # Creates control-plane tables
│   ├── 01_staleness_detector.ipynb     # Find stale tables
│   ├── 02_tag_compliance_scanner.ipynb # Check required tags
│   ├── 03_naming_convention_enforcer.ipynb
│   ├── 04_metadata_enricher.ipynb      # LLM-assisted descriptions
│   ├── 05_certification_workflow.ipynb
│   ├── 06_owner_notifier.ipynb
│   ├── 07_reconciliation_planner.ipynb # Migration plans + bridge views
│   ├── 08_cost_attribution_tracker.py
│   ├── 09_monthly_housekeeping.py      # Expire bridges, archive old findings
│   ├── 10–15: drift, jira, domains, semantic review
│   ├── config.py
│   └── lib/                            # Shared library code + tests
├── policies/policy.yml                 # Your rules (edit this)
├── resources/
│   ├── jobs.yml                        # Job definitions
│   ├── alerts.yml                      # Alert definitions
│   └── schemas.yml
├── dashboards/                         # Pre-built Lakeview dashboard
├── examples/                           # Sample outputs
├── docs/
│   ├── ARCHITECTURE.md
│   └── PERMISSIONS.md                  # Full SP permission requirements
├── databricks.yml                      # DAB bundle config
├── 00_config.ipynb                     # Interactive setup
└── CONTRIBUTING.md
```

## What this is not

- **Not a replacement for UC governance features.** As Databricks ships native capabilities (Discover, Domains, quality monitors), modules here become less necessary. That's the goal.
- **Not an agent.** It's a batch job. It runs on a schedule, writes findings, and shuts down.
- **Not safe to run unreviewed.** The reconciliation planner can create bridge views. Read the plans it generates before approving execution.

## Requirements

- Databricks Runtime 14.3+ (serverless recommended)
- Unity Catalog enabled workspace
- Python 3.10+
- `databricks-sdk`, `pyyaml`, `jinja2`
- (Optional) FMAPI access for metadata enrichment — any Foundation Model API endpoint works (defaults to `databricks-claude-haiku-4-5`)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Issues and PRs welcome.

## License

Apache 2.0. See [LICENSE](LICENSE).
