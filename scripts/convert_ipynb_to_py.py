"""One-time migration: convert .ipynb notebooks to Databricks .py source format.

Run from a notebook cell:
    exec(open("/Workspace/Users/kevin.ippen@databricks.com/uc-steward/scripts/convert_ipynb_to_py.py").read())
"""
import json, os

SRC_DIR = "/Workspace/Users/kevin.ippen@databricks.com/uc-steward/src"

TARGETS = [
    "01_staleness_detector",
    "02_tag_compliance_scanner",
    "03_naming_convention_enforcer",
    "04_metadata_enricher",
    "05_certification_workflow",
    "06_owner_notifier",
    "07_reconciliation_planner",
]

def ipynb_to_dbpy(ipynb_path):
    with open(ipynb_path, "r") as f:
        nb = json.load(f)
    lines = ["# Databricks notebook source\n"]
    for i, cell in enumerate(nb.get("cells", [])):
        cell_type = cell.get("cell_type", "code")
        source = "".join(cell.get("source", []))
        db_meta = cell.get("metadata", {}).get("application/vnd.databricks.v1+cell", {})
        title = db_meta.get("title", "")
        show_title = db_meta.get("showTitle", False)
        if i > 0:
            lines.append("\n# COMMAND ----------\n\n")
        if title and show_title:
            lines.append(f"# DBTITLE 1,{title}\n")
        if cell_type == "markdown":
            md_lines = source.split("\n")
            if md_lines and md_lines[0].strip().startswith("%md"):
                lines.append(f"# MAGIC {md_lines[0]}\n")
                for ml in md_lines[1:]:
                    lines.append(f"# MAGIC {ml}\n" if ml else "# MAGIC\n")
            else:
                lines.append("# MAGIC %md\n")
                for ml in md_lines:
                    lines.append(f"# MAGIC {ml}\n" if ml else "# MAGIC\n")
        else:
            if source.startswith("%md") or source.startswith("%sql"):
                for cl in source.split("\n"):
                    lines.append(f"# MAGIC {cl}\n" if cl else "# MAGIC\n")
            else:
                lines.append(source)
                if not source.endswith("\n"):
                    lines.append("\n")
    return "".join(lines)

converted = 0
for name in TARGETS:
    ipynb_path = os.path.join(SRC_DIR, f"{name}.ipynb")
    py_path = os.path.join(SRC_DIR, f"{name}.py")
    if os.path.exists(py_path) and not os.path.exists(ipynb_path):
        print(f"  SKIP {name} (already .py)")
        continue
    if not os.path.exists(ipynb_path):
        print(f"  SKIP {name} (.ipynb not found)")
        continue
    try:
        py_content = ipynb_to_dbpy(ipynb_path)
        os.remove(ipynb_path)
        with open(py_path, "w") as f:
            f.write(py_content)
        converted += 1
        print(f"  OK {name}.ipynb -> {name}.py ({len(py_content):,} chars)")
    except Exception as e:
        print(f"  FAIL {name}: {e}")
print(f"\nConverted: {converted}/{len(TARGETS)}")
