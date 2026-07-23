# Contributing to UC Steward

Thanks for your interest in UC Steward! This document covers the basics.

## Getting Started

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes
4. Run tests: `pytest src/lib/tests/ -v`
5. Validate the bundle: `databricks bundle validate --target dev`
6. Commit with a descriptive message
7. Open a Pull Request

## Development Setup

```bash
git clone https://github.com/kevin-ippen/uc-steward.git
cd uc-steward

# Install test dependencies
pip install pytest pyyaml

# Run tests (no Spark required — tests use mocks)
pytest src/lib/tests/ -v

# Validate bundle config
databricks bundle validate --target dev
```

## Code Style

- Python: follow PEP 8, type hints encouraged
- SQL: uppercase keywords, lowercase identifiers
- YAML: 2-space indent, quote strings with special chars
- All SQL interpolation MUST use `safe_table_ref()` or `validate_identifier()`

## Architecture Principles

1. **Detection is separate from remediation** — scanners find problems, the reconciliation planner resolves them
2. **Never auto-execute destructive actions** — plans are proposed, not applied
3. **Idempotent by default** — all writes use MERGE, safe to re-run
4. **Graceful degradation** — missing permissions cause SKIP, not FAIL
5. **Configuration over code** — governance rules live in `policies/policy.yml`

## Adding a New Scanner

1. Create `src/NN_your_scanner.ipynb`
2. Add a task to `resources/jobs.yml` in the appropriate phase
3. Write findings to `scan_results` via MERGE
4. Add tests to `src/lib/tests/`
5. Update the README

## Adding a Library Module

1. Create `src/lib/your_module.py`
2. Add to `__all__` and update `__init__.py`
3. Add unit tests in `src/lib/tests/test_your_module.py`
4. Wire into the appropriate notebook

## Reporting Issues

Use the GitHub issue templates:
- **Bug Report** — something isn't working as documented
- **Feature Request** — new scanner, integration, or behavior

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
