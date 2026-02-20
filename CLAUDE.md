# CLAUDE.md

This file captures the **current state of the repository** so coding agents can keep their core assumptions aligned with what actually exists.

## Project Overview

**Operations Intelligence Analyzer** is a production-focused OEE analysis suite for manufacturing operations. It ingests MES exports (including MES-style exports), classifies downtime causes, computes production-weighted KPIs, and emits Excel/PDF/web outputs.

Primary implementation lives in `mes-oee-analyzer/` (repository path retained for compatibility), with a thin root-level `streamlit_app.py` launcher.

## Current Core Assumptions (verified)

1. **Tests are the strongest source of behavioral truth.**
   - The repository currently has 207 passing tests across core math, parser behavior, report generation, shift reporting, photo analysis, standards parsing, and operations intelligence.
2. **`analyze.py` remains the orchestration center.**
   - It owns fuzzy loading, metric calculation, narrative generation, and Excel output assembly.
3. **`shift_report.py` is the active deep-dive report path.**
   - Legacy references to `third_shift_report.py` / `third_shift_targets.py` are outdated and should not be assumed.
4. **Normalization and ingestion are modularized.**
   - `data_normalization.py`, `ingest_router.py`, and `canonical_schema.py` exist and are part of the current ingestion strategy.
5. **Persistence is optional and mixed-mode.**
   - Local file artifacts (`history.jsonl` / trend JSON) are still expected, while `db.py` supports optional Supabase-backed persistence.
6. **Streamlit entrypoint exists in two places for convenience.**
   - Root `streamlit_app.py` forwards into package app code in `mes-oee-analyzer/streamlit_app.py`.
7. **Branding was generalized, but paths remain stable.**
   - User-facing naming is now "Operations Intelligence Analyzer" while historical `mes-*` file/repo names remain in place.

## Commands

```bash
# Install dependencies
pip install -r mes-oee-analyzer/requirements.txt

# Run all tests
python -m pytest mes-oee-analyzer/ -v

# Run a focused suite
python -m pytest mes-oee-analyzer/test_core.py -v

# Run Streamlit UI (root launcher)
streamlit run streamlit_app.py

# CLI analysis
python mes-oee-analyzer/analyze.py <oee_export.xlsx> [--downtime kb.json]
```

No linter/formatter is enforced in this repository.

## Architecture

```
Input (Excel/JSON/Photos/Passdowns)
  → Ingestion + Normalization
  → Analysis Engine
  → Reporting + Trend Intelligence
```

### Active Modules

- **`analyze.py`** — Main OEE pipeline: fuzzy sheet/column matching, weighted KPIs, downtime narratives, and workbook generation.
- **`shared.py`** — Canonical domain constants and logic: fault keywords, product normalization, rated speeds, and helper utilities.
- **`parse_mes.py`** — MES export parsing and format detection.
- **`parse_passdown.py`** — Shift passdown parsing into analyzable downtime/event records.
- **`data_normalization.py`** — Header/value normalization and canonical field preparation.
- **`ingest_router.py`** — Routes incoming files to parser/normalization pathways.
- **`canonical_schema.py`** — Shared schema definitions for normalized structures.
- **`oee_history.py`** — Run history and trend/SPC intelligence.
- **`analysis_report.py`** — PDF analysis report generation.
- **`shift_report.py`** — Shift deep-dive reporting path.
- **`photo_analysis.py`** — Vision-assisted issue extraction from photos/attachments.
- **`operations_intelligence.py`** — Action scoring, trend anomaly support, and handoff intelligence.
- **`db.py`** — Optional Supabase persistence and schema helpers.
- **`streamlit_app.py`** — Interactive web app.

## Design Expectations

- Prefer **production-weighted metrics** (`Σ(metric × hours) / Σ(hours)`) over naive averaging.
- Maintain **fuzzy compatibility** with real-world export variability (sheet names and column headers).
- Keep **fault classification precedence** deterministic and centralized in shared logic.
- Preserve **append-only analysis memory/trend behavior** unless explicitly asked to migrate it.
