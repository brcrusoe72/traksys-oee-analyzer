# CLAUDE.md

Contributor guidance for coding agents working in this repository.

## Project Overview

Operations Intelligence Analyzer ingests MES exports, computes production-weighted KPIs, classifies downtime causes, and generates Excel/PDF/web outputs.

Primary implementation path: `operations-intelligence-analyzer/`  
Root launcher: `streamlit_app.py`

Compatibility note: Compatible with MES exports, including common vendor formats.

## Commands

```bash
# Install dependencies
pip install -r operations-intelligence-analyzer/requirements.txt

# Run tests
python -m pytest operations-intelligence-analyzer/test_core.py -v
python -m pytest operations-intelligence-analyzer/test_analysis_report.py -v
python -m pytest operations-intelligence-analyzer/ -v

# Run app
streamlit run streamlit_app.py

# CLI analysis
python operations-intelligence-analyzer/analyze.py <oee_export.xlsx> [--downtime kb.json]
```

## Current Core Assumptions

1. Tests are the primary behavioral truth and should be updated with parser/logic changes.
2. `analyze.py` remains the orchestration center for ingest, metric calculation, and workbook output.
3. `shift_report.py` is the active deep-dive reporting path.
4. Ingestion is modular (`parse_mes.py`, `parse_passdown.py`, `data_normalization.py`, `ingest_router.py`).
5. Persistence is mixed-mode: local history files are standard; Supabase in `db.py` is optional.
6. Root `streamlit_app.py` is a thin launcher to package app code.
7. Naming is vendor-neutral in product-facing text and identifiers unless interoperability requires otherwise.

## Active Modules

- `operations-intelligence-analyzer/analyze.py`
- `operations-intelligence-analyzer/parse_mes.py`
- `operations-intelligence-analyzer/parse_passdown.py`
- `operations-intelligence-analyzer/data_normalization.py`
- `operations-intelligence-analyzer/ingest_router.py`
- `operations-intelligence-analyzer/canonical_schema.py`
- `operations-intelligence-analyzer/oee_history.py`
- `operations-intelligence-analyzer/shift_report.py`
- `operations-intelligence-analyzer/analysis_report.py`
- `operations-intelligence-analyzer/photo_analysis.py`
- `operations-intelligence-analyzer/operations_intelligence.py`
- `operations-intelligence-analyzer/db.py`
- `operations-intelligence-analyzer/streamlit_app.py`

## Design Expectations

- Use production-weighted metrics over naive averaging.
- Preserve fuzzy compatibility for sheet/header variability.
- Keep fault classification precedence deterministic and centralized.
- Preserve append-only memory/trend behavior unless explicitly asked to migrate it.
- Keep naming vendor-neutral in public-facing text and primary identifiers.
