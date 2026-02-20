<<<<<<< HEAD
# MES OEE Analyzer — Full Repository Assessment and Next-Build Plan
=======
# Operations Intelligence Analyzer - Full Repository Assessment and Next-Build Plan
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)

## Repository status

<<<<<<< HEAD
## What this repository is
This project is a **production-focused manufacturing analytics toolkit** that ingests MES/MES exports and generates:
- Multi-sheet Excel analysis workbooks,
- Shift deep-dive reports,
- Optional PDF reports,
- Trend/SPC insights over historical runs,
- Optional AI-assisted context extraction from uploaded photos,
- Optional Supabase persistence for equipment knowledge and run history.

At a high level, the repo contains:
1. A Python analysis engine (`analyze.py`) for OEE + downtime decomposition,
2. Data ingestion/parsing modules for raw MES and shift passdown files,
3. A Streamlit UI for file upload and report generation,
4. A historical trend engine (`oee_history.py`) for control limits and run intelligence,
5. A database adapter (`db.py`) plus SQL schema for Supabase-backed persistence,
6. Unit tests covering core math, reporting, deep history, shift reporting, and photo-analysis transforms.
=======
- Primary source folder: `operations-intelligence-analyzer/`
- Root launcher: `streamlit_app.py`
- Root dependency entrypoint: `requirements.txt`

Compatibility note: Compatible with MES exports, including common vendor formats.
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)

## What this repository provides

<<<<<<< HEAD
### Root-level structure
- `streamlit_app.py` (root): thin launcher that imports and executes app code from `mes-oee-analyzer/streamlit_app.py` so Streamlit Cloud can run from repo root.
- `requirements.txt` (root): app dependencies for deployment.
- `CLAUDE.md`: architecture and command reference for contributors.
- `mes-oee-analyzer/`: primary source folder.

### Core package folder: `mes-oee-analyzer/`
=======
- OEE and downtime analysis engine
- Shift deep-dive reporting
- Optional PDF generation
- Historical trend/SPC analysis
- Optional image/context extraction
- Optional Supabase persistence

## Core layout
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)

- `operations-intelligence-analyzer/analyze.py`: core analysis orchestration
- `operations-intelligence-analyzer/parse_mes.py`: MES parser and format detection
- `operations-intelligence-analyzer/parse_passdown.py`: passdown parser
- `operations-intelligence-analyzer/shift_report.py`: shift deep dive reports
- `operations-intelligence-analyzer/oee_history.py`: append-only history and trend logic
- `operations-intelligence-analyzer/streamlit_app.py`: interactive UI
- `operations-intelligence-analyzer/db.py`: optional Supabase adapter
- `operations-intelligence-analyzer/schema.sql`: persistence schema

<<<<<<< HEAD
#### 2) Parsing/ingestion
- `parse_mes.py`
  - Handles raw MES “OEE Period Detail” interval-based exports.
  - Normalizes timestamps, shift naming, shift-hour rollups, and metric extraction.
- `parse_passdown.py`
  - Parses operator passdown spreadsheets and maps them into downtime/event structures compatible with analysis.
=======
## Recommended roadmap
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)

1. Reliability
- Keep CI required for pull requests.
- Add/expand golden regression fixtures for report and parser outputs.

2. Modularity
- Continue splitting large analysis pathways into testable modules.
- Strengthen typed interfaces for major data structures.

3. Operations intelligence
- Improve action ranking by impact and confidence.
- Expand shift handoff outputs and trend alerting.

4. Platformization
- Add multi-line and multi-plant profile support.
- Harden migration/versioning flow for persistence mode.

## Internal naming guideline

Use vendor-neutral identifiers and public naming. Avoid vendor trademarks in product names, public slugs, metadata, screenshots, and primary docs unless interoperability explicitly requires a vendor label.
