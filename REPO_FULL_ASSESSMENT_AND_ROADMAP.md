# Operations Intelligence Analyzer - Repository Assessment and Roadmap

## Repository Status

- Canonical source path: `operations-intelligence-analyzer/`
- Root launcher: `streamlit_app.py`
- Root dependency entrypoint: `requirements.txt`
- Compatibility note: Compatible with MES exports, including common vendor formats.

## What This Repository Provides

- OEE and downtime analysis engine
- Shift deep-dive reporting
- PDF report generation
- Trend/SPC analysis with run memory
- Optional image/context extraction
- Optional Supabase persistence

## Core Layout

- `operations-intelligence-analyzer/analyze.py`: core analysis orchestration
- `operations-intelligence-analyzer/parse_mes.py`: MES parser and format detection
- `operations-intelligence-analyzer/parse_passdown.py`: passdown parser
- `operations-intelligence-analyzer/shift_report.py`: shift deep-dive reports
- `operations-intelligence-analyzer/oee_history.py`: append-only history and trend logic
- `operations-intelligence-analyzer/streamlit_app.py`: interactive UI
- `operations-intelligence-analyzer/analysis_report.py`: PDF reporting
- `operations-intelligence-analyzer/db.py`: optional Supabase adapter
- `operations-intelligence-analyzer/schema.sql`: persistence schema

## Recommended Roadmap

1. Reliability
- Keep CI required for pull requests.
- Expand golden regression fixtures for parser and reporting outputs.

2. Modularity
- Continue splitting large pathways into testable modules.
- Strengthen typed interfaces for key data structures.

3. Operations Intelligence
- Improve action ranking by impact and confidence.
- Expand shift handoff output and trend alerting.

4. Platformization
- Add multi-line and multi-plant profile support.
- Harden migration/versioning flow for persistence mode.

## Internal Naming Guideline

Use vendor-neutral identifiers and public naming. Avoid vendor trademarks in product names, public slugs, metadata, screenshots, and primary docs unless interoperability explicitly requires a vendor label.
