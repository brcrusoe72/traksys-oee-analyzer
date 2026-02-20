# Operations Intelligence Analyzer

Operations intelligence for manufacturing lines. Upload MES exports and get actionable OEE, downtime, and shift-level analysis in Excel and PDF formats.

Live demo: https://operations-intelligence-analyzer-esh6dt3bptdjg83ubda4wb.streamlit.app/

Compatibility note: Compatible with MES exports, including common vendor formats.

## What It Does

- Production-weighted OEE and loss analysis
- Downtime Pareto and fault classification
- Shift deep dives with hour-by-hour patterns
- Trend/SPC support with historical learning memory
- Optional photo/context extraction
- Streamlit upload and report delivery flow

## Quick Start

```bash
git clone https://github.com/brcrusoe72/operations-intelligence-analyzer.git
cd operations-intelligence-analyzer
pip install -r operations-intelligence-analyzer/requirements.txt
streamlit run streamlit_app.py
```

CLI usage:

```bash
python operations-intelligence-analyzer/analyze.py your_export.xlsx
python operations-intelligence-analyzer/analyze.py your_export.xlsx --downtime kb.json
```

## Inputs

- OEE exports (single-sheet or multi-sheet)
- Event summary/downtime exports
- Passdown/context files
- Optional photo uploads

## Architecture

```text
Raw MES Data -> Parse/Normalize -> Analysis Engine -> Learning Memory -> Reports
```

| Module | Role |
|---|---|
| `operations-intelligence-analyzer/analyze.py` | Core analysis orchestration and Excel outputs |
| `operations-intelligence-analyzer/streamlit_app.py` | Interactive web application |
| `operations-intelligence-analyzer/shift_report.py` | Shift deep-dive reporting |
| `operations-intelligence-analyzer/oee_history.py` | Trend/SPC and run memory |
| `operations-intelligence-analyzer/analysis_report.py` | PDF report generation |
| `operations-intelligence-analyzer/parse_mes.py` | MES parsing and format detection |
| `operations-intelligence-analyzer/parse_passdown.py` | Passdown parsing |
| `operations-intelligence-analyzer/photo_analysis.py` | Optional image/context extraction |
| `operations-intelligence-analyzer/shared.py` | Shared constants and domain helpers |
| `operations-intelligence-analyzer/data_normalization.py` | Header mapping and value normalization |
| `operations-intelligence-analyzer/operations_intelligence.py` | Action scoring and handoff logic |
| `operations-intelligence-analyzer/db.py` | Optional Supabase persistence |

## Vendor-Neutral Naming

Do not introduce vendor trademarks into product names, public slugs, metadata, or screenshots unless technical interoperability explicitly requires it.

## Tests

```bash
python -m pytest operations-intelligence-analyzer/ -v
```

## License

MIT
