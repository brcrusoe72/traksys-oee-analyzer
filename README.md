# 📊 Traksys OEE Analyzer

**Production-grade OEE analysis for food manufacturing.** Upload your Traksys/MES exports, get back actionable insights — shift deep dives, downtime Pareto, fault classification, SPC trends, and prioritized recommendations.

Built by a manufacturing engineer who got tired of spreadsheet hell.

---

## What It Does

- **Automated OEE Analysis** — Production-weighted calculations (not naive averages) across availability, performance, and quality
- **Multi-Sheet Excel Reports** — Shift summaries, hourly breakdowns, downtime Pareto, dead hour narratives, and prioritized action items
- **PDF Executive Reports** — Compact 2-page scorecards consolidating up to 6 daily analyses
- **SPC Trend Detection** — Historical run tracking with Shewhart control limits, Nelson Rules violations, and chronic vs. acute downtime classification
- **Smart Fault Classification** — Automatic categorization into Equipment, Process, Scheduled, and Data Gap buckets using domain-specific keyword taxonomy
- **Fuzzy Format Handling** — Resilient to Traksys export variations with 50+ header mappings and positional fallback
- **Streamlit Web UI** — Upload files, explore results interactively, download reports

## Quick Start

### Prerequisites

- Python 3.9+
- pip

### Install

```bash
git clone https://github.com/brcrusoe72/traksys-oee-analyzer.git
cd traksys-oee-analyzer
pip install -r traksys-oee-analyzer/requirements.txt
```

### Run the Web App

```bash
streamlit run streamlit_app.py
```

Then open [http://localhost:8501](http://localhost:8501) and upload your OEE export.

### CLI Usage

```bash
# Full OEE analysis → Excel workbook
python traksys-oee-analyzer/analyze.py your_export.xlsx

# With downtime knowledge base
python traksys-oee-analyzer/analyze.py your_export.xlsx --downtime kb.json
```

## Supported Input Formats

| Format | Description |
|--------|-------------|
| **Pre-processed OEE workbook** | Excel with a `DayShiftHour` sheet (standard Traksys export) |
| **Raw OEE Period Detail** | Block-based interval export (13 rows per time period) |
| **Shift passdown spreadsheet** | Operator handoff notes (Area/Issue/Time/Notes format) |
| **Downtime knowledge base** | JSON file with equipment context and baselines |

## Output

### Excel Workbook (5+ sheets)
- **Summary** — Shift-level OEE with production-weighted rollups
- **Hourly Detail** — Hour-by-hour availability, performance, quality
- **Downtime Pareto** — Ranked fault categories with cumulative impact
- **Dead Hours** — Narrative explanations for zero-production periods
- **Actions** — Prioritized recommendations based on analysis

### PDF Report
- Page 1: Multi-day scorecard with OEE trends
- Page 2: Root cause summary and action items

### SPC / Trend Analysis
- Control charts with ±3σ limits
- Nelson Rules violation flags
- Chronic vs. acute downtime classification
- Run-over-run intelligence via append-only history

## Architecture

```
Input (Excel/JSON) → Parsing → Analysis Engine → Reporting (Excel/PDF/Web)
```

| Module | Role |
|--------|------|
| `analyze.py` | Core OEE engine — fuzzy loading, weighted math, workbook generation |
| `shared.py` | Domain constants — fault keywords, product normalization, rated speeds |
| `parse_traksys.py` | Raw Traksys export parser with format auto-detection |
| `parse_passdown.py` | Operator passdown spreadsheet parser |
| `oee_history.py` | Append-only JSONL history + SPC trend engine |
| `shift_report.py` | 13-sheet shift deep dive generator |
| `analysis_report.py` | PDF executive report builder |
| `streamlit_app.py` | Web interface for upload → analysis → download |
| `operations_intelligence.py` | Action scoring, handoff packets, anomaly detection |
| `db.py` | Optional Supabase persistence layer |

## Key Design Decisions

**Production-weighted OEE:** Always `Σ(metric × hours) / Σ(hours)`, never simple averages. Short bad intervals don't skew the picture.

**Fuzzy matching everywhere:** Headers vary across Traksys versions and plant configurations. The analyzer maps 50+ column name variants and uses positional fallback when names don't match.

**Classification hierarchy:** `Unassigned → Scheduled → Micro Stops → Process → Equipment → Fallback → Unclassified`. Order matters — more specific categories take priority.

## Tech Stack

Python · pandas · NumPy · openpyxl · xlsxwriter · fpdf2 · Streamlit · Altair

## Tests

```bash
# Run all tests
python -m pytest traksys-oee-analyzer/ -v

# Specific test suites
python -m pytest traksys-oee-analyzer/test_core.py -v          # Core OEE math
python -m pytest traksys-oee-analyzer/test_shift_report.py -v  # Shift deep dives
python -m pytest traksys-oee-analyzer/test_deep_history.py -v  # SPC/trend engine
```

## Roadmap

- [ ] Generic CSV ingestion (any OEE data, not just Traksys)
- [ ] Live demo on Streamlit Community Cloud
- [ ] PyPI package (`pip install oee-analyzer`)
- [ ] REST API for integration with other systems
- [ ] Multi-plant support

## License

MIT

---

*Built with domain knowledge from food manufacturing floors, not just tutorials.*
