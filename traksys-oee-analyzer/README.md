# Traksys OEE Analyzer

**[Try it live](https://traksys-oee-analyzer-esh6dt3bptdjg83ubda4wb.streamlit.app/)**

Production-grade OEE analysis suite for food manufacturing. Reads Traksys/MES data exports and generates multi-sheet Excel reports with shift deep dives, downtime Pareto analysis, fault classification, and prioritized action recommendations.

Built by a CI engineer who got tired of the Pareto analysis never getting done.

## What It Does

Drop in your Traksys OEE export. Get back a formatted, color-coded Excel workbook with:

- **Executive Summary** — KPIs, date range, cases produced, top/bottom shifts
- **Shift Deep Dives** — Hour-by-hour patterns, worst hours, consistency scores, day-of-week breakdowns
- **Loss Breakdown** — Availability vs Performance vs Quality by shift
- **Downtime Pareto** — Top causes ranked by total minutes with fault classification
- **Fault Classification** — Equipment, Micro Stops, Process/Changeover, Scheduled, Data Gaps
- **Worst Hours** — The 25 worst OEE hours with root cause analysis
- **What to Focus On** — Prioritized action items with 5-step investigation plans

## Two Tools

| Script | Purpose | Output |
|--------|---------|--------|
| `analyze.py` | Plant-wide OEE + downtime analysis across all shifts | 12-15 sheet Excel |
| `shift_report.py` | Deep dive on any shift with product-level granularity + target tracking | 13-15 sheet Excel + email .txt |

## Quick Start

```bash
pip install -r requirements.txt

# Basic OEE analysis
python analyze.py your_oee_export.xlsx

# Full analysis with downtime reason codes
python analyze.py your_oee_export.xlsx --downtime knowledge_base.json

# Shift deep dive (any shift) with product data
python shift_report.py oee_export.xlsx --shift "3rd" --downtime kb.json --product product_data.json

# Same tool works for any shift
python shift_report.py oee_export.xlsx --shift "1st"
python shift_report.py oee_export.xlsx --shift "2nd"
```

## Streamlit App

**Live at:** https://traksys-oee-analyzer-esh6dt3bptdjg83ubda4wb.streamlit.app/

Or run locally:

```bash
streamlit run streamlit_app.py
```

Upload your Excel file, optionally add downtime JSON, click Analyze, download the result.

The app also includes a **Standards reference** panel (line/product targets, cases per pallet, pieces per case), sourced from `standards_reference.csv`, so operations teams can use plant standards as an in-app reference.

## Input Data Format

### OEE Export (Excel)
Your Traksys export needs these sheets:
- **DayShiftHour** — Hourly data with columns: Shift Date, Shift, Shift Hour, Time Block, Block Start, Block End, Cases/Hr, OEE (%), Total Cases, Total Hours, Availability, Performance, Quality, Intervals

### Downtime Knowledge Base (JSON, optional)
```json
{
  "downtime_reason_codes": [
    {"reason": "Caser - Riverwood", "total_minutes": 8489, "total_occurrences": 2282}
  ],
  "pareto_top_10": {
    "rankings": [...]
  }
}
```

## Who This Is For

- **CI Engineers** who never have time to build the Pareto
- **Plant Managers** who want numbers from the machine, not opinions
- **Supervisors** who need shift-level accountability data
- **Anyone** running Traksys, Vorne, or similar MES systems with OEE exports

## The Philosophy

- Numbers from the machine, not opinions
- No paragraphs — numbers first, short action items
- Red/yellow/green so you can scan in 10 seconds
- Every sheet answers "so what?" with specific next steps

## License

MIT
