<<<<<<< HEAD
# 📊 Operations Intelligence Analyzer
=======
# Operations Intelligence Analyzer
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)

Operations intelligence for manufacturing. Upload raw MES exports and receive actionable analysis: fault classification, downtime Pareto, SPC trends, shift deep dives, and prioritized recommendations.

Live demo: https://operations-intelligence-analyzer-esh6dt3bptdjg83ubda4wb.streamlit.app/

<<<<<<< HEAD
### [🚀 Live Demo](https://operations-intelligence-analyzer.streamlit.app/)
=======
Compatibility note: Compatible with MES exports, including common vendor formats.
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)

## What it does

- Production-weighted OEE and loss analysis
- Downtime Pareto with cause classification
- Shift-level deep dives and dead-hour narratives
- AI-assisted photo/context issue extraction
- Historical trend memory and anomaly support
- Excel and PDF outputs plus Streamlit UI

<<<<<<< HEAD
### 📈 Analysis Engine
- **Production-weighted OEE** — `Σ(metric × hours) / Σ(hours)`, not naive averages that let short bad intervals skew the picture
- **Automatic fault classification** — Equipment, Process, Scheduled, Data Gap — using domain-specific keyword taxonomy
- **Downtime Pareto** — Ranked by impact with cumulative tracking
- **Dead hour narratives** — Human-readable explanations for zero-production periods
- **Multi-line support** — Analyze multiple production lines simultaneously with per-line breakdowns

### 📷 AI Photo Analysis
- Upload floor photos, work orders, or shift notes
- OpenAI Vision extracts equipment issues, severity, and estimated duration
- Findings merge directly into the analysis pipeline and shift narratives

### 🧠 Learning Memory
- Remembers every analysis run (append-only JSONL history)
- Deduplicates identical uploads automatically
- Tracks revisions when same-period data changes
- Builds institutional knowledge over time

### 📊 SPC Trend Detection
- Shewhart control charts with ±3σ limits
- Nelson Rules violation detection
- Chronic vs. acute downtime classification
- Run-over-run intelligence across historical analyses

### 🎯 Operations Intelligence
- Action item scoring and prioritization
- Shift handoff packet generation
- Trend anomaly detection
- Per-product granularity with consistency scoring

### 📄 Reporting
- **Multi-sheet Excel workbooks** — Summary, hourly detail, downtime Pareto, dead hours, prioritized actions
- **2-page PDF executive reports** — Scorecard + root cause/actions (consolidates up to 6 daily analyses)
- **Interactive web dashboard** — Upload, explore, download

## Input Formats

The analyzer handles raw MES data — not just tidy spreadsheets:

| Format | Example | What It Contains |
|--------|---------|-----------------|
| **OEE Period Detail** | 69-column MES export, 3000+ hourly rows | OEE, MTBF/MTTR, TEEP, availability loss seconds, production units |
| **Event Overview** | 21-column event log, 50K-70K+ records | Every downtime event with fault codes, durations, equipment IDs |
| **Pre-processed workbook** | DayShiftHour format | Cleaned hourly OEE by shift |
| **Shift passdown** | Operator handoff notes | Area/Issue/Time/Notes from the floor |
| **Photos** | Floor shots, work orders | AI-extracted equipment issues |
| **Knowledge base** | JSON downtime context | Equipment baselines and historical patterns |

### Fuzzy format handling
Don't worry about exact column names. The analyzer maps 50+ header variants with positional fallback — it adapts to your export format, not the other way around.

## Quick Start

### Install

```bash
git clone https://github.com/brcrusoe72/operations-intelligence-analyzer.git
cd operations-intelligence-analyzer
pip install -r requirements.txt
```

### Run the Web App
=======
## Quick start
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)

```bash
git clone https://github.com/brcrusoe72/operations-intelligence-analyzer.git
cd operations-intelligence-analyzer
pip install -r operations-intelligence-analyzer/requirements.txt
streamlit run streamlit_app.py
```

CLI example:

```bash
<<<<<<< HEAD
# Full OEE analysis → Excel workbook
python <package_dir>/analyze.py your_export.xlsx

# With downtime context
python <package_dir>/analyze.py your_export.xlsx --downtime kb.json
=======
python operations-intelligence-analyzer/analyze.py your_export.xlsx
python operations-intelligence-analyzer/analyze.py your_export.xlsx --downtime kb.json
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)
```

## Inputs

- OEE period-detail exports
- Event summary/downtime exports
- Pre-processed hourly workbooks
- Shift passdown files
- Optional photo/context files

## Architecture

```text
Raw MES Data -> Parsing/Normalization -> Analysis Engine -> Learning Memory -> Reporting
```

<<<<<<< HEAD
| Module | Lines | Role |
|--------|-------|------|
| `analyze.py` | 2,500 | Core OEE engine — fuzzy loading, weighted math, workbook generation |
| `streamlit_app.py` | 1,100 | Multi-tab web interface with learning memory panel |
| `shift_report.py` | 1,400 | 13-sheet shift deep dive — hourly patterns, product granularity, day-of-week breakdowns |
| `oee_history.py` | 1,150 | Append-only JSONL history + SPC trend engine with Nelson Rules |
| `analysis_report.py` | 925 | PDF executive report builder |
| `MES parser module` | 550 | Raw MES export parser with format auto-detection |
| `photo_analysis.py` | 510 | AI vision pipeline — photos → equipment issues → downtime dictionaries |
| `parse_passdown.py` | 310 | Operator passdown parser with auto-format detection |
| `shared.py` | 370 | Domain constants — fault keywords, product normalization, rated speeds |
| `data_normalization.py` | 140 | Header mapping, column coercion, derived column generation |
| `operations_intelligence.py` | 90 | Action scoring, handoff packets, anomaly detection |
| `db.py` | 350 | Optional Supabase persistence for equipment knowledge and run history |
=======
Core parser module: `operations-intelligence-analyzer/parse_mes.py`
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)

## Vendor-neutral compatibility

This project is vendor-neutral by design. It accepts common MES export shapes and normalizes them into a canonical internal schema.

<<<<<<< HEAD
**Fuzzy matching everywhere** — Headers vary across MES versions, plant configs, and export settings. The analyzer handles it with 50+ mappings and positional fallback.
=======
## Naming guideline
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)

Do not introduce vendor trademarks into product names, public slugs, or primary identifiers unless technical interoperability requires it. Prefer neutral terms such as MES, operations, line, and shift.

## Tests

```bash
<<<<<<< HEAD
python -m pytest -v
=======
python -m pytest operations-intelligence-analyzer/ -v
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)
```

## License

MIT
