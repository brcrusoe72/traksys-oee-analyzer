# 📊 Operations Intelligence Analyzer

**Operations intelligence for manufacturing.** Upload raw MES exports — OEE metrics, event logs, shift passdowns, even floor photos — and get back actionable analysis: fault classification, downtime Pareto, SPC trends, shift deep dives, and prioritized recommendations.

Built by a manufacturing engineer who got tired of spreadsheet hell.

### [🚀 Live Demo](https://operations-intelligence-analyzer.streamlit.app/)

---

## What It Does

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

```bash
streamlit run streamlit_app.py
```

Then open [http://localhost:8501](http://localhost:8501) and upload your data.

### CLI

```bash
# Full OEE analysis → Excel workbook
python <package_dir>/analyze.py your_export.xlsx

# With downtime context
python <package_dir>/analyze.py your_export.xlsx --downtime kb.json
```

## Architecture

```
Raw MES Data → Parsing/Normalization → Analysis Engine → Learning Memory → Reporting
                                            ↑
                                    AI Photo Analysis
                                    Event Classification
                                    SPC Trend Engine
                                    Operations Intelligence
```

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

## Key Design Decisions

**Production-weighted metrics** — OEE is always weighted by production hours. A 15-minute interval with 20% OEE doesn't tank an 8-hour shift average.

**Fuzzy matching everywhere** — Headers vary across MES versions, plant configs, and export settings. The analyzer handles it with 50+ mappings and positional fallback.

**Classification hierarchy** — `Unassigned → Scheduled → Micro Stops → Process → Equipment → Fallback → Unclassified`. More specific categories take priority.

**Learning memory** — Every run is fingerprinted and stored. Duplicates are ignored. The system gets smarter with each analysis.

**Photo analysis as first-class input** — Floor photos aren't just attachments. They're parsed by AI, converted to structured downtime data, and merged into the analysis pipeline with shift-specific matching.

## Tech Stack

Python · pandas · NumPy · openpyxl · xlsxwriter · fpdf2 · Streamlit · Altair · OpenAI (vision) · Supabase (optional)

## Tests

```bash
python -m pytest -v
```

Covers core OEE math, fault classification, parser behaviors, report assembly, shift deep dives, SPC/trend structures, and photo analysis transforms.

## Roadmap

- [ ] PyPI package (`pip install oee-analyzer`)
- [ ] REST API for system integration
- [ ] Multi-plant support
- [ ] Real-time MES connection (beyond file uploads)
- [ ] Configurable alert thresholds

## License

MIT

---

*Built with domain knowledge from food manufacturing floors — not just tutorials.*
