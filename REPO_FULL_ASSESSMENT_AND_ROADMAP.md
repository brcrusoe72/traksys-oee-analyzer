# MES OEE Analyzer — Full Repository Assessment and Next-Build Plan

## Pull/Sync Status
- Attempted to `git pull --ff-only`, but this checkout has no configured remote/tracking branch, so the repository cannot be pulled/synced automatically in its current state.
- Current branch: `work`.

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

## Repository layout and responsibilities

### Root-level structure
- `streamlit_app.py` (root): thin launcher that imports and executes app code from `mes-oee-analyzer/streamlit_app.py` so Streamlit Cloud can run from repo root.
- `requirements.txt` (root): app dependencies for deployment.
- `CLAUDE.md`: architecture and command reference for contributors.
- `mes-oee-analyzer/`: primary source folder.

### Core package folder: `mes-oee-analyzer/`

#### 1) Analysis engine and reporting
- `analyze.py`
  - Main orchestration layer for OEE analysis and workbook generation.
  - Implements robust sheet/column normalization and fuzzy mapping.
  - Uses weighted aggregation for OEE-related metrics and generates action-focused outputs.
- `shift_report.py`
  - Shift-specific deep-dive report generator with product granularity and operational diagnostics.
- `analysis_report.py`
  - PDF report generation (compact executive artifact).

#### 2) Parsing/ingestion
- `parse_mes.py`
  - Handles raw MES “OEE Period Detail” interval-based exports.
  - Normalizes timestamps, shift naming, shift-hour rollups, and metric extraction.
- `parse_passdown.py`
  - Parses operator passdown spreadsheets and maps them into downtime/event structures compatible with analysis.

#### 3) Shared domain model
- `shared.py`
  - Core constants and taxonomy: fault-classification keywords, product normalization, target rates, pack mappings, and equipment keyword maps.
  - Single source of truth for business rules used across modules.

#### 4) Historical intelligence
- `oee_history.py`
  - Writes append-only run history (`history.jsonl`) and deeper hourly/shift history.
  - Maintains “tended” trend outputs with SPC-style logic and trend insights.

#### 5) Web and AI context
- `streamlit_app.py`
  - Multi-tab UI (daily analysis + trend analysis).
  - Supports multiple OEE uploads and optional downtime/context files.
- `photo_analysis.py`
  - Converts image-derived findings into downtime dictionaries/frames that can be merged into the analytics path.

#### 6) Data persistence
- `db.py`
  - Optional Supabase client adapter, equipment lookup, baseline retrieval/upsert, and run-history persistence.
- `schema.sql`
  - SQL schema + seed data for equipment knowledge, downtime baselines, and run history.

#### 7) Tests
- `test_core.py`: validates weighted OEE math, fault classification, parser behaviors, and narrative helpers.
- `test_analysis_report.py`: validates PDF/report assembly behavior.
- `test_shift_report.py`: validates shift deep-dive logic.
- `test_deep_history.py`: validates historical persistence/trend structures.
- `test_photo_analysis.py`: validates photo findings conversion and shift matching.

## Architectural strengths
1. **Operational resiliency in ingestion**
   - Fuzzy/alias sheet and header mapping tolerates field naming drift common in real-world exports.
2. **Correct weighted KPI philosophy**
   - Uses weighted aggregation (not naive averaging), preserving KPI integrity under unequal interval durations.
3. **Action-oriented outputs**
   - Designed for practical plant follow-up (top losses, shift narratives, recommendations) not just dashboarding.
4. **Multiple output modalities**
   - CLI + Streamlit + Excel + PDF gives flexibility for engineering, management, and frontline audiences.
5. **Expandable intelligence layer**
   - Historical run logging and optional DB persistence provide a path from one-off reporting to continuous monitoring.

## Current gaps/opportunities
1. **Packaging/layout inconsistency**
   - Duplicate root/package-level app files and requirements can confuse onboarding and deployment ownership.
2. **No explicit CI pipeline**
   - Tests exist but are not clearly wired to mandatory automated checks on every change.
3. **Monolithic core script size risk**
   - `analyze.py` is feature-rich but large; this increases regression risk and slows targeted evolution.
4. **Schema/application contract hardening**
   - DB integration is optional and runtime-tolerant, but stronger typed contracts/migrations would improve reliability.
5. **Observability and benchmark governance**
   - There is trend intelligence, but no explicit metric SLAs or automated drift alerts surfaced as release gates.

## What to build next (recommended roadmap)

## Phase 1 (Immediate): Reliability and release confidence
### 1.1 Add CI with required test matrix
Build:
- Add GitHub Actions workflow:
  - Python version matrix (e.g., 3.10/3.11),
  - Install deps,
  - Run all pytest suites,
  - Optionally run a smoke CLI invocation against fixture data.
Impact:
- Prevents regressions in core manufacturing math and parser behavior.
- Increases trust in frequent operational updates.
Benefit to whole system:
- Raises baseline quality across **all entry points** (CLI, Streamlit, PDF, trend engine).

### 1.2 Introduce golden-file regression fixtures
Build:
- Curated fixture exports + expected report snapshots (selected tabs/summaries).
Impact:
- Detects silent report-format or calculation drift.
Benefit:
- Protects downstream users who depend on stable worksheet semantics.

### 1.3 Standardize dependency and runtime strategy
Build:
- Single source of requirements (possibly split into runtime/dev extras),
- lockfile or reproducible build strategy.
Impact:
- Reduces “works on my machine” issues and deployment drift.
Benefit:
- Faster onboarding and safer upgrades.

## Phase 2 (Near-term): Modularization and domain clarity
### 2.1 Extract analysis domains from `analyze.py`
Build:
- Split into submodules:
  - ingestion normalization,
  - KPI computation,
  - downtime/fault analysis,
  - narrative generation,
  - workbook rendering.
Impact:
- Smaller, testable units and lower change blast radius.
Benefit:
- Faster feature delivery and easier debugging.

### 2.2 Typed data contracts
Build:
- Define typed models (dataclasses or Pydantic) for key records:
  - hourly row,
  - shift summary,
  - downtime event,
  - report payload.
Impact:
- Explicit assumptions, better editor/runtime validation.
Benefit:
- Prevents class of bugs from loose dict/DataFrame handoffs.

### 2.3 Harden parser compatibility matrix
Build:
- Capture known exporter variants and validate parser compatibility per variant via tests.
Impact:
- More predictable ingestion under customer-specific export differences.
Benefit:
- Fewer support incidents, better portability between plants.

## Phase 3 (Mid-term): Product-level operational intelligence
### 3.1 Add recommendation scoring engine
Build:
- Rank action items by expected value = (lost-case opportunity × confidence × effort factor).
Impact:
- Focuses teams on highest ROI fixes.
Benefit:
- Converts analytics into prioritized execution planning.

### 3.2 Shift handoff packet generation
Build:
- Auto-generate shift-specific email/one-pager with:
  - top 3 losses,
  - containment actions,
  - owner + due date placeholders.
Impact:
- Tightens operator-supervisor-engineering feedback loop.
Benefit:
- Improves follow-through and measurable closure rate.

### 3.3 Trend anomaly alerting
Build:
- Trigger “attention flags” from `oee_history` trend outputs (e.g., recurring top-cause spikes).
Impact:
- Earlier issue detection before major throughput losses accumulate.
Benefit:
- Improves preventive maintenance and scheduling decisions.

## Phase 4 (Strategic): Platformization
### 4.1 Multi-line / multi-plant tenancy model
Build:
- Parameterize line/plant profiles (targets, product taxonomies, equipment maps).
Impact:
- Expands applicability beyond a single line profile.
Benefit:
- Turns tool into reusable internal platform.

### 4.2 Strong Supabase-first mode with migrations
Build:
- Migration scripts, versioned schema evolution, and optional “offline sync” mode.
Impact:
- Reliable persistence and controlled data model evolution.
Benefit:
- Enables historical analytics at scale and governance-grade traceability.

### 4.3 API service layer (optional)
Build:
- Expose core analysis as service endpoints for ERP/MES integration.
Impact:
- Enables automation and orchestration beyond manual file upload.
Benefit:
- Integrates analytics into enterprise workflows.

## Cross-system impact summary
If you execute the roadmap in order:
- **Quality increases first** (CI + regressions),
- then **change velocity increases** (modularization + contracts),
- then **business value per analysis run increases** (prioritized recommendations + alerts),
- then **organizational scalability increases** (multi-line/platform model).

This sequence is intentionally staged so each phase de-risks the next.

## Suggested first sprint (2 weeks)
1. Add CI workflow with full pytest run.
2. Create 2–3 fixture datasets and one golden summary snapshot test.
3. Extract one high-risk subsystem from `analyze.py` (e.g., column/sheet normalization) into its own module with dedicated tests.
4. Add a short CONTRIBUTING.md with dev setup + test expectations.

Success criteria:
- Every PR has automated pass/fail checks,
- One modularization slice merged without behavior drift,
- Baseline regression fixtures in place,
- Team can onboard and run tests in under 15 minutes.
