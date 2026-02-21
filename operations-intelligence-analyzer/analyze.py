"""
MES OEE + Downtime Analyzer
complete analysis with shift-level deep dives and fault classification.

Usage:
  python analyze.py oee_export.xlsx
  python analyze.py oee_export.xlsx --downtime knowledge_base.json
"""

import sys
import os
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from shared import EXCLUDE_REASONS, EQUIPMENT_KEYWORDS, SHIFT_HOURS, _PRODUCT_CODE_TO_PACK, classify_fault, get_target_cph
from data_normalization import (
    HEADER_TO_INTERNAL as _HEADER_TO_INTERNAL,
    NUMERIC_COLUMNS as _NUMERIC_COLUMNS,
    normalize_col as _normalize_col,
    smart_rename as _smart_rename,
    coerce_numerics as _coerce_numerics,
    derive_columns as _derive_columns,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
EXCEL_EPOCH = datetime(1899, 12, 30)


def _weighted_mean(values, weights):
    """Production-weighted average, excluding zero-weight entries."""
    mask = weights > 0
    if not mask.any():
        return 0.0
    return float((values[mask] * weights[mask]).sum() / weights[mask].sum())


def excel_date_to_datetime(serial):
    if pd.isna(serial):
        return pd.NaT
    if isinstance(serial, (pd.Timestamp, datetime)):
        return serial
    try:
        return EXCEL_EPOCH + timedelta(days=float(serial))
    except (TypeError, ValueError):
        return pd.NaT


# ---------------------------------------------------------------------------
# Sheet-name matching helpers
# ---------------------------------------------------------------------------
EXPECTED_SHEETS = {
    "DayShiftHour": {
        "aliases": ["dayshifthour", "day_shift_hour", "day shift hour",
                     "hourly", "hourlydata", "hourly_data", "hourly data"],
        "columns": [
            "shift_date", "shift", "time_block", "shift_hour",
            "total_hours", "product_code", "job", "good_cases",
            "bad_cases", "total_cases", "availability", "performance",
            "quality", "oee_pct"
        ],
    },
    "DayShift_Summary": {
        "aliases": ["dayshift_summary", "dayshiftsummary", "day_shift_summary",
                     "day shift summary", "daily_summary", "dailysummary",
                     "daily summary"],
        "columns": [
            "shift_date", "shift", "total_hours", "good_cases",
            "bad_cases", "total_cases", "oee_pct"
        ],
    },
    "Shift_Summary": {
        "aliases": ["shift_summary", "shiftsummary", "shift summary",
                     "overall_summary", "overallsummary", "overall summary",
                     "summary"],
        "columns": [
            "shift", "total_hours", "good_cases",
            "bad_cases", "total_cases", "oee_pct"
        ],
    },
    "ShiftHour_Summary": {
        "aliases": ["shifthour_summary", "shifthoursummary",
                     "shift_hour_summary", "shift hour summary",
                     "hour_summary", "hoursummary", "hour summary",
                     "houravg", "hour_avg", "hour avg"],
        "columns": [
            "shift", "shift_hour", "availability", "performance", "oee_pct"
        ],
    },
}


def _normalize(name):
    """Lower-case, strip whitespace and underscores for fuzzy comparison."""
    return name.lower().strip().replace("_", "").replace(" ", "")


# Column mapping and normalization helpers are centralized in
# data_normalization.py and imported above.


def _aggregate_oee(df):
    """Calculate OEE components from aggregate totals, not averages of per-hour ratios.

    Excludes intervals with zero scheduled time or zero production.
    Returns (availability, performance, quality, oee) where A/P/Q are 0-1
    fractions and OEE is 0-100 percentage.
    """
    mask = (df["total_hours"] > 0) & (df["total_cases"] > 0)
    active = df[mask]

    if len(active) == 0:
        return 0.0, 0.0, 0.0, 0.0

    # Availability = Sum(Production Time) / Sum(Scheduled Time)
    # where Production Time per interval = availability_i * total_hours_i
    scheduled_time = active["total_hours"].sum()
    production_time = (active["availability"] * active["total_hours"]).sum()
    availability = production_time / scheduled_time if scheduled_time > 0 else 0.0

    # Performance weighted by production time
    # = Sum(perf_i * production_time_i) / Sum(production_time_i)
    performance = (
        (active["performance"] * active["availability"] * active["total_hours"]).sum()
        / production_time
    ) if production_time > 0 else 0.0

    # Quality = Sum(Good Cases) / Sum(Total Cases)
    total_cases = active["total_cases"].sum()
    good_cases = active["good_cases"].sum() if "good_cases" in active.columns else total_cases
    quality = good_cases / total_cases if total_cases > 0 else 0.0

    # OEE = A * P * Q (as 0-100 percentage)
    oee = availability * performance * quality * 100

    return availability, performance, quality, oee


def _compute_utilization(df):
    """What % of scheduled time actually had production?

    Looks at hourly intervals: scheduled = total_hours > 0,
    producing = total_cases > 0.  Returns tuple:
        (utilization_pct, producing_hours, scheduled_hours, dead_hours_count)
    """
    scheduled = df[df["total_hours"] > 0]
    scheduled_hours = scheduled["total_hours"].sum()
    producing = scheduled[scheduled["total_cases"] > 0]
    producing_hours = producing["total_hours"].sum()
    dead_count = len(scheduled) - len(producing)
    utilization = producing_hours / scheduled_hours * 100 if scheduled_hours > 0 else 0.0
    return utilization, producing_hours, scheduled_hours, dead_count


def _build_dead_hour_narrative(hourly):
    """Group consecutive dead hours (0 cases) into outage blocks.

    A "dead hour" is an interval with total_hours > 0 but total_cases == 0.
    Consecutive dead hours on the same date and shift become an "outage block".
    Isolated dead hours are classified as "scattered".

    Returns a list of dicts, each representing one block or scattered hour:
        {date_str, shift, first_hour, last_hour, n_hours, pattern}
    Also returns a summary dict:
        {total_dead, consecutive_hours, scattered_hours, n_blocks}
    """
    dead = hourly[(hourly["total_hours"] > 0) & (hourly["total_cases"] == 0)].copy()
    if len(dead) == 0:
        return [], {"total_dead": 0, "consecutive_hours": 0,
                     "scattered_hours": 0, "n_blocks": 0}

    dead = dead.sort_values(["date_str", "shift", "shift_hour"]).reset_index(drop=True)

    blocks = []
    current_block = {
        "date_str": dead.iloc[0]["date_str"],
        "shift": dead.iloc[0]["shift"],
        "first_hour": int(dead.iloc[0]["shift_hour"]),
        "last_hour": int(dead.iloc[0]["shift_hour"]),
        "n_hours": 1,
    }

    for i in range(1, len(dead)):
        row = dead.iloc[i]
        prev = dead.iloc[i - 1]
        # Consecutive if same date, same shift, and hour increments by 1
        if (row["date_str"] == prev["date_str"]
                and row["shift"] == prev["shift"]
                and int(row["shift_hour"]) == int(prev["shift_hour"]) + 1):
            current_block["last_hour"] = int(row["shift_hour"])
            current_block["n_hours"] += 1
        else:
            blocks.append(current_block)
            current_block = {
                "date_str": row["date_str"],
                "shift": row["shift"],
                "first_hour": int(row["shift_hour"]),
                "last_hour": int(row["shift_hour"]),
                "n_hours": 1,
            }
    blocks.append(current_block)

    # Classify each block
    for b in blocks:
        if b["n_hours"] >= 2:
            b["pattern"] = "consecutive"
        else:
            b["pattern"] = "scattered"

    consecutive_hours = sum(b["n_hours"] for b in blocks if b["pattern"] == "consecutive")
    scattered_hours = sum(b["n_hours"] for b in blocks if b["pattern"] == "scattered")
    n_blocks = sum(1 for b in blocks if b["pattern"] == "consecutive")

    summary = {
        "total_dead": len(dead),
        "consecutive_hours": consecutive_hours,
        "scattered_hours": scattered_hours,
        "n_blocks": n_blocks,
    }

    return blocks, summary


def _correlate_dead_hours_with_events(dead_blocks, events_df, hourly):
    """Annotate dead hour blocks with machine-data causes from individual events.

    For each dead block, finds events that overlap those clock hours and
    aggregates them by reason code. Also looks up what product was running.

    Args:
        dead_blocks: list of dicts from _build_dead_hour_narrative()
        events_df: DataFrame with columns [reason, start_time, end_time, shift, duration_minutes]
        hourly: hourly DataFrame with date_str, shift, shift_hour, product_code columns

    Returns:
        list of dead blocks enriched with 'causes' (str) and 'product' (str)
    """
    from parse_mes import SHIFT_STARTS

    if len(events_df) == 0 or len(dead_blocks) == 0:
        return dead_blocks

    # Build event-to-clock-hour lookup: (date_str, clock_hour) -> [(reason, overlap_min)]
    hour_events = {}
    for _, ev in events_df.iterrows():
        start = ev["start_time"]
        end = ev.get("end_time")
        if pd.isna(start) or start is None:
            continue
        if pd.isna(end) or end is None:
            # No end time — use start hour only, full duration
            end = start + timedelta(minutes=ev["duration_minutes"]) if ev["duration_minutes"] > 0 else start + timedelta(hours=1)

        # Walk through each clock hour this event spans
        cur = start.replace(minute=0, second=0, microsecond=0)
        while cur < end:
            next_hr = cur + timedelta(hours=1)
            # Overlap: max(start, cur) to min(end, next_hr)
            overlap_start = max(start, cur)
            overlap_end = min(end, next_hr)
            overlap_min = (overlap_end - overlap_start).total_seconds() / 60.0
            if overlap_min > 0:
                date_str = cur.strftime("%Y-%m-%d")
                clock_hour = cur.hour
                key = (date_str, clock_hour)
                if key not in hour_events:
                    hour_events[key] = []
                hour_events[key].append((ev["reason"], round(overlap_min, 1)))
            cur = next_hr

    # Build product lookup from hourly data: (date_str, shift_hour) -> product_code
    product_lookup = {}
    if "product_code" in hourly.columns:
        for _, hr in hourly.iterrows():
            key = (hr["date_str"], hr["shift"], int(hr["shift_hour"]))
            pc = hr.get("product_code", "")
            if pc and str(pc).strip():
                product_lookup[key] = str(pc).strip()

    # Annotate each dead block
    enriched = []
    for block in dead_blocks:
        b = dict(block)
        date_str = b["date_str"]
        shift = b["shift"]
        first_hr = b["first_hour"]
        last_hr = b["last_hour"]

        # Determine shift start hour to convert shift_hour -> clock_hour
        # Normalize shift name for SHIFT_STARTS lookup
        shift_key = None
        for k in SHIFT_STARTS:
            if k.lower().split()[0] in shift.lower():
                shift_key = k
                break
        start_hour = SHIFT_STARTS.get(shift_key, 7) if shift_key else 7

        # Collect events across all clock hours in this block
        all_events = []
        products = set()
        for sh in range(first_hr, last_hr + 1):
            clock_hour = (start_hour + sh - 1) % 24
            # Calendar date: for 3rd shift, hours after midnight are next calendar day
            cal_date_str = date_str
            if "3rd" in shift.lower() and clock_hour < 7:
                try:
                    cal_date = datetime.strptime(date_str, "%Y-%m-%d").date() + timedelta(days=1)
                    cal_date_str = cal_date.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            key = (cal_date_str, clock_hour)
            if key in hour_events:
                all_events.extend(hour_events[key])

            # Look up product
            prod_key = (date_str, shift, sh)
            if prod_key in product_lookup:
                products.add(product_lookup[prod_key])

        # Aggregate events by reason
        if all_events:
            reason_totals = {}
            for reason, mins in all_events:
                reason_totals[reason] = reason_totals.get(reason, 0) + mins
            sorted_reasons = sorted(reason_totals.items(), key=lambda x: -x[1])
            cause_parts = [f"{r}: {m:.0f} min" for r, m in sorted_reasons[:5]]
            b["causes"] = "; ".join(cause_parts)
        else:
            b["causes"] = ""

        b["product"] = ", ".join(sorted(products)) if products else ""

        # Combine into annotation string
        annotation = b["causes"]
        if b["product"]:
            annotation += f" — running {b['product']}" if annotation else b["product"]
        b["cause_annotation"] = annotation

        enriched.append(b)

    return enriched


def _match_sheet(expected_name, available_sheets, already_matched):
    """Return the actual sheet name that best matches *expected_name*.

    Match strategy (first hit wins):
      1. Exact match (case-insensitive)
      2. Normalized match (ignore spaces/underscores/case)
      3. Alias list match
      4. Column-count heuristic (match by number of columns)
    """
    info = EXPECTED_SHEETS[expected_name]
    norm_expected = _normalize(expected_name)
    remaining = [s for s in available_sheets if s not in already_matched]

    # 1. exact (case-insensitive)
    for s in remaining:
        if s.lower().strip() == expected_name.lower():
            return s

    # 2. normalized
    for s in remaining:
        if _normalize(s) == norm_expected:
            return s

    # 3. alias list
    for s in remaining:
        if _normalize(s) in info["aliases"]:
            return s

    # 4. column-count heuristic — only if a single remaining sheet has the
    #    right number of columns (avoids ambiguity)
    expected_ncols = len(info["columns"])
    col_matches = []
    for s in remaining:
        try:
            df = pd.read_excel(
                available_sheets["__filepath__"], sheet_name=s, nrows=0
            )
            if len(df.columns) == expected_ncols:
                col_matches.append(s)
        except Exception:
            pass
    if len(col_matches) == 1:
        return col_matches[0]

    return None


def _resolve_sheets(filepath):
    """Return a dict mapping canonical sheet names → actual sheet names.

    Raises ValueError with a helpful message when sheets can't be matched.
    """
    xls = pd.ExcelFile(filepath)
    available = xls.sheet_names
    xls.close()

    # stash filepath so the column-count heuristic can read sheets
    avail_map = {"__filepath__": filepath}
    for s in available:
        avail_map[s] = s

    matched = {}
    unmatched = []
    for canonical in EXPECTED_SHEETS:
        actual = _match_sheet(canonical, avail_map, set(matched.values()))
        if actual:
            matched[canonical] = actual
        else:
            unmatched.append(canonical)

    if unmatched:
        sheet_list = ", ".join(f"'{s}'" for s in available)
        missing_list = ", ".join(f"'{s}'" for s in unmatched)
        raise ValueError(
            f"Could not find matching worksheet(s) for: {missing_list}.\n"
            f"Your file contains these sheets: [{sheet_list}].\n"
            f"Expected sheets: DayShiftHour (14 cols), DayShift_Summary (7 cols), "
            f"Shift_Summary (6 cols), ShiftHour_Summary (5 cols).\n"
            f"Rename your sheets to match or check that you uploaded the right file."
        )

    return matched


def _build_summary_frames_from_hourly(hourly):
    """Build shift/day summaries from a normalized hourly dataframe."""
    work = hourly.copy()
    work["_is_prod"] = (work["availability"] > 0) | (work["total_cases"] > 0)
    work["_prod_hours"] = work["total_hours"] * work["_is_prod"]
    work["_w_oee"] = work["oee_pct"] * work["_prod_hours"]

    shift_summary = (
        work.groupby(["shift_date", "shift"], as_index=False)
        .agg(
            total_hours=("total_hours", "sum"),
            good_cases=("good_cases", "sum"),
            bad_cases=("bad_cases", "sum"),
            total_cases=("total_cases", "sum"),
            _prod_hours=("_prod_hours", "sum"),
            _w_oee=("_w_oee", "sum"),
            hour_blocks=("shift_hour", "count"),
        )
    )
    shift_summary["oee_pct"] = (
        shift_summary["_w_oee"] / shift_summary["_prod_hours"].replace(0, np.nan)
    ).fillna(0)
    shift_summary["cases_per_hour"] = (
        shift_summary["total_cases"] / shift_summary["total_hours"].replace(0, np.nan)
    ).fillna(0)
    shift_summary.drop(columns=["_w_oee", "_prod_hours"], inplace=True)
    shift_summary["date"] = pd.to_datetime(shift_summary["shift_date"], errors="coerce")
    shift_summary["date_str"] = shift_summary["date"].dt.strftime("%Y-%m-%d")

    overall = (
        work.groupby("shift", as_index=False)
        .agg(
            total_hours=("total_hours", "sum"),
            good_cases=("good_cases", "sum"),
            bad_cases=("bad_cases", "sum"),
            total_cases=("total_cases", "sum"),
            _prod_hours=("_prod_hours", "sum"),
            _w_oee=("_w_oee", "sum"),
            n_intervals=("shift_hour", "count"),
        )
    )
    overall["oee_pct"] = (overall["_w_oee"] / overall["_prod_hours"].replace(0, np.nan)).fillna(0)
    overall["cases_per_hour"] = (overall["total_cases"] / overall["total_hours"].replace(0, np.nan)).fillna(0)
    overall.drop(columns=["_w_oee", "_prod_hours"], inplace=True)

    hour_avg = (
        work.groupby(["shift", "shift_hour"], as_index=False)
        .agg(
            time_block=("time_block", "first"),
            total_hours=("total_hours", "sum"),
            _prod_hours=("_prod_hours", "sum"),
            _w_oee=("_w_oee", "sum"),
            total_cases=("total_cases", "sum"),
            availability=("availability", "mean"),
            performance=("performance", "mean"),
        )
    )
    hour_avg["oee_pct"] = (hour_avg["_w_oee"] / hour_avg["_prod_hours"].replace(0, np.nan)).fillna(0)
    hour_avg["cases_per_hour"] = (hour_avg["total_cases"] / hour_avg["total_hours"].replace(0, np.nan)).fillna(0)
    hour_avg.drop(columns=["_w_oee", "_prod_hours", "total_cases"], inplace=True)
    return shift_summary, overall, hour_avg


def _canonical_shift_label(val):
    """Normalize common shift labels to canonical names."""
    s = str(val).strip().lower()
    if not s or s == "nan":
        return None
    if "1" in s or "first" in s:
        return "1st Shift"
    if "2" in s or "second" in s:
        return "2nd Shift"
    if "3" in s or "third" in s:
        return "3rd Shift"
    return str(val).strip()


def _shift_from_hour(hour_val):
    """Infer shift from a 24h clock hour."""
    try:
        h = int(float(hour_val)) % 24
    except (TypeError, ValueError):
        return None
    if 7 <= h < 15:
        return "1st Shift"
    if 15 <= h < 23:
        return "2nd Shift"
    return "3rd Shift"


def _loose_single_sheet_rename(df):
    """Looser header mapping for one-sheet files with non-standard headers."""
    rename = {}
    for col in df.columns:
        norm = _normalize_col(col)
        if norm in _HEADER_TO_INTERNAL:
            rename[col] = _HEADER_TO_INTERNAL[norm]
            continue
        if "shift" in norm and "hour" not in norm:
            rename[col] = "shift"
        elif norm in {"date", "productiondate", "workdate", "day"}:
            rename[col] = "shift_date"
        elif "starttime" in norm or "timestamp" in norm or norm == "start":
            rename[col] = "time_block"
        elif norm in {"hour", "hr", "hour24"}:
            rename[col] = "shift_hour"
        elif "duration" in norm or norm in {"hours", "hrs"}:
            rename[col] = "total_hours"
        elif norm in {"good", "goodcount"}:
            rename[col] = "good_cases"
        elif norm in {"bad", "rejects", "scrap"}:
            rename[col] = "bad_cases"
        elif norm in {"total", "totalcount"}:
            rename[col] = "total_cases"
        elif "oee" in norm:
            rename[col] = "oee_pct"
        elif "avail" in norm:
            rename[col] = "availability"
        elif "perf" in norm:
            rename[col] = "performance"
        elif "qual" in norm:
            rename[col] = "quality"
        elif "product" in norm:
            rename[col] = "product_code"
        elif "job" in norm or "line" in norm:
            rename[col] = "job"

    if rename:
        return df.rename(columns=rename)
    return df


def _load_traksys_oee_overview(filepath, sheet_name, raw_peek):
    """Parse MES OEE Overview exports with Start/End/System/KPI Calc metadata rows.

    Traksys OEE Overview format:
      Row 1: Start | <start_date> | ...
      Row 2: End   | <end_date>   | ...
      Row 3: System| <line_name>  | ... | <header: GroupValue> | <header: GroupLabel> | ...
      Row 4: KPI Calc | <kpi_name> | ...
      Row 5: Shift | <shift_filter> | ...
      Row 6+: data rows with columns [GroupValue, GroupLabel, ..., Value, ...]
    """
    # Extract metadata from the first few rows.
    line_name = str(raw_peek.iloc[2, 1]).strip() if len(raw_peek) > 2 else "All"
    # Normalize line name: "Line 1 - Gallon" -> "Line 1"
    import re as _re
    _line_match = _re.match(r"(Line\s*\d+)", line_name, _re.IGNORECASE)
    if _line_match:
        line_label = _line_match.group(1)
    else:
        line_label = line_name

    # Find the header row — look for "GroupValue" or similar in the first row (row 1).
    # In this format, columns D+ of row 1 are the actual data headers.
    # Read the full file with row 1 as header (0-indexed: header=0).
    raw = pd.read_excel(filepath, sheet_name=sheet_name, header=None)

    # The true data headers are in row 1 (the "Start" row), columns D onward.
    # Row 1 col A="Start", col B=<date>, col C=None, col D="GroupValue", col E="GroupLabel", ...
    header_row_idx = 0
    header_vals = raw.iloc[header_row_idx].tolist()

    # Metadata rows to skip: rows 1-5 (End, System, KPI Calc, Shift, possibly Product).
    # Data starts where column A is no longer a known metadata label.
    metadata_labels = {"end", "system", "kpi calc", "shift", "product", ""}
    data_start_idx = 1
    for i in range(1, min(len(raw), 10)):
        label = str(raw.iloc[i, 0]).strip().lower()
        if label not in metadata_labels:
            data_start_idx = i
            break
        data_start_idx = i + 1

    # Build a clean DataFrame from data rows with proper headers.
    data = raw.iloc[data_start_idx:].copy()
    data.columns = [str(v).strip() if pd.notna(v) else f"col_{idx}" for idx, v in enumerate(header_vals)]
    data = data.dropna(how="all").reset_index(drop=True)

    # Map Traksys OEE Overview columns to internal schema.
    # GroupValue = timestamp (datetime), Value = OEE % (already 0-100 scale)
    col_renames = {}
    for col in data.columns:
        cl = col.lower().strip()
        if cl == "groupvalue":
            col_renames[col] = "timestamp"
        elif cl == "value":
            col_renames[col] = "oee_pct"
        elif cl == "valuedecimal":
            col_renames[col] = "oee_decimal"
    data = data.rename(columns=col_renames)

    # Parse timestamp and derive shift/hour columns.
    if "timestamp" in data.columns:
        data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
        data = data.dropna(subset=["timestamp"])
        data["shift_date"] = data["timestamp"].dt.date.astype(str)
        data["shift_hour"] = data["timestamp"].dt.hour + 1
        data["time_block"] = data["timestamp"].dt.strftime("%H:%M")
        data["shift"] = data["shift_hour"].apply(_shift_from_hour)

    # Coerce OEE and related numeric columns.
    for col in ["oee_pct", "oee_decimal"]:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0.0)

    # Map remaining Traksys columns to internal names.
    _traksys_to_internal = {
        "intervalseconds": "interval_seconds",
        "netoperationseconds": "net_operation_seconds",
        "runtimeseconds": "run_time_seconds",
        "ratelossseconds": "rate_loss_seconds",
        "totalcalculationunits": "total_calc_units",
        "goodcalculationunits": "good_calc_units",
        "badcalculationunits": "bad_calc_units",
        "totaldisplayunits": "total_cases",
        "gooddisplayunits": "good_cases",
        "baddisplayunits": "bad_cases",
        "mtbfseconds": "mtbf_seconds",
        "mttrseconds": "mttr_seconds",
        "systemnotscheduledseconds": "not_scheduled_seconds",
        "legalloss seconds": "legal_loss_seconds",
    }
    rename_map = {}
    for col in data.columns:
        cl = col.lower().strip().replace(" ", "")
        if cl in _traksys_to_internal and _traksys_to_internal[cl] not in data.columns:
            rename_map[col] = _traksys_to_internal[cl]
    data = data.rename(columns=rename_map)

    # Derive availability/performance/quality if present as columns.
    # Traksys exports may have these directly (as % values already mapped via _smart_rename).
    for metric in ["availability", "performance", "quality"]:
        if metric not in data.columns:
            # Check for Traksys-named versions
            for col in data.columns:
                if col.lower().strip() == metric:
                    data[metric] = pd.to_numeric(data[col], errors="coerce").fillna(0.0)
                    break
        if metric in data.columns:
            data[metric] = pd.to_numeric(data[metric], errors="coerce").fillna(0.0)

    # Derive total_hours from interval_seconds.
    if "total_hours" not in data.columns:
        if "interval_seconds" in data.columns:
            data["total_hours"] = pd.to_numeric(data["interval_seconds"], errors="coerce").fillna(3600) / 3600.0
        else:
            data["total_hours"] = 1.0

    # Derive total_cases/good_cases/bad_cases.
    for col in ["total_cases", "good_cases", "bad_cases"]:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0)
    if "total_cases" not in data.columns:
        data["total_cases"] = data.get("good_cases", 0) + data.get("bad_cases", 0)
    if "good_cases" not in data.columns:
        data["good_cases"] = data.get("total_cases", 0)
    if "bad_cases" not in data.columns:
        data["bad_cases"] = 0

    # Derive cases_per_hour.
    if "cases_per_hour" not in data.columns:
        total_hrs = data["total_hours"].replace(0, np.nan)
        data["cases_per_hour"] = (data["total_cases"] / total_hrs).fillna(0)

    # Derive product_code from column if available, else empty.
    if "product_code" not in data.columns:
        data["product_code"] = ""

    # Add line label.
    data["line"] = line_label

    # Final derived columns.
    data["date"] = pd.to_datetime(data["shift_date"], errors="coerce")
    data["date_str"] = data["date"].dt.strftime("%Y-%m-%d")
    data["day_of_week"] = data["date"].dt.day_name()

    # Filter out non-production shifts.
    if "shift" in data.columns:
        data["shift"] = data["shift"].map(_canonical_shift_label)
        _non_production = {"No Shift", "no shift", None}
        data = data[~data["shift"].isin(_non_production)].reset_index(drop=True)

    hourly = data
    print(f"  Parsed Traksys OEE Overview: {len(hourly)} hourly records, line={line_label}")
    shift_summary, overall, hour_avg = _build_summary_frames_from_hourly(hourly)
    return hourly, shift_summary, overall, hour_avg


def _load_single_sheet_oee(filepath):
    """Fallback parser for one-sheet OEE exports (often named 'Data')."""
    xls = pd.ExcelFile(filepath)
    if len(xls.sheet_names) != 1:
        raise ValueError("single-sheet fallback only applies to one-sheet OEE files")
    sheet_name = xls.sheet_names[0]
    xls.close()

    # Check for MES OEE Overview format (Start/End/System/KPI Calc metadata rows).
    raw_peek = pd.read_excel(filepath, sheet_name=sheet_name, header=None, nrows=10)
    if len(raw_peek) >= 3 and str(raw_peek.iloc[0, 0]).strip().lower() == "start":
        return _load_traksys_oee_overview(filepath, sheet_name, raw_peek)

    # Some exports include one or more non-header rows before the real header.
    raw = pd.read_excel(filepath, sheet_name=sheet_name, header=None)
    best_row = 0
    best_score = -1
    scan_rows = min(len(raw), 20)
    expected_norm = {_normalize_col(c) for c in EXPECTED_SHEETS["DayShiftHour"]["columns"]}
    for idx in range(scan_rows):
        row_vals = raw.iloc[idx].tolist()
        row_norm = {_normalize_col(v) for v in row_vals if str(v).strip() and str(v).lower() != "nan"}
        score = len([v for v in row_norm if v in _HEADER_TO_INTERNAL or v in expected_norm])
        if score > best_score:
            best_score = score
            best_row = idx

    if best_score >= 3:
        hourly = raw.iloc[best_row + 1 :].copy()
        hourly.columns = [str(v).strip() for v in raw.iloc[best_row].tolist()]
        hourly = hourly.dropna(axis=1, how="all").dropna(how="all")
    else:
        hourly = pd.read_excel(filepath, sheet_name=sheet_name)

    try:
        hourly = _smart_rename(hourly, EXPECTED_SHEETS["DayShiftHour"]["columns"])
    except ValueError:
        pass
    # Always run looser mapping too so partially-matched files still normalize.
    hourly = _loose_single_sheet_rename(hourly)
    hourly = _coerce_numerics(hourly)
    hourly = _derive_columns(hourly)

    # Fill required columns if missing from source export.
    if "good_cases" not in hourly.columns:
        hourly["good_cases"] = hourly.get("total_cases", 0)
    if "bad_cases" not in hourly.columns:
        hourly["bad_cases"] = 0.0
    if "availability" not in hourly.columns:
        hourly["availability"] = 0.0
    if "performance" not in hourly.columns:
        hourly["performance"] = 0.0
    if "quality" not in hourly.columns:
        hourly["quality"] = 1.0
    if "line" not in hourly.columns:
        hourly["line"] = "All"

    if "shift" in hourly.columns:
        hourly["shift"] = hourly["shift"].map(_canonical_shift_label)

    needed = {"shift_date", "shift", "shift_hour", "total_hours", "total_cases", "oee_pct"}
    if "shift_hour" not in hourly.columns and "time_block" in hourly.columns:
        import warnings as _w
        with _w.catch_warnings():
            _w.simplefilter("ignore", UserWarning)
            parsed_tb = pd.to_datetime(hourly["time_block"], errors="coerce")
        if parsed_tb.notna().any():
            hourly.loc[parsed_tb.notna(), "shift_hour"] = parsed_tb[parsed_tb.notna()].dt.hour + 1
            if "shift_date" not in hourly.columns:
                hourly["shift_date"] = parsed_tb.dt.date.astype(str)

    # If date/hour are still missing, try to infer from any datetime-like column.
    if "shift_date" not in hourly.columns or "shift_hour" not in hourly.columns:
        import warnings as _w
        for col in hourly.columns:
            with _w.catch_warnings():
                _w.simplefilter("ignore", UserWarning)
                parsed = pd.to_datetime(hourly[col], errors="coerce")
            if parsed.notna().sum() < 1:
                continue
            if "shift_date" not in hourly.columns:
                hourly["shift_date"] = parsed.dt.date.astype(str)
            if "shift_hour" not in hourly.columns:
                hourly["shift_hour"] = parsed.dt.hour + 1
            if "time_block" not in hourly.columns:
                hourly["time_block"] = parsed.dt.strftime("%H:%M")
            break

    # If hour is present under an unmapped column name, recover it.
    if "shift_hour" not in hourly.columns:
        for col in hourly.columns:
            ncol = _normalize_col(col)
            if ncol in {"hour", "hr", "starthour", "hourofday"}:
                cand = pd.to_numeric(hourly[col], errors="coerce")
                if cand.notna().any():
                    hourly["shift_hour"] = cand
                    break

    if "shift" not in hourly.columns and "shift_hour" in hourly.columns:
        hourly["shift"] = hourly["shift_hour"].apply(_shift_from_hour)

    if "total_hours" not in hourly.columns:
        hourly["total_hours"] = 1.0

    if "total_cases" not in hourly.columns and "good_cases" in hourly.columns:
        hourly["total_cases"] = hourly["good_cases"] + hourly.get("bad_cases", 0)

    missing = sorted(c for c in needed if c not in hourly.columns)
    if missing:
        raise ValueError(
            f"One-sheet OEE file is missing required columns: {', '.join(missing)}"
        )

    hourly["date"] = pd.to_datetime(hourly["shift_date"], errors="coerce")
    mask = hourly["date"].isna()
    if mask.any():
        hourly.loc[mask, "date"] = hourly.loc[mask, "shift_date"].apply(excel_date_to_datetime)
    hourly["date_str"] = hourly["date"].dt.strftime("%Y-%m-%d")
    hourly["day_of_week"] = hourly["date"].dt.day_name()

    shift_summary, overall, hour_avg = _build_summary_frames_from_hourly(hourly)
    return hourly, shift_summary, overall, hour_avg


# ---------------------------------------------------------------------------
# Load OEE Data
# ---------------------------------------------------------------------------
def load_oee_data(filepath):
    print(f"Reading OEE data: {filepath}")
    try:
        sheet_map = _resolve_sheets(filepath)
    except ValueError as e:
        # Fallback for one-sheet exports from MES systems (commonly named "Data").
        fallback = _load_single_sheet_oee(filepath)
        print("  Matched sheets: single-sheet fallback")
        return fallback
    print(f"  Matched sheets: {sheet_map}")

    # --- DayShiftHour (hourly detail) ---
    hourly = pd.read_excel(filepath, sheet_name=sheet_map["DayShiftHour"])
    hourly = _smart_rename(hourly, EXPECTED_SHEETS["DayShiftHour"]["columns"])
    hourly = _coerce_numerics(hourly)
    hourly = _derive_columns(hourly)
    hourly["date"] = hourly["shift_date"].apply(excel_date_to_datetime)
    hourly["date_str"] = hourly["date"].dt.strftime("%Y-%m-%d")
    hourly["day_of_week"] = hourly["date"].dt.day_name()

    # --- DayShift_Summary (daily by shift) ---
    shift_summary = pd.read_excel(filepath, sheet_name=sheet_map["DayShift_Summary"])
    shift_summary = _smart_rename(shift_summary, EXPECTED_SHEETS["DayShift_Summary"]["columns"])
    shift_summary = _coerce_numerics(shift_summary)
    shift_summary = _derive_columns(shift_summary)
    shift_summary["date"] = shift_summary["shift_date"].apply(excel_date_to_datetime)
    shift_summary["date_str"] = shift_summary["date"].dt.strftime("%Y-%m-%d")

    # --- Shift_Summary (overall by shift) ---
    overall = pd.read_excel(filepath, sheet_name=sheet_map["Shift_Summary"])
    overall = _smart_rename(overall, EXPECTED_SHEETS["Shift_Summary"]["columns"])
    overall = _coerce_numerics(overall)
    overall = _derive_columns(overall)

    # --- ShiftHour_Summary (average by shift & hour) ---
    hour_avg = pd.read_excel(filepath, sheet_name=sheet_map["ShiftHour_Summary"])
    hour_avg = _smart_rename(hour_avg, EXPECTED_SHEETS["ShiftHour_Summary"]["columns"])
    hour_avg = _coerce_numerics(hour_avg)
    hour_avg = _derive_columns(hour_avg)

    # Filter out non-production rows (e.g. "No Shift")
    _non_production = {"No Shift", "no shift"}
    for _df in [hourly, shift_summary, overall, hour_avg]:
        if "shift" in _df.columns:
            mask = ~_df["shift"].astype(str).str.strip().isin(_non_production)
            _df.drop(_df[~mask].index, inplace=True)
            _df.reset_index(drop=True, inplace=True)

    print(f"  {len(hourly)} hourly records, {hourly['date_str'].nunique()} days")
    return hourly, shift_summary, overall, hour_avg


# ---------------------------------------------------------------------------
# Load Downtime Knowledge Base (JSON from Claude.ai)
# ---------------------------------------------------------------------------
def load_downtime_data(json_path):
    print(f"Reading downtime data: {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        kb = json.load(f)

    reasons = kb.get("downtime_reason_codes", [])
    reasons_df = pd.DataFrame(reasons)
    if len(reasons_df) > 0:
        reasons_df["total_minutes"] = pd.to_numeric(reasons_df["total_minutes"], errors="coerce").fillna(0)
        reasons_df["total_occurrences"] = pd.to_numeric(reasons_df["total_occurrences"], errors="coerce").fillna(0)
        if "total_hours" in reasons_df.columns:
            reasons_df["total_hours"] = pd.to_numeric(reasons_df["total_hours"], errors="coerce").fillna(0)
        else:
            reasons_df["total_hours"] = reasons_df["total_minutes"] / 60.0

    pareto = kb.get("pareto_top_10", {})
    pareto_rankings = pareto.get("rankings", [])
    pareto_df = pd.DataFrame(pareto_rankings)

    findings = kb.get("key_findings", [])
    shift_samples = kb.get("sample_data", {}).get("shift_report_sample_sheet_1_05_26", [])
    event_samples = kb.get("sample_data", {}).get("event_summary_first_20_events", [])
    meta = kb.get("metadata", {})
    oee_summary = meta.get("oee_period_summary", {})

    print(f"  {len(reasons)} reason codes, {len(pareto_rankings)} pareto items, {len(findings)} findings")
    return {
        "reasons_df": reasons_df,
        "pareto_df": pareto_df,
        "findings": findings,
        "shift_samples": shift_samples,
        "event_samples": event_samples,
        "meta": meta,
        "oee_summary": oee_summary,
        "pareto_raw": pareto,
    }


# ---------------------------------------------------------------------------
# Shift Deep Dive builder
# ---------------------------------------------------------------------------
def build_shift_deep_dive(shift_name, hourly, shift_summary, hour_avg, overall, plant_avg_oee, plant_avg_cph):
    """Build a multi-section deep dive for one shift."""
    sh = hourly[hourly["shift"] == shift_name].copy()
    ss = shift_summary[shift_summary["shift"] == shift_name].copy()
    ha = hour_avg[hour_avg["shift"] == shift_name].copy()
    ov = overall[overall["shift"] == shift_name]

    if len(sh) == 0:
        return None

    rows = []

    # --- Section 1: Shift Scorecard ---
    rows.append({"Section": "SHIFT SCORECARD", "Metric": "", "Value": "", "Detail": ""})

    shift_avail, shift_perf, shift_qual, shift_oee = _aggregate_oee(sh)
    shift_cases = sh["total_cases"].sum()
    shift_hours = sh["total_hours"].sum()
    n_days = sh["date_str"].nunique()
    shift_cph = shift_cases / (n_days * SHIFT_HOURS) if n_days > 0 else 0

    oee_vs_plant = shift_oee - plant_avg_oee
    oee_indicator = "above" if oee_vs_plant > 0 else "below"
    cph_vs_plant = shift_cph - plant_avg_cph
    cph_indicator = "above" if cph_vs_plant > 0 else "below"

    rows.append({"Section": "", "Metric": "OEE", "Value": f"{shift_oee:.1f}%",
                 "Detail": f"{abs(oee_vs_plant):.1f} pts {oee_indicator} plant avg ({plant_avg_oee:.1f}%)"})
    rows.append({"Section": "", "Metric": "Availability", "Value": f"{shift_avail:.1%}",
                 "Detail": "% of time the line was running"})
    rows.append({"Section": "", "Metric": "Performance", "Value": f"{shift_perf:.1%}",
                 "Detail": "% of rated speed when running"})
    rows.append({"Section": "", "Metric": "Quality", "Value": f"{shift_qual:.1%}",
                 "Detail": "% good product"})
    rows.append({"Section": "", "Metric": "Cases/Hour", "Value": f"{shift_cph:,.0f}",
                 "Detail": f"{abs(cph_vs_plant):,.0f} CPH {cph_indicator} plant avg ({plant_avg_cph:,.0f})"})
    rows.append({"Section": "", "Metric": "Total Cases", "Value": f"{shift_cases:,.0f}",
                 "Detail": f"over {n_days} days"})
    rows.append({"Section": "", "Metric": "Total Hours", "Value": f"{shift_hours:,.1f}",
                 "Detail": ""})

    # --- Where is OEE being lost? ---
    avail_loss = (1 - shift_avail) * 100
    perf_loss = (1 - shift_perf) * 100
    qual_loss = (1 - shift_qual) * 100
    total_loss = avail_loss + perf_loss + qual_loss
    if total_loss > 0:
        avail_share = avail_loss / total_loss * 100
        perf_share = perf_loss / total_loss * 100
        qual_share = qual_loss / total_loss * 100
    else:
        avail_share = perf_share = qual_share = 0

    rows.append({"Section": "", "Metric": "", "Value": "", "Detail": ""})
    rows.append({"Section": "WHERE IS OEE LOST?", "Metric": "", "Value": "", "Detail": ""})
    rows.append({"Section": "", "Metric": "Availability Loss", "Value": f"{avail_loss:.1f}%",
                 "Detail": f"{avail_share:.0f}% of total loss — line not running (stops, changeovers, breakdowns)"})
    rows.append({"Section": "", "Metric": "Performance Loss", "Value": f"{perf_loss:.1f}%",
                 "Detail": f"{perf_share:.0f}% of total loss — line running slow (micro stops, speed loss)"})
    rows.append({"Section": "", "Metric": "Quality Loss", "Value": f"{qual_loss:.1f}%",
                 "Detail": f"{qual_share:.0f}% of total loss — rejected product"})

    biggest_loss = "Availability" if avail_loss >= perf_loss else "Performance"
    rows.append({"Section": "", "Metric": "Primary Loss", "Value": biggest_loss,
                 "Detail": f"Focus here first — it accounts for the biggest share of OEE loss on this shift"})

    # --- Section 2: Hour-by-Hour Pattern ---
    has_cph = "cases_per_hour" in ha.columns and ha["cases_per_hour"].sum() > 0
    has_avail = "availability" in ha.columns and "performance" in ha.columns
    rows.append({"Section": "", "Metric": "", "Value": "", "Detail": ""})
    if has_cph:
        rows.append({"Section": "HOUR-BY-HOUR PATTERN", "Metric": "Hour", "Value": "Avg OEE %", "Detail": "Avg Cases/Hr"})
    else:
        rows.append({"Section": "HOUR-BY-HOUR PATTERN", "Metric": "Hour", "Value": "Avg OEE %", "Detail": "Avail / Perf"})

    ha_sorted = ha.sort_values("shift_hour")
    for _, hrow in ha_sorted.iterrows():
        hour_label = hrow["time_block"] if hrow.get("time_block") else f"{int(hrow['shift_hour'])}:00"
        if has_cph:
            detail = f"{hrow['cases_per_hour']:,.0f}"
        elif has_avail:
            detail = f"{hrow['availability']:.0%} / {hrow['performance']:.0%}"
        else:
            detail = ""
        rows.append({
            "Section": "", "Metric": f"Hr {int(hrow['shift_hour'])} ({hour_label})",
            "Value": f"{hrow['oee_pct']:.1f}%",
            "Detail": detail
        })

    # First hour vs rest (use minimum hour in the shift, not hardcoded 1)
    if len(ha_sorted) > 1:
        min_hour = ha_sorted["shift_hour"].min()
        first_hr_oee = ha_sorted[ha_sorted["shift_hour"] == min_hour]["oee_pct"].values
        rest = ha_sorted[ha_sorted["shift_hour"] != min_hour]
        rest_oee = (_weighted_mean(rest["oee_pct"], rest["total_hours"])
                    if "total_hours" in rest.columns else rest["oee_pct"].mean())
        if len(first_hr_oee) > 0:
            gap = rest_oee - first_hr_oee[0]
            if gap > 2:
                rows.append({"Section": "", "Metric": "Startup Gap",
                             "Value": f"-{gap:.1f} pts",
                             "Detail": f"First hour is {gap:.1f} OEE points below the rest of the shift"})

    # Best and worst hours
    if len(ha_sorted) > 0:
        best_hr = ha_sorted.loc[ha_sorted["oee_pct"].idxmax()]
        worst_hr = ha_sorted.loc[ha_sorted["oee_pct"].idxmin()]
        if has_cph:
            best_detail = f"{best_hr['cases_per_hour']:,.0f} CPH"
            worst_detail = f"{worst_hr['cases_per_hour']:,.0f} CPH"
        elif has_avail:
            best_detail = f"Avail {best_hr['availability']:.0%} / Perf {best_hr['performance']:.0%}"
            worst_detail = f"Avail {worst_hr['availability']:.0%} / Perf {worst_hr['performance']:.0%}"
        else:
            best_detail = ""
            worst_detail = ""
        rows.append({"Section": "", "Metric": "Best Hour",
                     "Value": f"Hr {int(best_hr['shift_hour'])} ({best_hr['oee_pct']:.1f}%)",
                     "Detail": best_detail})
        rows.append({"Section": "", "Metric": "Worst Hour",
                     "Value": f"Hr {int(worst_hr['shift_hour'])} ({worst_hr['oee_pct']:.1f}%)",
                     "Detail": worst_detail})

    # --- Section 3: Day-by-Day Trend ---
    rows.append({"Section": "", "Metric": "", "Value": "", "Detail": ""})
    rows.append({"Section": "DAY-BY-DAY TREND", "Metric": "Date", "Value": "OEE %", "Detail": "Cases/Hr"})
    ss_sorted = ss.sort_values("date_str")
    for _, srow in ss_sorted.iterrows():
        rows.append({
            "Section": "", "Metric": srow["date_str"],
            "Value": f"{srow['oee_pct']:.1f}%",
            "Detail": f"{srow['cases_per_hour']:,.0f} CPH / {srow['total_cases']:,.0f} cases"
        })

    # Trend direction
    if len(ss_sorted) >= 7:
        fh = ss_sorted.head(len(ss_sorted) // 2)
        sh2 = ss_sorted.tail(len(ss_sorted) // 2)
        first_half = _weighted_mean(fh["oee_pct"], fh["total_hours"]) if "total_hours" in fh.columns else fh["oee_pct"].mean()
        second_half = _weighted_mean(sh2["oee_pct"], sh2["total_hours"]) if "total_hours" in sh2.columns else sh2["oee_pct"].mean()
        direction = "improving" if second_half > first_half + 1 else "declining" if second_half < first_half - 1 else "flat"
        rows.append({"Section": "", "Metric": "Trend",
                     "Value": direction.upper(),
                     "Detail": f"First half avg: {first_half:.1f}% → Second half avg: {second_half:.1f}%"})

    # --- Section 4: Day of Week Breakdown ---
    rows.append({"Section": "", "Metric": "", "Value": "", "Detail": ""})
    rows.append({"Section": "DAY OF WEEK", "Metric": "Day", "Value": "Avg OEE %", "Detail": "# Shift-Days"})
    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    sh_dow_rows = []
    for day_name in dow_order:
        day_data = sh[sh["day_of_week"] == day_name]
        if len(day_data) == 0:
            continue
        _, _, _, day_oee = _aggregate_oee(day_data)
        day_total_cases = day_data["total_cases"].sum()
        day_total_hours = day_data["total_hours"].sum()
        n_dow_days = day_data["date_str"].nunique()
        day_cph = day_total_cases / (n_dow_days * SHIFT_HOURS) if n_dow_days > 0 else 0
        n_hours = len(day_data)
        sh_dow_rows.append({"day_of_week": day_name, "avg_oee": day_oee,
                            "avg_cph": day_cph, "n_hours": n_hours})
    sh_dow = pd.DataFrame(sh_dow_rows).set_index("day_of_week") if sh_dow_rows else pd.DataFrame()
    for day_name, drow in sh_dow.iterrows():
        rows.append({
            "Section": "", "Metric": day_name,
            "Value": f"{drow['avg_oee']:.1f}%",
            "Detail": f"{drow['avg_cph']:,.0f} CPH / {int(drow['n_hours'])} hours"
        })

    if len(sh_dow) > 1:
        best_d = sh_dow["avg_oee"].idxmax()
        worst_d = sh_dow["avg_oee"].idxmin()
        d_gap = sh_dow.loc[best_d, "avg_oee"] - sh_dow.loc[worst_d, "avg_oee"]
        if d_gap > 3:
            rows.append({"Section": "", "Metric": "Day Gap",
                         "Value": f"{d_gap:.0f} pts",
                         "Detail": f"Best: {best_d} ({sh_dow.loc[best_d, 'avg_oee']:.1f}%) / Worst: {worst_d} ({sh_dow.loc[worst_d, 'avg_oee']:.1f}%)"})

    # --- Section 5: Worst Hours ---
    rows.append({"Section": "", "Metric": "", "Value": "", "Detail": ""})
    rows.append({"Section": "WORST 10 HOURS", "Metric": "Date / Time", "Value": "OEE %", "Detail": "What happened"})
    shift_worst = sh[sh["total_hours"] >= 0.5].nsmallest(10, "oee_pct")
    for _, wrow in shift_worst.iterrows():
        # Determine primary loss driver for this hour
        a = wrow["availability"]
        p = wrow["performance"]
        q = wrow["quality"]
        if a < p and a < q:
            driver = f"Availability {a:.0%} — line stopped"
        elif p < a and p < q:
            driver = f"Performance {p:.0%} — running slow"
        else:
            driver = f"Quality {q:.0%}" if q < 0.95 else f"Avail {a:.0%} / Perf {p:.0%}"
        rows.append({
            "Section": "",
            "Metric": f"{wrow['date_str']} {wrow['day_of_week'][:3]} {wrow['time_block']}",
            "Value": f"{wrow['oee_pct']:.1f}%",
            "Detail": f"{wrow['cases_per_hour']:,.0f} CPH — {driver}"
        })

    # --- Section 6: Consistency Score ---
    rows.append({"Section": "", "Metric": "", "Value": "", "Detail": ""})
    rows.append({"Section": "CONSISTENCY", "Metric": "", "Value": "", "Detail": ""})
    active_sh = sh[(sh["total_hours"] > 0) & (sh["total_cases"] > 0)]
    std_oee = active_sh["oee_pct"].std() if len(active_sh) > 1 else 0
    rows.append({"Section": "", "Metric": "OEE Std Deviation",
                 "Value": f"{std_oee:.1f}",
                 "Detail": "Lower = more consistent. High variation means some hours are good, others collapse."})

    n_active = len(active_sh)
    pct_below_20 = (active_sh["oee_pct"] < 20).sum() / n_active * 100 if n_active > 0 else 0
    pct_above_50 = (active_sh["oee_pct"] > 50).sum() / n_active * 100 if n_active > 0 else 0
    rows.append({"Section": "", "Metric": "Hours below 20% OEE",
                 "Value": f"{(active_sh['oee_pct'] < 20).sum()} ({pct_below_20:.0f}%)",
                 "Detail": "These are near-zero production hours — investigate each one"})
    rows.append({"Section": "", "Metric": "Hours above 50% OEE",
                 "Value": f"{(active_sh['oee_pct'] > 50).sum()} ({pct_above_50:.0f}%)",
                 "Detail": "Good hours — the line CAN run well. What's different?"})

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Fault Classification builder
# ---------------------------------------------------------------------------
def build_fault_classification(downtime):
    """Classify all reason codes into fault categories and summarize."""
    reasons_df = downtime["reasons_df"].copy()
    if len(reasons_df) == 0:
        return None, None

    # Classify each reason
    reasons_df["fault_category"] = reasons_df["reason"].apply(classify_fault)
    reasons_df["avg_min_per_event"] = (reasons_df["total_minutes"] / reasons_df["total_occurrences"]).round(1)

    # --- Detail table: every reason with its classification ---
    detail = reasons_df.sort_values(["fault_category", "total_minutes"], ascending=[True, False])
    detail_out = detail[["fault_category", "reason", "total_occurrences", "total_minutes",
                          "total_hours", "avg_min_per_event"]].copy()
    detail_out.columns = ["Fault Category", "Reason Code", "Events", "Total Minutes",
                          "Total Hours", "Avg Min/Event"]

    # --- Summary table: totals by category ---
    summary = (
        reasons_df.groupby("fault_category")
        .agg(
            total_events=("total_occurrences", "sum"),
            total_minutes=("total_minutes", "sum"),
            total_hours=("total_hours", "sum"),
            n_codes=("reason", "count"),
        )
        .sort_values("total_minutes", ascending=False)
        .reset_index()
    )
    grand_total_min = summary["total_minutes"].sum()
    summary["pct_of_all_downtime"] = (summary["total_minutes"] / grand_total_min * 100).round(1)

    # Who owns it?
    ownership = {
        "Equipment / Mechanical": "Maintenance / Reliability",
        "Micro Stops": "Engineering + Operators (sensor tuning, line adjustments)",
        "Process / Changeover": "CI / Operations (SMED, standard work, staging)",
        "Scheduled / Non-Production": "Planning / Management (schedule optimization)",
        "Data Gap (uncoded)": "Supervisors (reason code discipline)",
        "Other / Unclassified": "Needs review — classify these reason codes",
    }
    summary["who_owns_this"] = summary["fault_category"].map(ownership).fillna("TBD")

    summary_out = summary[["fault_category", "n_codes", "total_events", "total_minutes",
                            "total_hours", "pct_of_all_downtime", "who_owns_this"]].copy()
    summary_out.columns = ["Fault Category", "# Reason Codes", "Total Events", "Total Minutes",
                            "Total Hours", "% of All Downtime", "Who Owns This"]

    return summary_out, detail_out


# ---------------------------------------------------------------------------
# Shift-Centric Helpers
# ---------------------------------------------------------------------------

# Canonical shift name mapping: data names → display names
_SHIFT_DISPLAY = {"1st": "1st Shift", "2nd": "2nd Shift", "3rd": "3rd Shift"}


def _shift_display_name(shift_name):
    """Map data shift names like '1st (7a-3p)' to clean display names."""
    for prefix, display in _SHIFT_DISPLAY.items():
        if shift_name.lower().startswith(prefix):
            return display
    return shift_name


def _compute_shift_data(shift_name, hourly, shift_summary, overall, downtime,
                         plant_avg_oee, plant_avg_cph):
    """Compute all metrics for one shift, filtered day-by-day.

    Returns a dict with:
      - scorecard: DataFrame (per-day + total rows)
      - loss_breakdown: DataFrame (Avail/Perf/Qual loss per day)
      - downtime_causes: DataFrame (top 10 causes)
      - hour_by_hour: DataFrame (per-date hour-by-hour detail)
      - dead_hours: DataFrame (dead hour blocks with causes)
      - worst_hours: DataFrame (top 10 worst hours)
      - raw: dict of scalar values for narrative generation
    """
    sh = hourly[hourly["shift"] == shift_name].copy()
    ss = shift_summary[shift_summary["shift"] == shift_name].copy()
    ov = overall[overall["shift"] == shift_name]

    if len(sh) == 0:
        return None

    has_downtime = downtime is not None and len(downtime.get("reasons_df", [])) > 0
    has_events = has_downtime and len(downtime.get("events_df", [])) > 0

    # --- Aggregate OEE ---
    shift_avail, shift_perf, shift_qual, shift_oee = _aggregate_oee(sh)
    shift_cases = sh["total_cases"].sum()
    shift_hours = sh["total_hours"].sum()
    n_days = sh["date_str"].nunique()
    shift_cph = shift_cases / (n_days * SHIFT_HOURS) if n_days > 0 else 0

    # Utilization
    util_pct, prod_hours, sched_hours, dead_count = _compute_utilization(sh)

    # Product-aware target
    if "product_code" in sh.columns:
        sh["_tgt_cph"] = sh["product_code"].apply(get_target_cph)
        benchmark_cph = sh[sh["total_hours"] >= 0.5]["cases_per_hour"].quantile(0.90) if len(sh[sh["total_hours"] >= 0.5]) > 0 else 0
        sh["_tgt_cph"] = sh["_tgt_cph"].fillna(benchmark_cph)
        target_cph_avg = (sh["_tgt_cph"] * sh["total_hours"]).sum() / shift_hours if shift_hours > 0 else benchmark_cph
        target_cases_total = target_cph_avg * n_days * SHIFT_HOURS
        sh.drop(columns=["_tgt_cph"], inplace=True, errors="ignore")
    else:
        benchmark_cph = sh[sh["total_hours"] >= 0.5]["cases_per_hour"].quantile(0.90) if len(sh[sh["total_hours"] >= 0.5]) > 0 else 0
        target_cph_avg = benchmark_cph
        target_cases_total = benchmark_cph * n_days * SHIFT_HOURS

    pct_of_target = shift_cases / target_cases_total * 100 if target_cases_total > 0 else 0
    cases_gap = target_cases_total - shift_cases

    # Loss breakdown
    avail_loss = (1 - shift_avail) * 100
    perf_loss = (1 - shift_perf) * 100
    qual_loss = (1 - shift_qual) * 100
    total_loss = avail_loss + perf_loss + qual_loss
    if total_loss > 0:
        primary_loss = "Availability" if avail_loss >= perf_loss and avail_loss >= qual_loss else \
                        "Performance" if perf_loss >= qual_loss else "Quality"
        primary_loss_pct = max(avail_loss, perf_loss, qual_loss) / total_loss * 100
    else:
        primary_loss = "None"
        primary_loss_pct = 0

    # --- SCORECARD (per-day rows) ---
    scorecard_rows = []
    dates = sorted(sh["date_str"].unique())
    for d in dates:
        d_data = sh[sh["date_str"] == d]
        da, dp, dq, doee = _aggregate_oee(d_data)
        d_cases = d_data["total_cases"].sum()
        d_hours = d_data["total_hours"].sum()
        d_cph = d_cases / SHIFT_HOURS
        d_util, d_prod, d_sched, d_dead = _compute_utilization(d_data)
        # Product target for this day
        if "product_code" in d_data.columns:
            d_data_cp = d_data.copy()
            d_data_cp["_tgt"] = d_data_cp["product_code"].apply(get_target_cph)
            d_bm = d_data[d_data["total_hours"] >= 0.5]["cases_per_hour"].quantile(0.90) if len(d_data[d_data["total_hours"] >= 0.5]) > 0 else 0
            d_data_cp["_tgt"] = d_data_cp["_tgt"].fillna(d_bm)
            d_target_cph = (d_data_cp["_tgt"] * d_data_cp["total_hours"]).sum() / d_hours if d_hours > 0 else d_bm
        else:
            d_target_cph = target_cph_avg
        scorecard_rows.append({
            "Date": d, "OEE %": round(doee, 1),
            "Availability %": round(da * 100, 1),
            "Performance %": round(dp * 100, 1),
            "Quality %": round(dq * 100, 1),
            "Cases/Hr": round(d_cph, 0),
            "Target CPH": round(d_target_cph, 0),
            "Total Cases": round(d_cases, 0),
            "Hours Scheduled": round(d_sched, 1),
            "Hours Producing": round(d_prod, 1),
            "Dead Hours": d_dead,
            "Utilization %": round(d_util, 1),
        })
    # Add totals row if multiple days
    if len(dates) > 1:
        scorecard_rows.append({
            "Date": "TOTAL", "OEE %": round(shift_oee, 1),
            "Availability %": round(shift_avail * 100, 1),
            "Performance %": round(shift_perf * 100, 1),
            "Quality %": round(shift_qual * 100, 1),
            "Cases/Hr": round(shift_cph, 0),
            "Target CPH": round(target_cph_avg, 0),
            "Total Cases": round(shift_cases, 0),
            "Hours Scheduled": round(sched_hours, 1),
            "Hours Producing": round(prod_hours, 1),
            "Dead Hours": dead_count,
            "Utilization %": round(util_pct, 1),
        })
    scorecard_df = pd.DataFrame(scorecard_rows)

    # --- LOSS BREAKDOWN (per day) ---
    loss_rows = []
    for d in dates:
        d_data = sh[sh["date_str"] == d]
        da, dp, dq, doee = _aggregate_oee(d_data)
        al = (1 - da) * 100
        pl = (1 - dp) * 100
        ql = (1 - dq) * 100
        tl = al + pl + ql
        driver = "Availability" if al >= pl and al >= ql else "Performance" if pl >= ql else "Quality"
        loss_rows.append({
            "Date": d,
            "Avail Loss %": round(al, 1),
            "Perf Loss %": round(pl, 1),
            "Qual Loss %": round(ql, 1),
            "Total Loss %": round(tl, 1),
            "Primary Driver": driver,
        })
    loss_breakdown_df = pd.DataFrame(loss_rows)

    # --- DOWNTIME CAUSES (top 10) ---
    downtime_causes_df = pd.DataFrame()
    top_cause_str = ""
    top_cause_min = 0
    top_cause_events = 0
    if has_events:
        events_df = downtime.get("events_df")
        if events_df is not None and len(events_df) > 0:
            # Filter events to this shift, excluding non-actionable reasons
            shift_events = events_df[events_df["shift"] == shift_name].copy()
            shift_events = shift_events[~shift_events["reason"].isin(EXCLUDE_REASONS)]
            if len(shift_events) > 0:
                cause_agg = shift_events.groupby("reason").agg(
                    Events=("duration_minutes", "size"),
                    Total_Min=("duration_minutes", "sum"),
                    Avg_Min=("duration_minutes", "mean"),
                ).reset_index()
                cause_agg = cause_agg.rename(columns={"reason": "Cause"})
                cause_agg["Avg_Min"] = cause_agg["Avg_Min"].round(1)
                cause_agg["Total_Min"] = cause_agg["Total_Min"].round(1)
                total_shift_min = cause_agg["Total_Min"].sum()
                cause_agg["% of Shift"] = (cause_agg["Total_Min"] / total_shift_min * 100).round(1) if total_shift_min > 0 else 0
                cause_agg = cause_agg.sort_values("Total_Min", ascending=False).head(10).reset_index(drop=True)
                cause_agg.columns = ["Cause", "Events", "Total Min", "Avg Min", "% of Shift"]
                downtime_causes_df = cause_agg
                if len(cause_agg) > 0:
                    top_cause_str = str(cause_agg.iloc[0]["Cause"])
                    if len(top_cause_str) > 80:
                        top_cause_str = top_cause_str[:77] + "..."
                    top_cause_min = cause_agg.iloc[0]["Total Min"]
                    top_cause_events = cause_agg.iloc[0]["Events"]
    elif has_downtime:
        # Fall back to reasons_df (not per-shift, but best available)
        reasons_df = downtime["reasons_df"].copy()
        actionable = reasons_df[~reasons_df["reason"].isin(EXCLUDE_REASONS)].sort_values("total_minutes", ascending=False).head(10)
        if len(actionable) > 0:
            total_min = actionable["total_minutes"].sum()
            dc = actionable[["reason", "total_occurrences", "total_minutes"]].copy()
            dc["avg_min"] = (dc["total_minutes"] / dc["total_occurrences"]).round(1)
            dc["pct"] = (dc["total_minutes"] / total_min * 100).round(1) if total_min > 0 else 0
            dc.columns = ["Cause", "Events", "Total Min", "Avg Min", "% of Shift"]
            downtime_causes_df = dc
            top_cause_str = str(dc.iloc[0]["Cause"])
            if len(top_cause_str) > 80:
                top_cause_str = top_cause_str[:77] + "..."
            top_cause_min = dc.iloc[0]["Total Min"]
            top_cause_events = dc.iloc[0]["Events"]

    # --- HOUR-BY-HOUR (per date) ---
    hbh_rows = []
    for d in dates:
        d_data = sh[sh["date_str"] == d].sort_values("shift_hour")
        for _, hrow in d_data.iterrows():
            annotation = ""
            # Annotate worst hours with loss type
            a = hrow["availability"]
            p = hrow["performance"]
            if hrow["total_cases"] == 0:
                annotation = "Line DOWN — 0 cases"
            elif a < 0.5:
                annotation = f"Avail {a:.0%} — stopped"
            elif p < 0.5:
                annotation = f"Perf {p:.0%} — slow"
            hbh_rows.append({
                "Date": d,
                "Hour": int(hrow["shift_hour"]),
                "Time": hrow.get("time_block", f"{int(hrow['shift_hour'])}:00"),
                "OEE %": round(hrow["oee_pct"], 1),
                "Cases/Hr": round(hrow["cases_per_hour"], 0),
                "Cases": round(hrow["total_cases"], 0),
                "What Happened": annotation,
            })
    hour_by_hour_df = pd.DataFrame(hbh_rows)

    # --- DEAD HOURS ---
    dead_blocks, dead_summary_info = _build_dead_hour_narrative(sh)
    if has_events and len(dead_blocks) > 0:
        dead_blocks = _correlate_dead_hours_with_events(
            dead_blocks, downtime["events_df"], sh)

    dh_rows = []
    for b in dead_blocks:
        if b["n_hours"] >= 2:
            hr_label = f"Hr {b['first_hour']}–{b['last_hour']}"
        else:
            hr_label = f"Hr {b['first_hour']}"
        dh_rows.append({
            "Date": b["date_str"],
            "Time Block": hr_label,
            "Hours": b["n_hours"],
            "Cause (Machine Data)": b.get("cause_annotation", b.get("causes", "")),
            "Product Running": b.get("product", ""),
        })
    dead_hours_df = pd.DataFrame(dh_rows) if dh_rows else pd.DataFrame(
        columns=["Date", "Time Block", "Hours", "Cause (Machine Data)", "Product Running"])

    # --- WORST HOURS (top 10) ---
    shift_active = sh[sh["total_hours"] >= 0.5]
    worst_10 = shift_active.nsmallest(10, "oee_pct") if len(shift_active) > 0 else pd.DataFrame()
    wh_rows = []
    for _, wrow in worst_10.iterrows():
        a = wrow["availability"]
        p = wrow["performance"]
        q = wrow["quality"]
        if wrow["total_cases"] == 0:
            driver = "Line DOWN — 0 cases"
        elif a < p and a < q:
            driver = f"Availability {a:.0%} — line stopped"
        elif p < a and p < q:
            driver = f"Performance {p:.0%} — running slow"
        else:
            driver = f"Mixed — Avail {a:.0%} / Perf {p:.0%}"

        # Event annotation if available
        event_str = ""
        if has_events:
            from parse_mes import SHIFT_STARTS
            events_df_w = downtime.get("events_df")
            if events_df_w is not None and len(events_df_w) > 0:
                shift_start = None
                for sn, sh_val in SHIFT_STARTS.items():
                    if sn in shift_name or shift_name in sn:
                        shift_start = sh_val
                        break
                if shift_start is not None:
                    clock_hour = (shift_start + int(wrow["shift_hour"]) - 1) % 24
                    cal_date = wrow["date_str"]
                    if "3rd" in shift_name.lower() and clock_hour < 7:
                        cal_date = (datetime.strptime(cal_date, "%Y-%m-%d").date()
                                    + timedelta(days=1)).strftime("%Y-%m-%d")
                    hr_start = datetime.strptime(f"{cal_date} {clock_hour:02d}:00:00", "%Y-%m-%d %H:%M:%S")
                    hr_end = hr_start + timedelta(hours=1)
                    # Events with exact timestamps: match by time overlap
                    has_end = events_df_w["end_time"].apply(lambda x: isinstance(x, datetime))
                    exact = events_df_w[has_end]
                    approx = events_df_w[~has_end]
                    overlaps_exact = exact[
                        (exact["start_time"] < hr_end) & (exact["end_time"] > hr_start)
                    ] if len(exact) > 0 else pd.DataFrame()
                    # Events without end_time (e.g. passdown): match by shift + date
                    overlaps_approx = pd.DataFrame()
                    if len(approx) > 0:
                        approx_match = approx[
                            (approx["shift"] == shift_name) &
                            (approx["start_time"].apply(
                                lambda x: x.strftime("%Y-%m-%d") if isinstance(x, datetime) else ""
                            ) == wrow["date_str"])
                        ]
                        if len(approx_match) > 0:
                            overlaps_approx = approx_match
                    overlaps = pd.concat([overlaps_exact, overlaps_approx], ignore_index=True)
                    if len(overlaps) > 0:
                        overlaps = overlaps.copy()
                        def _calc_overlap(e):
                            if isinstance(e["end_time"], datetime):
                                return (min(e["end_time"], hr_end) - max(e["start_time"], hr_start)).total_seconds() / 60
                            return e.get("duration_minutes", 0)
                        overlaps["overlap"] = overlaps.apply(_calc_overlap, axis=1)
                        top = overlaps.groupby("reason")["overlap"].sum().sort_values(ascending=False)
                        event_str = "; ".join(f"{r}: {m:.0f}min" for r, m in top.head(3).items())

        prod = ""
        if "product_code" in wrow.index:
            prod = str(wrow.get("product_code", "")).strip()

        what = driver
        if event_str:
            what += f" | {event_str}"
        if prod:
            what += f" | {prod}"

        wh_rows.append({
            "Date": wrow["date_str"],
            "Hour": int(wrow["shift_hour"]),
            "OEE %": round(wrow["oee_pct"], 1),
            "Cases/Hr": round(wrow["cases_per_hour"], 0),
            "What Happened": what,
        })
    worst_hours_df = pd.DataFrame(wh_rows) if wh_rows else pd.DataFrame(
        columns=["Date", "Hour", "OEE %", "Cases/Hr", "What Happened"])

    # --- Raw values for narrative ---
    raw = {
        "shift_name": _shift_display_name(shift_name),
        "shift_name_data": shift_name,
        "oee": shift_oee,
        "avail": shift_avail,
        "perf": shift_perf,
        "qual": shift_qual,
        "cases": shift_cases,
        "hours": shift_hours,
        "cph": shift_cph,
        "target_cph": target_cph_avg,
        "target_cases": target_cases_total,
        "pct_of_target": pct_of_target,
        "cases_gap": cases_gap,
        "n_days": n_days,
        "util_pct": util_pct,
        "prod_hours": prod_hours,
        "sched_hours": sched_hours,
        "dead_count": dead_count,
        "dead_hours_total": dead_summary_info["total_dead"],
        "plant_avg_oee": plant_avg_oee,
        "plant_avg_cph": plant_avg_cph,
        "avail_loss": avail_loss,
        "perf_loss": perf_loss,
        "qual_loss": qual_loss,
        "primary_loss": primary_loss,
        "primary_loss_pct": primary_loss_pct,
        "top_cause": top_cause_str,
        "top_cause_min": top_cause_min,
        "top_cause_events": top_cause_events,
        "operator_downtime_min": downtime_causes_df["Total Min"].sum() if len(downtime_causes_df) > 0 else 0,
        "operator_downtime_events": int(downtime_causes_df["Events"].sum()) if len(downtime_causes_df) > 0 else 0,
        "downtime_causes": [(str(row["Cause"]), float(row["Total Min"]), int(row["Events"]))
                            for _, row in downtime_causes_df.head(5).iterrows()] if len(downtime_causes_df) > 0 else [],
        "dead_hour_causes": [b.get("cause_annotation", b.get("causes", ""))
                            for b in dead_blocks if b.get("cause_annotation") or b.get("causes")],
    }

    return {
        "scorecard": scorecard_df,
        "loss_breakdown": loss_breakdown_df,
        "downtime_causes": downtime_causes_df,
        "hour_by_hour": hour_by_hour_df,
        "dead_hours": dead_hours_df,
        "worst_hours": worst_hours_df,
        "raw": raw,
    }


def _build_shift_narrative(shift_data):
    """Generate 3-paragraph narrative from computed shift data.

    Paragraph 1: What happened (OEE, cases, target, utilization)
    Paragraph 2: Why (loss driver, downtime causes, dead hours)
    Paragraph 3: The fix (top 2-3 actionable items with evidence)
    """
    r = shift_data["raw"]

    # --- Paragraph 1: What happened ---
    oee_vs_plant = r["oee"] - r["plant_avg_oee"]
    comp_str = f"{abs(oee_vs_plant):.1f} points {'above' if oee_vs_plant > 0 else 'below'} plant average ({r['plant_avg_oee']:.1f}%)"

    if r["cases_gap"] > 0:
        gap_str = f"{r['cases_gap']:,.0f} cases short"
    else:
        gap_str = f"{abs(r['cases_gap']):,.0f} cases over"

    p1 = (
        f"{r['shift_name']} averaged {r['oee']:.1f}% OEE across {r['n_days']} day(s), "
        f"producing {r['cases']:,.0f} cases in {r['prod_hours']:.1f} producing hours "
        f"({r['util_pct']:.0f}% utilization). {comp_str}. "
        f"Target was {r['target_cases']:,.0f} cases; actual delivery was "
        f"{r['pct_of_target']:.0f}% of target ({gap_str})."
    )

    # --- Paragraph 2: Why — lead with the specific issue, not OEE jargon ---
    parts2 = []

    dt_causes = r.get("downtime_causes", [])
    op_min = r.get("operator_downtime_min", 0)
    op_events = r.get("operator_downtime_events", 0)
    oee_says_perf = r["primary_loss"] == "Performance"
    operators_say_avail = op_min > 0 and op_events > 0

    if r["top_cause"] and dt_causes:
        # Lead with the #1 specific issue — always
        top = dt_causes[0]  # (cause_name, total_min, events)
        down_hrs = top[1] / 60
        parts2.append(
            f"#1 issue: {top[0]} -- {top[1]:.0f} min across {top[2]} events "
            f"({down_hrs:.1f} hrs of downtime)."
        )
        # Additional causes
        if len(dt_causes) >= 2:
            others = [f"{c[0]} ({c[1]:.0f} min)" for c in dt_causes[1:3]]
            parts2.append(f"Also: {', '.join(others)}.")

        # Add OEE context briefly
        if oee_says_perf and operators_say_avail and op_min >= 60:
            parts2.append(
                f"OEE categorizes the gap as Performance ({r['primary_loss_pct']:.0f}% of loss) "
                f"but operators logged {op_events} stop events totaling {op_min:,.0f} min -- "
                f"the stops are the real problem, not line speed."
            )
        else:
            parts2.append(
                f"OEE breakdown: {r['primary_loss']} was the largest loss component "
                f"({r['primary_loss_pct']:.0f}% of total loss)."
            )

        if r["primary_loss"] == "Performance" and not (oee_says_perf and operators_say_avail and op_min >= 60):
            parts2.append(
                f"When running, line averaged {r['cph']:,.0f} CPH vs "
                f"{r['target_cph']:,.0f} target."
            )
    elif r["primary_loss"] == "Quality":
        bad_cases = r["cases"] * (1 - r["qual"])
        parts2.append(
            f"Quality was the main problem: {r['qual_loss']:.1f}% loss "
            f"(~{bad_cases:,.0f} rejected cases)."
        )
    else:
        # No downtime data — fall back to OEE-only language
        parts2.append(
            f"The primary loss driver was {r['primary_loss']} "
            f"({r['primary_loss_pct']:.0f}% of total loss)."
        )
        if r["primary_loss"] == "Availability":
            dead_str = f"{r['dead_hours_total']} hours" if r["dead_hours_total"] > 0 else "some time"
            parts2.append(
                f"The line wasn't running for {dead_str} -- "
                f"no specific cause identified in machine data."
            )
        elif r["primary_loss"] == "Performance":
            parts2.append(
                f"When running, line averaged {r['cph']:,.0f} CPH vs "
                f"{r['target_cph']:,.0f} target -- speed losses without a clear single cause."
            )

    # Dead hour annotation with specific causes
    if r["primary_loss"] != "Availability" and not (oee_says_perf and operators_say_avail and op_min >= 60) and r["dead_hours_total"] > 0:
        dead_causes = r.get("dead_hour_causes", [])
        if dead_causes:
            unique_causes = list(dict.fromkeys(c for c in dead_causes if c))[:3]
            cause_note = f" Causes: {'; '.join(unique_causes)}."
        else:
            cause_note = ""
        parts2.append(
            f"Additionally, {r['dead_hours_total']} dead hours had zero production.{cause_note}"
        )

    p2 = " ".join(parts2)

    # --- Paragraph 3: The fix ---
    fix_parts = []
    actions = []

    if r["top_cause"]:
        recoverable = r["top_cause_min"] / 60 * r["target_cph"] * 0.5
        actions.append(
            f"Reduce {r['top_cause']} — {r['top_cause_min']:.0f} min across "
            f"{r['top_cause_events']} events; 50% reduction recovers ~{recoverable:,.0f} cases"
        )

    if r["primary_loss"] == "Availability" and r["avail_loss"] > 15:
        avail_hrs_lost = (1 - r["avail"]) * r["hours"]
        dt_causes = r.get("downtime_causes", [])
        if dt_causes and len(dt_causes) >= 2:
            cause_names = " and ".join(c[0] for c in dt_causes[:2])
            actions.append(
                f"Improve availability from {r['avail']:.0%} — "
                f"{avail_hrs_lost:.0f} hrs lost, driven by {cause_names}"
            )
        else:
            actions.append(
                f"Improve availability from {r['avail']:.0%} — "
                f"{avail_hrs_lost:.0f} hrs of scheduled time not running"
            )
    elif r["primary_loss"] == "Performance" and r["perf_loss"] > 10:
        if operators_say_avail and op_min >= 60:
            avail_hrs_lost = (1 - r["avail"]) * r["hours"]
            actions.append(
                f"Reduce stops first — operators reported {op_min:,.0f} min of downtime; "
                f"availability is {r['avail']:.0%} ({avail_hrs_lost:.0f} hrs lost)"
            )
        else:
            dt_causes = r.get("downtime_causes", [])
            if dt_causes:
                actions.append(
                    f"Close speed gap — running at {r['cph']:,.0f} CPH vs "
                    f"{r['target_cph']:,.0f} target; investigate {dt_causes[0][0]}"
                )
            else:
                actions.append(
                    f"Close speed gap — running at {r['cph']:,.0f} CPH vs {r['target_cph']:,.0f} target"
                )

    if r["dead_hours_total"] > 0:
        recoverable_dead = r["dead_hours_total"] * r["target_cph"]
        actions.append(
            f"Recovering {r['dead_hours_total']} dead hours at {r['target_cph']:,.0f} CPH "
            f"would add ~{recoverable_dead:,.0f} cases"
        )

    if actions:
        for i, action in enumerate(actions[:3], 1):
            fix_parts.append(f"({i}) {action}")
        p3 = "Focus on: " + ". ".join(fix_parts) + "."
    else:
        p3 = "No major action items identified — shift is performing near target."

    return f"{p1}\n\n{p2}\n\n{p3}"


def _build_plant_summary(hourly, shift_summary, overall, downtime):
    """Build the Plant Summary sheet with KPIs, shift comparison, loss breakdown, daily trend.

    Returns a dict with sub-tables (same structure as shift sheets for write_excel dispatch).
    """
    total_cases = hourly["total_cases"].sum()
    total_hours = hourly["total_hours"].sum()
    n_shift_days = hourly.groupby(["date_str", "shift"]).ngroups
    avg_cph = total_cases / (n_shift_days * SHIFT_HOURS) if n_shift_days > 0 else 0
    avg_avail, avg_perf, avg_qual, avg_oee = _aggregate_oee(hourly)
    util_pct, prod_hours, sched_hours, dead_count = _compute_utilization(hourly)
    n_days = hourly["date_str"].nunique()
    date_min = hourly["date"].min().strftime("%Y-%m-%d") if len(hourly) > 0 else ""
    date_max = hourly["date"].max().strftime("%Y-%m-%d") if len(hourly) > 0 else ""

    # Product-aware target
    benchmark_cph = hourly[hourly["total_hours"] >= 0.5]["cases_per_hour"].quantile(0.90) if len(hourly[hourly["total_hours"] >= 0.5]) > 0 else 0
    if "product_code" in hourly.columns:
        hourly_cp = hourly.copy()
        hourly_cp["_tgt"] = hourly_cp["product_code"].apply(get_target_cph)
        hourly_cp["_tgt"] = hourly_cp["_tgt"].fillna(benchmark_cph)
        product_target_cph = (hourly_cp["_tgt"] * hourly_cp["total_hours"]).sum() / total_hours if total_hours > 0 else benchmark_cph
        product_target_total = product_target_cph * n_shift_days * SHIFT_HOURS
    else:
        product_target_cph = benchmark_cph
        product_target_total = benchmark_cph * n_shift_days * SHIFT_HOURS

    has_downtime = downtime is not None and len(downtime.get("reasons_df", [])) > 0
    top_cause_str = ""
    if has_downtime:
        reasons_df = downtime["reasons_df"]
        actionable = reasons_df[~reasons_df["reason"].isin(EXCLUDE_REASONS)].sort_values("total_minutes", ascending=False)
        if len(actionable) > 0:
            top_cause_str = f"{actionable.iloc[0]['reason']} ({actionable.iloc[0]['total_hours']:.0f} hrs)"

    # --- KPIs ---
    kpi_rows = [
        {"Metric": "Overall OEE", "Value": f"{avg_oee:.1f}%"},
        {"Metric": "OEE Gap to 50% Target", "Value": f"{50.0 - avg_oee:.1f} points"},
        {"Metric": "Total Cases", "Value": f"{total_cases:,.0f}"},
        {"Metric": "Cases vs Target (Plant Std)",
         "Value": f"{total_cases - product_target_total:+,.0f} ({total_cases / product_target_total * 100:.0f}%)" if product_target_total > 0 else "N/A"},
        {"Metric": "Utilization",
         "Value": f"{util_pct:.0f}% ({prod_hours:.1f} of {sched_hours:.1f} hrs)"},
        {"Metric": "Top Downtime Cause", "Value": top_cause_str if top_cause_str else "N/A"},
    ]
    kpi_df = pd.DataFrame(kpi_rows)

    # --- Shift Comparison ---
    has_events = downtime is not None and len(downtime.get("events_df", [])) > 0
    events_df = downtime.get("events_df") if has_events else None
    comp_rows = []
    for (date_val, sname), s_data in hourly.groupby(["date_str", "shift"]):
        sa, sp, sq, soee = _aggregate_oee(s_data)
        s_cases = s_data["total_cases"].sum()
        s_hours = s_data["total_hours"].sum()
        s_cph = s_cases / SHIFT_HOURS
        # Product target for this shift-day
        if "product_code" in s_data.columns:
            s_data_cp = s_data.copy()
            s_data_cp["_tgt"] = s_data_cp["product_code"].apply(get_target_cph)
            s_bm = s_data[s_data["total_hours"] >= 0.5]["cases_per_hour"].quantile(0.90) if len(s_data[s_data["total_hours"] >= 0.5]) > 0 else 0
            s_data_cp["_tgt"] = s_data_cp["_tgt"].fillna(s_bm)
            s_target_cph = (s_data_cp["_tgt"] * s_data_cp["total_hours"]).sum() / s_hours if s_hours > 0 else s_bm
        else:
            s_target_cph = product_target_cph
        s_pct = s_cph / s_target_cph * 100 if s_target_cph > 0 else 0

        # Product — most common product_code for this date+shift, shown as pack type
        s_product = ""
        if "product_code" in s_data.columns:
            mode = s_data["product_code"].mode()
            if len(mode) > 0 and not pd.isna(mode.iloc[0]):
                raw = str(mode.iloc[0])
                s_product = _PRODUCT_CODE_TO_PACK.get(raw, _PRODUCT_CODE_TO_PACK.get(raw.upper(), raw))

        # Top Issue — from events_df filtered to this date AND shift
        # Photo events have start_time=NaT (no date); include them alongside
        # date-matched events so photo-extracted issues surface here.
        s_top_issue = ""
        s_top_issue_min = 0
        if events_df is not None and len(events_df) > 0:
            display_shift = _shift_display_name(sname)
            shift_ev = events_df[events_df["shift"] == display_shift].copy()
            if len(shift_ev) > 0 and "start_time" in shift_ev.columns:
                shift_ev["_date"] = shift_ev["start_time"].apply(
                    lambda x: x.strftime("%Y-%m-%d") if not pd.isna(x) and hasattr(x, "strftime") else "")
                # Include events matching this date + photo events (empty _date)
                day_shift_ev = shift_ev[(shift_ev["_date"] == date_val) | (shift_ev["_date"] == "")]
                day_shift_ev = day_shift_ev[~day_shift_ev["reason"].isin(EXCLUDE_REASONS)]
                if len(day_shift_ev) > 0:
                    cause_agg = day_shift_ev.groupby("reason")["duration_minutes"].sum()
                    top_cause = cause_agg.sort_values(ascending=False).head(1)
                    if not pd.isna(top_cause.index[0]):
                        s_top_issue = str(top_cause.index[0])
                    s_top_issue_min = round(top_cause.iloc[0], 0)

        comp_rows.append({
            "Date": date_val,
            "Shift": _shift_display_name(sname),
            "Product": s_product,
            "OEE %": round(soee, 1),
            "Cases": round(s_cases, 0),
            "CPH": round(s_cph, 0),
            "Target CPH": round(s_target_cph, 0),
            "% of Target": round(s_pct, 1),
            "Avail %": round(sa * 100, 1),
            "Perf %": round(sp * 100, 1),
            "Qual %": round(sq * 100, 1),
            "Top Issue": s_top_issue,
            "Top Issue Min": s_top_issue_min,
        })
    shift_comp_df = pd.DataFrame(comp_rows).sort_values(["Date", "Shift"]).reset_index(drop=True)

    # --- Loss Breakdown by Shift ---
    loss_rows = []
    for (date_val, sname), s_data in hourly.groupby(["date_str", "shift"]):
        sa, sp, sq, soee = _aggregate_oee(s_data)
        al = (1 - sa) * 100
        pl = (1 - sp) * 100
        ql = (1 - sq) * 100
        driver = "Availability" if al >= pl and al >= ql else "Performance" if pl >= ql else "Quality"
        s_cases = s_data["total_cases"].sum()
        if "cases_gap" in s_data.columns:
            cases_lost = s_data["cases_gap"].sum()
        else:
            cases_lost = 0
        loss_rows.append({
            "Date": date_val,
            "Shift": _shift_display_name(sname),
            "Avail Loss %": round(al, 1),
            "Perf Loss %": round(pl, 1),
            "Qual Loss %": round(ql, 1),
            "Primary Driver": driver,
            "Cases Lost": round(cases_lost, 0),
        })
    loss_df = pd.DataFrame(loss_rows).sort_values(["Date", "Shift"]).reset_index(drop=True)

    # --- Daily Trend ---
    # Compute weighted-average target CPH per row (inflation cancels in ratio)
    if "product_code" in hourly.columns:
        hourly_cp2 = hourly.copy()
        hourly_cp2["_tgt"] = hourly_cp2["product_code"].apply(get_target_cph)
        hourly_cp2["_tgt"] = hourly_cp2["_tgt"].fillna(benchmark_cph)
        hourly_cp2["_tgt_cases"] = hourly_cp2["_tgt"] * hourly_cp2["total_hours"]
    else:
        hourly_cp2 = hourly.copy()
        hourly_cp2["_tgt_cases"] = benchmark_cph * hourly_cp2["total_hours"]

    daily = (
        hourly_cp2.groupby("date_str")
        .agg(total_cases=("total_cases", "sum"),
             total_hours=("total_hours", "sum"),
             target_cases=("_tgt_cases", "sum"))
        .reset_index()
    )
    # Correct hours: n_shifts_per_day × SHIFT_HOURS
    n_shifts_daily = hourly.groupby("date_str")["shift"].nunique().rename("n_shifts")
    daily = daily.merge(n_shifts_daily, on="date_str", how="left")
    daily["actual_hours"] = daily["n_shifts"] * SHIFT_HOURS
    # Fix target_cases: weighted-avg target CPH × actual hours
    daily["target_cph"] = (daily["target_cases"] / daily["total_hours"].replace(0, np.nan)).fillna(0)
    daily["target_cases"] = daily["target_cph"] * daily["actual_hours"]
    # Weighted OEE
    shift_summary_cp = shift_summary.copy()
    shift_summary_cp["_w"] = shift_summary_cp["oee_pct"] * shift_summary_cp["total_hours"]
    daily_oee = (
        shift_summary_cp.groupby("date_str")
        .agg(_w=("_w", "sum"), _hrs=("total_hours", "sum"))
        .reset_index()
    )
    daily_oee["avg_oee"] = (daily_oee["_w"] / daily_oee["_hrs"].replace(0, np.nan)).fillna(0).round(1)
    daily = daily.merge(daily_oee[["date_str", "avg_oee"]], on="date_str", how="left")
    daily["cph"] = (daily["total_cases"] / daily["actual_hours"].replace(0, np.nan)).fillna(0).round(0)
    daily["target_cph"] = daily["target_cph"].round(0)
    daily["pct_target"] = (daily["total_cases"] / daily["target_cases"].replace(0, np.nan) * 100).fillna(0).round(1)
    daily = daily.sort_values("date_str")

    daily_trend_df = daily[["date_str", "actual_hours", "cph", "target_cph",
                            "total_cases", "target_cases", "pct_target", "avg_oee"]].copy()
    daily_trend_df.columns = ["Date", "Sched Hours", "Cases/Hr", "Target CPH",
                              "Actual Cases", "Target Cases", "% of Target", "OEE %"]
    daily_trend_df["Sched Hours"] = daily_trend_df["Sched Hours"].round(1)
    daily_trend_df["Actual Cases"] = daily_trend_df["Actual Cases"].round(0)
    daily_trend_df["Target Cases"] = daily_trend_df["Target Cases"].round(0)

    return {
        "title": "Plant Summary — Line 2 Flex",
        "subtitle": f"{date_min} to {date_max} · {n_days} day(s) analyzed",
        "kpis": kpi_df,
        "shift_comparison": shift_comp_df,
        "loss_breakdown": loss_df,
        "daily_trend": daily_trend_df,
    }


# ---------------------------------------------------------------------------
# Main Analysis
# ---------------------------------------------------------------------------
def analyze(hourly, shift_summary, overall, hour_avg, downtime=None,
            photo_findings=None):
    """Produce shift-centric analysis: Plant Summary + per-shift sheets + What to Focus On.

    Parameters
    ----------
    photo_findings : list of (name, findings_dict) tuples, optional
        Results from photo analysis (equipment issues, shift notes, production
        notes).  Surfaced as dedicated action items in the "What to Focus On"
        sheet.

    Returns dict where:
      - "Plant Summary" → dict with sub-tables (title, subtitle, kpis, shift_comparison, etc.)
      - "1st Shift" / "2nd Shift" / "3rd Shift" → dict with narrative + sub-tables
      - "What to Focus On" → DataFrame (unchanged)
    """
    results = {}

    # === CORE METRICS ===
    total_cases = hourly["total_cases"].sum()
    total_hours = hourly["total_hours"].sum()
    n_shift_days = hourly.groupby(["date_str", "shift"]).ngroups
    avg_cph = total_cases / (n_shift_days * SHIFT_HOURS) if n_shift_days > 0 else 0
    avg_avail, avg_perf, avg_qual, avg_oee = _aggregate_oee(hourly)

    good_hours = hourly[hourly["total_hours"] >= 0.5]
    benchmark_cph = good_hours["cases_per_hour"].quantile(0.90) if len(good_hours) > 0 else 0
    target_cph = benchmark_cph  # fallback for non-product-aware paths

    # Product-aware target: use plant standards when product is known
    if "product_code" in hourly.columns:
        hourly["_prod_target_cph"] = hourly["product_code"].apply(get_target_cph)
        hourly["_prod_target_cph"] = hourly["_prod_target_cph"].fillna(benchmark_cph)
        hourly["cases_gap"] = (hourly["_prod_target_cph"] - hourly["cases_per_hour"]).clip(lower=0) * hourly["total_hours"]
        product_target_total = (hourly["_prod_target_cph"] * hourly["total_hours"]).sum()
        product_target_cph_avg = product_target_total / total_hours if total_hours > 0 else benchmark_cph
        hourly.drop(columns=["_prod_target_cph"], inplace=True, errors="ignore")
    else:
        hourly["cases_gap"] = (benchmark_cph - hourly["cases_per_hour"]).clip(lower=0) * hourly["total_hours"]
        product_target_total = benchmark_cph * total_hours
        product_target_cph_avg = benchmark_cph

    total_cases_lost = hourly["cases_gap"].sum()

    date_min = hourly["date"].min().strftime("%Y-%m-%d")
    date_max = hourly["date"].max().strftime("%Y-%m-%d")
    n_days = hourly["date_str"].nunique()

    avail_loss = (1 - avg_avail) * 100
    perf_loss = (1 - avg_perf) * 100

    has_downtime = downtime is not None and len(downtime.get("reasons_df", [])) > 0
    has_events = has_downtime and len(downtime.get("events_df", [])) > 0

    shifts_sorted = overall.sort_values("oee_pct", ascending=False)
    top_shift = shifts_sorted.iloc[0]
    bot_shift = shifts_sorted.iloc[-1]

    # ===================================================================
    # SHEET 1: PLANT SUMMARY
    # ===================================================================
    results["Plant Summary"] = _build_plant_summary(hourly, shift_summary, overall, downtime)

    # ===================================================================
    # SHEETS 2-4: PER-SHIFT SHEETS
    # ===================================================================
    shift_order = ["1st (7a-3p)", "2nd (3p-11p)", "3rd (11p-7a)"]
    actual_shifts = [
        s for s in hourly["shift"].dropna().tolist()
        if str(s).strip() and str(s).lower() != "nan"
    ]
    actual_shifts = list(dict.fromkeys(actual_shifts))
    if not any(s in actual_shifts for s in shift_order):
        shift_order = sorted(actual_shifts, key=lambda x: str(x))

    for shift_name in shift_order:
        if shift_name not in actual_shifts:
            continue
        shift_data = _compute_shift_data(
            shift_name, hourly, shift_summary, overall, downtime,
            avg_oee, avg_cph
        )
        if shift_data is not None:
            narrative = _build_shift_narrative(shift_data)
            display_name = _shift_display_name(shift_name)
            results[display_name] = {
                "narrative": narrative,
                "scorecard": shift_data["scorecard"],
                "loss_breakdown": shift_data["loss_breakdown"],
                "downtime_causes": shift_data["downtime_causes"],
                "hour_by_hour": shift_data["hour_by_hour"],
                "dead_hours": shift_data["dead_hours"],
                "worst_hours": shift_data["worst_hours"],
                "raw": shift_data["raw"],
            }

    # ===================================================================
    # SHEET 5: WHAT TO FOCUS ON
    # ===================================================================
    # Old tabs (Executive Summary, Shift Deep Dives, Shift Comparison,
    # Loss Breakdown, Dead Hours, Shift Downtime, Downtime Pareto,
    # Fault Summary/Detail, Worst Hours, Daily Trend, Shift x Day OEE)
    # are now absorbed into Plant Summary + per-shift sheets above.

    OEE_TARGET = 50.0  # Plant target OEE %

    recs = []
    priority = 1

    # -------------------------------------------------------------------
    # Collect shift summaries for cross-comparison
    # -------------------------------------------------------------------
    _DISPLAY_SHIFTS = ["1st Shift", "2nd Shift", "3rd Shift"]
    shift_raws = {}
    for sname in _DISPLAY_SHIFTS:
        if sname in results and isinstance(results[sname], dict) and "raw" in results[sname]:
            shift_raws[sname] = results[sname]["raw"]
    active_shifts = list(shift_raws.keys())
    n_active_shifts = len(active_shifts)

    # Build unified cause table: {cause: {shift: (total_min, events)}}
    cause_by_shift = {}
    for sname, raw in shift_raws.items():
        for cause_name, total_min, events in raw.get("downtime_causes", []):
            if cause_name not in cause_by_shift:
                cause_by_shift[cause_name] = {}
            cause_by_shift[cause_name][sname] = (total_min, int(events))

    # Rank causes by total minutes across all shifts
    cause_totals = []
    for cause_name, shift_dict in cause_by_shift.items():
        c_total_min = sum(v[0] for v in shift_dict.values())
        c_total_events = sum(v[1] for v in shift_dict.values())
        n_shifts_hit = len(shift_dict)
        cause_totals.append((cause_name, c_total_min, c_total_events, n_shifts_hit, shift_dict))
    cause_totals.sort(key=lambda x: x[1], reverse=True)

    # Dead hours per shift
    dead_by_shift = {}
    for sname, raw in shift_raws.items():
        dead_by_shift[sname] = {
            "total": raw.get("dead_hours_total", 0),
            "causes": raw.get("dead_hour_causes", []),
        }
    total_dead = sum(d["total"] for d in dead_by_shift.values())

    # -------------------------------------------------------------------
    # ACTION 1: #1 Equipment Issue — cross-shift comparison
    # -------------------------------------------------------------------
    if cause_totals:
        top = cause_totals[0]
        c_name, c_min, c_events, c_n_shifts, c_shifts = top
        c_avg = c_min / c_events if c_events > 0 else 0
        worst_shift_entry = max(c_shifts.items(), key=lambda x: x[1][0])
        best_shift_entry = min(c_shifts.items(), key=lambda x: x[1][0])

        shift_parts = []
        for sname in active_shifts:
            if sname in c_shifts:
                sm, se = c_shifts[sname]
                shift_parts.append(f"{sname}: {sm:.0f} min / {se} events")
            else:
                shift_parts.append(f"{sname}: 0 min")
        shift_breakdown = "; ".join(shift_parts)

        if c_n_shifts >= n_active_shifts and n_active_shifts > 1:
            pattern_note = f"Hits all {n_active_shifts} shifts — systemic issue, needs PM/reliability review."
        elif c_n_shifts == 1:
            pattern_note = f"Only affects {worst_shift_entry[0]} — investigate operating practice, staffing, or product mix on that shift."
        else:
            pattern_note = f"Affects {c_n_shifts} of {n_active_shifts} shifts — investigate why some shifts avoid it."

        recoverable = (c_min / 60) * 0.5 * avg_cph

        recs.append({
            "Priority": priority,
            "Finding": (
                f"#1 equipment issue: {c_name} — {c_min:.0f} min / {c_events} events "
                f"(avg {c_avg:.0f} min/event). {worst_shift_entry[0]} bears {worst_shift_entry[1][0]:.0f} min."
            ),
            "The Work": (
                f"By shift: {shift_breakdown}. "
                f"{pattern_note} "
                f"50% reduction recovers ~{c_min/2/60:.0f} hrs = ~{recoverable:,.0f} cases."
            ),
            "Step 1": f"Pull 2 weeks of {c_name} events. Sort by duration — find the top 10 longest stops.",
            "Step 2": (
                f"Focus on {worst_shift_entry[0]}: {worst_shift_entry[1][0]:.0f} min vs "
                f"{best_shift_entry[1][0]:.0f} min on {best_shift_entry[0]}. "
                f"What's different? Crew, product, time of shift?"
            ),
            "Step 3": f"Walk the line during the next {c_name} event on {worst_shift_entry[0]}. Time every step: detect, respond, diagnose, fix, restart.",
            "Step 4": "5-Why on top 3 failure modes with maintenance. If systemic: PM task. If shift-specific: SOP or training gap.",
            "Step 5": f"Target: {worst_shift_entry[0]} matches {best_shift_entry[0]} within 4 weeks. Track weekly.",
        })
        priority += 1

    # -------------------------------------------------------------------
    # ACTION 2: Shift-Specific Issues — what's different per shift
    # -------------------------------------------------------------------
    shift_specific_items = []
    for c_name, c_min, c_events, c_n_shifts, c_shifts in cause_totals[1:6]:
        if c_min < 30:
            continue
        for sname, (sm, se) in c_shifts.items():
            if c_min > 0 and sm / c_min > 0.70 and c_n_shifts < n_active_shifts:
                shift_specific_items.append((c_name, sname, sm, se))

    if shift_specific_items:
        findings = [f"{sname}: {cname} ({sm:.0f} min / {se} events)"
                    for cname, sname, sm, se in shift_specific_items[:4]]
        recs.append({
            "Priority": priority,
            "Finding": "Shift-specific equipment issues — different shifts, different problems",
            "The Work": (
                "; ".join(findings) + ". "
                "These causes concentrate on one shift (>70% of their total time). "
                "Different equipment per shift = different root causes. Could be operating practice, product mix, or crew experience."
            ),
            "Step 1": "For each shift-specific cause: is it the same product? Same time of shift? Same crew running that station?",
            "Step 2": "Compare SOPs between shifts. Are centerlines documented and posted?",
            "Step 3": "If product-related: check if that shift runs more of the difficult product.",
            "Step 4": "If crew-related: targeted training on the specific equipment, not generic.",
            "Step 5": "Track weekly by shift. If the cause follows a person or product, the fix is different than if it follows the clock.",
        })
        priority += 1

    # -------------------------------------------------------------------
    # ACTION 3: Dead Hours — cross-shift pattern
    # -------------------------------------------------------------------
    if total_dead > 2 and n_active_shifts >= 1:
        worst_dead = max(dead_by_shift.items(), key=lambda x: x[1]["total"])
        best_dead = min(dead_by_shift.items(), key=lambda x: x[1]["total"])

        shift_dead_details = []
        for sname in active_shifts:
            d = dead_by_shift.get(sname, {"total": 0, "causes": []})
            causes_str = "; ".join(d["causes"][:2]) if d["causes"] else "no cause identified"
            shift_dead_details.append(f"{sname}: {d['total']} dead hrs ({causes_str})")

        # Are the causes the same or different across shifts?
        all_equip = []
        for d in dead_by_shift.values():
            for c in d.get("causes", []):
                equip = c.split(":")[0].strip() if ":" in c else c.strip()
                if equip:
                    all_equip.append(equip)
        unique_equip = list(dict.fromkeys(all_equip))
        if len(unique_equip) <= 1 and unique_equip:
            cause_pattern = f"Same root cause across shifts ({unique_equip[0]}) — systemic reliability issue."
        elif len(unique_equip) > 1:
            cause_pattern = f"Different causes per shift ({', '.join(unique_equip[:3])}) — different problems need different fixes."
        else:
            cause_pattern = "Causes not identified in machine data — needs investigation."

        recoverable_dead = total_dead * avg_cph

        recs.append({
            "Priority": priority,
            "Finding": (
                f"{total_dead} dead hours across shifts — "
                f"{worst_dead[0]} worst ({worst_dead[1]['total']} hrs), "
                f"{best_dead[0]} best ({best_dead[1]['total']} hrs)"
            ),
            "The Work": (
                "; ".join(shift_dead_details) + ". "
                f"{cause_pattern} "
                f"Recovering even half = ~{recoverable_dead * 0.5:,.0f} cases."
            ),
            "Step 1": f"Start with {worst_dead[0]}: what happened during those {worst_dead[1]['total']} hrs? Check shift passdown, operator notes.",
            "Step 2": f"If {best_dead[0]} avoids dead hours — what do they do differently? Product, startup routine, crew?",
            "Step 3": "For multi-hour blocks: was it a breakdown, a changeover that ran long, or planned downtime miscoded?",
            "Step 4": "Expected duration vs actual: a 2-hour job that took 4 hours is a root cause question.",
            "Step 5": f"Target: reduce dead hours from {total_dead} to {max(total_dead // 2, 1)} over next 4 weeks.",
        })
        priority += 1

    # -------------------------------------------------------------------
    # ACTION 4: Shift Performance Gap — with root cause
    # -------------------------------------------------------------------
    if n_active_shifts >= 2:
        shifts_by_oee = sorted(shift_raws.items(), key=lambda x: x[1]["oee"], reverse=True)
        best_name, best_raw = shifts_by_oee[0]
        worst_name, worst_raw = shifts_by_oee[-1]
        gap = best_raw["oee"] - worst_raw["oee"]

        if gap > 3:
            best_primary = best_raw["primary_loss"]
            worst_primary = worst_raw["primary_loss"]
            best_top = best_raw.get("downtime_causes", [])
            worst_top = worst_raw.get("downtime_causes", [])
            best_top_cause = best_top[0] if best_top else None
            worst_top_cause = worst_top[0] if worst_top else None

            explain_parts = [
                (f"{best_name}: {best_raw['oee']:.1f}% OEE, {best_raw['cph']:,.0f} CPH, "
                 f"{best_raw['cases']:,.0f} cases, primary loss = {best_primary}"),
                (f"{worst_name}: {worst_raw['oee']:.1f}% OEE, {worst_raw['cph']:,.0f} CPH, "
                 f"{worst_raw['cases']:,.0f} cases, primary loss = {worst_primary}"),
            ]
            if worst_top_cause:
                explain_parts.append(
                    f"{worst_name}'s #1 downtime: {worst_top_cause[0]} "
                    f"({worst_top_cause[1]:.0f} min / {worst_top_cause[2]} events)")
            if best_top_cause:
                explain_parts.append(
                    f"{best_name}'s #1 downtime: {best_top_cause[0]} "
                    f"({best_top_cause[1]:.0f} min / {best_top_cause[2]} events)")

            dead_worst = worst_raw.get("dead_hours_total", 0)
            dead_best = best_raw.get("dead_hours_total", 0)
            if dead_worst > dead_best:
                explain_parts.append(
                    f"Dead hours: {worst_name} has {dead_worst} vs {best_name}'s {dead_best} "
                    f"— {dead_worst - dead_best} extra hrs of zero production.")

            cases_if_matched = gap / 100 * worst_raw.get("target_cph", avg_cph) * worst_raw["n_days"] * SHIFT_HOURS

            if worst_primary != best_primary:
                finding_text = (
                    f"{worst_name} trails {best_name} by {gap:.1f} OEE points — "
                    f"different root cause: {worst_primary} vs {best_primary}")
            else:
                finding_text = (
                    f"{worst_name} trails {best_name} by {gap:.1f} OEE points — "
                    f"same loss type ({worst_primary}) but worse execution")

            step2 = (
                f"{worst_name}'s top issue is {worst_top_cause[0]} — does {best_name} see the same equipment? "
                f"If not, it's a shift-specific problem."
                if worst_top_cause else
                f"No downtime data for {worst_name} — pull MES event logs manually for that shift."
            )

            recs.append({
                "Priority": priority,
                "Finding": finding_text,
                "The Work": ". ".join(explain_parts) + f". If {worst_name} matched {best_name}: ~{cases_if_matched:,.0f} more cases.",
                "Step 1": f"Compare {worst_name} and {best_name} shift deep-dive tabs side by side — hour-by-hour patterns.",
                "Step 2": step2,
                "Step 3": f"Shadow both shifts for startup/changeover. Time the first 60 min of each.",
                "Step 4": "Build a best-practice checklist from what the better shift does. Pilot on the lagging shift.",
                "Step 5": f"Target: close {gap/2:.0f} OEE points within 4 weeks. Re-run analysis to verify.",
            })
            priority += 1

    # -------------------------------------------------------------------
    # ACTION 5: Startup Loss — per-shift detail
    # -------------------------------------------------------------------
    if "shift_hour" in hourly.columns:
        startup_by_shift = {}
        for sname in actual_shifts:
            sh_data = hourly[hourly["shift"] == sname]
            if len(sh_data) == 0:
                continue
            min_hr = sh_data["shift_hour"].min()
            first_hr = sh_data[sh_data["shift_hour"] == min_hr]
            other_hrs = sh_data[sh_data["shift_hour"] != min_hr]
            if len(first_hr) > 0 and len(other_hrs) > 0:
                first_oee = (_weighted_mean(first_hr["oee_pct"], first_hr["total_hours"])
                             if "total_hours" in first_hr.columns else first_hr["oee_pct"].mean())
                other_oee = (_weighted_mean(other_hrs["oee_pct"], other_hrs["total_hours"])
                             if "total_hours" in other_hrs.columns else other_hrs["oee_pct"].mean())
                display = _shift_display_name(sname)
                startup_by_shift[display] = {
                    "first_hr_oee": first_oee,
                    "other_oee": other_oee,
                    "gap": other_oee - first_oee,
                }

        bad_startups = {s: v for s, v in startup_by_shift.items() if v["gap"] > 5}
        if bad_startups:
            worst_startup = max(bad_startups.items(), key=lambda x: x[1]["gap"])
            best_startup = min(startup_by_shift.items(), key=lambda x: x[1]["gap"])

            detail_parts = [f"{sname}: 1st hr {v['first_hr_oee']:.0f}% vs rest {v['other_oee']:.0f}% (gap: {v['gap']:.0f} pts)"
                           for sname, v in sorted(startup_by_shift.items(), key=lambda x: x[1]["gap"], reverse=True)]

            recs.append({
                "Priority": priority,
                "Finding": (
                    f"Startup loss worst on {worst_startup[0]} — "
                    f"{worst_startup[1]['gap']:.0f} pt OEE gap between 1st hour and rest of shift"),
                "The Work": (
                    ". ".join(detail_parts) + ". "
                    f"{best_startup[0]} has the smallest gap ({best_startup[1]['gap']:.0f} pts) — their startup routine is the benchmark."),
                "Step 1": f"Observe {worst_startup[0]} startup: time from bell to first good case. Document every step and wait.",
                "Step 2": f"Compare to {best_startup[0]}: what's staged before the shift starts? Materials, settings, passdown?",
                "Step 3": "Build startup checklist from best-shift practice. Materials staged, settings verified, passdown under 10 min.",
                "Step 4": f"Pilot on {worst_startup[0]} for 1 week. Measure 1st-hour OEE before and after.",
                "Step 5": f"Target: {worst_startup[0]} 1st-hour OEE from {worst_startup[1]['first_hr_oee']:.0f}% to {worst_startup[1]['first_hr_oee'] + worst_startup[1]['gap']/2:.0f}% within 3 weeks.",
            })
            priority += 1

    # -------------------------------------------------------------------
    # ACTION 6: Uncoded Events — per-shift accountability
    # -------------------------------------------------------------------
    if has_events:
        events_df_all = downtime.get("events_df")
        if events_df_all is not None and len(events_df_all) > 0:
            unassigned_by_shift = {}
            for sname in actual_shifts:
                display = _shift_display_name(sname)
                shift_ev = events_df_all[events_df_all["shift"] == sname]
                if len(shift_ev) == 0:
                    continue
                uncoded = shift_ev[shift_ev["reason"].isin(["Unassigned", "Unknown"])]
                total_ev = len(shift_ev)
                uncoded_min = uncoded["duration_minutes"].sum() if len(uncoded) > 0 else 0
                uncoded_ev = len(uncoded)
                unassigned_by_shift[display] = {
                    "events": uncoded_ev,
                    "minutes": uncoded_min,
                    "pct": uncoded_ev / total_ev * 100 if total_ev > 0 else 0,
                    "total_events": total_ev,
                }

            total_uncoded_min = sum(v["minutes"] for v in unassigned_by_shift.values())
            if total_uncoded_min > 60:
                worst_uncoded = max(unassigned_by_shift.items(), key=lambda x: x[1]["minutes"])
                best_uncoded = min(unassigned_by_shift.items(), key=lambda x: x[1]["pct"])

                detail_parts = [f"{sname}: {v['events']} uncoded / {v['minutes']:.0f} min ({v['pct']:.0f}% of events)"
                               for sname, v in sorted(unassigned_by_shift.items(), key=lambda x: x[1]["minutes"], reverse=True)]

                recs.append({
                    "Priority": priority,
                    "Finding": (
                        f"{total_uncoded_min:.0f} min uncoded — {worst_uncoded[0]} worst "
                        f"({worst_uncoded[1]['minutes']:.0f} min / {worst_uncoded[1]['pct']:.0f}% uncoded)"),
                    "The Work": (
                        "; ".join(detail_parts) + ". "
                        f"These {total_uncoded_min:.0f} minutes could be hiding the real #1 cause. "
                        f"{best_uncoded[0]} has the best coding rate ({best_uncoded[1]['pct']:.0f}% uncoded)."),
                    "Step 1": f"Review MES reason code tree with {worst_uncoded[0]} leads. Are codes confusing or missing common causes?",
                    "Step 2": "Simplify: 15-20 actionable codes. Merge duplicates, drop obsolete.",
                    "Step 3": f"Coach {worst_uncoded[0]} supervisors: 'If you can't code it, write a note. No blanks.'",
                    "Step 4": "Weekly audit: pull uncoded events by shift. Review with shift leads. Code retroactively.",
                    "Step 5": f"Target: all shifts below 5% uncoded. Currently {worst_uncoded[0]} is at {worst_uncoded[1]['pct']:.0f}%.",
                })
                priority += 1

    # -------------------------------------------------------------------
    # FALLBACK: No downtime events — OEE/dead-hour comparison only
    # -------------------------------------------------------------------
    if not cause_totals and n_active_shifts >= 2:
        comparison_parts = []
        for sname, raw in sorted(shift_raws.items(), key=lambda x: x[1]["oee"], reverse=True):
            comparison_parts.append(
                f"{sname}: {raw['oee']:.1f}% OEE (Avail {raw['avail']:.0%}, Perf {raw['perf']:.0%}), "
                f"{raw['cases']:,.0f} cases, {raw.get('dead_hours_total', 0)} dead hrs")
        recs.append({
            "Priority": priority,
            "Finding": "Shift comparison (no event data available for equipment-level breakdown)",
            "The Work": ". ".join(comparison_parts) + ". Upload downtime event data to see equipment-level Pareto by shift.",
            "Step 1": "The shift deep-dive tabs show OEE, cases, dead hours per shift — compare those.",
            "Step 2": "To get equipment-level action items, export MES downtime events and include with the OEE data.",
            "Step 3": "Focus on the shift with the most dead hours — that's the biggest opportunity without event data.",
            "Step 4": "Talk to operators on each shift about what stops the line. Their input fills the data gap.",
            "Step 5": "Once event data is available, re-run for equipment Pareto by shift.",
        })
        priority += 1

    # -------------------------------------------------------------------
    # PHOTO FINDINGS
    # -------------------------------------------------------------------
    if photo_findings:
        photo_issues = []
        photo_notes = []
        for pname, findings in photo_findings:
            if not isinstance(findings, dict) or "error" in findings:
                continue
            for issue in findings.get("issues", []):
                equip = issue.get("equipment", "Unknown")
                desc = issue.get("description", "")
                dur = issue.get("duration_minutes")
                shift = issue.get("shift", "")
                severity = issue.get("severity", "")
                dur_str = f" ({dur} min)" if dur else ""
                shift_str = f" [{shift}]" if shift else ""
                sev_str = f" — {severity}" if severity else ""
                photo_issues.append(f"{equip}: {desc}{dur_str}{shift_str}{sev_str}")
            for note in findings.get("shift_notes", []):
                photo_notes.append(note)
            for note in findings.get("production_notes", []):
                photo_notes.append(note)

        if photo_issues:
            issues_text = "; ".join(photo_issues[:8])
            notes_text = (" | Notes: " + "; ".join(photo_notes[:4])) if photo_notes else ""
            recs.append({
                "Priority": priority,
                "Finding": f"Context photos flagged {len(photo_issues)} issue(s)",
                "The Work": (
                    f"Photo-extracted findings: {issues_text}.{notes_text} "
                    f"Cross-reference with machine data above — same events or additional issues not in MES?"
                ),
                "Step 1": "Do these match machine-data downtime causes? If yes: confirms accuracy.",
                "Step 2": "If new issues: add to downtime tracking so they appear in future Pareto.",
                "Step 3": "Use photo context (work orders, notes) to understand WHY, not just WHAT.",
                "Step 4": "For multi-hour blocks with photo context: compare expected vs actual duration.",
                "Step 5": "Continue uploading context photos — they fill gaps machine data can't capture.",
            })
            priority += 1

    # -------------------------------------------------------------------
    # BOTTOM LINE — shift-aware
    # -------------------------------------------------------------------
    lever_parts = []
    if cause_totals:
        top = cause_totals[0]
        worst_s = max(top[4].items(), key=lambda x: x[1][0])
        lever_parts.append(f"#1 equipment: {top[0]} ({top[1]:.0f} min, worst on {worst_s[0]})")
    if n_active_shifts >= 2:
        shifts_by_oee = sorted(shift_raws.items(), key=lambda x: x[1]["oee"], reverse=True)
        gap = shifts_by_oee[0][1]["oee"] - shifts_by_oee[-1][1]["oee"]
        if gap > 3:
            lever_parts.append(f"Shift gap: {shifts_by_oee[-1][0]} trails by {gap:.1f} pts")
    if total_dead > 2:
        lever_parts.append(f"Dead hours: {total_dead} hrs recoverable")

    recs.append({
        "Priority": priority,
        "Finding": f"Bottom line: {total_cases_lost:,.0f} cases lost over {n_days} days ({total_cases_lost/n_days:,.0f}/day)",
        "The Work": (
            f"Actual: {total_cases:,.0f} cases at {avg_cph:,.0f} CPH. "
            + (f"Top levers: {'; '.join(lever_parts)}. " if lever_parts else "")
            + f"Every 1 OEE point = ~{total_cases/avg_oee*1/n_days:,.0f} cases/day." if avg_oee > 0 else ""
        ),
        "Step 1": "Fix the #1 equipment cause and close the shift gap — that's 80% of the opportunity.",
        "Step 2": "Don't try to fix everything. Pick 2 items from above. Assign owners and dates.",
        "Step 3": "Re-run this analysis in 2-4 weeks with fresh data. Track which shift improved.",
        "Step 4": f"Track OEE weekly. Current: {avg_oee:.1f}%. Target: {OEE_TARGET:.0f}%.",
        "Step 5": f"A 10% OEE improvement = ~{total_cases_lost/n_days*0.1:,.0f} more cases/day." if n_days > 0 else "Track progress weekly.",
    })

    results["What to Focus On"] = pd.DataFrame(recs)

    return results


# ---------------------------------------------------------------------------
# Excel Writer
# ---------------------------------------------------------------------------
def _write_df_table(ws, df, start_row, header_fmt, formats=None):
    """Write a DataFrame as a table with headers. Returns next available row."""
    if len(df) == 0:
        return start_row

    # Headers
    for col_num, col_name in enumerate(df.columns):
        ws.write(start_row, col_num, col_name, header_fmt)

    # Data rows
    for row_num in range(len(df)):
        for col_num in range(len(df.columns)):
            ws.write(start_row + 1 + row_num, col_num, df.iloc[row_num, col_num])

    return start_row + 1 + len(df) + 2  # +2 blank rows after table


def _write_plant_summary_sheet(workbook, writer, data, formats):
    """Write the Plant Summary sheet with multiple sections."""
    ws_name = "Plant Summary"
    ws = workbook.add_worksheet(ws_name)

    title_fmt = formats["title"]
    subtitle_fmt = formats["subtitle"]
    section_fmt = formats["section"]
    header_fmt = formats["header"]
    narrative_fmt = formats.get("narrative", subtitle_fmt)

    row = 0
    ws.write(row, 0, data.get("title", "Plant Summary"), title_fmt)
    row += 1
    ws.write(row, 0, data.get("subtitle", ""), subtitle_fmt)
    row += 2

    # --- KPIs ---
    ws.write(row, 0, "Plant KPIs", section_fmt)
    row += 1
    kpis = data.get("kpis", pd.DataFrame())
    row = _write_df_table(ws, kpis, row, header_fmt)

    # --- Shift Comparison ---
    ws.write(row, 0, "Shift Comparison", section_fmt)
    row += 1
    comp = data.get("shift_comparison", pd.DataFrame())
    comp_start = row
    row = _write_df_table(ws, comp, row, header_fmt)
    # OEE color scale
    if "OEE %" in comp.columns and len(comp) > 0:
        col_idx = list(comp.columns).index("OEE %")
        ws.conditional_format(comp_start + 1, col_idx, comp_start + len(comp), col_idx, {
            "type": "3_color_scale",
            "min_color": "#F8696B", "mid_color": "#FFEB84", "max_color": "#63BE7B",
        })

    # --- Loss Breakdown by Shift ---
    ws.write(row, 0, "Loss Breakdown by Shift", section_fmt)
    row += 1
    loss = data.get("loss_breakdown", pd.DataFrame())
    row = _write_df_table(ws, loss, row, header_fmt)

    # --- Daily Trend ---
    ws.write(row, 0, "Daily Trend", section_fmt)
    row += 1
    daily = data.get("daily_trend", pd.DataFrame())
    daily_start = row
    row = _write_df_table(ws, daily, row, header_fmt)
    if "OEE %" in daily.columns and len(daily) > 0:
        col_idx = list(daily.columns).index("OEE %")
        ws.conditional_format(daily_start + 1, col_idx, daily_start + len(daily), col_idx, {
            "type": "3_color_scale",
            "min_color": "#F8696B", "mid_color": "#FFEB84", "max_color": "#63BE7B",
        })

    # Column widths
    ws.set_column(0, 0, 28)
    ws.set_column(1, 1, 18)
    for c in range(2, 10):
        ws.set_column(c, c, 14)

    return ws_name


def _write_shift_sheet(workbook, writer, sheet_name, data, formats):
    """Write a per-shift sheet with narrative, tables, and charts."""
    ws = workbook.add_worksheet(sheet_name[:31])

    title_fmt = formats["title"]
    subtitle_fmt = formats["subtitle"]
    section_fmt = formats["section"]
    header_fmt = formats["header"]
    narrative_fmt = formats["narrative"]

    row = 0

    # --- A. NARRATIVE ---
    ws.write(row, 0, sheet_name, title_fmt)
    row += 1
    ws.write(row, 0, f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}", subtitle_fmt)
    row += 2

    narrative = data.get("narrative", "")
    if narrative:
        # Merge A-H for narrative text
        ws.merge_range(row, 0, row + 6, 7, narrative, narrative_fmt)
        row += 9

    # --- B. SCORECARD ---
    ws.write(row, 0, "Scorecard", section_fmt)
    row += 1
    scorecard = data.get("scorecard", pd.DataFrame())
    sc_start = row
    row = _write_df_table(ws, scorecard, row, header_fmt)
    if "OEE %" in scorecard.columns and len(scorecard) > 0:
        col_idx = list(scorecard.columns).index("OEE %")
        ws.conditional_format(sc_start + 1, col_idx, sc_start + len(scorecard), col_idx, {
            "type": "3_color_scale",
            "min_color": "#F8696B", "mid_color": "#FFEB84", "max_color": "#63BE7B",
        })

    # --- C. LOSS BREAKDOWN + PIE CHART ---
    ws.write(row, 0, "Loss Breakdown", section_fmt)
    row += 1
    loss = data.get("loss_breakdown", pd.DataFrame())
    loss_start = row
    row = _write_df_table(ws, loss, row, header_fmt)

    # Pie chart: A/P/Q loss split (use last row or aggregate)
    if len(loss) > 0:
        # Build summary for pie: total losses across all days
        raw = data.get("raw", {})
        avail_l = raw.get("avail_loss", 0)
        perf_l = raw.get("perf_loss", 0)
        qual_l = raw.get("qual_loss", 0)

        # Write pie data in a helper block
        pie_row = row
        ws.write(pie_row, 0, "Loss Type")
        ws.write(pie_row, 1, "Loss %")
        ws.write(pie_row + 1, 0, "Availability")
        ws.write(pie_row + 1, 1, round(avail_l, 1))
        ws.write(pie_row + 2, 0, "Performance")
        ws.write(pie_row + 2, 1, round(perf_l, 1))
        ws.write(pie_row + 3, 0, "Quality")
        ws.write(pie_row + 3, 1, round(qual_l, 1))

        chart = workbook.add_chart({"type": "pie"})
        chart.add_series({
            "name": "Loss Breakdown",
            "categories": [sheet_name[:31], pie_row + 1, 0, pie_row + 3, 0],
            "values": [sheet_name[:31], pie_row + 1, 1, pie_row + 3, 1],
            "points": [
                {"fill": {"color": "#E74C3C"}},  # red = availability
                {"fill": {"color": "#F39C12"}},  # orange = performance
                {"fill": {"color": "#3498DB"}},  # blue = quality
            ],
        })
        chart.set_title({"name": f"{sheet_name} — Where is OEE Lost?"})
        chart.set_size({"width": 400, "height": 300})
        ws.insert_chart(4, pie_row, chart)  # col E
        row = pie_row + 5

    row += 2

    # --- D. DOWNTIME CAUSES + BAR CHART ---
    ws.write(row, 0, "Downtime Causes (Top 10)", section_fmt)
    row += 1
    causes = data.get("downtime_causes", pd.DataFrame())
    causes_start = row
    row = _write_df_table(ws, causes, row, header_fmt)

    if len(causes) > 0 and "Total Min" in causes.columns:
        n = len(causes)
        cause_col = list(causes.columns).index("Cause")
        min_col = list(causes.columns).index("Total Min")

        chart = workbook.add_chart({"type": "bar"})
        chart.add_series({
            "name": "Total Minutes",
            "categories": [sheet_name[:31], causes_start + 1, cause_col,
                          causes_start + n, cause_col],
            "values": [sheet_name[:31], causes_start + 1, min_col,
                       causes_start + n, min_col],
            "fill": {"color": "#1B2A4A"},
        })
        chart.set_title({"name": f"{sheet_name} — Top Downtime Causes"})
        chart.set_y_axis({"reverse": True})
        chart.set_size({"width": 500, "height": 350})
        chart.set_legend({"none": True})
        ws.insert_chart(4, row, chart)  # col E
        row += 2

    row += 2

    # --- E. HOUR-BY-HOUR ---
    ws.write(row, 0, "Hour-by-Hour Detail", section_fmt)
    row += 1
    hbh = data.get("hour_by_hour", pd.DataFrame())
    hbh_start = row
    row = _write_df_table(ws, hbh, row, header_fmt)
    if "OEE %" in hbh.columns and len(hbh) > 0:
        col_idx = list(hbh.columns).index("OEE %")
        ws.conditional_format(hbh_start + 1, col_idx, hbh_start + len(hbh), col_idx, {
            "type": "3_color_scale",
            "min_color": "#F8696B", "mid_color": "#FFEB84", "max_color": "#63BE7B",
        })

    # --- F. DEAD HOURS ---
    dead = data.get("dead_hours", pd.DataFrame())
    if len(dead) > 0:
        ws.write(row, 0, "Dead Hours", section_fmt)
        row += 1
        row = _write_df_table(ws, dead, row, header_fmt)

    # --- G. WORST HOURS ---
    worst = data.get("worst_hours", pd.DataFrame())
    if len(worst) > 0:
        ws.write(row, 0, "Worst Hours (Top 10)", section_fmt)
        row += 1
        wh_start = row
        row = _write_df_table(ws, worst, row, header_fmt)
        if "OEE %" in worst.columns and len(worst) > 0:
            col_idx = list(worst.columns).index("OEE %")
            ws.conditional_format(wh_start + 1, col_idx, wh_start + len(worst), col_idx, {
                "type": "3_color_scale",
                "min_color": "#F8696B", "mid_color": "#FFEB84", "max_color": "#63BE7B",
            })

    # Column widths
    ws.set_column(0, 0, 16)   # Date / labels
    ws.set_column(1, 1, 14)   # secondary
    ws.set_column(2, 2, 14)
    ws.set_column(3, 3, 14)
    ws.set_column(4, 4, 14)
    ws.set_column(5, 5, 14)
    ws.set_column(6, 6, 50)   # What Happened / Cause
    ws.set_column(7, 7, 30)   # Product

    return sheet_name[:31]


def write_excel(results, output_path):
    """Write analysis results to Excel. Handles both dict-based (shift sheets,
    Plant Summary) and DataFrame-based (What to Focus On) entries."""
    print(f"Writing: {output_path}")

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        workbook = writer.book

        formats = {
            "header": workbook.add_format({
                "bold": True, "bg_color": "#1B2A4A", "font_color": "white",
                "border": 1, "text_wrap": True, "valign": "vcenter", "font_size": 11
            }),
            "title": workbook.add_format({"bold": True, "font_size": 14, "font_color": "#1B2A4A"}),
            "subtitle": workbook.add_format({"italic": True, "font_size": 10, "font_color": "#666666"}),
            "section": workbook.add_format({
                "bold": True, "font_size": 11, "font_color": "#1B2A4A",
                "bottom": 2, "bottom_color": "#1B2A4A"
            }),
            "narrative": workbook.add_format({
                "text_wrap": True, "valign": "top", "font_size": 10,
                "font_color": "#333333",
            }),
        }

        # Sheet order: Plant Summary, shifts (1st, 2nd, 3rd), What to Focus On
        sheet_order = [
            "Plant Summary",
            "1st Shift", "2nd Shift", "3rd Shift",
            "What to Focus On",
        ]

        first_ws_name = None

        for sheet_name in sheet_order:
            if sheet_name not in results:
                continue

            data = results[sheet_name]

            if sheet_name == "Plant Summary" and isinstance(data, dict):
                ws_name = _write_plant_summary_sheet(workbook, writer, data, formats)
                if first_ws_name is None:
                    first_ws_name = ws_name

            elif isinstance(data, dict) and "narrative" in data:
                # Per-shift sheet
                ws_name = _write_shift_sheet(workbook, writer, sheet_name, data, formats)
                if first_ws_name is None:
                    first_ws_name = ws_name

            elif isinstance(data, pd.DataFrame):
                # DataFrame sheet (What to Focus On)
                safe_name = sheet_name[:31]
                data.to_excel(writer, sheet_name=safe_name, startrow=2, index=False)
                ws = writer.sheets[safe_name]

                ws.write(0, 0, sheet_name, formats["title"])
                ws.write(1, 0, f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}", formats["subtitle"])

                for col_num, col_name in enumerate(data.columns):
                    ws.write(2, col_num, col_name, formats["header"])

                # Auto-width
                for col_num, col_name in enumerate(data.columns):
                    max_len = max(
                        data[col_name].astype(str).map(len).max() if len(data) > 0 else 0,
                        len(str(col_name))
                    )
                    ws.set_column(col_num, col_num, min(max_len + 4, 60))

                # What to Focus On specific formatting
                if sheet_name == "What to Focus On":
                    ws.set_column(1, 1, 70)   # Finding
                    ws.set_column(2, 2, 100)  # The Work (evidence)
                    ws.set_column(3, 7, 58)   # Steps 1-5

                if first_ws_name is None:
                    first_ws_name = safe_name

        # Activate Plant Summary as landing page
        if first_ws_name and first_ws_name in writer.sheets:
            writer.sheets[first_ws_name].activate()

    print(f"Done! Open: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = sys.argv[1:]
    oee_file = None
    downtime_file = None

    i = 0
    while i < len(args):
        if args[i] == "--downtime" and i + 1 < len(args):
            downtime_file = args[i + 1]
            i += 2
        elif not args[i].startswith("-"):
            oee_file = args[i]
            i += 1
        else:
            i += 1

    if oee_file is None:
        oee_file = os.path.join(os.path.dirname(__file__), "..",
                                "shift_oee_cases_by_day_shift_with_shift_totals.xlsx")

    oee_file = os.path.abspath(oee_file)
    if not os.path.exists(oee_file):
        print(f"Error: OEE file not found: {oee_file}")
        sys.exit(1)

    from parse_mes import detect_file_type, parse_oee_period_detail
    oee_type = detect_file_type(oee_file)
    if oee_type == "oee_period_detail":
        print("  Detected: MES OEE Period Detail export")
        hourly, shift_summary, overall, hour_avg = parse_oee_period_detail(oee_file)
    else:
        hourly, shift_summary, overall, hour_avg = load_oee_data(oee_file)

    downtime = None
    if downtime_file:
        downtime_file = os.path.abspath(downtime_file)
        if os.path.exists(downtime_file):
            if downtime_file.lower().endswith(".json"):
                downtime = load_downtime_data(downtime_file)
            else:
                from parse_mes import detect_file_type, parse_event_summary
                dt_type = detect_file_type(downtime_file)
                if dt_type == "event_summary":
                    print("  Detected: MES Event Summary export")
                    downtime = parse_event_summary(downtime_file)
                elif dt_type == "passdown":
                    from parse_passdown import parse_passdown
                    print("  Detected: Shift Passdown Report")
                    downtime = parse_passdown(downtime_file)
                else:
                    print(f"  Warning: Unrecognized downtime file format ({dt_type})")
        else:
            print(f"Warning: Downtime file not found: {downtime_file}")

    # Single file per analysis run (days shown within each shift sheet, not split)
    basename = os.path.splitext(os.path.basename(oee_file))[0]
    output_dir = os.path.dirname(oee_file)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    suffix = "_FULL_ANALYSIS" if downtime else "_ANALYSIS"

    n_days = hourly["date_str"].nunique()
    dates = sorted(
        d for d in hourly["date_str"].dropna().astype(str).unique()
        if d and d.lower() != "nan"
    )
    print(f"\n{'='*60}")
    print(f"  Analyzing: {', '.join(dates)} ({n_days} day(s))")
    print(f"{'='*60}")

    results = analyze(hourly, shift_summary, overall, hour_avg, downtime)

    output_path = os.path.join(output_dir, f"{basename}{suffix}_{timestamp}.xlsx")
    write_excel(results, output_path)

    # Console summary
    _print_summary(results, output_path)


def _print_summary(results, output_path):
    """Print console summary for the analysis."""
    print("\n" + "=" * 60)
    print("QUICK SUMMARY")
    print("=" * 60)

    # Plant Summary KPIs
    plant = results.get("Plant Summary", {})
    if isinstance(plant, dict):
        kpis = plant.get("kpis", pd.DataFrame())
        if len(kpis) > 0:
            for _, row in kpis.iterrows():
                print(f"  {row['Metric']}: {row['Value']}")

    # Per-shift summary
    for shift_name in ["1st Shift", "2nd Shift", "3rd Shift"]:
        if shift_name in results and isinstance(results[shift_name], dict):
            raw = results[shift_name].get("raw", {})
            print(f"\n  --- {shift_name.upper()} ---")
            print(f"    OEE: {raw.get('oee', 0):.1f}%")
            print(f"    Cases: {raw.get('cases', 0):,.0f}")
            print(f"    CPH: {raw.get('cph', 0):,.0f} (target: {raw.get('target_cph', 0):,.0f})")
            print(f"    Primary loss: {raw.get('primary_loss', 'N/A')}")

    # Top actions
    if "What to Focus On" in results:
        print("\nTOP ACTIONS:")
        focus_df = results["What to Focus On"]
        for _, row in focus_df.head(5).iterrows():
            print(f"\n  #{row['Priority']}: {row['Finding']}")
            print(f"     Step 1: {row['Step 1']}")

    print(f"\nSheets: {', '.join(results.keys())}")
    print(f"Full analysis: {output_path}")


if __name__ == "__main__":
    main()

