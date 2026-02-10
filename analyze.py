"""
Traksys OEE + Downtime Analyzer
================================
Reads Traksys OEE exports AND downtime event data to generate a
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

from shared import EXCLUDE_REASONS, EQUIPMENT_KEYWORDS, classify_fault

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
    return EXCEL_EPOCH + timedelta(days=float(serial))


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


# ---------------------------------------------------------------------------
# Column Mapping Helpers — support both positional and header-based formats
# ---------------------------------------------------------------------------
def _normalize_col(name):
    """Normalize a column header for fuzzy matching."""
    return str(name).lower().strip().replace("_", "").replace(" ", "")


# Maps normalized header names found in user uploads to internal column names.
_HEADER_TO_INTERNAL = {
    # Date / time columns
    "date": "shift_date",
    "shiftdate": "shift_date",
    "shift": "shift",
    "hour": "shift_hour",
    "shifthour": "shift_hour",
    "starttime": "time_block",
    "timeblock": "time_block",
    "blockstart": "block_start",
    "blockend": "block_end",
    # Volume / duration
    "hours": "total_hours",
    "durationhours": "total_hours",
    "totalhours": "total_hours",
    "productcode": "product_code",
    "job": "job",
    "goodcases": "good_cases",
    "badcases": "bad_cases",
    "totalcases": "total_cases",
    "casesperhour": "cases_per_hour",
    "cases/hr": "cases_per_hour",
    "cph": "cases_per_hour",
    # OEE metrics
    "oee": "oee_pct",
    "oeepct": "oee_pct",
    "oee(%)": "oee_pct",
    "avgoee": "oee_pct",
    "availability": "availability",
    "avgavailability": "availability",
    "performance": "performance",
    "avgperformance": "performance",
    "quality": "quality",
    # Counts
    "intervals": "intervals",
    "nintervals": "n_intervals",
    "hourblocks": "hour_blocks",
}

# Columns that must be numeric
_NUMERIC_COLUMNS = {
    "shift_hour", "total_hours", "total_cases", "cases_per_hour",
    "oee_pct", "availability", "performance", "quality",
    "good_cases", "bad_cases", "intervals", "n_intervals", "hour_blocks",
}


def _smart_rename(df, expected_columns):
    """Rename DataFrame columns using header-name matching, falling back to positional.

    Strategy:
      1. Normalize each header and look it up in _HEADER_TO_INTERNAL.
      2. If enough expected columns are found by name, use name-based mapping.
      3. Otherwise fall back to positional assignment (original behaviour).
    """
    header_map = {}
    claimed = set()
    for col in df.columns:
        norm = _normalize_col(col)
        if norm in _HEADER_TO_INTERNAL:
            internal = _HEADER_TO_INTERNAL[norm]
            if internal not in claimed:
                header_map[col] = internal
                claimed.add(internal)

    expected_set = set(expected_columns)
    matched = claimed & expected_set

    # Use header mapping if it covers a meaningful portion of expected columns
    if len(matched) >= max(2, len(expected_set) * 0.3):
        return df.rename(columns=header_map)

    # Fall back to positional assignment when column count matches exactly
    if len(df.columns) == len(expected_columns):
        df.columns = expected_columns
        return df

    # Last resort: apply whatever header matches we found
    if header_map:
        return df.rename(columns=header_map)

    raise ValueError(
        f"Cannot map columns: expected {len(expected_columns)} columns "
        f"({', '.join(expected_columns[:5])}...), "
        f"got {len(df.columns)} columns ({', '.join(str(c) for c in df.columns[:5])}...)"
    )


def _coerce_numerics(df):
    """Ensure columns that should be numeric are actually numeric."""
    for col in df.columns:
        if col in _NUMERIC_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def _derive_columns(df):
    """Compute missing derived columns from available data."""
    # total_cases from good_cases + bad_cases
    if "total_cases" not in df.columns and "good_cases" in df.columns:
        if "bad_cases" in df.columns:
            df["total_cases"] = df["good_cases"] + df["bad_cases"]
        else:
            df["total_cases"] = df["good_cases"]

    # cases_per_hour from total_cases / total_hours
    if "cases_per_hour" not in df.columns:
        if "total_cases" in df.columns and "total_hours" in df.columns:
            mask = df["total_hours"] > 0
            df["cases_per_hour"] = 0.0
            df.loc[mask, "cases_per_hour"] = (
                df.loc[mask, "total_cases"] / df.loc[mask, "total_hours"]
            )
        else:
            df["cases_per_hour"] = 0.0

    # oee_pct: scale from 0-1 to 0-100 if all values are <=1
    if "oee_pct" in df.columns:
        oee_vals = pd.to_numeric(df["oee_pct"], errors="coerce").dropna()
        if len(oee_vals) > 0 and oee_vals.max() <= 1.0:
            df["oee_pct"] = df["oee_pct"] * 100

    # time_block: convert full datetimes to HH:MM display format
    if "time_block" in df.columns:
        sample = df["time_block"].dropna().head(5)
        if len(sample) > 0:
            first = sample.iloc[0]
            if isinstance(first, (pd.Timestamp, datetime)):
                df["time_block"] = df["time_block"].apply(
                    lambda x: x.strftime("%H:%M") if isinstance(x, (pd.Timestamp, datetime)) else str(x)
                )

    # time_block: create from shift_hour if missing entirely
    if "time_block" not in df.columns:
        if "shift_hour" in df.columns:
            df["time_block"] = df["shift_hour"].apply(
                lambda h: f"{int(h)}:00" if pd.notna(h) else ""
            )
        else:
            df["time_block"] = ""

    return df


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


# ---------------------------------------------------------------------------
# Load OEE Data
# ---------------------------------------------------------------------------
def load_oee_data(filepath):
    print(f"Reading OEE data: {filepath}")
    sheet_map = _resolve_sheets(filepath)
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
    shift_cph = ov["cases_per_hour"].values[0] if len(ov) > 0 else (shift_cases / shift_hours if shift_hours > 0 else 0)
    n_days = sh["date_str"].nunique()

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
        day_cph = day_total_cases / day_total_hours if day_total_hours > 0 else 0
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
# Main Analysis
# ---------------------------------------------------------------------------
def analyze(hourly, shift_summary, overall, hour_avg, downtime=None):
    results = {}

    # === CORE METRICS ===
    total_cases = hourly["total_cases"].sum()
    total_hours = hourly["total_hours"].sum()
    avg_cph = total_cases / total_hours if total_hours > 0 else 0
    avg_avail, avg_perf, avg_qual, avg_oee = _aggregate_oee(hourly)

    good_hours = hourly[hourly["total_hours"] >= 0.5]
    target_cph = good_hours["cases_per_hour"].quantile(0.90)

    hourly["cases_gap"] = (target_cph - hourly["cases_per_hour"]).clip(lower=0) * hourly["total_hours"]
    total_cases_lost = hourly["cases_gap"].sum()

    date_min = hourly["date"].min().strftime("%Y-%m-%d")
    date_max = hourly["date"].max().strftime("%Y-%m-%d")
    n_days = hourly["date_str"].nunique()

    avail_loss = (1 - avg_avail) * 100
    perf_loss = (1 - avg_perf) * 100

    has_downtime = downtime is not None and len(downtime.get("reasons_df", [])) > 0

    shifts_sorted = overall.sort_values("oee_pct", ascending=False)
    top_shift = shifts_sorted.iloc[0]
    bot_shift = shifts_sorted.iloc[-1]

    # ===================================================================
    # TAB 1: EXECUTIVE SUMMARY
    # ===================================================================
    exec_data = {"Metric": [], "Value": []}

    def add_exec(metric, value):
        exec_data["Metric"].append(metric)
        exec_data["Value"].append(value)

    add_exec("Date Range", f"{date_min} to {date_max}")
    add_exec("Days Analyzed", n_days)
    add_exec("Total Cases Produced", f"{total_cases:,.0f}")
    add_exec("Total Production Hours", f"{total_hours:,.1f}")
    add_exec("Average Cases/Hour", f"{avg_cph:,.0f}")
    add_exec("90th Percentile CPH (Benchmark)", f"{target_cph:,.0f}")
    add_exec("", "")
    add_exec("Average OEE", f"{avg_oee:.1f}%")
    add_exec("Average Availability", f"{avg_avail:.1%}")
    add_exec("Average Performance", f"{avg_perf:.1%}")
    add_exec("Average Quality", f"{avg_qual:.1%}")
    add_exec("", "")
    add_exec("Best Shift", f"{top_shift['shift']} ({top_shift['oee_pct']:.1f}% OEE)")
    add_exec("Worst Shift", f"{bot_shift['shift']} ({bot_shift['oee_pct']:.1f}% OEE)")
    add_exec("Shift Gap", f"{top_shift['oee_pct'] - bot_shift['oee_pct']:.1f} OEE points")
    add_exec("", "")
    add_exec("Est. Cases Lost vs Benchmark", f"{total_cases_lost:,.0f}")
    add_exec("Cases Lost Per Day", f"{total_cases_lost / n_days:,.0f}")

    if has_downtime:
        add_exec("", "")
        add_exec("--- DOWNTIME CONTEXT ---", "")
        meta = downtime.get("meta", {})
        oee_sum = downtime.get("oee_summary", {})
        if meta.get("line"):
            add_exec("Line", meta["line"])
        if oee_sum.get("overall_oee"):
            add_exec("Period OEE (from Traksys)", f"{oee_sum['overall_oee']:.1%}")
        if oee_sum.get("availability_loss_hrs"):
            add_exec("Total Availability Loss", f"{oee_sum['availability_loss_hrs']:.1f} hours")

        top_3 = downtime["reasons_df"][
            ~downtime["reasons_df"]["reason"].isin(EXCLUDE_REASONS)
        ].sort_values("total_minutes", ascending=False).head(3)
        for i, (_, row) in enumerate(top_3.iterrows()):
            add_exec(f"#{i+1} Downtime Cause", f"{row['reason']} ({row['total_hours']:.0f} hrs / {int(row['total_occurrences'])} events)")

    results["Executive Summary"] = pd.DataFrame(exec_data)

    # ===================================================================
    # TABS 2-4: SHIFT DEEP DIVES (3rd first, then 2nd, then 1st)
    # ===================================================================
    # Determine shift order: worst first
    shift_order = ["3rd (11p-7a)", "2nd (3p-11p)", "1st (7a-3p)"]
    # Fallback: use whatever shift names exist in the data
    actual_shifts = hourly["shift"].unique().tolist()
    if not any(s in actual_shifts for s in shift_order):
        shift_order = sorted(actual_shifts, key=lambda s: overall[overall["shift"] == s]["oee_pct"].values[0] if len(overall[overall["shift"] == s]) > 0 else 999)

    for shift_name in shift_order:
        if shift_name not in actual_shifts:
            continue
        dive_df = build_shift_deep_dive(
            shift_name, hourly, shift_summary, hour_avg, overall, avg_oee, avg_cph
        )
        if dive_df is not None:
            # Shorten tab name to fit Excel 31-char limit
            short = shift_name.split("(")[0].strip() if "(" in shift_name else shift_name
            tab_name = f"{short} Deep Dive"
            results[tab_name] = dive_df

    # ===================================================================
    # TAB 5: SHIFT COMPARISON (side by side)
    # ===================================================================
    # Select core columns that exist, build a clean comparison table
    comp_cols = ["shift", "oee_pct", "total_cases", "total_hours", "cases_per_hour"]
    comp_cols = [c for c in comp_cols if c in overall.columns]
    shift_comp = overall[comp_cols].copy()
    for col in ["cases_per_hour", "total_cases", "total_hours"]:
        if col in shift_comp.columns:
            shift_comp[col] = shift_comp[col].round(0 if col != "total_hours" else 1)
    if "oee_pct" in shift_comp.columns:
        shift_comp["oee_pct"] = shift_comp["oee_pct"].round(1)
    # Add good/bad cases if available
    for col in ["good_cases", "bad_cases"]:
        if col in overall.columns:
            shift_comp[col] = overall[col].round(0)
    shift_comp = shift_comp.sort_values("oee_pct", ascending=False)
    rename_map = {
        "shift": "Shift", "oee_pct": "OEE %", "total_cases": "Total Cases",
        "total_hours": "Total Hours", "cases_per_hour": "Cases/Hr",
        "good_cases": "Good Cases", "bad_cases": "Bad Cases",
    }
    shift_comp = shift_comp.rename(columns=rename_map)
    results["Shift Comparison"] = shift_comp

    # ===================================================================
    # TAB 6: LOSS BREAKDOWN
    # ===================================================================
    loss_rows = []
    for shift_name_lb in hourly["shift"].unique():
        shift_data = hourly[hourly["shift"] == shift_name_lb]
        sa, sp, sq, soee = _aggregate_oee(shift_data)
        loss_rows.append({
            "shift": shift_name_lb,
            "avg_availability": sa,
            "avg_performance": sp,
            "avg_quality": sq,
            "avg_oee": round(soee, 1),
            "total_cases_lost": round(shift_data["cases_gap"].sum(), 0),
        })
    loss_by_shift = pd.DataFrame(loss_rows)
    loss_by_shift["avail_loss_%"] = ((1 - loss_by_shift["avg_availability"]) * 100).round(1)
    loss_by_shift["perf_loss_%"] = ((1 - loss_by_shift["avg_performance"]) * 100).round(1)
    loss_by_shift["qual_loss_%"] = ((1 - loss_by_shift["avg_quality"]) * 100).round(1)

    # Add primary loss driver per shift
    loss_by_shift["primary_loss"] = loss_by_shift.apply(
        lambda r: "Availability" if r["avail_loss_%"] >= r["perf_loss_%"] else "Performance", axis=1)

    loss_out = loss_by_shift[["shift", "avg_oee", "avail_loss_%", "perf_loss_%", "qual_loss_%",
                               "primary_loss", "total_cases_lost"]].copy()
    loss_out.columns = ["Shift", "Avg OEE %", "Avail Loss %", "Perf Loss %", "Qual Loss %",
                        "Primary Loss Driver", "Cases Lost"]
    results["Loss Breakdown"] = loss_out

    # ===================================================================
    # TAB 7: DOWNTIME PARETO
    # ===================================================================
    if has_downtime:
        reasons_df = downtime["reasons_df"].copy()
        actionable = reasons_df[~reasons_df["reason"].isin(EXCLUDE_REASONS)].copy()
        actionable = actionable.sort_values("total_minutes", ascending=False).reset_index(drop=True)

        total_actionable_min = actionable["total_minutes"].sum()
        actionable["pct_of_total"] = (actionable["total_minutes"] / total_actionable_min * 100).round(1)
        actionable["cumulative_pct"] = actionable["pct_of_total"].cumsum().round(1)
        actionable["avg_min_per_event"] = (actionable["total_minutes"] / actionable["total_occurrences"]).round(1)
        actionable["fault_category"] = actionable["reason"].apply(classify_fault)

        pareto_out = actionable[["reason", "fault_category", "total_occurrences", "total_minutes",
                                  "total_hours", "avg_min_per_event", "pct_of_total", "cumulative_pct"]].copy()
        pareto_out.columns = ["Cause", "Fault Type", "Events", "Total Minutes", "Total Hours",
                              "Avg Min/Event", "% of Total", "Cumulative %"]
        results["Downtime Pareto"] = pareto_out

    # ===================================================================
    # TAB 8: FAULT CLASSIFICATION (mechanic vs operator vs process)
    # ===================================================================
    if has_downtime:
        fault_summary, fault_detail = build_fault_classification(downtime)
        if fault_summary is not None:
            results["Fault Summary"] = fault_summary
        if fault_detail is not None:
            results["Fault Detail"] = fault_detail

    # ===================================================================
    # TAB 9: WORST HOURS (all shifts)
    # ===================================================================
    worst = (
        hourly[hourly["total_hours"] >= 0.5].nsmallest(25, "oee_pct")
        [["date_str", "day_of_week", "shift", "time_block", "cases_per_hour",
          "oee_pct", "availability", "performance", "quality", "total_cases", "cases_gap"]]
        .copy()
    )
    worst["cases_per_hour"] = worst["cases_per_hour"].round(0)
    worst["oee_pct"] = worst["oee_pct"].round(1)
    worst["availability"] = (worst["availability"] * 100).round(1)
    worst["performance"] = (worst["performance"] * 100).round(1)
    worst["quality"] = (worst["quality"] * 100).round(1)
    worst["cases_gap"] = worst["cases_gap"].round(0)
    worst.columns = ["Date", "Day", "Shift", "Hour", "Cases/Hr", "OEE %",
                     "Avail %", "Perf %", "Qual %", "Cases", "Cases Lost"]
    results["Worst Hours"] = worst

    # ===================================================================
    # TAB 10: DAILY TREND
    # ===================================================================
    shift_summary["_w_oee"] = shift_summary["oee_pct"] * shift_summary["total_hours"]
    daily = (
        shift_summary.groupby("date_str")
        .agg(total_cases=("total_cases", "sum"), total_hours=("total_hours", "sum"),
             _w_oee=("_w_oee", "sum"), n_shifts=("shift", "count"))
        .reset_index()
    )
    daily["avg_oee"] = (daily["_w_oee"] / daily["total_hours"].replace(0, np.nan)).fillna(0).round(1)
    daily.drop(columns=["_w_oee"], inplace=True)
    shift_summary.drop(columns=["_w_oee"], inplace=True, errors="ignore")
    daily["cases_per_hour"] = (daily["total_cases"] / daily["total_hours"]).round(0)
    daily["total_cases"] = daily["total_cases"].round(0)
    daily = daily.sort_values("date_str")
    daily["oee_7day_avg"] = daily["avg_oee"].rolling(7, min_periods=1).mean().round(1)
    daily["cph_7day_avg"] = daily["cases_per_hour"].rolling(7, min_periods=1).mean().round(0)
    daily_out = daily[["date_str", "n_shifts", "total_cases", "cases_per_hour",
                       "cph_7day_avg", "avg_oee", "oee_7day_avg"]].copy()
    daily_out.columns = ["Date", "Shifts", "Total Cases", "Cases/Hr", "CPH 7-Day Avg", "OEE %", "OEE 7-Day Avg"]
    results["Daily Trend"] = daily_out

    # ===================================================================
    # TAB 11: SHIFT x DAY HEATMAP
    # ===================================================================
    pivot = shift_summary.pivot_table(
        index="date_str", columns="shift", values="oee_pct", aggfunc="first"
    ).round(1).reset_index()
    pivot.columns.name = None
    pivot = pivot.rename(columns={"date_str": "Date"})
    results["Shift x Day OEE"] = pivot

    # ===================================================================
    # TAB 12: WHAT TO FOCUS ON (conclusion with step-by-step actions)
    # ===================================================================
    recs = []
    priority = 1

    if has_downtime:
        reasons_df = downtime["reasons_df"]
        actionable_reasons = reasons_df[~reasons_df["reason"].isin(EXCLUDE_REASONS)]
        top_reason = actionable_reasons.sort_values("total_minutes", ascending=False).iloc[0] if len(actionable_reasons) > 0 else None

        if top_reason is not None:
            recs.append({
                "Priority": priority,
                "Finding": f"#1 loss: {top_reason['reason']} — {top_reason['total_hours']:.0f} hours / {int(top_reason['total_occurrences'])} events",
                "Step 1": f"Pull the last 2 weeks of {top_reason['reason']} events from Traksys. Sort by duration — find the top 10 longest stops.",
                "Step 2": f"Walk the line during the next {top_reason['reason']} event. Document what operators see: what failed, what they did, how long each step took.",
                "Step 3": f"Run a 5-Why or fishbone with maintenance on the top 3 failure modes. Identify root causes vs symptoms.",
                "Step 4": f"Build countermeasures (PM task, spare parts staging, SOP change, sensor adjustment) and assign owners with dates.",
                "Step 5": f"Track {top_reason['reason']} hours weekly. Target 50% reduction = ~{top_reason['total_hours']/2:.0f} hours recovered.",
            })
            priority += 1

        # Unassigned check
        unassigned = reasons_df[reasons_df["reason"].isin(["Unassigned", "Unknown"])]
        if len(unassigned) > 0:
            total_unassigned_hrs = unassigned["total_hours"].sum()
            total_unassigned_events = int(unassigned["total_occurrences"].sum())
            if total_unassigned_hrs > 5:
                recs.append({
                    "Priority": priority,
                    "Finding": f"{total_unassigned_hrs:.0f} hours uncoded ({total_unassigned_events} events marked Unassigned/Unknown)",
                    "Step 1": "Review the Traksys reason code tree. Are codes confusing, too many, or missing common causes?",
                    "Step 2": "Simplify: aim for 15-20 actionable codes, not 100. Merge duplicates, remove obsolete ones.",
                    "Step 3": "Coach supervisors at shift start: 'If you can't code it, write a note. No blanks.'",
                    "Step 4": "Set up a weekly check — pull unassigned events, review with shift leads, assign codes retroactively.",
                    "Step 5": f"Target: get Unassigned below 5% of total events (currently {total_unassigned_events} events).",
                })
                priority += 1

        # Short stops
        short_stops = reasons_df[reasons_df["reason"] == "Short Stop"]
        if len(short_stops) > 0:
            ss = short_stops.iloc[0]
            if ss["total_hours"] > 10:
                avg_sec = ss["total_minutes"] * 60 / ss["total_occurrences"] if ss["total_occurrences"] > 0 else 0
                recs.append({
                    "Priority": priority,
                    "Finding": f"{int(ss['total_occurrences'])} short stops totaling {ss['total_hours']:.0f} hours — avg {avg_sec:.0f} sec each",
                    "Step 1": "Get short stop data by location/sensor if Traksys tracks it. Find the top 3 trigger points.",
                    "Step 2": "Observe the line during peak short-stop periods. Watch for: jams at transfers, sensor trips, product orientation.",
                    "Step 3": "Check sensor sensitivity, conveyor speeds at transitions, guide rail gaps. Small adjustments cut stops 30%+.",
                    "Step 4": "For each top location: document the fix, test for one shift, verify reduction in data.",
                    "Step 5": f"Track weekly. Goal: reduce from {int(ss['total_occurrences'])} to under {int(ss['total_occurrences'] * 0.7)} events.",
                })
                priority += 1

        # Next equipment causes
        equip_reasons = actionable_reasons[
            ~actionable_reasons["reason"].isin(["Unassigned", "Unknown", "Short Stop", "Day Code Change"])
        ].sort_values("total_minutes", ascending=False)

        if len(equip_reasons) >= 2:
            items = []
            for idx in range(1, min(3, len(equip_reasons))):
                r = equip_reasons.iloc[idx]
                items.append(f"{r['reason']} ({r['total_hours']:.0f} hrs / {int(r['total_occurrences'])} events)")
            if items:
                recs.append({
                    "Priority": priority,
                    "Finding": f"Next equipment losses: {', '.join(items)}",
                    "Step 1": "Don't start these until #1 is underway. Queue them as next reliability projects.",
                    "Step 2": "Pull event logs for each. Do they spike on certain shifts, products, or days?",
                    "Step 3": "Check PM schedules — are these assets getting regular preventive maintenance?",
                    "Step 4": "Talk to operators and mechanics: what do they see? What parts keep failing?",
                    "Step 5": "Prioritize whichever has the clearest failure pattern. Start a focused kaizen.",
                })
                priority += 1

    # Shift gap
    gap = top_shift["oee_pct"] - bot_shift["oee_pct"]
    if gap > 3:
        cph_gap = top_shift["cases_per_hour"] - bot_shift["cases_per_hour"]
        recs.append({
            "Priority": priority,
            "Finding": f"{bot_shift['shift']} underperforms {top_shift['shift']} by {gap:.1f} OEE points ({cph_gap:,.0f} fewer cases/hr)",
            "Step 1": f"See the shift deep dive tabs — compare hour-by-hour patterns between shifts.",
            "Step 2": f"Shadow {top_shift['shift']} for a full shift. Document what they do differently.",
            "Step 3": f"Interview leads on both shifts. Ask: what slows you down? What's not ready when you start?",
            "Step 4": "Build a standard startup/changeover checklist from best practices. Pilot on worst shift.",
            "Step 5": f"Track weekly by shift. Goal: close the gap by {gap/2:.0f} OEE points within 4 weeks.",
        })
        priority += 1

    # Availability vs Performance
    if avail_loss > perf_loss * 1.3:
        recs.append({
            "Priority": priority,
            "Finding": f"Availability ({avg_avail:.0%}) is the primary OEE drag — line not running {avail_loss:.0f}% of the time",
            "Step 1": "See 'Fault Summary' tab — Equipment/Mechanical is likely the biggest bucket.",
            "Step 2": "Time the top 3 changeover types with a stopwatch. Document every step and wait.",
            "Step 3": "Apply SMED: separate internal vs external tasks. Stage materials before the line stops.",
            "Step 4": "For breakdowns: review PM compliance and spare parts availability with maintenance.",
            "Step 5": "Target: improve availability from {:.0f}% to {:.0f}% in 6 weeks.".format(avg_avail * 100, avg_avail * 100 + 5),
        })
        priority += 1
    elif perf_loss > avail_loss * 1.3:
        recs.append({
            "Priority": priority,
            "Finding": f"Performance ({avg_perf:.0%}) is the primary OEE drag — line running slow when up",
            "Step 1": "See 'Fault Summary' tab — Micro Stops are likely a big contributor.",
            "Step 2": "Check rated speed vs actual on HMI. Are operators running below target? Why?",
            "Step 3": "Look for minor stops not captured as downtime — jams operators clear without logging.",
            "Step 4": "Review centerline settings: documented, posted, followed shift to shift?",
            "Step 5": "Target: improve performance from {:.0f}% to {:.0f}% in 6 weeks.".format(avg_perf * 100, avg_perf * 100 + 5),
        })
        priority += 1
    else:
        recs.append({
            "Priority": priority,
            "Finding": f"Both Availability ({avg_avail:.0%}) and Performance ({avg_perf:.0%}) are significant losses",
            "Step 1": "See 'Fault Summary' tab — Equipment losses hit availability, Micro Stops hit performance.",
            "Step 2": "Attack availability first — usually faster to fix (changeovers, material staging, startup).",
            "Step 3": "Simultaneously investigate performance: rated speed vs actual, minor stops, cycle time.",
            "Step 4": "Pick the single biggest contributor from each. Run a focused improvement on each.",
            "Step 5": "Target: +5 OEE points in 6 weeks from combined gains.",
        })
        priority += 1

    # Catastrophic hours
    worst_hours = hourly[hourly["oee_pct"] < 15]
    if len(worst_hours) > 0:
        n_catastrophic = len(worst_hours)
        recs.append({
            "Priority": priority,
            "Finding": f"{n_catastrophic} hours had OEE below 15% — see 'Worst Hours' tab and shift deep dives",
            "Step 1": "Cross-reference each with Traksys downtime events. What happened?",
            "Step 2": "Look for patterns: same shift? Same day? Same time block?",
            "Step 3": "For the most common cause, build a specific countermeasure.",
            "Step 4": "Assign an owner and a completion date for each countermeasure.",
            "Step 5": f"Target: cut catastrophic hours from {n_catastrophic} to under {max(n_catastrophic // 2, 5)}.",
        })
        priority += 1

    # Startup — compare each shift's first hour to the rest
    # Uses minimum hour per shift (clock hours) rather than hardcoded shift_hour == 1
    if "shift_hour" in hour_avg.columns:
        shift_first_hours = hour_avg.groupby("shift")["shift_hour"].min()
        first_mask = hour_avg.apply(
            lambda r: r["shift_hour"] == shift_first_hours.get(r["shift"], -1), axis=1
        )
        first_hour = hour_avg[first_mask]
        other_hours = hour_avg[~first_mask]
    else:
        first_hour = pd.DataFrame()
        other_hours = pd.DataFrame()
    if len(first_hour) > 0 and len(other_hours) > 0:
        first_avg_oee = (_weighted_mean(first_hour["oee_pct"], first_hour["total_hours"])
                         if "total_hours" in first_hour.columns else first_hour["oee_pct"].mean())
        other_avg_oee = (_weighted_mean(other_hours["oee_pct"], other_hours["total_hours"])
                         if "total_hours" in other_hours.columns else other_hours["oee_pct"].mean())
        if first_avg_oee < other_avg_oee - 3:
            oee_gap = other_avg_oee - first_avg_oee
            recs.append({
                "Priority": priority,
                "Finding": f"First hour averages {first_avg_oee:.1f}% OEE vs {other_avg_oee:.1f}% — {oee_gap:.0f} point startup loss",
                "Step 1": "See each shift deep dive — first hour performance is broken out.",
                "Step 2": "Observe shift start: time from bell to first good case off the line.",
                "Step 3": "Build a startup checklist: materials staged, settings verified, passdown in <10 min.",
                "Step 4": "Consider 15-min shift overlap so outgoing crew keeps line running during handoff.",
                "Step 5": f"Goal: close startup gap from {oee_gap:.0f} points to under 3 points.",
            })
            priority += 1

    # Total gap
    recs.append({
        "Priority": priority,
        "Finding": f"Total gap vs benchmark: ~{total_cases_lost:,.0f} cases over {n_days} days ({total_cases_lost/n_days:,.0f}/day)",
        "Step 1": "This gap = current output vs what the line produces on good hours (top 10%).",
        "Step 2": "The line CAN produce at benchmark rate — it just doesn't consistently.",
        "Step 3": "Pick the top 2-3 actions above and execute. Don't try to fix everything at once.",
        "Step 4": "Re-run this analysis in 4-6 weeks with fresh data to measure progress.",
        "Step 5": "A 10% OEE improvement recovers ~{:,.0f} cases/day.".format(total_cases_lost / n_days * 0.1),
    })

    results["What to Focus On"] = pd.DataFrame(recs)

    # ===================================================================
    # TAB: AI FINDINGS
    # ===================================================================
    if has_downtime and downtime.get("findings"):
        findings_data = []
        for i, finding in enumerate(downtime["findings"]):
            findings_data.append({"#": i + 1, "Finding": finding})
        results["AI Findings"] = pd.DataFrame(findings_data)

    # ===================================================================
    # TAB: SHIFT REPORT SAMPLES
    # ===================================================================
    if has_downtime and downtime.get("shift_samples"):
        samples = downtime["shift_samples"]
        report_rows = []
        for s in samples:
            if s.get("col_7") or s.get("col_8"):
                report_rows.append({
                    "Shift": s.get("col_3", ""),
                    "Line": s.get("col_4", ""),
                    "Area/Machine": s.get("col_7", ""),
                    "Issue": s.get("col_8", ""),
                    "Action": s.get("col_14", ""),
                    "Result": s.get("col_15", ""),
                    "Status": s.get("col_16", ""),
                    "Time (min)": s.get("col_17", ""),
                })
        if report_rows:
            results["Shift Report Sample"] = pd.DataFrame(report_rows)

    return results


# ---------------------------------------------------------------------------
# Excel Writer
# ---------------------------------------------------------------------------
def write_excel(results, output_path):
    print(f"Writing: {output_path}")

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        workbook = writer.book

        header_fmt = workbook.add_format({
            "bold": True, "bg_color": "#1B2A4A", "font_color": "white",
            "border": 1, "text_wrap": True, "valign": "vcenter", "font_size": 11
        })
        title_fmt = workbook.add_format({"bold": True, "font_size": 14, "font_color": "#1B2A4A"})
        subtitle_fmt = workbook.add_format({"italic": True, "font_size": 10, "font_color": "#666666"})
        section_fmt = workbook.add_format({
            "bold": True, "font_size": 11, "font_color": "#1B2A4A",
            "bottom": 2, "bottom_color": "#1B2A4A"
        })

        # Tab order: story flow
        sheet_order = [
            "Executive Summary",
            # Shift deep dives will be inserted dynamically
            "Shift Comparison", "Loss Breakdown",
            "Downtime Pareto", "Fault Summary", "Fault Detail",
            "Worst Hours", "Daily Trend", "Shift x Day OEE",
            "What to Focus On",
            "AI Findings", "Shift Report Sample",
        ]

        # Insert shift deep dive tabs after Executive Summary
        dive_tabs = [k for k in results if "Deep Dive" in k]
        final_order = ["Executive Summary"] + dive_tabs + [s for s in sheet_order if s != "Executive Summary"]

        for sheet_name in final_order:
            if sheet_name not in results:
                continue

            df = results[sheet_name]
            safe_name = sheet_name[:31]
            df.to_excel(writer, sheet_name=safe_name, startrow=2, index=False)
            ws = writer.sheets[safe_name]

            ws.write(0, 0, sheet_name, title_fmt)
            ws.write(1, 0, f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}", subtitle_fmt)

            for col_num, col_name in enumerate(df.columns):
                ws.write(2, col_num, col_name, header_fmt)

            # Auto-width
            for col_num, col_name in enumerate(df.columns):
                max_len = max(
                    df[col_name].astype(str).map(len).max() if len(df) > 0 else 0,
                    len(str(col_name))
                )
                ws.set_column(col_num, col_num, min(max_len + 4, 60))

            # OEE color scales
            for oee_label in ["OEE %", "Avg OEE %", "OEE 7-Day Avg"]:
                if oee_label in df.columns:
                    col_idx = list(df.columns).index(oee_label)
                    ws.conditional_format(3, col_idx, 3 + len(df), col_idx, {
                        "type": "3_color_scale",
                        "min_color": "#F8696B", "mid_color": "#FFEB84", "max_color": "#63BE7B",
                    })

            # Downtime Pareto color scales
            if sheet_name == "Downtime Pareto" and "Total Minutes" in df.columns:
                col_idx = list(df.columns).index("Total Minutes")
                ws.conditional_format(3, col_idx, 3 + len(df), col_idx, {
                    "type": "3_color_scale",
                    "min_color": "#63BE7B", "mid_color": "#FFEB84", "max_color": "#F8696B",
                })

            # Shift Deep Dive formatting
            if "Deep Dive" in sheet_name:
                ws.set_column(0, 0, 25)  # Section
                ws.set_column(1, 1, 35)  # Metric
                ws.set_column(2, 2, 18)  # Value
                ws.set_column(3, 3, 70)  # Detail

                # Bold section headers
                for row_num in range(len(df)):
                    val = df.iloc[row_num].get("Section", "")
                    if val and str(val).strip():
                        ws.write(row_num + 3, 0, val, section_fmt)

            # What to Focus On
            if sheet_name == "What to Focus On":
                ws.set_column(1, 1, 70)  # Finding
                ws.set_column(2, 6, 58)  # Steps 1-5

            # Fault Summary
            if sheet_name == "Fault Summary":
                ws.set_column(0, 0, 28)  # Category
                ws.set_column(6, 6, 55)  # Who owns this
                if "% of All Downtime" in df.columns:
                    col_idx = list(df.columns).index("% of All Downtime")
                    ws.conditional_format(3, col_idx, 3 + len(df), col_idx, {
                        "type": "3_color_scale",
                        "min_color": "#63BE7B", "mid_color": "#FFEB84", "max_color": "#F8696B",
                    })

            # Fault Detail
            if sheet_name == "Fault Detail":
                ws.set_column(0, 0, 28)
                ws.set_column(1, 1, 30)

            # AI Findings
            if sheet_name == "AI Findings":
                ws.set_column(1, 1, 100)

            # Shift Report Sample
            if sheet_name == "Shift Report Sample":
                ws.set_column(3, 3, 55)
                ws.set_column(4, 4, 40)
                ws.set_column(5, 5, 35)

            # Shift x Day heatmap
            if sheet_name == "Shift x Day OEE":
                for col_num in range(1, len(df.columns)):
                    ws.conditional_format(3, col_num, 3 + len(df), col_num, {
                        "type": "3_color_scale",
                        "min_color": "#F8696B", "mid_color": "#FFEB84", "max_color": "#63BE7B",
                    })

        # Activate Executive Summary as landing page
        if "Executive Summary" in results:
            writer.sheets["Executive Summary"].activate()

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

    hourly, shift_summary, overall, hour_avg = load_oee_data(oee_file)

    downtime = None
    if downtime_file:
        downtime_file = os.path.abspath(downtime_file)
        if os.path.exists(downtime_file):
            downtime = load_downtime_data(downtime_file)
        else:
            print(f"Warning: Downtime file not found: {downtime_file}")

    results = analyze(hourly, shift_summary, overall, hour_avg, downtime)

    basename = os.path.splitext(os.path.basename(oee_file))[0]
    output_dir = os.path.dirname(oee_file)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    suffix = "_FULL_ANALYSIS" if downtime else "_ANALYSIS"
    output_path = os.path.join(output_dir, f"{basename}{suffix}_{timestamp}.xlsx")

    write_excel(results, output_path)

    # Console summary
    print("\n" + "=" * 60)
    print("QUICK SUMMARY")
    print("=" * 60)
    exec_df = results["Executive Summary"]
    for _, row in exec_df.iterrows():
        if row["Metric"]:
            print(f"  {row['Metric']}: {row['Value']}")

    # Shift deep dives
    for key in results:
        if "Deep Dive" in key:
            dd = results[key]
            scorecard = dd[dd["Section"] == "SHIFT SCORECARD"]
            if len(scorecard) > 0:
                print(f"\n  --- {key.upper()} ---")
                detail_rows = dd[(dd["Section"] == "") & (dd["Metric"] != "")].head(7)
                for _, r in detail_rows.iterrows():
                    print(f"    {r['Metric']}: {r['Value']}  {r['Detail']}")

    # Fault summary
    if "Fault Summary" in results:
        print("\n  --- FAULT CLASSIFICATION ---")
        for _, row in results["Fault Summary"].iterrows():
            print(f"    {row['Fault Category']}: {row['Total Hours']:.0f} hrs ({row['% of All Downtime']}%) -> {row['Who Owns This']}")

    print("\nTOP ACTIONS:")
    focus_df = results["What to Focus On"]
    for _, row in focus_df.head(5).iterrows():
        print(f"\n  #{row['Priority']}: {row['Finding']}")
        print(f"     Step 1: {row['Step 1']}")

    if "Downtime Pareto" in results:
        print("\nDOWNTIME PARETO (top 5):")
        pareto = results["Downtime Pareto"].head(5)
        for _, row in pareto.iterrows():
            print(f"  {row['Cause']} [{row['Fault Type']}]: {row['Total Minutes']:,.0f} min / {int(row['Events'])} events ({row['% of Total']}%)")

    print(f"\nFull analysis: {output_path}")


if __name__ == "__main__":
    main()
