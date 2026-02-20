"""
History + SPC Engine for MES OEE Analyzer
==============================================
Two-layer architecture:
  1. history.jsonl   — append-only raw log (receipt of every run)
  2. plant_trends.json — tended summary with SPC, determinations, classifications

The "gardener" (tend_garden) runs after every save_run() and consolidates
raw history into structured intelligence: control limits, Nelson Rules,
chronic/acute downtime classification, trend tests, and shift analysis.

Dependencies: json, pandas, numpy (all already available).
Optional: pymannkendall (pip install pymannkendall) for statistical trend test.
"""

import json
import os
import hashlib
from datetime import datetime

import numpy as np
import pandas as pd

from shared import SHIFT_HOURS

_DIR = os.path.dirname(__file__)
HISTORY_FILE = os.path.join(_DIR, "history.jsonl")
TRENDS_FILE = os.path.join(_DIR, "plant_trends.json")
HOURLY_FILE = os.path.join(_DIR, "hourly_history.jsonl")
SHIFT_DAILY_FILE = os.path.join(_DIR, "shift_daily_history.jsonl")


_SHIFT_ALIASES = {
    "1st": "1st Shift", "2nd": "2nd Shift", "3rd": "3rd Shift",
    "1st (7a-3p)": "1st Shift", "2nd (3p-11p)": "2nd Shift", "3rd (11p-7a)": "3rd Shift",
}


def _normalize_shift(raw):
    """Map data-native shift names like '3rd (11p-7a)' to '3rd Shift'."""
    s = str(raw).strip()
    return _SHIFT_ALIASES.get(s, s)


def _sha256_text(text):
    """Stable SHA256 hex for text payloads."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _stable_df_fingerprint(df, cols, sort_cols=None):
    """Create a stable fingerprint for selected DataFrame columns.

    - Missing columns are filled with empty strings.
    - Numeric columns are rounded to 4 decimals to avoid noise.
    - Output is sorted for deterministic hashing.
    """
    if df is None or len(df) == 0:
        return _sha256_text("[]")

    cols = list(cols or [])
    work = pd.DataFrame()
    for c in cols:
        if c in df.columns:
            work[c] = df[c]
        else:
            work[c] = ""

    # Normalize datetimes and numbers for deterministic hashing.
    for c in work.columns:
        if pd.api.types.is_datetime64_any_dtype(work[c]):
            work[c] = pd.to_datetime(work[c], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
        else:
            as_num = pd.to_numeric(work[c], errors="coerce")
            if as_num.notna().any():
                work[c] = as_num.round(4).fillna(0.0)
            else:
                work[c] = work[c].astype(str).str.strip()

    if sort_cols:
        valid_sort = [c for c in sort_cols if c in work.columns]
        if valid_sort:
            work = work.sort_values(valid_sort, kind="stable")
    work = work.reset_index(drop=True)

    payload = work.to_json(orient="records", date_format="iso")
    return _sha256_text(payload)


def _compute_dataset_fingerprint(hourly, shift_summary, results):
    """Compute a content fingerprint for idempotent ingest."""
    h_cols = [
        "date_str", "shift", "shift_hour", "line",
        "total_cases", "total_hours", "oee_pct",
        "availability", "performance", "quality", "product_code",
    ]
    h_fp = _stable_df_fingerprint(hourly, h_cols, sort_cols=["date_str", "shift", "shift_hour", "line"])

    s_cols = ["date_str", "shift", "total_cases", "total_hours", "oee_pct", "cases_per_hour"]
    s_fp = _stable_df_fingerprint(shift_summary, s_cols, sort_cols=["date_str", "shift"])

    # Include top-level KPI snapshot to guard against edge parsing differences.
    plant_data = results.get("Plant Summary", {})
    kpis = plant_data.get("kpis", pd.DataFrame()) if isinstance(plant_data, dict) else pd.DataFrame()
    k_fp = _stable_df_fingerprint(kpis, ["Metric", "Value"], sort_cols=["Metric"])

    return _sha256_text(f"{h_fp}|{s_fp}|{k_fp}")


def _load_records_raw():
    """Load all raw history records from disk."""
    if not os.path.exists(HISTORY_FILE) or os.path.getsize(HISTORY_FILE) == 0:
        return []
    records = []
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _latest_record_for_period(date_from, date_to):
    """Return latest record for a date range, or None."""
    latest = None
    for r in _load_records_raw():
        if r.get("date_from") == date_from and r.get("date_to") == date_to:
            latest = r
    return latest


# =========================================================================
# Layer 1: Raw append log
# =========================================================================

def save_run(results, hourly, shift_summary, overall, downtime=None, ingest_meta=None, output_format=None):
    """Extract key metrics from an analysis run, append to history.jsonl,
    then tend the garden to update plant_trends.json."""

    date_min = hourly["date"].min()
    date_max = hourly["date"].max()
    n_days = hourly["date_str"].nunique()
    total_cases = float(hourly["total_cases"].sum())
    total_hours = float(hourly["total_hours"].sum())
    n_shift_days = hourly.groupby(["date_str", "shift"]).ngroups
    avg_cph = total_cases / (n_shift_days * SHIFT_HOURS) if n_shift_days > 0 else 0.0

    # Extract KPIs from Plant Summary (new shift-centric structure)
    # or fall back to Executive Summary (legacy structure)
    avg_oee = avg_avail = avg_perf = avg_qual = 0.0
    cases_lost = 0.0
    utilization = 0.0

    plant_data = results.get("Plant Summary")
    exec_df = results.get("Executive Summary")  # legacy fallback
    if isinstance(plant_data, dict):
        kpis = plant_data.get("kpis", pd.DataFrame())
        if len(kpis) > 0:
            lookup = dict(zip(kpis["Metric"].astype(str).str.strip(), kpis["Value"]))
            avg_oee = _parse_pct(lookup.get("Overall OEE", "0"))
            avg_avail = _parse_pct(lookup.get("Average Availability", "0"))
            avg_perf = _parse_pct(lookup.get("Average Performance", "0"))
            avg_qual = _parse_pct(lookup.get("Average Quality", "0"))
            cases_lost = _parse_num(lookup.get("Est. Cases Lost vs Benchmark", "0"))
            utilization = _parse_pct(lookup.get("Utilization", "0"))
    elif exec_df is not None:
        lookup = dict(zip(exec_df["Metric"].astype(str).str.strip(), exec_df["Value"]))
        avg_oee = _parse_pct(lookup.get("Average OEE", "0"))
        avg_avail = _parse_pct(lookup.get("Average Availability", "0"))
        avg_perf = _parse_pct(lookup.get("Average Performance", "0"))
        avg_qual = _parse_pct(lookup.get("Average Quality", "0"))
        cases_lost = _parse_num(lookup.get("Est. Cases Lost vs Benchmark", "0"))
        utilization = _parse_pct(lookup.get("Utilization (% Time Producing)", "0"))

    # Extract per-shift data from new structure or fall back to overall DataFrame
    shifts = []
    for shift_label in ["1st Shift", "2nd Shift", "3rd Shift"]:
        shift_data = results.get(shift_label)
        if isinstance(shift_data, dict):
            raw = shift_data.get("raw", {})
            shift_rec = {
                "shift": raw.get("shift_name", shift_label),
                "oee_pct": round(float(raw.get("oee", 0)), 1),
                "cases_per_hour": round(float(raw.get("cph", 0)), 0),
                "total_cases": round(float(raw.get("total_cases", 0)), 0),
                "primary_loss": str(raw.get("primary_loss", "")),
            }
            shifts.append(shift_rec)

    if not shifts:
        # Legacy fallback: read from overall DataFrame
        loss_df = results.get("Loss Breakdown")
        for _, row in overall.iterrows():
            shift_rec = {
                "shift": str(row["shift"]),
                "oee_pct": round(float(row["oee_pct"]), 1),
                "cases_per_hour": round(float(row.get("cases_per_hour", 0)), 0),
                "total_cases": round(float(row.get("total_cases", 0)), 0),
            }
            if loss_df is not None:
                match = loss_df[loss_df["Shift"] == row["shift"]]
                if len(match) > 0:
                    shift_rec["primary_loss"] = str(match.iloc[0].get("Primary Loss Driver", ""))
            shifts.append(shift_rec)

    # Extract top downtime causes from shift data or legacy Pareto
    top_downtime = []
    for shift_label in ["1st Shift", "2nd Shift", "3rd Shift"]:
        shift_data = results.get(shift_label)
        if isinstance(shift_data, dict):
            dt_df = shift_data.get("downtime_causes", pd.DataFrame())
            if len(dt_df) > 0:
                for _, row in dt_df.head(3).iterrows():
                    cause_name = str(row.get("Cause", row.get("cause", "")))
                    minutes = float(row.get("Total Min", row.get("total_min", 0)))
                    pct = float(row.get("% of Shift", row.get("pct_of_shift", 0)))
                    if cause_name and cause_name not in {d["cause"] for d in top_downtime}:
                        top_downtime.append({
                            "cause": cause_name,
                            "minutes": round(minutes, 0),
                            "pct_of_total": round(pct, 1),
                        })
    top_downtime = sorted(top_downtime, key=lambda x: x["minutes"], reverse=True)[:5]

    if not top_downtime:
        pareto_df = results.get("Downtime Pareto")  # legacy fallback
        if pareto_df is not None and len(pareto_df) > 0:
            for _, row in pareto_df.head(5).iterrows():
                top_downtime.append({
                    "cause": str(row["Cause"]),
                    "minutes": round(float(row["Total Minutes"]), 0),
                    "pct_of_total": round(float(row["% of Total"]), 1),
                })

    date_from = date_min.strftime("%Y-%m-%d")
    date_to = date_max.strftime("%Y-%m-%d")
    period_key = f"{date_from}:{date_to}"
    dataset_fingerprint = _compute_dataset_fingerprint(hourly, shift_summary, results)
    latest_for_period = _latest_record_for_period(date_from, date_to)

    # Idempotent ingest: exact same data for the same period should not create
    # a new run or alter trend memory.
    if latest_for_period and latest_for_period.get("dataset_fingerprint") == dataset_fingerprint:
        deduped = dict(latest_for_period)
        deduped["ingest_status"] = "duplicate_ignored"
        deduped["duplicate_of_run_id"] = latest_for_period.get("run_id")
        return deduped

    revision = 1
    supersedes_run_id = None
    if latest_for_period is not None:
        revision = int(latest_for_period.get("revision", 1)) + 1
        supersedes_run_id = latest_for_period.get("run_id")

    record = {
        "run_id": datetime.now().isoformat(),
        "date_from": date_from,
        "date_to": date_to,
        "period_key": period_key,
        "dataset_fingerprint": dataset_fingerprint,
        "revision": revision,
        "supersedes_run_id": supersedes_run_id,
        "n_days": int(n_days),
        "avg_oee": round(avg_oee, 1),
        "avg_availability": round(avg_avail, 1),
        "avg_performance": round(avg_perf, 1),
        "avg_quality": round(avg_qual, 1),
        "utilization": round(utilization, 1),
        "avg_cph": round(avg_cph, 0),
        "total_cases": round(total_cases, 0),
        "total_hours": round(total_hours, 1),
        "cases_lost": round(cases_lost, 0),
        "shifts": shifts,
        "top_downtime": top_downtime,
        "ingest_meta": ingest_meta or {},
        "output_format": output_format or "excel",
    }

    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    run_id = record["run_id"]

    # --- Deep history: persist hourly rows ---
    try:
        with open(HOURLY_FILE, "a", encoding="utf-8") as f:
            for _, row in hourly.iterrows():
                date_val = row.get("date")
                date_str = (date_val.strftime("%Y-%m-%d")
                            if hasattr(date_val, "strftime")
                            else str(row.get("date_str", "")))
                dow = (date_val.strftime("%A")
                       if hasattr(date_val, "strftime")
                       else str(row.get("day_of_week", "")))
                shift_raw = str(row.get("shift", ""))
                h_rec = {
                    "run_id": run_id,
                    "date": date_str,
                    "dow": dow,
                    "shift": _normalize_shift(shift_raw),
                    "hour": int(row.get("shift_hour", 0)),
                    "hours": round(float(row.get("total_hours", 0)), 2),
                    "cases": round(float(row.get("total_cases", 0)), 0),
                    "good": round(float(row.get("good_cases", 0)), 0),
                    "avail": round(float(row.get("availability", 0)), 3),
                    "perf": round(float(row.get("performance", 0)), 3),
                    "qual": round(float(row.get("quality", 0)), 3),
                    "oee": round(float(row.get("oee_pct", 0)), 1),
                    "cph": round(float(row.get("cases_per_hour", 0)), 0),
                    "product": str(row.get("product_code", "")),
                }
                f.write(json.dumps(h_rec) + "\n")
    except Exception:
        pass  # deep history failure should never block the main save

    # --- Deep history: persist shift-daily rows ---
    try:
        # Pre-compute dead hours per date+shift from hourly data
        dead_counts = {}
        for _, row in hourly.iterrows():
            date_val = row.get("date")
            ds = (date_val.strftime("%Y-%m-%d")
                  if hasattr(date_val, "strftime")
                  else str(row.get("date_str", "")))
            shift_raw = str(row.get("shift", ""))
            key = (ds, _normalize_shift(shift_raw))
            if float(row.get("total_cases", 0)) == 0:
                dead_counts[key] = dead_counts.get(key, 0) + 1

        with open(SHIFT_DAILY_FILE, "a", encoding="utf-8") as f:
            for _, row in shift_summary.iterrows():
                date_val = row.get("date")
                date_str = (date_val.strftime("%Y-%m-%d")
                            if hasattr(date_val, "strftime")
                            else str(row.get("date_str", "")))
                dow = (date_val.strftime("%A")
                       if hasattr(date_val, "strftime")
                       else str(row.get("day_of_week", "")))
                shift_raw = str(row.get("shift", ""))
                shift_norm = _normalize_shift(shift_raw)
                key = (date_str, shift_norm)
                sd_rec = {
                    "run_id": run_id,
                    "date": date_str,
                    "dow": dow,
                    "shift": shift_norm,
                    "hours": round(float(row.get("total_hours", 0)), 1),
                    "cases": round(float(row.get("total_cases", 0)), 0),
                    "good": round(float(row.get("good_cases", 0)), 0),
                    "oee": round(float(row.get("oee_pct", 0)), 1),
                    "cph": round(float(row.get("cases_per_hour",
                                 row.get("total_cases", 0) / max(row.get("total_hours", 1), 0.01))), 0),
                    "dead": dead_counts.get(key, 0),
                }
                f.write(json.dumps(sd_rec) + "\n")
    except Exception:
        pass  # deep history failure should never block the main save

    # Persist to Supabase if configured
    try:
        from db import save_run_to_db
        save_run_to_db(record)
    except Exception:
        pass  # database failure should never block the save

    # Tend the garden after every save
    try:
        tend_garden()
    except Exception:
        pass  # gardener failure should never block the save

    return record


def load_history():
    """Read history.jsonl and return structured DataFrames.
    Returns dict with keys: runs, shifts, downtime. Or None if empty."""
    if not os.path.exists(HISTORY_FILE) or os.path.getsize(HISTORY_FILE) == 0:
        return None

    records = _load_records_raw()

    if not records:
        return None

    runs = pd.DataFrame([{
        "run_id": r["run_id"],
        "date_from": r["date_from"],
        "date_to": r["date_to"],
        "n_days": r["n_days"],
        "avg_oee": r["avg_oee"],
        "avg_availability": r["avg_availability"],
        "avg_performance": r["avg_performance"],
        "avg_quality": r["avg_quality"],
        "utilization": r.get("utilization", 0.0),
        "avg_cph": r["avg_cph"],
        "total_cases": r["total_cases"],
        "total_hours": r["total_hours"],
        "cases_lost": r["cases_lost"],
    } for r in records])

    shift_rows = []
    for r in records:
        for s in r.get("shifts", []):
            shift_rows.append({
                "run_id": r["run_id"], "date_from": r["date_from"],
                "shift": s["shift"], "oee_pct": s["oee_pct"],
                "cases_per_hour": s.get("cases_per_hour", 0),
                "total_cases": s.get("total_cases", 0),
                "primary_loss": s.get("primary_loss", ""),
            })
    shifts = pd.DataFrame(shift_rows) if shift_rows else pd.DataFrame()

    dt_rows = []
    for r in records:
        for d in r.get("top_downtime", []):
            dt_rows.append({
                "run_id": r["run_id"], "date_from": r["date_from"],
                "cause": d["cause"], "minutes": d["minutes"],
                "pct_of_total": d["pct_of_total"],
            })
    downtime = pd.DataFrame(dt_rows) if dt_rows else pd.DataFrame()

    return {"runs": runs, "shifts": shifts, "downtime": downtime}


def load_learning_ledger(limit=200):
    """Return learning-ingest ledger as DataFrame (latest first).

    Columns:
      run_id, date_from, date_to, period_key, revision,
      supersedes_run_id, dataset_fingerprint_short, ingested_at
    """
    records = _load_records_raw()
    if not records:
        return pd.DataFrame()

    rows = []
    for r in records:
        rid = str(r.get("run_id", ""))
        rows.append({
            "run_id": rid,
            "date_from": r.get("date_from", ""),
            "date_to": r.get("date_to", ""),
            "period_key": r.get("period_key", f"{r.get('date_from', '')}:{r.get('date_to', '')}"),
            "revision": int(r.get("revision", 1)),
            "supersedes_run_id": r.get("supersedes_run_id") or "",
            "dataset_fingerprint_short": str(r.get("dataset_fingerprint", ""))[:12],
            "ingested_at": rid,
        })

    df = pd.DataFrame(rows)
    # run_id is ISO timestamp in this app, so lexical desc = newest first.
    df = df.sort_values("run_id", ascending=False).reset_index(drop=True)
    if limit and len(df) > int(limit):
        df = df.head(int(limit)).copy()
    return df


def load_hourly_history():
    """Read hourly_history.jsonl → DataFrame or None if empty/missing."""
    if not os.path.exists(HOURLY_FILE) or os.path.getsize(HOURLY_FILE) == 0:
        return None
    records = []
    with open(HOURLY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    if not records:
        return None
    return pd.DataFrame(records)


def load_shift_daily_history():
    """Read shift_daily_history.jsonl → DataFrame or None if empty/missing."""
    if not os.path.exists(SHIFT_DAILY_FILE) or os.path.getsize(SHIFT_DAILY_FILE) == 0:
        return None
    records = []
    with open(SHIFT_DAILY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    if not records:
        return None
    return pd.DataFrame(records)


# =========================================================================
# SPC Engine — Statistical Process Control
# =========================================================================

def _shewhart_limits(series):
    """Shewhart individuals control chart limits.
    Returns (mean, ucl, lcl, sigma)."""
    mean = float(series.mean())
    sigma = float(series.std(ddof=1)) if len(series) > 1 else 0.0
    return mean, mean + 3 * sigma, mean - 3 * sigma, sigma


def _nelson_rules(runs_df):
    """Apply Nelson Rules 1, 2, 3, 5 to OEE series.
    Returns list of finding strings referencing actual date periods."""
    series = runs_df["avg_oee"].values
    dates = runs_df["date_from"].values
    n = len(series)
    if n < 3:
        return []

    mean, ucl, lcl, sigma = _shewhart_limits(pd.Series(series))
    if sigma == 0:
        return []

    findings = []

    # Rule 1: Point beyond 3-sigma
    for i in range(n):
        if series[i] > ucl:
            findings.append(
                f"Run {dates[i]}: OEE {series[i]:.1f}% is above upper control limit "
                f"({ucl:.1f}%) — unusually high performance, investigate what went right")
        elif series[i] < lcl:
            findings.append(
                f"Run {dates[i]}: OEE {series[i]:.1f}% is below lower control limit "
                f"({lcl:.1f}%) — special cause, investigate what broke")

    # Rule 2: 9 consecutive points on same side of mean
    if n >= 9:
        above = series > mean
        for i in range(n - 8):
            window = above[i:i + 9]
            if all(window):
                findings.append(
                    f"9 consecutive runs above mean ({mean:.1f}%) from {dates[i]} to "
                    f"{dates[i + 8]} — sustained improvement detected")
                break
            if not any(window):
                findings.append(
                    f"9 consecutive runs below mean ({mean:.1f}%) from {dates[i]} to "
                    f"{dates[i + 8]} — sustained decline, this is not random")
                break

    # Rule 3: 6 consecutive points steadily increasing or decreasing
    if n >= 6:
        for i in range(n - 5):
            window = series[i:i + 6]
            diffs = np.diff(window)
            if all(d > 0 for d in diffs):
                findings.append(
                    f"6 consecutive improving runs from {dates[i]} to {dates[i + 5]} "
                    f"— upward trend")
                break
            if all(d < 0 for d in diffs):
                findings.append(
                    f"6 consecutive declining runs from {dates[i]} to {dates[i + 5]} "
                    f"— downward trend")
                break

    # Rule 5: 2 of 3 consecutive points beyond 2-sigma (same side)
    sig2_upper = mean + 2 * sigma
    sig2_lower = mean - 2 * sigma
    if n >= 3:
        for i in range(n - 2):
            window = series[i:i + 3]
            above_2 = sum(1 for v in window if v > sig2_upper)
            below_2 = sum(1 for v in window if v < sig2_lower)
            if above_2 >= 2:
                findings.append(
                    f"2 of 3 runs near {dates[i + 1]} are above +2 sigma ({sig2_upper:.1f}%) "
                    f"— probable upward shift")
                break
            if below_2 >= 2:
                findings.append(
                    f"2 of 3 runs near {dates[i + 1]} are below -2 sigma ({sig2_lower:.1f}%) "
                    f"— probable downward shift")
                break

    return findings


# =========================================================================
# Trend Test
# =========================================================================

def _trend_test(series):
    """Test for trend in OEE series.
    Uses pymannkendall if available, otherwise linear regression fallback."""
    if len(series) < 5:
        return None

    try:
        import pymannkendall as mk
        result = mk.original_test(series)
        if result.p < 0.10:
            return (f"OEE trend: {result.trend}, {result.slope:+.2f} pts/run "
                    f"(p={result.p:.3f}, Mann-Kendall)")
        else:
            return f"OEE trend: no statistically significant trend (p={result.p:.2f})"
    except ImportError:
        return _simple_trend_test(series)


def _simple_trend_test(series):
    """Fallback trend test using linear regression slope."""
    x = np.arange(len(series))
    y = series.values

    slope, intercept = np.polyfit(x, y, 1)
    y_pred = slope * x + intercept
    ss_res = ((y - y_pred) ** 2).sum()
    ss_tot = ((y - y.mean()) ** 2).sum()
    r_sq = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

    if abs(slope) < 0.3 or r_sq < 0.1:
        return f"OEE trend: flat ({slope:+.2f} pts/run, R²={r_sq:.2f})"

    direction = "improving" if slope > 0 else "declining"
    return f"OEE trend: {direction} at {slope:+.1f} pts/run (R²={r_sq:.2f})"


# =========================================================================
# Downtime Classification — chronic vs acute vs emerging
# =========================================================================

def _classify_downtime(runs_df, downtime_df):
    """Classify each downtime cause as chronic, emerging, or acute.
    Returns list of dicts sorted by total minutes descending."""
    if len(downtime_df) == 0 or len(runs_df) < 2:
        return []

    n_runs = len(runs_df)
    cause_stats = (
        downtime_df.groupby("cause")
        .agg(total_minutes=("minutes", "sum"), appearances=("run_id", "nunique"))
        .reset_index()
    )

    # Which causes appeared in the last 4 runs?
    recent_run_ids = set(runs_df["run_id"].tail(min(4, n_runs)).values)

    # Which causes were #1 in each run?
    rank1_counts = {}
    for rid in runs_df["run_id"].values:
        run_dt = downtime_df[downtime_df["run_id"] == rid]
        if len(run_dt) > 0:
            top_cause = run_dt.sort_values("minutes", ascending=False).iloc[0]["cause"]
            rank1_counts[top_cause] = rank1_counts.get(top_cause, 0) + 1

    # Count consecutive recent appearances (from most recent backward)
    run_ids_ordered = list(runs_df["run_id"].values)
    results = []
    for _, row in cause_stats.iterrows():
        cause = row["cause"]
        appearances = int(row["appearances"])
        total_min = float(row["total_minutes"])
        pct_runs = appearances / n_runs

        # Count consecutive streak from the end
        cause_run_ids = set(downtime_df[downtime_df["cause"] == cause]["run_id"].values)
        streak = 0
        for rid in reversed(run_ids_ordered):
            if rid in cause_run_ids:
                streak += 1
            else:
                break

        in_recent = sum(1 for rid in recent_run_ids if rid in cause_run_ids)
        times_rank1 = rank1_counts.get(cause, 0)

        # Classification logic
        if streak >= 4 or (appearances >= 4 and pct_runs >= 0.6):
            status = "chronic"
        elif in_recent >= 2 and streak >= 2 and appearances < 4:
            status = "emerging"
        else:
            status = "intermittent"

        results.append({
            "cause": cause,
            "status": status,
            "appearances": appearances,
            "total_minutes": round(total_min, 0),
            "pct_runs": round(pct_runs * 100, 0),
            "current_streak": streak,
            "times_rank1": times_rank1,
        })

    return sorted(results, key=lambda x: x["total_minutes"], reverse=True)


# =========================================================================
# Shift Analysis
# =========================================================================

def _analyze_shifts(runs_df, shifts_df, plant_mean):
    """Analyze per-shift trends. Returns dict of shift_name -> stats."""
    if len(shifts_df) == 0 or len(runs_df) < 2:
        return {}

    shift_trends = {}
    for shift_name in shifts_df["shift"].unique():
        sdata = shifts_df[shifts_df["shift"] == shift_name].merge(
            runs_df[["run_id", "date_from"]], on="run_id"
        ).sort_values("date_from")

        if len(sdata) < 2:
            continue

        current = float(sdata.iloc[-1]["oee_pct"])
        avg_4 = round(float(sdata.tail(4)["oee_pct"].mean()), 1)
        below_count = int((sdata["oee_pct"] < plant_mean).sum())

        # Direction from last 3 runs
        if len(sdata) >= 3:
            last3 = sdata.tail(3)["oee_pct"].values
            if all(last3[i] < last3[i + 1] for i in range(len(last3) - 1)):
                direction = "improving"
            elif all(last3[i] > last3[i + 1] for i in range(len(last3) - 1)):
                direction = "declining"
            else:
                direction = "stable"
        else:
            direction = "stable"

        shift_trends[shift_name] = {
            "current_oee": round(current, 1),
            "4run_avg": avg_4,
            "direction": direction,
            "runs_below_plant_mean": below_count,
            "total_runs": len(sdata),
        }

    return shift_trends


# =========================================================================
# The Gardener — tend_garden()
# =========================================================================

def tend_garden():
    """Consolidate raw history into structured trends with SPC analysis.
    Reads history.jsonl, writes plant_trends.json."""
    history = load_history()
    if history is None:
        return None

    runs = history["runs"]
    shifts = history["shifts"]
    downtime = history["downtime"]

    # --- Deduplicate: same date range analyzed twice → keep latest ---
    runs_deduped = runs.drop_duplicates(
        subset=["date_from", "date_to"], keep="last"
    ).reset_index(drop=True)

    valid_ids = set(runs_deduped["run_id"])
    if len(shifts) > 0:
        shifts = shifts[shifts["run_id"].isin(valid_ids)].reset_index(drop=True)
    if len(downtime) > 0:
        downtime = downtime[downtime["run_id"].isin(valid_ids)].reset_index(drop=True)

    n_runs = len(runs_deduped)
    oee_series = runs_deduped["avg_oee"]
    findings = []

    # --- SPC: Shewhart control limits ---
    spc = {}
    if n_runs >= 3:
        mean, ucl, lcl, sigma = _shewhart_limits(oee_series)
        spc = {
            "mean": round(mean, 1),
            "ucl": round(ucl, 1),
            "lcl": round(lcl, 1),
            "sigma": round(sigma, 2),
        }

        # Nelson Rules
        nelson = _nelson_rules(runs_deduped)
        findings.extend(nelson)

    # --- Trend test ---
    trend_result = _trend_test(oee_series)
    if trend_result:
        findings.append(trend_result)

    # --- Week-over-week ---
    wow = None
    if n_runs >= 2:
        latest = runs_deduped.iloc[-1]
        previous = runs_deduped.iloc[-2]
        oee_d = round(latest["avg_oee"] - previous["avg_oee"], 1)
        cph_d = round(latest["avg_cph"] - previous["avg_cph"], 0)
        wow = {
            "oee_delta": oee_d,
            "cph_delta": cph_d,
            "latest_period": f"{latest['date_from']} to {latest['date_to']}",
            "previous_period": f"{previous['date_from']} to {previous['date_to']}",
        }
        direction = "up" if oee_d > 0 else "down" if oee_d < 0 else "flat"
        findings.append(
            f"Week-over-week: OEE {direction} {abs(oee_d):.1f} pts "
            f"({previous['avg_oee']:.1f}% → {latest['avg_oee']:.1f}%)")

    # --- Downtime classification ---
    dt_classes = _classify_downtime(runs_deduped, downtime)
    chronic = [d for d in dt_classes if d["status"] == "chronic"]
    for d in chronic:
        extra = f", #1 cause in {d['times_rank1']} runs" if d["times_rank1"] > 0 else ""
        findings.append(
            f"{d['cause']}: CHRONIC — present in {d['appearances']}/{n_runs} runs "
            f"({d['current_streak']} consecutive), "
            f"{d['total_minutes']:,.0f} total minutes{extra}")
    emerging = [d for d in dt_classes if d["status"] == "emerging"]
    for d in emerging:
        findings.append(
            f"{d['cause']}: EMERGING — appeared in last {d['current_streak']} "
            f"consecutive runs, watch this one")

    # --- Shift analysis ---
    plant_mean = float(oee_series.mean()) if n_runs > 0 else 0
    shift_trends = _analyze_shifts(runs_deduped, shifts, plant_mean)
    for sname, sdata in shift_trends.items():
        if sdata["runs_below_plant_mean"] >= sdata["total_runs"] * 0.8 and sdata["total_runs"] >= 3:
            findings.append(
                f"{sname}: below plant mean in {sdata['runs_below_plant_mean']}/"
                f"{sdata['total_runs']} runs — consistent underperformer")
        if sdata["direction"] == "declining" and sdata["total_runs"] >= 3:
            findings.append(
                f"{sname}: declining 3 consecutive runs "
                f"(current {sdata['current_oee']:.1f}%, 4-run avg {sdata['4run_avg']:.1f}%)")

    # --- Quality check ---
    if n_runs >= 3:
        if (runs_deduped["avg_quality"] > 97).all():
            findings.append(
                f"Quality: consistently >{runs_deduped['avg_quality'].min():.0f}% across "
                f"all runs — not the problem, focus on Availability and Performance")

    # --- Primary loss pattern ---
    if n_runs >= 3 and len(shifts) > 0 and "primary_loss" in shifts.columns:
        loss_counts = shifts["primary_loss"].value_counts()
        if len(loss_counts) > 0:
            top_loss = loss_counts.index[0]
            top_pct = loss_counts.iloc[0] / len(shifts) * 100
            if top_pct > 60:
                findings.append(
                    f"Primary loss driver: {top_loss} in {top_pct:.0f}% of shift-runs "
                    f"— this is the lever to pull")

    # --- Deep history analytics ---
    deep_history = {}
    try:
        hourly_df = load_hourly_history()
        sd_df = load_shift_daily_history()

        # Deduplicate: keep latest run_id for each (date, shift, hour) / (date, shift)
        if hourly_df is not None and len(hourly_df) > 0:
            hourly_df = hourly_df[hourly_df["run_id"].isin(valid_ids)]
            hourly_df = hourly_df.drop_duplicates(
                subset=["date", "shift", "hour"], keep="last"
            ).reset_index(drop=True)

            # --- Hour-of-day pattern (aggregate) ---
            hod = hourly_df.groupby("hour").agg(
                avg_oee=("oee", "mean"),
                avg_cph=("cph", "mean"),
                total_rows=("oee", "count"),
                dead_rows=("cases", lambda x: (x == 0).sum()),
            ).reset_index()
            hod["dead_pct"] = round(hod["dead_rows"] / hod["total_rows"] * 100, 1)
            hod["avg_oee"] = hod["avg_oee"].round(1)
            hod["avg_cph"] = hod["avg_cph"].round(0)
            deep_history["hour_of_day"] = hod.to_dict(orient="records")

            # --- Hour-of-day by shift ---
            hod_shift = hourly_df.groupby(["hour", "shift"]).agg(
                avg_oee=("oee", "mean"),
                avg_cph=("cph", "mean"),
                total_rows=("oee", "count"),
                dead_rows=("cases", lambda x: (x == 0).sum()),
            ).reset_index()
            hod_shift["dead_pct"] = round(hod_shift["dead_rows"] / hod_shift["total_rows"] * 100, 1)
            hod_shift["avg_oee"] = hod_shift["avg_oee"].round(1)
            hod_shift["avg_cph"] = hod_shift["avg_cph"].round(0)
            deep_history["hour_of_day_by_shift"] = hod_shift.to_dict(orient="records")

            # Determinations: worst/best hour
            if len(hod) > 1:
                worst_h = hod.loc[hod["avg_oee"].idxmin()]
                best_h = hod.loc[hod["avg_oee"].idxmax()]
                findings.append(
                    f"Hour-of-day: Hour {int(worst_h['hour'])} is worst "
                    f"({worst_h['avg_oee']:.1f}% OEE), "
                    f"Hour {int(best_h['hour'])} is best "
                    f"({best_h['avg_oee']:.1f}% OEE)")
                # Dead hour hotspot
                worst_dead = hod.loc[hod["dead_pct"].idxmax()]
                if worst_dead["dead_pct"] > 0:
                    findings.append(
                        f"Dead hour hotspot: Hour {int(worst_dead['hour'])} has "
                        f"{worst_dead['dead_pct']:.0f}% zero-production rate")

        if sd_df is not None and len(sd_df) > 0:
            sd_df = sd_df[sd_df["run_id"].isin(valid_ids)]
            sd_df = sd_df.drop_duplicates(
                subset=["date", "shift"], keep="last"
            ).reset_index(drop=True)

            # --- Day-of-week pattern ---
            dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday",
                         "Friday", "Saturday", "Sunday"]
            dow = sd_df.groupby("dow").agg(
                avg_oee=("oee", "mean"),
                avg_cph=("cph", "mean"),
                shifts_analyzed=("oee", "count"),
                total_dead=("dead", "sum"),
            ).reset_index()
            dow["avg_oee"] = dow["avg_oee"].round(1)
            dow["avg_cph"] = dow["avg_cph"].round(0)
            dow["total_dead"] = dow["total_dead"].astype(int)
            # Sort by day-of-week order
            dow["_sort"] = dow["dow"].apply(
                lambda x: dow_order.index(x) if x in dow_order else 99)
            dow = dow.sort_values("_sort").drop(columns="_sort")
            deep_history["day_of_week"] = dow.to_dict(orient="records")

            # Day-of-week determination
            if len(dow) > 1:
                worst_day = dow.loc[dow["avg_oee"].idxmin()]
                best_day = dow.loc[dow["avg_oee"].idxmax()]
                spread = round(best_day["avg_oee"] - worst_day["avg_oee"], 1)
                if spread > 3:
                    findings.append(
                        f"Day-of-week spread: {spread} pts "
                        f"({worst_day['dow']} {worst_day['avg_oee']:.1f}% → "
                        f"{best_day['dow']} {best_day['avg_oee']:.1f}%)")

            # --- Shift gap trending (per-date per-shift OEE + 7-day rolling) ---
            sd_sorted = sd_df.sort_values(["date", "shift"])
            gap_records = []
            for shift_name in sd_sorted["shift"].unique():
                sdata = sd_sorted[sd_sorted["shift"] == shift_name].copy()
                sdata = sdata.sort_values("date")
                sdata["rolling_7d"] = sdata["oee"].rolling(7, min_periods=1).mean().round(1)
                for _, row in sdata.iterrows():
                    gap_records.append({
                        "date": row["date"],
                        "shift": row["shift"],
                        "oee": round(float(row["oee"]), 1),
                        "rolling_7d": round(float(row["rolling_7d"]), 1),
                    })
            deep_history["shift_gap_trend"] = gap_records

            # --- Shift consistency (std dev, CV%, min/max per shift) ---
            consistency = []
            for shift_name in sd_df["shift"].unique():
                sdata = sd_df[sd_df["shift"] == shift_name]["oee"]
                if len(sdata) >= 2:
                    std = float(sdata.std(ddof=1))
                    mean_val = float(sdata.mean())
                    cv = (std / mean_val * 100) if mean_val > 0 else 0.0
                    consistency.append({
                        "shift": shift_name,
                        "std_dev": round(std, 1),
                        "cv_pct": round(cv, 1),
                        "min_oee": round(float(sdata.min()), 1),
                        "max_oee": round(float(sdata.max()), 1),
                        "range": round(float(sdata.max() - sdata.min()), 1),
                        "n": len(sdata),
                    })
            deep_history["shift_consistency"] = consistency
    except Exception:
        pass  # deep history analytics failure should never block trends

    # --- Build output ---
    trends = {
        "last_tended": datetime.now().isoformat(),
        "total_runs": n_runs,
        "total_runs_raw": len(runs),
        "duplicates_removed": len(runs) - n_runs,
        "spc": spc,
        "week_over_week": wow,
        "shift_trends": shift_trends,
        "downtime_classifications": dt_classes,
        "deep_history": deep_history,
        "determinations": findings,
        "runs": runs_deduped.to_dict(orient="records"),
        "shifts": shifts.to_dict(orient="records") if len(shifts) > 0 else [],
        "downtime": downtime.to_dict(orient="records") if len(downtime) > 0 else [],
    }

    with open(TRENDS_FILE, "w", encoding="utf-8") as f:
        json.dump(trends, f, indent=2, default=str)

    # Update Supabase baselines if configured
    try:
        from db import upsert_baseline
        if len(downtime) > 0:
            cause_stats = (
                downtime.groupby("cause")
                .agg(
                    avg_minutes=("minutes", "mean"),
                    std_minutes=("minutes", "std"),
                    min_minutes=("minutes", "min"),
                    max_minutes=("minutes", "max"),
                    n_events=("minutes", "count"),
                )
                .reset_index()
            )
            for _, row in cause_stats.iterrows():
                std_val = float(row["std_minutes"]) if pd.notna(row["std_minutes"]) else 0
                upsert_baseline(
                    cause=row["cause"],
                    avg_minutes=float(row["avg_minutes"]),
                    std_minutes=std_val,
                    min_minutes=float(row["min_minutes"]),
                    max_minutes=float(row["max_minutes"]),
                    n_events=int(row["n_events"]),
                )
    except Exception:
        pass  # database failure should never block gardening

    # Compact raw JSONL files to reclaim space from duplicates
    try:
        compact_history(valid_ids)
    except Exception:
        pass  # compaction failure should never block gardening

    return trends


# =========================================================================
# Layer 2: Read tended trends
# =========================================================================

def load_trends():
    """Load plant_trends.json. Returns dict or None.
    If the file doesn't exist but history does, tends the garden first."""
    if not os.path.exists(TRENDS_FILE):
        # Try to build from raw history
        if os.path.exists(HISTORY_FILE):
            return tend_garden()
        return None

    with open(TRENDS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


# =========================================================================
# Garbage Collection — compact raw JSONL files
# =========================================================================

def compact_history(valid_ids=None):
    """Rewrite JSONL files to remove stale/duplicate rows.

    If valid_ids is provided, only rows with matching run_id are kept.
    Also deduplicates: history by (date_from, date_to), hourly by
    (date, shift, hour), shift_daily by (date, shift) — keeping latest.

    Returns dict with compaction stats.
    """
    stats = {"history": 0, "hourly": 0, "shift_daily": 0}

    # --- Compact history.jsonl ---
    if os.path.exists(HISTORY_FILE) and os.path.getsize(HISTORY_FILE) > 0:
        records = []
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        before = len(records)
        if valid_ids is not None:
            records = [r for r in records if r["run_id"] in valid_ids]
        else:
            # Dedup by (date_from, date_to), keep last
            seen = {}
            for r in records:
                key = (r["date_from"], r["date_to"])
                seen[key] = r
            records = list(seen.values())
        stats["history"] = before - len(records)
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, default=str) + "\n")
        if valid_ids is None:
            valid_ids = {r["run_id"] for r in records}

    # --- Compact hourly_history.jsonl ---
    if os.path.exists(HOURLY_FILE) and os.path.getsize(HOURLY_FILE) > 0:
        records = []
        with open(HOURLY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        before = len(records)
        if valid_ids:
            records = [r for r in records if r.get("run_id") in valid_ids]
        # Dedup by (date, shift, hour), keep last
        seen = {}
        for r in records:
            key = (r.get("date"), r.get("shift"), r.get("hour"))
            seen[key] = r
        records = list(seen.values())
        stats["hourly"] = before - len(records)
        with open(HOURLY_FILE, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, default=str) + "\n")

    # --- Compact shift_daily_history.jsonl ---
    if os.path.exists(SHIFT_DAILY_FILE) and os.path.getsize(SHIFT_DAILY_FILE) > 0:
        records = []
        with open(SHIFT_DAILY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        before = len(records)
        if valid_ids:
            records = [r for r in records if r.get("run_id") in valid_ids]
        # Dedup by (date, shift), keep last
        seen = {}
        for r in records:
            key = (r.get("date"), r.get("shift"))
            seen[key] = r
        records = list(seen.values())
        stats["shift_daily"] = before - len(records)
        with open(SHIFT_DAILY_FILE, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, default=str) + "\n")

    return stats


# =========================================================================
# Helpers
# =========================================================================

def _parse_pct(val):
    """Parse '29.5%' or '85.3%' to float."""
    s = str(val).strip().rstrip("%")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_num(val):
    """Parse '1,234' or '1234.5' to float."""
    s = str(val).strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0
