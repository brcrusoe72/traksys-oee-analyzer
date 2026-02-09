"""
History + SPC Engine for Traksys OEE Analyzer
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
from datetime import datetime

import numpy as np
import pandas as pd

_DIR = os.path.dirname(__file__)
HISTORY_FILE = os.path.join(_DIR, "history.jsonl")
TRENDS_FILE = os.path.join(_DIR, "plant_trends.json")


# =========================================================================
# Layer 1: Raw append log
# =========================================================================

def save_run(results, hourly, shift_summary, overall, downtime=None):
    """Extract key metrics from an analysis run, append to history.jsonl,
    then tend the garden to update plant_trends.json."""

    date_min = hourly["date"].min()
    date_max = hourly["date"].max()
    n_days = hourly["date_str"].nunique()
    total_cases = float(hourly["total_cases"].sum())
    total_hours = float(hourly["total_hours"].sum())
    avg_cph = total_cases / total_hours if total_hours > 0 else 0.0

    exec_df = results.get("Executive Summary")
    avg_oee = avg_avail = avg_perf = avg_qual = 0.0
    cases_lost = 0.0
    if exec_df is not None:
        lookup = dict(zip(exec_df["Metric"].astype(str).str.strip(), exec_df["Value"]))
        avg_oee = _parse_pct(lookup.get("Average OEE", "0"))
        avg_avail = _parse_pct(lookup.get("Average Availability", "0"))
        avg_perf = _parse_pct(lookup.get("Average Performance", "0"))
        avg_qual = _parse_pct(lookup.get("Average Quality", "0"))
        cases_lost = _parse_num(lookup.get("Est. Cases Lost vs Benchmark", "0"))

    shifts = []
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

    top_downtime = []
    pareto_df = results.get("Downtime Pareto")
    if pareto_df is not None and len(pareto_df) > 0:
        for _, row in pareto_df.head(5).iterrows():
            top_downtime.append({
                "cause": str(row["Cause"]),
                "minutes": round(float(row["Total Minutes"]), 0),
                "pct_of_total": round(float(row["% of Total"]), 1),
            })

    record = {
        "run_id": datetime.now().isoformat(),
        "date_from": date_min.strftime("%Y-%m-%d"),
        "date_to": date_max.strftime("%Y-%m-%d"),
        "n_days": int(n_days),
        "avg_oee": round(avg_oee, 1),
        "avg_availability": round(avg_avail, 1),
        "avg_performance": round(avg_perf, 1),
        "avg_quality": round(avg_qual, 1),
        "avg_cph": round(avg_cph, 0),
        "total_cases": round(total_cases, 0),
        "total_hours": round(total_hours, 1),
        "cases_lost": round(cases_lost, 0),
        "shifts": shifts,
        "top_downtime": top_downtime,
    }

    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

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

    records = []
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

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
        "determinations": findings,
        "runs": runs_deduped.to_dict(orient="records"),
        "shifts": shifts.to_dict(orient="records") if len(shifts) > 0 else [],
        "downtime": downtime.to_dict(orient="records") if len(downtime) > 0 else [],
    }

    with open(TRENDS_FILE, "w", encoding="utf-8") as f:
        json.dump(trends, f, indent=2, default=str)

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
