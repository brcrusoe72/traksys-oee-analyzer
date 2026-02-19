"""
Shift Deep Dive — Standalone Analysis Report
==============================================
Generates a polished, presentation-ready Excel report
focused on any shift's performance on Line 2.

Replaces third_shift_report.py and third_shift_targets.py.

Usage:
  python shift_report.py oee_export.xlsx --shift "3rd" [--downtime kb.json] [--product prod.json]
"""

import sys
import os
import json
import re
from datetime import datetime, timedelta
from collections import Counter, defaultdict
import pandas as pd
import numpy as np

from analyze import (
    _aggregate_oee, _compute_utilization, _build_dead_hour_narrative,
    _correlate_dead_hours_with_events, load_oee_data,
)
from shared import (
    EXCLUDE_REASONS, SHIFT_HOURS, classify_fault, normalize_product,
    PRODUCT_RATED_SPEED, PRODUCT_PACK_TYPE, PRODUCT_TARGET, PRODUCT_PACK,
    IS_TRAYED,
    extract_equipment_mentions, summarize_issues, classify_support,
)

LINE_NAME = "Line 2 - Flex (Labeling)"


# ---------------------------------------------------------------------------
# Shift detection and benchmark selection
# ---------------------------------------------------------------------------

def detect_shifts(hourly):
    """Return sorted list of available shift names from data."""
    return sorted(hourly["shift"].unique().tolist())


def _detect_shift(actual_shifts, pattern):
    """Find the actual shift name matching a pattern like '3rd'."""
    for s in actual_shifts:
        if pattern.lower() in s.lower():
            return s
    return None


def pick_benchmark_shift(hourly, overall, target_shift):
    """Auto-select the best OTHER shift as benchmark (highest CPH, not target)."""
    other = overall[overall["shift"] != target_shift].copy()
    if len(other) == 0:
        return None
    best_idx = other["cases_per_hour"].idxmax()
    return other.loc[best_idx, "shift"]


def _shift_label(shift_name):
    """Extract short label like '3rd' from full shift name like '3rd (11p-7a)'."""
    if not shift_name:
        return ""
    m = re.match(r"(\d+\w*)", shift_name)
    return m.group(1) if m else shift_name.split()[0] if shift_name else ""


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data(oee_path, dt_path=None, product_path=None, shift_pattern="3rd"):
    """Load OEE data and optional downtime/product data, filtered to target shift."""
    hourly, shift_summary, overall, hour_avg = load_oee_data(oee_path)

    downtime = None
    if dt_path and os.path.exists(dt_path):
        if dt_path.lower().endswith(".json"):
            with open(dt_path, "r", encoding="utf-8") as f:
                kb = json.load(f)
            reasons_df = pd.DataFrame(kb.get("downtime_reason_codes", []))
            if len(reasons_df) > 0:
                for col in ["total_minutes", "total_occurrences", "total_hours"]:
                    reasons_df[col] = pd.to_numeric(reasons_df[col], errors="coerce").fillna(0)
            downtime = {
                "reasons_df": reasons_df,
                "findings": kb.get("key_findings", []),
                "shift_samples": kb.get("sample_data", {}).get("shift_report_sample_sheet_1_05_26", []),
                "meta": kb.get("metadata", {}),
                "oee_summary": kb.get("metadata", {}).get("oee_period_summary", {}),
            }
        else:
            from parse_mes import detect_file_type, parse_event_summary
            dt_type = detect_file_type(dt_path)
            if dt_type == "event_summary":
                downtime = parse_event_summary(dt_path)

    product_data = None
    if product_path and os.path.exists(product_path):
        with open(product_path, "r", encoding="utf-8") as f:
            pdata = json.load(f)

        runs = pd.DataFrame(pdata.get("product_runs", []))
        if len(runs) > 0:
            # Match shift in product data (e.g. "3rd Shift")
            shift_label_in_data = None
            for s in runs["shift"].unique():
                if shift_pattern.lower() in s.lower():
                    shift_label_in_data = s
                    break
            if shift_label_in_data:
                runs = runs[runs["shift"] == shift_label_in_data].copy()
            else:
                runs = runs.head(0)

            runs["product_family"] = runs["product"].apply(normalize_product)
            runs["oee_display"] = pd.to_numeric(runs["oee_pct"], errors="coerce") * 100
            runs["cases_produced"] = pd.to_numeric(runs["cases_produced"], errors="coerce")
            runs["downtime_minutes"] = pd.to_numeric(runs["downtime_minutes"], errors="coerce")
            runs["changeover_minutes"] = pd.to_numeric(runs["changeover_minutes"], errors="coerce")
            runs["equipment_mentioned"] = runs["notes"].apply(extract_equipment_mentions)

        product_data = {
            "runs": runs,
            "products_ref": pd.DataFrame(pdata.get("products", [])),
            "changeovers": pd.DataFrame(pdata.get("changeovers", [])),
            "meta": pdata.get("metadata", {}),
        }

    return hourly, shift_summary, overall, hour_avg, downtime, product_data


def load_downtime_pareto(downtime_path):
    """Load MES machine data for target tracking sheets."""
    if not downtime_path or not os.path.exists(downtime_path):
        return None, None, None
    with open(downtime_path, "r", encoding="utf-8") as f:
        kb = json.load(f)
    reason_codes = kb.get("downtime_reason_codes", [])
    pareto_data = kb.get("pareto_top_10", {})
    oee_summary = kb.get("metadata", {}).get("oee_period_summary", {})
    return reason_codes, pareto_data, oee_summary


# ---------------------------------------------------------------------------
# Target tracking (from third_shift_targets.py)
# ---------------------------------------------------------------------------

def aggregate_daily(runs):
    """Aggregate multiple runs of same product on same date."""
    runs = runs.copy()
    runs["is_trayed"] = runs["product_family"].apply(lambda x: x in IS_TRAYED)
    runs["equipment"] = runs["notes"].apply(extract_equipment_mentions)
    runs = runs.dropna(subset=["cases_produced", "oee_display"], how="all").copy()
    runs = runs[~((runs["cases_produced"].fillna(0) < 50) & (runs["oee_display"].fillna(0) < 1))].copy()

    if len(runs) == 0:
        return pd.DataFrame()

    grouped = (
        runs.groupby(["date", "product_family"])
        .agg(
            total_cases=("cases_produced", "sum"),
            avg_oee=("oee_display", "mean"),
            total_dt=("downtime_minutes", "sum"),
            total_co=("changeover_minutes", "sum"),
            n_runs=("oee_display", "count"),
            notes_combined=("notes", lambda x: ";; ".join([str(n) for n in x if pd.notna(n)])),
            equip_combined=("equipment", lambda x: list(set(e for sublist in x for e in sublist))),
            is_trayed=("is_trayed", "first"),
        )
        .reset_index()
        .sort_values("date")
    )
    return grouped


_TARGET_COL_KEYS = [
    "Week", "Date", "Day", "Product", "Pack", "Target",
    "Actual", "Gap", "OEE%", "Status", "DT min", "CO min",
    "Equipment Hit", "Notes",
]


def _target_empty_row():
    return {k: "" for k in _TARGET_COL_KEYS}


def _target_week_summary(week_label, cases_list, oee_list, hits, total):
    total_cases = sum(c for c in cases_list if c > 0)
    avg_oee = np.mean([o for o in oee_list if o > 0]) if any(o > 0 for o in oee_list) else 0
    return {
        "Week": f">> {week_label}", "Date": "", "Day": f"{total}d",
        "Product": "", "Pack": "", "Target": "",
        "Actual": f"{total_cases:,.0f}", "Gap": "", "OEE%": f"{avg_oee:.1f}",
        "Status": f"{hits}/{total} hit", "DT min": "", "CO min": "",
        "Equipment Hit": "", "Notes": "",
    }


def build_week_by_week(daily):
    """Build Week by Week sheet with HIT/MISSED/CLOSE target tracking."""
    rows = []
    daily = daily.copy()
    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily["week_label"] = daily["date_dt"].apply(
        lambda d: f"Wk of {(d - timedelta(days=d.weekday())).strftime('%b %d')}")
    daily["day_name"] = daily["date_dt"].dt.day_name()
    daily = daily.sort_values("date_dt")

    current_week = None
    week_cases, week_oees = [], []
    week_hits = week_total = 0

    for _, r in daily.iterrows():
        wk = r["week_label"]
        if wk != current_week:
            if current_week is not None and week_total > 0:
                rows.append(_target_week_summary(current_week, week_cases, week_oees, week_hits, week_total))
                rows.append(_target_empty_row())
            current_week = wk
            week_cases, week_oees = [], []
            week_hits = week_total = 0

        target = PRODUCT_TARGET.get(r["product_family"], 0)
        pack = PRODUCT_PACK.get(r["product_family"], "")
        cases = r["total_cases"] if pd.notna(r["total_cases"]) else 0
        oee = r["avg_oee"] if pd.notna(r["avg_oee"]) else 0
        gap = cases - target if target > 0 else 0
        dt = r["total_dt"] if pd.notna(r["total_dt"]) else ""
        co = r["total_co"] if pd.notna(r["total_co"]) else ""

        if target == 0:
            status = ""
        elif cases >= target:
            status = "HIT"
            week_hits += 1
        elif cases >= target * 0.85:
            status = "CLOSE"
        else:
            status = "MISSED"

        issues = summarize_issues(r["notes_combined"])
        equip_short = ", ".join(r["equip_combined"][:3]) if r["equip_combined"] else ""
        week_cases.append(cases)
        week_oees.append(oee)
        week_total += 1

        rows.append({
            "Week": wk, "Date": r["date"], "Day": r["day_name"][:3],
            "Product": r["product_family"], "Pack": pack,
            "Target": f"{target:,}" if target > 0 else "",
            "Actual": f"{cases:,.0f}" if cases > 0 else "",
            "Gap": f"{gap:+,.0f}" if target > 0 and cases > 0 else "",
            "OEE%": f"{oee:.1f}" if oee > 0 else "",
            "Status": status,
            "DT min": f"{dt:.0f}" if dt != "" and dt > 0 else "",
            "CO min": f"{co:.0f}" if co != "" and co > 0 else "",
            "Equipment Hit": equip_short,
            "Notes": issues if status in ("MISSED", "CLOSE", "") else "",
        })

    if current_week is not None and week_total > 0:
        rows.append(_target_week_summary(current_week, week_cases, week_oees, week_hits, week_total))

    # Period summary
    rows.append(_target_empty_row())
    all_cases = daily["total_cases"].sum()
    all_oee = daily["avg_oee"].mean()
    total_runs = len(daily)
    target_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_TARGET.get(x, 0) > 0)]
    if len(target_runs) > 0:
        hits = sum(1 for _, r in target_runs.iterrows()
                   if pd.notna(r["total_cases"]) and r["total_cases"] >= PRODUCT_TARGET.get(r["product_family"], 0))
        hit_rate = hits / len(target_runs) * 100
    else:
        hits, hit_rate = 0, 0

    rows.append({
        "Week": "TOTAL",
        "Date": f"{daily['date'].min()} to {daily['date'].max()}",
        "Day": f"{total_runs}d", "Product": "", "Pack": "", "Target": "",
        "Actual": f"{all_cases:,.0f}", "Gap": "", "OEE%": f"{all_oee:.1f}",
        "Status": f"{hits}/{len(target_runs)} ({hit_rate:.0f}%)",
        "DT min": "", "CO min": "", "Equipment Hit": "", "Notes": "",
    })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Narrative sheets (from third_shift_targets.py)
# ---------------------------------------------------------------------------

def _ds_section(rows, title):
    rows.append({"Section": title, "Detail": ""})

def _ds_row(rows, detail):
    rows.append({"Section": "", "Detail": detail})

def _ds_blank(rows):
    rows.append({"Section": "", "Detail": ""})


def build_data_says(daily, runs, shift_label, reason_codes=None, pareto=None, oee_summary=None):
    """Build 'The Data Says' sheet — data-driven support case."""
    rows = []
    all_oee = daily["avg_oee"].mean()
    std_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_PACK.get(x, "") in ("8pk", "12pk"))]
    tray_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_PACK.get(x, "") == "Trayed")]
    target_runs_df = daily[daily["product_family"].apply(lambda x: PRODUCT_TARGET.get(x, 0) > 0)]

    if len(target_runs_df) > 0:
        hits = sum(1 for _, r in target_runs_df.iterrows()
                   if pd.notna(r["total_cases"]) and r["total_cases"] >= PRODUCT_TARGET.get(r["product_family"], 0))
        hit_rate = hits / len(target_runs_df) * 100
    else:
        hit_rate, hits = 0, 0
    total_days = len(daily)

    _ds_section(rows, "BOTTOM LINE")
    _ds_row(rows, f"Hit target: {hits}/{len(target_runs_df)} nights ({hit_rate:.0f}%)")
    _ds_row(rows, f"Avg OEE: {all_oee:.1f}%")
    if len(std_runs) > 0:
        _ds_row(rows, f"Standard products: {std_runs['avg_oee'].mean():.1f}% OEE avg ({len(std_runs)} nights)")
    if len(tray_runs) > 0:
        _ds_row(rows, f"Trayed products: {tray_runs['avg_oee'].mean():.1f}% OEE avg ({len(tray_runs)} nights)")

    best_day = daily.loc[daily["avg_oee"].idxmax()]
    worst_day = daily.loc[daily["avg_oee"].idxmin()]
    _ds_row(rows, f"Best night: {best_day['date']} — {best_day['avg_oee']:.1f}% OEE, "
                  f"{best_day['total_cases']:,.0f} cases ({best_day['product_family']})")
    _ds_row(rows, f"Worst night: {worst_day['date']} — {worst_day['avg_oee']:.1f}% OEE, "
                  f"{worst_day['total_cases']:,.0f} cases ({worst_day['product_family']})")
    _ds_row(rows, "The line CAN run. The best nights prove it. The question is why it doesn't every night.")
    _ds_blank(rows)

    riverwood_min = 0
    riverwood_events = 0
    if reason_codes:
        _ds_section(rows, "THE MACHINE SAYS — MES DATA, LINE 2")
        if oee_summary:
            avail = oee_summary.get("availability", 0) * 100
            perf = oee_summary.get("performance", 0) * 100
            qual = oee_summary.get("quality", 0) * 100
            overall_oee = oee_summary.get("overall_oee", 0) * 100
            avail_loss_hrs = oee_summary.get("availability_loss_hrs", 0)
            prod_hrs = oee_summary.get("production_hrs", 0)
            net_hrs = oee_summary.get("net_operation_hrs", 0)
            _ds_row(rows, f"OEE: {overall_oee:.1f}% | Availability: {avail:.1f}% | Performance: {perf:.1f}% | Quality: {qual:.1f}%")
            _ds_row(rows, f"Availability loss: {avail_loss_hrs:.0f} hrs out of {net_hrs:.0f} hrs scheduled")
            _ds_row(rows, f"Production time: {prod_hrs:.0f} hrs — and running at {perf:.0f}% speed when up")
        _ds_blank(rows)

        _ds_section(rows, "DOWNTIME PARETO — WHERE THE TIME GOES")
        _ds_row(rows, "Source: MES event data. This is what the machine recorded.")
        _ds_blank(rows)

        kayat_tray_min = kayat_shrink_min = kayat_wrap_min = 0
        kayat_tray_events = kayat_shrink_events = kayat_wrap_events = 0

        for rc in reason_codes:
            r = rc["reason"]
            if r == "Caser - Riverwood":
                riverwood_min = rc["total_minutes"]
                riverwood_events = rc["total_occurrences"]
            elif r == "Tray Packer - Kayat":
                kayat_tray_min = rc["total_minutes"]
                kayat_tray_events = rc["total_occurrences"]
            elif r == "Shrink Tunnel - Kayat":
                kayat_shrink_min = rc["total_minutes"]
                kayat_shrink_events = rc["total_occurrences"]
            elif r == "Case Wrapper - Kayat":
                kayat_wrap_min = rc["total_minutes"]
                kayat_wrap_events = rc["total_occurrences"]

        full_caser_min = riverwood_min + kayat_tray_min + kayat_shrink_min + kayat_wrap_min
        full_caser_events = riverwood_events + kayat_tray_events + kayat_shrink_events + kayat_wrap_events

        skip_reasons = {"Not Scheduled", "Caser - Riverwood", "Tray Packer - Kayat",
                        "Shrink Tunnel - Kayat", "Case Wrapper - Kayat", "Break-Lunch", "Day Code Change"}
        ranked = []
        if riverwood_events > 0:
            ranked.append(("Riverwood (all products)", riverwood_min, riverwood_events,
                           riverwood_min / riverwood_events))
        for rc in reason_codes:
            if rc["reason"] in skip_reasons or rc["total_minutes"] < 20:
                continue
            ranked.append((rc["reason"], rc["total_minutes"], rc["total_occurrences"],
                           rc["total_minutes"] / rc["total_occurrences"] if rc["total_occurrences"] else 0))
        ranked.sort(key=lambda x: x[1], reverse=True)

        for i, (name, mins, events, avg) in enumerate(ranked[:8], 1):
            hrs = mins / 60
            _ds_row(rows, f"#{i}  {name}")
            _ds_row(rows, f"     {mins:,.0f} min ({hrs:,.0f} hrs) | {events:,} events | avg {avg:.1f} min/event")
        _ds_blank(rows)

        _ds_row(rows, "TRAYED PRODUCTS — Full caser system total:")
        _ds_row(rows, f"  Riverwood: {riverwood_min:,.0f} min | Tray Packer: {kayat_tray_min:,.0f} min | "
                      f"Shrink Tunnel: {kayat_shrink_min:,.0f} min | Wrapper: {kayat_wrap_min:,.0f} min")
        _ds_row(rows, f"  TOTAL: {full_caser_min:,.0f} min ({full_caser_min/60:,.0f} hrs) | {full_caser_events:,} events")
        if len(tray_runs) > 0 and len(std_runs) > 0:
            _ds_row(rows, f"  This is why trayed OEE is {tray_runs['avg_oee'].mean():.0f}% vs "
                          f"{std_runs['avg_oee'].mean():.0f}% for standard — the extra equipment kills it.")
        _ds_blank(rows)

        for rc in reason_codes:
            if rc["reason"] == "Unassigned":
                _ds_row(rows, f"DATA PROBLEM: 'Unassigned' = {rc['total_minutes']:,.0f} min ({rc['total_hours']:.0f} hrs) "
                              f"with NO reason code. {rc['total_occurrences']} events with no cause recorded.")
                break
        _ds_blank(rows)
    else:
        _ds_section(rows, "TOP EQUIPMENT ISSUES — FROM OPERATOR NOTES")
        _ds_row(rows, "(MES downtime file not provided — using shift report notes)")
        _ds_blank(rows)
        equip_counts = Counter()
        for _, r in runs.dropna(subset=["notes"]).iterrows():
            for eq in extract_equipment_mentions(r["notes"]):
                equip_counts[eq] += 1
        for i, (eq, cnt) in enumerate(equip_counts.most_common(6), 1):
            pct = cnt / total_days * 100
            _ds_row(rows, f"#{i}  {eq}: {cnt}/{total_days} nights ({pct:.0f}%)")
        _ds_blank(rows)

    _ds_section(rows, "CREW CAPABILITY — SAME PRODUCT, DIFFERENT RESULTS")
    prod_spread = (
        daily.groupby("product_family")
        .agg(n=("avg_oee", "count"), avg=("avg_oee", "mean"),
             best=("avg_oee", "max"), worst=("avg_oee", "min"))
        .sort_values("avg", ascending=False)
    )
    prod_spread = prod_spread[prod_spread["n"] >= 3]

    if len(prod_spread) > 0:
        for fam, ps in prod_spread.iterrows():
            spread = ps["best"] - ps["worst"]
            _ds_row(rows, f"{fam} ({int(ps['n'])} nights): "
                          f"Best {ps['best']:.0f}% / Worst {ps['worst']:.0f}% / "
                          f"Spread {spread:.0f} pts / Avg {ps['avg']:.0f}%")
        max_spread_fam = prod_spread.index[(prod_spread["best"] - prod_spread["worst"]).argmax()]
        max_spread = prod_spread.loc[max_spread_fam, "best"] - prod_spread.loc[max_spread_fam, "worst"]
        _ds_blank(rows)
        _ds_row(rows, f"Same product, same line, same speed. {max_spread:.0f}-point spread = crew difference.")
        _ds_row(rows, "Find what the best nights do. Train everyone to that. Track by crew.")
    _ds_blank(rows)

    _ds_section(rows, "WHO DOES WHAT — DEFINE IT")
    _ds_row(rows, "OPERATOR: Clear jams, reset faults, clean glue, adjust labels, reposition cases")
    _ds_row(rows, "MECHANIC: Chains, motors, belts, electrical, pneumatics, sensor replacement")
    _ds_row(rows, "GRAY AREA (leadership decides): Curling bar swap, speed changes, sensor repositioning, guide rail adjustments")
    _ds_blank(rows)

    valid_runs = runs.dropna(subset=["notes"])
    op_count = sum(1 for _, r in valid_runs.iterrows()
                   if any(w in str(r.get("notes", "")).lower() for w in ["adjusted", "cleaned", "cleared", "reset", "grabbed"]))
    mech_count = sum(1 for _, r in valid_runs.iterrows()
                     if any(w in str(r.get("notes", "")).lower() for w in ["maintenance", "mechanic"]))
    wait_count = sum(1 for _, r in valid_runs.iterrows()
                     if any(w in str(r.get("notes", "")).lower() for w in ["called maintenance", "waiting for", "only one mechanic"]))

    _ds_row(rows, f"Notes show: Operators took action {op_count}/{len(valid_runs)} shifts. "
                  f"Called maintenance {mech_count} shifts. Waited for mechanic {wait_count} shifts.")
    _ds_row(rows, "When operators wait, the line is down. Define roles. Train first-response. Reduce wait time.")
    _ds_blank(rows)

    _ds_section(rows, "FIX THESE 3 THINGS")
    _ds_row(rows, "1. CASER RELIABILITY")
    _ds_row(rows, "   PM: chains, fiber guides, glue system. Stock spare parts AT the line. Operator first-response card for fiber jams.")
    if reason_codes:
        _ds_row(rows, f"   Machine data: {riverwood_min:,.0f} min lost to Riverwood alone.")
    _ds_blank(rows)

    _ds_row(rows, f"2. DEDICATED {shift_label.upper()} SHIFT MECHANIC")
    _ds_row(rows, f"   When the mechanic is shared across lines, {shift_label} shift waits. Every minute waiting = line down.")
    if wait_count > 0:
        _ds_row(rows, f"   'Waiting for mechanic' appears in {wait_count} shift reports.")
    _ds_blank(rows)

    _ds_row(rows, "3. REASON CODE DISCIPLINE")
    if reason_codes:
        for rc in reason_codes:
            if rc["reason"] == "Unassigned":
                _ds_row(rows, f"   {rc['total_minutes']:,.0f} min ({rc['total_hours']:.0f} hrs) of downtime "
                              f"has NO reason code. {rc['total_occurrences']} events with no cause recorded.")
                break
    _ds_row(rows, "   Can't target what you can't measure. Fix reason coding first.")
    _ds_blank(rows)

    _ds_section(rows, "TARGET")
    _ds_row(rows, f"Current: {all_oee:.0f}% OEE, hitting target {hit_rate:.0f}% of nights")
    _ds_row(rows, f"Goal: {min(all_oee + 5, 45):.0f}% OEE, hitting target 50%+ of nights, in 4 weeks")
    _ds_row(rows, "How: Fix #1 caser, add mechanic, enforce reason codes, train crews to best-night standard")
    _ds_blank(rows)
    _ds_row(rows, "Re-run this report in 4 weeks. The numbers will show if the needle moved.")

    return pd.DataFrame(rows)


def build_sendable(daily, runs, shift_label, reason_codes=None, oee_summary=None):
    """Build a short text block for email/Teams."""
    lines = []
    all_oee = daily["avg_oee"].mean()
    total_days = len(daily)
    std_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_PACK.get(x, "") in ("8pk", "12pk"))]
    tray_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_PACK.get(x, "") == "Trayed")]
    target_runs_df = daily[daily["product_family"].apply(lambda x: PRODUCT_TARGET.get(x, 0) > 0)]

    if len(target_runs_df) > 0:
        hits = sum(1 for _, r in target_runs_df.iterrows()
                   if pd.notna(r["total_cases"]) and r["total_cases"] >= PRODUCT_TARGET.get(r["product_family"], 0))
        hit_rate = hits / len(target_runs_df) * 100
    else:
        hits, hit_rate = 0, 0

    lines.append(f"Line 2 — {shift_label} Shift — Performance Summary")
    lines.append(f"{daily['date'].min()} to {daily['date'].max()} ({total_days} production days)")
    lines.append("")
    lines.append(f"Hit target: {hits}/{len(target_runs_df)} nights ({hit_rate:.0f}%)")
    lines.append(f"Avg OEE: {all_oee:.1f}%")
    if len(std_runs) > 0:
        lines.append(f"  Standard: {std_runs['avg_oee'].mean():.1f}% OEE ({len(std_runs)} nights)")
    if len(tray_runs) > 0:
        lines.append(f"  Trayed: {tray_runs['avg_oee'].mean():.1f}% OEE ({len(tray_runs)} nights)")
    lines.append("")

    if reason_codes:
        lines.append("Top downtime (MES machine data):")
        rw_min = 0
        for rc in reason_codes:
            if rc["reason"] == "Caser - Riverwood":
                rw_min = rc["total_minutes"]
                rw_hrs = rc["total_hours"]
                rw_events = rc["total_occurrences"]
                break
        if rw_min > 0:
            lines.append(f"  #1 Riverwood caser: {rw_min:,.0f} min ({rw_hrs:.0f} hrs) — {rw_events:,} events")
        skip = {"Not Scheduled", "Caser - Riverwood", "Break-Lunch", "Day Code Change",
                "Tray Packer - Kayat", "Shrink Tunnel - Kayat", "Case Wrapper - Kayat"}
        rank = 2
        for rc in sorted(reason_codes, key=lambda x: x["total_minutes"], reverse=True):
            if rc["reason"] in skip or rc["total_minutes"] < 100:
                continue
            lines.append(f"  #{rank} {rc['reason']}: {rc['total_minutes']:,.0f} min ({rc['total_hours']:.0f} hrs)")
            rank += 1
            if rank > 5:
                break
        lines.append("")

    lines.append("Ask:")
    lines.append("  1. Schedule a PM on the Riverwood caser — chains, fiber guides, glue system")
    lines.append("  2. Stock spare parts at Line 2 (chain links, fiber guides, curling bars)")
    lines.append("")
    lines.append("Full data in the attached spreadsheet.")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Core report builder
# ---------------------------------------------------------------------------

def build_report(hourly, shift_summary, overall, hour_avg, downtime, product_data, shift_pattern="3rd"):
    """Build all report sheets for the target shift."""
    actual_shifts = hourly["shift"].unique().tolist()
    target_shift = _detect_shift(actual_shifts, shift_pattern)
    if not target_shift:
        raise ValueError(f"No shift matching '{shift_pattern}' found in data. Available: {actual_shifts}")

    benchmark_shift = pick_benchmark_shift(hourly, overall, target_shift)
    shift_label = _shift_label(target_shift)
    bench_label = _shift_label(benchmark_shift) if benchmark_shift else None

    ht = hourly[hourly["shift"] == target_shift].copy()
    hb = hourly[hourly["shift"] == benchmark_shift].copy() if benchmark_shift else pd.DataFrame()
    sst = shift_summary[shift_summary["shift"] == target_shift].copy()
    hat = hour_avg[hour_avg["shift"] == target_shift].copy()
    hab = hour_avg[hour_avg["shift"] == benchmark_shift].copy() if benchmark_shift else pd.DataFrame()

    date_min = hourly["date"].min().strftime("%B %d, %Y")
    date_max = hourly["date"].max().strftime("%B %d, %Y")
    n_days = ht["date_str"].nunique()

    plant_avail, plant_perf, plant_qual, plant_oee = _aggregate_oee(hourly)
    n_shift_days_plant = hourly.groupby(["date_str", "shift"]).ngroups
    plant_cph = hourly["total_cases"].sum() / (n_shift_days_plant * SHIFT_HOURS) if n_shift_days_plant > 0 else 0

    st_avail, st_perf, st_qual, st_oee = _aggregate_oee(ht)
    st_cph_row = overall[overall["shift"] == target_shift]
    st_cph = st_cph_row["cases_per_hour"].values[0] if len(st_cph_row) > 0 else 0
    st_cases = ht["total_cases"].sum()
    st_hours = ht["total_hours"].sum()

    if benchmark_shift and len(hb) > 0:
        sb_avail, sb_perf, sb_qual, sb_oee = _aggregate_oee(hb)
        sb_cph_row = overall[overall["shift"] == benchmark_shift]
        sb_cph = sb_cph_row["cases_per_hour"].values[0] if len(sb_cph_row) > 0 else 0
    else:
        sb_avail = sb_perf = sb_qual = sb_oee = sb_cph = 0

    good_hours = ht[ht["total_hours"] >= 0.5]
    if len(good_hours) > 0:
        target_cph = good_hours["cases_per_hour"].quantile(0.90)
        ht["cases_gap"] = (target_cph - ht["cases_per_hour"]).clip(lower=0) * ht["total_hours"]

    st_util, st_prod_hrs, st_sched_hrs, st_dead = _compute_utilization(ht)
    if len(hb) > 0:
        sb_util, sb_prod_hrs, sb_sched_hrs, sb_dead = _compute_utilization(hb)
    else:
        sb_util = sb_dead = 0
    plant_util, _, _, plant_dead = _compute_utilization(hourly)

    has_downtime = downtime is not None and len(downtime.get("reasons_df", [])) > 0
    has_events = has_downtime and len(downtime.get("events_df", [])) > 0
    has_product = product_data is not None and len(product_data.get("runs", [])) > 0

    sheets = {}

    avail_loss = (1 - st_avail) * 100
    perf_loss = (1 - st_perf) * 100
    if perf_loss > avail_loss:
        primary = "PERFORMANCE"
        bench_str = f"{bench_label} shift runs at {sb_perf:.0%}. " if benchmark_shift else ""
        primary_detail = (f"When the line IS running, it's only hitting {st_perf:.0%} of rated speed. "
                          f"{bench_str}The line is up but slow — "
                          f"micro stops, speed losses, and cycle time gaps are eating output.")
    else:
        primary = "AVAILABILITY"
        bench_str = f"{bench_label} shift keeps it running {sb_avail:.0%} of their shift. " if benchmark_shift else ""
        primary_detail = (f"The line is down {avail_loss:.0f}% of the time. {bench_str}"
                          f"Breakdowns, changeovers, and material waits are the gap.")

    product_insight_lines = []
    if has_product:
        pruns = product_data["runs"]
        valid_runs = pruns.dropna(subset=["oee_display"])
        if len(valid_runs) > 0:
            by_family = valid_runs.groupby("product_family").agg(
                avg_oee=("oee_display", "mean"), n_runs=("oee_display", "count"),
                total_cases=("cases_produced", "sum"),
            ).sort_values("avg_oee")
            worst_prod = by_family.index[0]
            best_prod = by_family.index[-1]
            vrc = valid_runs.copy()
            vrc["pack_type"] = vrc["product_family"].map(PRODUCT_PACK_TYPE).fillna("Unknown")
            std_oee = vrc[vrc["pack_type"].str.startswith("Standard")]["oee_display"].mean()
            tray_oee = vrc[vrc["pack_type"].str.startswith("Trayed")]["oee_display"].mean()
            if pd.notna(std_oee) and pd.notna(tray_oee):
                product_insight_lines.append(
                    f"PRODUCT MIX MATTERS: Standard products avg {std_oee:.1f}% OEE vs trayed products at {tray_oee:.1f}% OEE.")
            product_insight_lines.append(
                f"Best product: {best_prod} ({by_family.loc[best_prod, 'avg_oee']:.1f}% OEE). "
                f"Worst: {worst_prod} ({by_family.loc[worst_prod, 'avg_oee']:.1f}% OEE).")

    # SHEET 1: OVERVIEW
    overview = []
    overview.append({"": f"{shift_label.upper()} SHIFT PERFORMANCE ANALYSIS", " ": ""})
    overview.append({"": LINE_NAME, " ": ""})
    overview.append({"": f"OEE Period: {date_min} — {date_max} ({n_days} shift-days)", " ": ""})
    if has_product:
        pmeta = product_data.get("meta", {})
        overview.append({"": f"Product Period: {pmeta.get('shift_report_date_range', 'see product tabs')}", " ": ""})
    overview.append({"": "", " ": ""})
    overview.append({"": "THE BOTTOM LINE", " ": ""})
    if benchmark_shift:
        overview.append({"": f"{shift_label} shift is running at {st_oee:.1f}% OEE — {sb_oee - st_oee:.1f} points behind {bench_label} shift.", " ": ""})
        overview.append({"": f"That gap costs {(sb_cph - st_cph) * st_hours / max(n_days, 1):,.0f} cases every night.", " ": ""})
    else:
        overview.append({"": f"{shift_label} shift is running at {st_oee:.1f}% OEE.", " ": ""})
    if product_insight_lines:
        overview.append({"": "", " ": ""})
        for line in product_insight_lines:
            overview.append({"": line, " ": ""})
    overview.append({"": "", " ": ""})
    overview.append({"": f"PRIMARY LOSS DRIVER: {primary}", " ": ""})
    overview.append({"": primary_detail, " ": ""})
    overview.append({"": "", " ": ""})
    overview.append({"": "HOW TO READ THIS REPORT", " ": ""})
    bench_name = f"{bench_label} shift" if bench_label else "benchmark"
    overview.append({"": f"Tab 2 — Scorecard: {shift_label} shift numbers vs plant average and {bench_name}", " ": ""})
    overview.append({"": "Tab 3 — Hour by Hour: when during the shift OEE drops and why", " ": ""})
    overview.append({"": "Tab 4 — Day by Day: the trend — getting better or worse?", " ": ""})
    overview.append({"": "Tab 5 — Worst Hours: the specific hours that collapsed", " ": ""})
    if benchmark_shift:
        overview.append({"": f"Tab 6 — vs {bench_label} Shift: side-by-side comparison", " ": ""})
    overview.append({"": "Last Tab — Recommended Actions: step-by-step plan to close the gap", " ": ""})
    sheets["Overview"] = pd.DataFrame(overview)

    # SHEET 2: SCORECARD
    sc = []
    bcol = f"{bench_label} Shift (Best)" if bench_label else "Benchmark"
    gcol = f"Gap vs {bench_label}" if bench_label else "Gap"
    tcol = f"{shift_label} Shift"

    def _sc(metric, vt, vp, vb, gap):
        return {"Metric": metric, tcol: vt, "Plant Avg": vp, bcol: vb, gcol: gap}

    sc.append(_sc(f"{shift_label.upper()} SHIFT SCORECARD", "", "", "", ""))
    sc.append(_sc("", "", "", "", ""))
    oee_gap = sb_oee - st_oee if benchmark_shift else 0
    sc.append(_sc("OEE", f"{st_oee:.1f}%", f"{plant_oee:.1f}%",
                  f"{sb_oee:.1f}%" if benchmark_shift else "", f"-{oee_gap:.1f} pts" if benchmark_shift else ""))
    sc.append(_sc("Availability", f"{st_avail:.1%}", f"{plant_avail:.1%}",
                  f"{sb_avail:.1%}" if benchmark_shift else "", f"-{(sb_avail-st_avail)*100:.1f} pts" if benchmark_shift else ""))
    sc.append(_sc("Performance", f"{st_perf:.1%}", f"{plant_perf:.1%}",
                  f"{sb_perf:.1%}" if benchmark_shift else "", f"-{(sb_perf-st_perf)*100:.1f} pts" if benchmark_shift else ""))
    sc.append(_sc("Quality", f"{st_qual:.1%}", f"{plant_qual:.1%}",
                  f"{sb_qual:.1%}" if benchmark_shift else "", f"-{(sb_qual-st_qual)*100:.1f} pts" if benchmark_shift else ""))
    sc.append(_sc("", "", "", "", ""))
    sc.append(_sc("Cases/Hour", f"{st_cph:,.0f}", f"{plant_cph:,.0f}",
                  f"{sb_cph:,.0f}" if benchmark_shift else "", f"-{sb_cph-st_cph:,.0f} CPH" if benchmark_shift else ""))
    sc.append(_sc("", "", "", "", ""))
    sc.append(_sc("Utilization", f"{st_util:.1f}%", f"{plant_util:.1f}%",
                  f"{sb_util:.1f}%" if benchmark_shift else "", ""))
    st_sched_count = len(ht[ht["total_hours"] > 0])
    sc.append(_sc("Dead Hours (0 Cases)", f"{st_dead} of {st_sched_count} ({st_dead/max(st_sched_count,1)*100:.0f}%)", "", "", ""))
    sc.append(_sc("", "", "", "", ""))
    sc.append(_sc("Total Cases", f"{st_cases:,.0f}", "", "", ""))
    sc.append(_sc("Production Hours", f"{st_hours:,.1f}", "", "", ""))
    sc.append(_sc("Shift-Days", f"{n_days}", "", "", ""))
    sc.append(_sc("", "", "", "", ""))
    sc.append({"Metric": "WHERE IS OEE LOST?", tcol: "Loss %", "Plant Avg": "Share of Total Loss", bcol: "", gcol: ""})
    total_loss = avail_loss + perf_loss + (1 - st_qual) * 100
    if total_loss > 0:
        sc.append(_sc("Availability Loss", f"{avail_loss:.1f}%", f"{avail_loss/total_loss*100:.0f}% of total loss", "", "Line not running"))
        sc.append(_sc("Performance Loss", f"{perf_loss:.1f}%", f"{perf_loss/total_loss*100:.0f}% of total loss", "", "Running slow"))
        sc.append(_sc("Quality Loss", f"{(1-st_qual)*100:.1f}%", f"{(1-st_qual)*100/total_loss*100:.0f}% of total loss", "", "Rejected product"))
    sc.append(_sc("", "", "", "", ""))
    sc.append({"Metric": "CONSISTENCY", tcol: "", "Plant Avg": "", bcol: "", gcol: ""})
    std_t = ht["oee_pct"].std()
    std_b = hb["oee_pct"].std() if len(hb) > 0 else 0
    below20 = (ht["oee_pct"] < 20).sum()
    above50 = (ht["oee_pct"] > 50).sum()
    total_hrs = len(ht)
    sc.append(_sc("OEE Std Deviation", f"{std_t:.1f}", "", f"{std_b:.1f}" if benchmark_shift else "", "Lower = more consistent"))
    sc.append(_sc("Hours below 20% OEE", f"{below20} of {total_hrs} ({below20/max(total_hrs,1)*100:.0f}%)", "", "", ""))
    sc.append(_sc("Hours above 50% OEE", f"{above50} of {total_hrs} ({above50/max(total_hrs,1)*100:.0f}%)", "", "", ""))
    sheets["Scorecard"] = pd.DataFrame(sc)

    # SHEET 3: HOUR BY HOUR
    hbh = []
    hbh.append({"Hour": f"HOUR-BY-HOUR PATTERN — {shift_label.upper()} SHIFT", "OEE %": "", "Cases/Hr": "", "Availability": "", "Performance": "", "Insight": ""})
    hbh.append({"Hour": "", "OEE %": "", "Cases/Hr": "", "Availability": "", "Performance": "", "Insight": ""})

    ht_hourly_avg = (
        ht.groupby("shift_hour")
        .agg(avg_oee=("oee_pct", "mean"), avg_cph=("cases_per_hour", "mean"),
             avg_avail=("availability", "mean"), avg_perf=("performance", "mean"), n=("oee_pct", "count"))
        .reset_index().sort_values("shift_hour")
    )
    ht_hourly_avg["time_block"] = ht_hourly_avg["shift_hour"].apply(lambda h: f"{int(h)}:00" if pd.notna(h) else "")

    if len(ht_hourly_avg) > 0:
        best_hr = ht_hourly_avg.loc[ht_hourly_avg["avg_oee"].idxmax()]
        worst_hr = ht_hourly_avg.loc[ht_hourly_avg["avg_oee"].idxmin()]
        min_hour = ht_hourly_avg["shift_hour"].min()
        for _, row in ht_hourly_avg.iterrows():
            hr_num = int(row["shift_hour"])
            insight = ""
            if hr_num == min_hour:
                rest_avg = ht_hourly_avg[ht_hourly_avg["shift_hour"] != min_hour]["avg_oee"].mean()
                gap = rest_avg - row["avg_oee"]
                if gap > 2:
                    insight = f"Startup: {gap:.0f} pts below rest of shift"
            if row["avg_oee"] == best_hr["avg_oee"]:
                insight = "BEST HOUR"
            if row["avg_oee"] == worst_hr["avg_oee"]:
                insight = "WORST HOUR"
            if row["avg_avail"] < 0.50:
                insight += " | Line down >50%" if insight else "Line down >50% of this hour"
            if row["avg_perf"] < 0.50:
                insight += " | Speed below 50%" if insight else "Speed below 50%"
            hbh.append({"Hour": f"Hour {hr_num} ({row['time_block']})", "OEE %": f"{row['avg_oee']:.1f}%",
                        "Cases/Hr": f"{row['avg_cph']:,.0f}", "Availability": f"{row['avg_avail']:.0%}",
                        "Performance": f"{row['avg_perf']:.0%}", "Insight": insight})

    if benchmark_shift and len(hab) > 0:
        hbh.append({"Hour": "", "OEE %": "", "Cases/Hr": "", "Availability": "", "Performance": "", "Insight": ""})
        hbh.append({"Hour": f"SAME HOURS — {bench_label.upper()} SHIFT COMPARISON", "OEE %": "", "Cases/Hr": "", "Availability": "", "Performance": "", "Insight": ""})
        hab_sorted = hab.sort_values("shift_hour") if "shift_hour" in hab.columns else hab
        for _, row in hab_sorted.iterrows():
            hr_num = int(row["shift_hour"])
            ht_match = ht_hourly_avg[ht_hourly_avg["shift_hour"] == hr_num]
            gap = ""
            if len(ht_match) > 0:
                diff = row["oee_pct"] - ht_match.iloc[0]["avg_oee"]
                gap = f"{bench_label} is +{diff:.1f} pts" if diff > 0 else f"{shift_label} is +{abs(diff):.1f} pts"
            hour_label = row.get("time_block", f"{hr_num}:00") or f"{hr_num}:00"
            cph_val = f"{row['cases_per_hour']:,.0f}" if "cases_per_hour" in hab.columns and hab["cases_per_hour"].sum() > 0 else ""
            avail_val = f"{row['availability']:.0%}" if "availability" in hab.columns else ""
            perf_val = f"{row['performance']:.0%}" if "performance" in hab.columns else ""
            hbh.append({"Hour": f"Hour {hr_num} ({hour_label})", "OEE %": f"{row['oee_pct']:.1f}%",
                        "Cases/Hr": cph_val, "Availability": avail_val, "Performance": perf_val, "Insight": gap})
    sheets["Hour by Hour"] = pd.DataFrame(hbh)

    # SHEET 4: DAY BY DAY
    dbd = []
    sst_sorted = sst.sort_values("date_str")
    ssb = shift_summary[shift_summary["shift"] == benchmark_shift].copy() if benchmark_shift else pd.DataFrame()
    date_product_map = {}
    date_notes_map = {}
    if has_product:
        for _, prow in product_data["runs"].iterrows():
            d = prow["date"]
            pf = prow["product_family"]
            date_product_map[d] = date_product_map.get(d, "") + (f", {pf}" if d in date_product_map else pf)
            notes = prow.get("notes", "")
            if notes and not pd.isna(notes):
                equips = prow.get("equipment_mentioned", [])
                if d in date_notes_map:
                    date_notes_map[d]["equips"].update(equips)
                else:
                    date_notes_map[d] = {"equips": set(equips)}

    dead_blocks_t, _ = _build_dead_hour_narrative(ht)
    if has_events:
        dead_blocks_t = _correlate_dead_hours_with_events(dead_blocks_t, downtime["events_df"], ht)
    dead_by_date = {}
    for b in dead_blocks_t:
        dead_by_date.setdefault(b["date_str"], []).append(b)

    bench_oee_col = f"{bench_label} Shift OEE" if bench_label else "Benchmark OEE"
    for _, row in sst_sorted.iterrows():
        date = row["date_str"]
        dow = pd.Timestamp(date).day_name()
        sb_day = ssb[ssb["date_str"] == date] if len(ssb) > 0 else pd.DataFrame()
        sb_oee_day = f"{sb_day.iloc[0]['oee_pct']:.1f}%" if len(sb_day) > 0 else ""
        flag = ""
        if row["oee_pct"] < 25: flag = "CRITICAL"
        elif row["oee_pct"] < 30: flag = "Poor"
        elif row["oee_pct"] > 45: flag = "Good"
        dead_info = ""
        date_blocks = dead_by_date.get(date, [])
        if date_blocks:
            n_dead = sum(b_["n_hours"] for b_ in date_blocks)
            parts = []
            for b_ in date_blocks:
                if b_["n_hours"] >= 2:
                    parts.append(f"Hr {b_['first_hour']}–{b_['last_hour']} ({b_['n_hours']}hr outage)")
                else:
                    parts.append(f"Hr {b_['first_hour']}")
            dead_info = f"{n_dead} dead: {', '.join(parts)}"
            machine_causes = [b_.get("cause_annotation", "") for b_ in date_blocks if b_.get("cause_annotation")]
            if machine_causes:
                dead_info += f" [{machine_causes[0]}]"
            elif date in date_notes_map and date_notes_map[date]["equips"]:
                dead_info += f" [{', '.join(sorted(date_notes_map[date]['equips']))}]"
        entry = {"Date": date, "Day": dow, "OEE %": f"{row['oee_pct']:.1f}%", "Cases/Hr": f"{row['cases_per_hour']:,.0f}",
                 "Total Cases": f"{row['total_cases']:,.0f}", bench_oee_col: sb_oee_day, "Dead Hours": dead_info, "Status": flag}
        if has_product:
            entry["Product Running"] = date_product_map.get(date, "")
        dbd.append(entry)

    if len(sst_sorted) >= 6:
        first_half = sst_sorted.head(len(sst_sorted) // 2)["oee_pct"].mean()
        second_half = sst_sorted.tail(len(sst_sorted) // 2)["oee_pct"].mean()
        if second_half > first_half + 1: trend = f"IMPROVING: {first_half:.1f}% -> {second_half:.1f}%"
        elif second_half < first_half - 1: trend = f"DECLINING: {first_half:.1f}% -> {second_half:.1f}%"
        else: trend = f"FLAT: {first_half:.1f}% / {second_half:.1f}%"
        empty = {"Date": "", "Day": "", "OEE %": "", "Cases/Hr": "", "Total Cases": "", bench_oee_col: "", "Dead Hours": "", "Status": ""}
        if has_product: empty["Product Running"] = ""
        dbd.append(empty)
        trend_row = {"Date": "TREND", "Day": trend, "OEE %": "", "Cases/Hr": "", "Total Cases": "", bench_oee_col: "", "Dead Hours": "", "Status": ""}
        if has_product: trend_row["Product Running"] = ""
        dbd.append(trend_row)

    if len(sst_sorted) > 0:
        best_day = sst_sorted.loc[sst_sorted["oee_pct"].idxmax()]
        worst_day = sst_sorted.loc[sst_sorted["oee_pct"].idxmin()]
        for label, day in [("BEST DAY", best_day), ("WORST DAY", worst_day)]:
            entry = {"Date": label, "Day": f"{day['date_str']} ({pd.Timestamp(day['date_str']).day_name()})",
                     "OEE %": f"{day['oee_pct']:.1f}%", "Cases/Hr": f"{day['cases_per_hour']:,.0f}",
                     "Total Cases": f"{day['total_cases']:,.0f}", bench_oee_col: "", "Dead Hours": "", "Status": ""}
            if has_product: entry["Product Running"] = date_product_map.get(day["date_str"], "")
            dbd.append(entry)

    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    empty = {"Date": "", "Day": "", "OEE %": "", "Cases/Hr": "", "Total Cases": "", bench_oee_col: "", "Dead Hours": "", "Status": ""}
    if has_product: empty["Product Running"] = ""
    dbd.append(empty)
    header = {"Date": "DAY OF WEEK AVG", "Day": "", "OEE %": "", "Cases/Hr": "", "Total Cases": "", bench_oee_col: "", "Dead Hours": "", "Status": ""}
    if has_product: header["Product Running"] = ""
    dbd.append(header)
    dow_t = ht.groupby("day_of_week").agg(avg_oee=("oee_pct", "mean"), avg_cph=("cases_per_hour", "mean"),
        total_cases=("total_cases", "sum"), n=("oee_pct", "count")).reindex(dow_order).dropna(how="all")
    for day_name, drow in dow_t.iterrows():
        flag = ""
        if drow["avg_oee"] == dow_t["avg_oee"].min() and dow_t["avg_oee"].max() - dow_t["avg_oee"].min() > 3: flag = "WORST DAY"
        if drow["avg_oee"] == dow_t["avg_oee"].max() and dow_t["avg_oee"].max() - dow_t["avg_oee"].min() > 3: flag = "BEST DAY"
        entry = {"Date": day_name, "Day": f"{int(drow['n'])} hours", "OEE %": f"{drow['avg_oee']:.1f}%",
                 "Cases/Hr": f"{drow['avg_cph']:,.0f}", "Total Cases": f"{drow['total_cases']:,.0f}",
                 bench_oee_col: "", "Dead Hours": "", "Status": flag}
        if has_product: entry["Product Running"] = ""
        dbd.append(entry)
    sheets["Day by Day"] = pd.DataFrame(dbd)

    # SHEET 5: WORST HOURS
    wh = []
    shift_worst = ht[ht["total_hours"] >= 0.5].nsmallest(20, "oee_pct")
    for _, row in shift_worst.iterrows():
        a, p, q = row["availability"], row["performance"], row["quality"]
        if a < 0.20: what = f"Line down most of the hour (Avail {a:.0%})"
        elif a < 0.50: what = f"Major stoppage — line up only {a:.0%} of the hour"
        elif p < 0.30: what = f"Line was up ({a:.0%}) but crawling at {p:.0%} speed"
        elif p < 0.50: what = f"Speed loss — {p:.0%} of rated speed despite {a:.0%} uptime"
        elif a < 0.70 and p < 0.70: what = f"Both: line down {(1-a)*100:.0f}% AND slow ({p:.0%} speed)"
        elif q < 0.95: what = f"Quality issue — {q:.1%} first pass"
        else: what = f"Avail {a:.0%} / Perf {p:.0%} — multiple small losses"
        wh.append({"Date": row["date_str"], "Day": row["day_of_week"], "Time": row["time_block"],
                   "OEE %": round(row["oee_pct"], 1), "Cases/Hr": round(row["cases_per_hour"], 0),
                   "Avail %": round(a*100, 1), "Perf %": round(p*100, 1), "Qual %": round(q*100, 1), "What Happened": what})
    sheets["Worst Hours"] = pd.DataFrame(wh)

    # SHEET 6: VS BENCHMARK
    if benchmark_shift:
        vs_name = f"vs {bench_label} Shift"
        vs = []
        vs.append({"Metric": f"{shift_label.upper()} vs {bench_label.upper()} SHIFT", tcol: "", f"{bench_label} Shift": "", "Difference": "", "What This Means": ""})
        vs.append({"Metric": "", tcol: "", f"{bench_label} Shift": "", "Difference": "", "What This Means": ""})
        avail_gap_vs = (sb_avail - st_avail) * 100
        perf_gap_vs = (sb_perf - st_perf) * 100
        cph_gap_vs = sb_cph - st_cph
        vs.append({"Metric": "OEE", tcol: f"{st_oee:.1f}%", f"{bench_label} Shift": f"{sb_oee:.1f}%", "Difference": f"-{sb_oee-st_oee:.1f} pts", "What This Means": f"{shift_label} produces {cph_gap_vs:,.0f} fewer cases/hr"})
        vs.append({"Metric": "Availability", tcol: f"{st_avail:.1%}", f"{bench_label} Shift": f"{sb_avail:.1%}", "Difference": f"-{avail_gap_vs:.1f} pts", "What This Means": f"Line stops more on {shift_label}" if avail_gap_vs > 2 else "Similar uptime"})
        vs.append({"Metric": "Performance", tcol: f"{st_perf:.1%}", f"{bench_label} Shift": f"{sb_perf:.1%}", "Difference": f"-{perf_gap_vs:.1f} pts", "What This Means": f"Line runs slower on {shift_label}" if perf_gap_vs > 2 else "Similar speed"})
        vs.append({"Metric": "Cases/Hour", tcol: f"{st_cph:,.0f}", f"{bench_label} Shift": f"{sb_cph:,.0f}", "Difference": f"-{cph_gap_vs:,.0f}", "What This Means": f"{cph_gap_vs:,.0f} cases/hr left on the table"})
        vs.append({"Metric": "", tcol: "", f"{bench_label} Shift": "", "Difference": "", "What This Means": ""})

        if len(ht_hourly_avg) > 0:
            vs.append({"Metric": "HOUR-BY-HOUR GAP", tcol: f"{shift_label} OEE", f"{bench_label} Shift": f"{bench_label} OEE", "Difference": "Gap", "What This Means": ""})
            for _, ht_row in ht_hourly_avg.iterrows():
                hr_num = int(ht_row["shift_hour"])
                hb_hr = hab[hab["shift_hour"] == hr_num] if len(hab) > 0 else pd.DataFrame()
                if len(hb_hr) > 0:
                    ht_val, hb_val = ht_row["avg_oee"], hb_hr.iloc[0]["oee_pct"]
                    gap = hb_val - ht_val
                    note = "BIG GAP" if gap > 10 else (f"{shift_label} WINS" if gap < 0 else "")
                    vs.append({"Metric": f"Hour {hr_num} ({ht_row['time_block']})", tcol: f"{ht_val:.1f}%", f"{bench_label} Shift": f"{hb_val:.1f}%", "Difference": f"{'-' if gap>0 else '+'}{abs(gap):.1f} pts", "What This Means": note})

        vs.append({"Metric": "", tcol: "", f"{bench_label} Shift": "", "Difference": "", "What This Means": ""})
        if perf_gap_vs > avail_gap_vs:
            vs.append({"Metric": "VERDICT", tcol: "", f"{bench_label} Shift": "", "Difference": "", "What This Means": f"Biggest gap: PERFORMANCE (-{perf_gap_vs:.1f} pts). Focus: micro stops, speed, operator response."})
        else:
            vs.append({"Metric": "VERDICT", tcol: "", f"{bench_label} Shift": "", "Difference": "", "What This Means": f"Biggest gap: AVAILABILITY (-{avail_gap_vs:.1f} pts). Focus: changeover, breakdown response, staging."})
        sheets[vs_name] = pd.DataFrame(vs)

    # SHEET 7: DOWNTIME CAUSES
    actionable = None
    if has_downtime:
        reasons_df = downtime["reasons_df"].copy()
        actionable = reasons_df[~reasons_df["reason"].isin(EXCLUDE_REASONS)].copy()
        actionable = actionable.sort_values("total_minutes", ascending=False).reset_index(drop=True)
        total_min = actionable["total_minutes"].sum()
        if total_min > 0:
            actionable["pct"] = (actionable["total_minutes"] / total_min * 100).round(1)
            actionable["cum_pct"] = actionable["pct"].cumsum().round(1)
        else:
            actionable["pct"] = 0; actionable["cum_pct"] = 0
        actionable["avg_min"] = actionable.apply(lambda r: round(r["total_minutes"]/r["total_occurrences"], 1) if r["total_occurrences"] > 0 else 0, axis=1)
        actionable["fault_type"] = actionable["reason"].apply(classify_fault)
        pareto_df = actionable[["reason", "fault_type", "total_occurrences", "total_minutes", "total_hours", "avg_min", "pct", "cum_pct"]].copy()
        pareto_df.columns = ["Cause", "Fault Type", "Events", "Total Minutes", "Total Hours", "Avg Min/Event", "% of Total", "Cumulative %"]
        sheets["Downtime Causes"] = pareto_df

    # SHEET 8: FAULT OWNERS
    if has_downtime:
        reasons_df = downtime["reasons_df"].copy()
        reasons_df["fault_type"] = reasons_df["reason"].apply(classify_fault)
        fault_sum = reasons_df.groupby("fault_type").agg(events=("total_occurrences", "sum"), minutes=("total_minutes", "sum"), hours=("total_hours", "sum"), n_codes=("reason", "count")).sort_values("minutes", ascending=False).reset_index()
        grand = fault_sum["minutes"].sum()
        fault_sum["pct"] = (fault_sum["minutes"] / max(grand, 1) * 100).round(1)
        ownership = {"Equipment / Mechanical": "Maintenance / Reliability team", "Micro Stops": "Engineering + Operators", "Process / Changeover": "CI / Operations", "Scheduled / Non-Production": "Planning / Management", "Data Gap (uncoded)": "Supervisors", "Other / Unclassified": "Review and reclassify"}
        fault_sum["owner"] = fault_sum["fault_type"].map(ownership).fillna("TBD")
        what_to_ask = {"Equipment / Mechanical": "Are PMs current? What parts keep failing?", "Micro Stops": "Where do short stops happen most?", "Process / Changeover": "How long is the average changeover?", "Scheduled / Non-Production": "Can non-production windows be reduced?", "Data Gap (uncoded)": "Why aren't operators coding these?", "Other / Unclassified": "Review and categorize properly."}
        fault_sum["question_to_ask"] = fault_sum["fault_type"].map(what_to_ask).fillna("")
        fo = fault_sum[["fault_type", "n_codes", "events", "hours", "pct", "owner", "question_to_ask"]].copy()
        fo.columns = ["Fault Category", "# Codes", "Events", "Hours", "% of All Downtime", "Who Owns This", "Question to Ask"]
        sheets["Fault Owners"] = fo

    # PRODUCT SHEETS (9-12)
    if has_product:
        pruns = product_data["runs"]
        valid = pruns.dropna(subset=["oee_display"]).copy()

        # Product Scorecard
        by_fam = valid.groupby("product_family").agg(n_runs=("oee_display", "count"), avg_oee=("oee_display", "mean"), min_oee=("oee_display", "min"), max_oee=("oee_display", "max"), total_cases=("cases_produced", "sum"), avg_cases=("cases_produced", "mean"), avg_dt=("downtime_minutes", "mean")).sort_values("avg_oee").reset_index()
        psc = []
        psc.append({"Product": f"{shift_label.upper()} SHIFT — OEE BY PRODUCT", "Pack Type": "", "Rated Speed": "", "Runs": "", "Avg OEE": "", "Best Run": "", "Worst Run": "", "Total Cases": "", "Avg Cases/Run": "", "Avg Downtime": "", "Status": ""})
        psc.append({"Product": "", "Pack Type": "", "Rated Speed": "", "Runs": "", "Avg OEE": "", "Best Run": "", "Worst Run": "", "Total Cases": "", "Avg Cases/Run": "", "Avg Downtime": "", "Status": ""})
        for _, frow in by_fam.iterrows():
            fam = frow["product_family"]
            status = ""
            if frow["avg_oee"] < 20: status = "CRITICAL"
            elif frow["avg_oee"] < 30: status = "Problem"
            elif frow["avg_oee"] > 45: status = "Solid"
            psc.append({"Product": fam, "Pack Type": PRODUCT_PACK_TYPE.get(fam, ""), "Rated Speed": f"{PRODUCT_RATED_SPEED.get(fam, '')} cpm" if PRODUCT_RATED_SPEED.get(fam) else "", "Runs": int(frow["n_runs"]), "Avg OEE": f"{frow['avg_oee']:.1f}%", "Best Run": f"{frow['max_oee']:.1f}%", "Worst Run": f"{frow['min_oee']:.1f}%", "Total Cases": f"{frow['total_cases']:,.0f}" if pd.notna(frow["total_cases"]) else "", "Avg Cases/Run": f"{frow['avg_cases']:,.0f}" if pd.notna(frow["avg_cases"]) else "", "Avg Downtime": f"{frow['avg_dt']:.0f} min" if pd.notna(frow["avg_dt"]) else "", "Status": status})
        sheets["Product Scorecard"] = pd.DataFrame(psc)

        # Every Run
        detail = []
        for _, r in pruns.sort_values("date").iterrows():
            oee_str = f"{r['oee_display']:.1f}%" if pd.notna(r["oee_display"]) else "—"
            cases_str = f"{r['cases_produced']:,.0f}" if pd.notna(r["cases_produced"]) else "—"
            status = ""
            if pd.notna(r["oee_display"]):
                if r["oee_display"] < 20: status = "CRITICAL"
                elif r["oee_display"] < 30: status = "Poor"
                elif r["oee_display"] > 45: status = "Good"
            notes = r.get("notes", "") or ""
            if len(notes) > 300: notes = notes[:297] + "..."
            detail.append({"Date": r["date"], "Product": r["product_family"], "OEE %": oee_str, "Cases": cases_str,
                          "Downtime (min)": f"{r['downtime_minutes']:.0f}" if pd.notna(r["downtime_minutes"]) else "—",
                          "Changeover (min)": f"{r['changeover_minutes']:.0f}" if pd.notna(r["changeover_minutes"]) else "—",
                          "Equipment Mentioned": ", ".join(r["equipment_mentioned"]) if r["equipment_mentioned"] else "—",
                          "Operator Notes": notes, "Status": status})
        sheets["Every Run"] = pd.DataFrame(detail)

        # Std vs Trayed
        valid["pack_type"] = valid["product_family"].map(PRODUCT_PACK_TYPE).fillna("Unknown")
        std_r = valid[valid["pack_type"].str.startswith("Standard")]
        tray_r = valid[valid["pack_type"].str.startswith("Trayed")]
        svt = []
        s_avg = std_r["oee_display"].mean() if len(std_r) > 0 else 0
        t_avg = tray_r["oee_display"].mean() if len(tray_r) > 0 else 0
        svt.append({"Metric": f"STANDARD vs TRAYED — {shift_label.upper()} SHIFT", "Standard (8pk/12pk)": "", "Trayed (6/4)": "", "Gap": "", "What This Means": ""})
        svt.append({"Metric": "", "Standard (8pk/12pk)": "", "Trayed (6/4)": "", "Gap": "", "What This Means": ""})
        svt.append({"Metric": "Average OEE", "Standard (8pk/12pk)": f"{s_avg:.1f}%", "Trayed (6/4)": f"{t_avg:.1f}%", "Gap": f"{s_avg-t_avg:.1f} pts", "What This Means": "Trayed products perform dramatically worse" if s_avg-t_avg > 10 else ""})
        svt.append({"Metric": "# Runs", "Standard (8pk/12pk)": str(len(std_r)), "Trayed (6/4)": str(len(tray_r)), "Gap": "", "What This Means": ""})
        svt.append({"Metric": "", "Standard (8pk/12pk)": "", "Trayed (6/4)": "", "Gap": "", "What This Means": ""})
        if s_avg - t_avg > 15:
            verdict = f"YES — trayed products are a major OEE drag ({t_avg:.1f}% vs {s_avg:.1f}%)."
        elif s_avg - t_avg > 5:
            verdict = f"Trayed products run {s_avg-t_avg:.1f} pts below standard. Gap is real but manageable."
        else:
            verdict = "Product format doesn't seem to be a major factor."
        svt.append({"Metric": "VERDICT", "Standard (8pk/12pk)": "", "Trayed (6/4)": "", "Gap": "", "What This Means": verdict})
        sheets["Std vs Trayed"] = pd.DataFrame(svt)

        # Equipment x Product
        equip_prod = defaultdict(lambda: defaultdict(int))
        equip_total = Counter()
        for _, r in valid.iterrows():
            for eq in r["equipment_mentioned"]:
                equip_prod[eq][r["product_family"]] += 1
                equip_total[eq] += 1
        sorted_equip = sorted(equip_total.items(), key=lambda x: -x[1])
        exb = []
        exb.append({"Equipment": f"EQUIPMENT ISSUES BY PRODUCT — {shift_label.upper()} SHIFT", "Total Mentions": "", "Top Product": "", "Mentions on Top Product": "", "Pattern": ""})
        exb.append({"Equipment": "", "Total Mentions": "", "Top Product": "", "Mentions on Top Product": "", "Pattern": ""})
        for eq, count in sorted_equip:
            prod_counts = equip_prod[eq]
            top_prod = max(prod_counts.items(), key=lambda x: x[1])
            top_pct = top_prod[1] / count * 100
            pattern = ""
            if top_pct > 60 and count >= 3: pattern = f"Concentrated on {top_prod[0]}"
            elif count >= 5: pattern = f"Across {len(prod_counts)} products — systemic"
            elif count >= 2: pattern = "Monitor"
            exb.append({"Equipment": eq, "Total Mentions": count, "Top Product": top_prod[0], "Mentions on Top Product": f"{top_prod[1]} ({top_pct:.0f}%)", "Pattern": pattern})
        if sorted_equip:
            exb.append({"Equipment": "", "Total Mentions": "", "Top Product": "", "Mentions on Top Product": "", "Pattern": ""})
            exb.append({"Equipment": "KEY FINDING", "Total Mentions": "", "Top Product": f"{sorted_equip[0][0]} is mentioned in {sorted_equip[0][1]} of {len(valid)} runs ({sorted_equip[0][1]/len(valid)*100:.0f}%). #1 equipment reliability issue.", "Mentions on Top Product": "", "Pattern": ""})
        sheets["Equipment x Product"] = pd.DataFrame(exb)

    # RECOMMENDED ACTIONS
    actions = []
    p = 1
    if has_downtime and actionable is not None and len(actionable) > 0:
        top = actionable.iloc[0]
        actions.append({"Priority": p, "Area": "#1 Equipment Loss", "Problem": f"{top['reason']}: {top['total_hours']:.0f} hrs / {int(top['total_occurrences'])} events ({top['pct']:.0f}%)", "Step 1": f"Pull {top['reason']} events for last 2 weeks.", "Step 2": f"Walk the line during next {top['reason']} failure.", "Step 3": "Run a 5-Why on top 3 failure modes.", "Step 4": "Build countermeasures: PM task, spare parts, SOP.", "Step 5": f"Track: {top['reason']} hrs/week. Target: 50% reduction."})
        p += 1

    if benchmark_shift:
        avail_gap_a = (sb_avail - st_avail) * 100
        perf_gap_a = (sb_perf - st_perf) * 100
        if perf_gap_a > avail_gap_a:
            actions.append({"Priority": p, "Area": f"Performance Gap ({shift_label} vs {bench_label})", "Problem": f"{shift_label} runs {perf_gap_a:.1f} pts below {bench_label} — line is up but slow", "Step 1": "Compare speed settings between shifts.", "Step 2": "Count micro stops per hour.", "Step 3": "Check for newer operators running conservative.", "Step 4": f"Document {bench_label}'s best practices.", "Step 5": "Build speed standard card per product."})
        else:
            actions.append({"Priority": p, "Area": f"Availability Gap ({shift_label} vs {bench_label})", "Problem": f"{shift_label} has {avail_gap_a:.1f} pts worse availability", "Step 1": "Compare changeover durations.", "Step 2": "Check material staging.", "Step 3": "Review breakdown response times.", "Step 4": f"Build checklist from {bench_label} practices.", "Step 5": "Pilot for 2 weeks. Track daily."})
        p += 1

    if len(ht_hourly_avg) > 0:
        _min_hr = ht_hourly_avg["shift_hour"].min()
        first_hr_oee = ht_hourly_avg[ht_hourly_avg["shift_hour"] == _min_hr]["avg_oee"].values
        rest_avg_a = ht_hourly_avg[ht_hourly_avg["shift_hour"] != _min_hr]["avg_oee"].mean()
        if len(first_hr_oee) > 0 and rest_avg_a - first_hr_oee[0] > 3:
            startup_gap = rest_avg_a - first_hr_oee[0]
            actions.append({"Priority": p, "Area": "Startup Loss", "Problem": f"First hour: {first_hr_oee[0]:.1f}% vs {rest_avg_a:.1f}% rest — {startup_gap:.0f} pt gap", "Step 1": f"Time shift start to first good case.", "Step 2": "Document what takes the time.", "Step 3": "Build startup checklist.", "Step 4": "Consider overlap with prior shift.", "Step 5": f"Target: first-hour OEE above {rest_avg_a-3:.0f}%."})
            p += 1

    target_oee = min(st_oee + 5, sb_oee) if benchmark_shift else st_oee + 5
    actions.append({"Priority": p, "Area": "Measurement & Follow-Up", "Problem": f"Current {shift_label} shift OEE: {st_oee:.1f}% — Target: {target_oee:.1f}%", "Step 1": "Pick top 2 actions. Focus beats breadth.", "Step 2": "Assign one owner per action.", "Step 3": "Review weekly with MES data.", "Step 4": "Re-run analysis in 4 weeks.", "Step 5": f"5 OEE points = ~{5*st_hours/max(n_days,1)/100*st_cph:,.0f} cases/night."})
    sheets["Recommended Actions"] = pd.DataFrame(actions)

    return sheets


# ---------------------------------------------------------------------------
# Excel output
# ---------------------------------------------------------------------------

def write_report(sheets, output_path, shift_label="3rd"):
    """Write all sheets to Excel with formatting."""
    print(f"Writing: {output_path}")

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        workbook = writer.book

        hdr = workbook.add_format({"bold": True, "bg_color": "#1B2A4A", "font_color": "white", "border": 1, "text_wrap": True, "valign": "vcenter", "font_size": 11})
        title_fmt = workbook.add_format({"bold": True, "font_size": 16, "font_color": "#1B2A4A"})
        subtitle_fmt = workbook.add_format({"italic": True, "font_size": 10, "font_color": "#666666"})
        section_fmt = workbook.add_format({"bold": True, "font_size": 12, "font_color": "#1B2A4A", "bottom": 2, "bottom_color": "#1B2A4A"})
        good_fmt = workbook.add_format({"bg_color": "#E8F5E9", "font_color": "#2E7D32"})
        bad_fmt = workbook.add_format({"bg_color": "#FFEBEE", "font_color": "#C62828"})
        warn_fmt = workbook.add_format({"bg_color": "#FFF8E1", "font_color": "#F57F17"})
        hit_fmt = workbook.add_format({"bg_color": "#C8E6C9", "font_color": "#1B5E20", "bold": True, "font_size": 10})
        miss_fmt = workbook.add_format({"bg_color": "#FFCDD2", "font_color": "#B71C1C", "bold": True, "font_size": 10})
        close_fmt = workbook.add_format({"bg_color": "#FFF9C4", "font_color": "#F57F17", "bold": True, "font_size": 10})
        gap_neg = workbook.add_format({"font_color": "#B71C1C", "font_size": 10})
        gap_pos = workbook.add_format({"font_color": "#1B5E20", "font_size": 10})
        data_fmt = workbook.add_format({"font_size": 10, "valign": "vcenter"})
        wrap_fmt = workbook.add_format({"text_wrap": True, "valign": "top", "font_size": 10})
        summary_fmt = workbook.add_format({"bold": True, "bg_color": "#E8EAF6", "font_color": "#1B2A4A", "top": 1, "bottom": 1, "font_size": 10})
        bold_detail = workbook.add_format({"bold": True, "font_size": 10, "font_color": "#B71C1C"})

        tab_order = ["Overview", "Scorecard", "Hour by Hour", "Day by Day", "Worst Hours"]
        for name in sheets:
            if name.startswith("vs "):
                tab_order.append(name)
                break
        tab_order.extend(["Downtime Causes", "Fault Owners", "Product Scorecard", "Every Run", "Std vs Trayed", "Equipment x Product", "Week by Week", "The Data Says", "Recommended Actions"])

        for sheet_name in tab_order:
            if sheet_name not in sheets:
                continue
            df = sheets[sheet_name]
            safe = sheet_name[:31]
            start_row = 2
            df.to_excel(writer, sheet_name=safe, startrow=start_row, index=False)
            ws = writer.sheets[safe]

            ws.write(0, 0, f"{shift_label} Shift — {sheet_name}", title_fmt)
            ws.write(1, 0, f"Line 2 Flex | Generated {datetime.now().strftime('%Y-%m-%d')}", subtitle_fmt)

            for col_num, col_name in enumerate(df.columns):
                ws.write(start_row, col_num, col_name, hdr)

            for col_num, col_name in enumerate(df.columns):
                max_len = max(df[col_name].astype(str).map(len).max() if len(df) > 0 else 0, len(str(col_name)))
                ws.set_column(col_num, col_num, min(max_len + 4, 65))

            if sheet_name == "Overview":
                ws.set_column(0, 0, 100)
                ws.hide_gridlines(2)
                for row_num in range(len(df)):
                    val = str(df.iloc[row_num].iloc[0])
                    if any(s in val for s in ["THE BOTTOM LINE", "HOW TO READ THIS REPORT", "PRIMARY LOSS DRIVER", "PRODUCT MIX MATTERS"]):
                        ws.write(row_num + start_row + 1, 0, val, section_fmt)

            if sheet_name == "Scorecard":
                ws.set_column(0, 0, 25)
                ws.set_column(1, 3, 22)
                ws.set_column(4, 4, 35)

            if sheet_name == "Hour by Hour":
                ws.set_column(0, 0, 28)
                ws.set_column(5, 5, 45)

            if sheet_name == "Day by Day":
                status_col = list(df.columns).index("Status") if "Status" in df.columns else 6
                ws.set_column(status_col, status_col, 12)
                if "Dead Hours" in df.columns:
                    ws.set_column(list(df.columns).index("Dead Hours"), list(df.columns).index("Dead Hours"), 45)
                if "Product Running" in df.columns:
                    ws.set_column(list(df.columns).index("Product Running"), list(df.columns).index("Product Running"), 35)
                for row_num in range(len(df)):
                    status = str(df.iloc[row_num].get("Status", ""))
                    if status == "CRITICAL": ws.write(row_num + start_row + 1, status_col, status, bad_fmt)
                    elif status == "Poor": ws.write(row_num + start_row + 1, status_col, status, warn_fmt)
                    elif status == "Good": ws.write(row_num + start_row + 1, status_col, status, good_fmt)

            if sheet_name == "Worst Hours":
                ws.set_column(8, 8, 55)
                if "OEE %" in df.columns:
                    col_idx = list(df.columns).index("OEE %")
                    ws.conditional_format(start_row + 1, col_idx, start_row + len(df), col_idx, {"type": "3_color_scale", "min_color": "#F8696B", "mid_color": "#FFEB84", "max_color": "#63BE7B"})

            if sheet_name.startswith("vs "):
                ws.set_column(0, 0, 25)
                ws.set_column(4, 4, 70)

            if sheet_name == "Downtime Causes":
                if "Total Minutes" in df.columns:
                    col_idx = list(df.columns).index("Total Minutes")
                    ws.conditional_format(start_row + 1, col_idx, start_row + len(df), col_idx, {"type": "3_color_scale", "min_color": "#63BE7B", "mid_color": "#FFEB84", "max_color": "#F8696B"})

            if sheet_name == "Fault Owners":
                ws.set_column(5, 5, 55)
                ws.set_column(6, 6, 60)

            if sheet_name == "Product Scorecard":
                ws.set_column(0, 0, 28)
                ws.set_column(1, 1, 18)
                if "Status" in df.columns:
                    s_col = list(df.columns).index("Status")
                    for row_num in range(len(df)):
                        status = str(df.iloc[row_num].get("Status", ""))
                        if status == "CRITICAL": ws.write(row_num + start_row + 1, s_col, status, bad_fmt)
                        elif status == "Problem": ws.write(row_num + start_row + 1, s_col, status, warn_fmt)
                        elif status == "Solid": ws.write(row_num + start_row + 1, s_col, status, good_fmt)

            if sheet_name == "Every Run":
                ws.set_column(0, 0, 12)
                ws.set_column(1, 1, 28)
                ws.set_column(6, 6, 40)
                ws.set_column(7, 7, 80)
                if "Status" in df.columns:
                    s_col = list(df.columns).index("Status")
                    for row_num in range(len(df)):
                        status = str(df.iloc[row_num].get("Status", ""))
                        if status == "CRITICAL": ws.write(row_num + start_row + 1, s_col, status, bad_fmt)
                        elif status == "Poor": ws.write(row_num + start_row + 1, s_col, status, warn_fmt)
                        elif status == "Good": ws.write(row_num + start_row + 1, s_col, status, good_fmt)

            if sheet_name == "Std vs Trayed":
                ws.set_column(0, 0, 25)
                ws.set_column(1, 2, 30)
                ws.set_column(4, 4, 70)
                for row_num in range(len(df)):
                    if str(df.iloc[row_num].get("Metric", "")) == "VERDICT":
                        ws.write(row_num + start_row + 1, 0, "VERDICT", section_fmt)

            if sheet_name == "Equipment x Product":
                ws.set_column(0, 0, 25)
                ws.set_column(2, 2, 30)
                ws.set_column(4, 4, 60)
                for row_num in range(len(df)):
                    if str(df.iloc[row_num].get("Equipment", "")) == "KEY FINDING":
                        ws.write(row_num + start_row + 1, 0, "KEY FINDING", section_fmt)
                        ws.set_column(2, 2, 85)

            if sheet_name == "Week by Week":
                ws.set_column(0, 0, 18, data_fmt)
                ws.set_column(1, 1, 11, data_fmt)
                ws.set_column(3, 3, 24, data_fmt)
                ws.set_column(12, 12, 30, data_fmt)
                ws.set_column(13, 13, 55, wrap_fmt)
                if "Status" in df.columns:
                    status_col = list(df.columns).index("Status")
                    gap_col = list(df.columns).index("Gap")
                    for row_num in range(len(df)):
                        status = str(df.iloc[row_num].get("Status", ""))
                        week_val = str(df.iloc[row_num].get("Week", ""))
                        gap_val = str(df.iloc[row_num].get("Gap", ""))
                        if status == "HIT": ws.write(row_num + start_row + 1, status_col, status, hit_fmt)
                        elif status == "MISSED": ws.write(row_num + start_row + 1, status_col, status, miss_fmt)
                        elif status == "CLOSE": ws.write(row_num + start_row + 1, status_col, status, close_fmt)
                        if gap_val.startswith("-"): ws.write(row_num + start_row + 1, gap_col, gap_val, gap_neg)
                        elif gap_val.startswith("+"): ws.write(row_num + start_row + 1, gap_col, gap_val, gap_pos)
                        if ">>" in week_val or "TOTAL" in week_val:
                            for c in range(len(df.columns)):
                                ws.write(row_num + start_row + 1, c, df.iloc[row_num].iloc[c], summary_fmt)
                ws.freeze_panes(start_row + 1, 0)
                ws.autofilter(start_row, 0, start_row + len(df), len(df.columns) - 1)

            if sheet_name == "The Data Says":
                ws.set_column(0, 0, 30)
                ws.set_column(1, 1, 100)
                ws.hide_gridlines(2)
                section_labels = {"BOTTOM LINE", "THE MACHINE SAYS — MES DATA, LINE 2", "DOWNTIME PARETO — WHERE THE TIME GOES", "CREW CAPABILITY — SAME PRODUCT, DIFFERENT RESULTS", "WHO DOES WHAT — DEFINE IT", "FIX THESE 3 THINGS", "TARGET", "TOP EQUIPMENT ISSUES — FROM OPERATOR NOTES"}
                for row_num in range(len(df)):
                    val = str(df.iloc[row_num].get("Section", ""))
                    detail = str(df.iloc[row_num].get("Detail", ""))
                    if val in section_labels: ws.write(row_num + start_row + 1, 0, val, section_fmt)
                    if detail.startswith(("1. ", "2. ", "3. ")): ws.write(row_num + start_row + 1, 1, detail, bold_detail)
                    elif detail.startswith("#"): ws.write(row_num + start_row + 1, 1, detail, workbook.add_format({"bold": True, "font_size": 10, "font_color": "#1B2A4A"}))
                    elif len(detail) > 50:
                        ws.write(row_num + start_row + 1, 1, detail, wrap_fmt)
                        ws.set_row(row_num + start_row + 1, max(15, min(45, len(detail) // 4)))

            if sheet_name == "Recommended Actions":
                ws.set_column(1, 1, 25)
                ws.set_column(2, 2, 70)
                ws.set_column(3, 7, 60)

        if "Overview" in sheets:
            writer.sheets["Overview"].activate()

    print(f"Done! Open: {output_path}")


def main():
    args = sys.argv[1:]
    oee_file = None
    downtime_file = None
    product_file = None
    shift_pattern = "3rd"

    i = 0
    while i < len(args):
        if args[i] == "--shift" and i + 1 < len(args):
            shift_pattern = args[i + 1]
            i += 2
        elif args[i] == "--downtime" and i + 1 < len(args):
            downtime_file = args[i + 1]
            i += 2
        elif args[i] == "--product" and i + 1 < len(args):
            product_file = args[i + 1]
            i += 2
        elif not args[i].startswith("-"):
            oee_file = args[i]
            i += 1
        else:
            i += 1

    if oee_file is None:
        print('Usage: python shift_report.py oee_export.xlsx --shift "3rd" [--downtime kb.json] [--product prod.json]')
        sys.exit(1)

    oee_file = os.path.abspath(oee_file)
    if not os.path.exists(oee_file):
        print(f"Error: OEE file not found: {oee_file}")
        sys.exit(1)

    hourly, shift_summary, overall, hour_avg, downtime, product_data = load_data(
        oee_file, downtime_file, product_file, shift_pattern)

    sheets = build_report(hourly, shift_summary, overall, hour_avg, downtime, product_data, shift_pattern)

    # Detect actual shift label for output naming
    actual_shifts = hourly["shift"].unique().tolist()
    target_shift = _detect_shift(actual_shifts, shift_pattern)
    shift_label = _shift_label(target_shift) if target_shift else shift_pattern

    # Add target tracking sheets if product data provided
    if product_data is not None and len(product_data.get("runs", [])) > 0:
        runs_copy = product_data["runs"].copy()
        daily = aggregate_daily(runs_copy)
        if len(daily) > 0:
            sheets["Week by Week"] = build_week_by_week(daily)
            reason_codes, pareto_data, oee_summary = None, None, None
            if downtime_file:
                reason_codes, pareto_data, oee_summary = load_downtime_pareto(downtime_file)
            sheets["The Data Says"] = build_data_says(daily, runs_copy, shift_label, reason_codes, pareto_data, oee_summary)

            # Email text
            summary = build_sendable(daily, runs_copy, shift_label, reason_codes, oee_summary)
            output_dir = os.path.dirname(oee_file)
            email_path = os.path.join(output_dir, f"Line2_{shift_label}_Shift_Targets_{datetime.now().strftime('%Y%m%d')}_EMAIL.txt")
            with open(email_path, "w", encoding="utf-8") as f:
                f.write(summary)
            print(f"Email text: {email_path}")

    output_dir = os.path.dirname(oee_file)
    output_path = os.path.join(output_dir, f"Line2_{shift_label}_Shift_Analysis_{datetime.now().strftime('%Y%m%d')}.xlsx")
    write_report(sheets, output_path, shift_label)

    # Console preview
    print("\n" + "=" * 60)
    print(f"{shift_label.upper()} SHIFT — LINE 2 FLEX — REPORT SUMMARY")
    print("=" * 60)
    sc = sheets.get("Scorecard")
    if sc is not None:
        for _, row in sc.iterrows():
            m = str(row.get("Metric", ""))
            if m and m not in ["", f"{shift_label.upper()} SHIFT SCORECARD", "WHERE IS OEE LOST?", "CONSISTENCY"]:
                v = str(row.iloc[1]) if len(row) > 1 else ""
                g = str(row.iloc[4]) if len(row) > 4 else ""
                if v:
                    gap_str = f"  ({g})" if g else ""
                    print(f"  {m}: {v}{gap_str}")

    if "Product Scorecard" in sheets:
        print(f"\nPRODUCT PERFORMANCE ({shift_label} Shift):")
        ps = sheets["Product Scorecard"]
        for _, row in ps.iterrows():
            prod = str(row.get("Product", ""))
            oee = str(row.get("Avg OEE", ""))
            status = str(row.get("Status", ""))
            if prod and oee and not prod.startswith(shift_label.upper()) and prod not in ["", "ALL PRODUCTS", "KEY FINDING"]:
                flag = f"  [{status}]" if status else ""
                print(f"  {prod}: {oee}{flag}")

    if "Recommended Actions" in sheets:
        print("\nRECOMMENDED ACTIONS:")
        for _, row in sheets["Recommended Actions"].iterrows():
            print(f"  #{row['Priority']} [{row['Area']}]: {str(row['Problem'])[:80]}...")

    print(f"\nFull report: {output_path}")


if __name__ == "__main__":
    main()
