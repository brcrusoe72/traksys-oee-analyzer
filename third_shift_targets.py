"""
3rd Shift Line 2 — Target Tracker & Support Case
==================================================
Simple 2-sheet report:
  Sheet 1: Daily performance by week — did we hit target? Why not?
  Sheet 2: The data says — machine data Pareto, crew capability, action items

Usage:
  python third_shift_targets.py --product product_data.json --downtime knowledge_base.json
"""

import sys
import os
import json
from datetime import datetime, timedelta
from collections import Counter, defaultdict
import pandas as pd
import numpy as np

from shared import (
    PRODUCT_NORMALIZE, normalize_product, PRODUCT_TARGET, PRODUCT_PACK,
    IS_TRAYED,
)

# Equipment scanning from operator notes
# Riverwood runs ALL products. Full caser system (Kayat) only runs trayed.
EQUIPMENT_SCAN = {
    "Riverwood": [
        "riverwood", "caser", "fiber jam", "fiber mispick",
        "fiber getting caught", "misformed cases", "ripping cases",
        "misshapped cases", "open flaps", "plastic drive bar",
    ],
    "Kayat (Tray/Shrink/Wrap)": [
        "tray packer", "kayat", "shrink tunnel", "shrink wrapper",
        "double-wrapped",
    ],
    "Labeler": [
        "bear labeler", "labeler a", "labeler b", "label machine",
        "flappers", "shiners", "shinner", "ripped labels",
        "loose labels", "labels weren't sticking", "curling bar",
        "label fingers",
    ],
    "Palletizer": [
        "palletizer", "misformed layers", "misshapped layers",
        "misshappen", "pallet conveyor",
    ],
    "Conveyors": [
        "conveyor", "conveyers", "overhead conveyor",
        "accumulation table", "overhead conveypr",
    ],
    "Depal": ["depal", "suction cup"],
    "Spiral": ["ryson", "spiral"],
    "Printer": ["diagraph", "print and apply", "laser jet", "laser printer", "no print"],
    "Stacker": ["double stacker", "case stacker"],
    "X-Ray": ["x-ray", "x ray"],
}


def extract_equipment(notes):
    if not notes or pd.isna(notes):
        return []
    text = notes.lower()
    found = []
    for equip, keywords in EQUIPMENT_SCAN.items():
        if any(kw in text for kw in keywords):
            found.append(equip)
    return found


def summarize_issues(notes):
    """Short issue summary from operator notes. No fluff."""
    if not notes or pd.isna(notes):
        return ""
    parts = [s.strip() for s in str(notes).split(";;") if s.strip()]
    key = []
    for part in parts:
        lower = part.lower().strip()
        if "x-ray" in lower and "failed" not in lower:
            continue
        if "both passed" in lower or "both yes" in lower:
            continue
        if lower.startswith(("set-up:", "start up:", "starting")) and len(part) < 45:
            continue
        clean = part.strip().rstrip(";").strip()
        if clean:
            key.append(clean)
    result = "; ".join(key[:2])  # Max 2 issues — keep it short
    return result[:180] + "..." if len(result) > 180 else result


def classify_support(equipment_list, notes):
    """Short support classification."""
    if not equipment_list:
        return ""
    needs = []
    equip_set = set(equipment_list)
    if "Riverwood" in equip_set:
        needs.append("Caser")
    if "Kayat (Tray/Shrink/Wrap)" in equip_set:
        needs.append("Kayat")
    if "Labeler" in equip_set:
        needs.append("Labeler")
    if "Palletizer" in equip_set:
        needs.append("Palletizer")
    if "Conveyors" in equip_set:
        needs.append("Conveyor")
    if "Depal" in equip_set:
        needs.append("Depal")
    if "Spiral" in equip_set:
        needs.append("Spiral")

    if notes:
        lower = str(notes).lower()
        if any(w in lower for w in ["labor", "checker", "short staff", "no checker"]):
            needs.append("Staffing")

    if len(equip_set) >= 3:
        return "MULTIPLE"
    return ", ".join(needs) if needs else ""


def load_product_data(product_path):
    with open(product_path, "r", encoding="utf-8") as f:
        pdata = json.load(f)

    runs = pd.DataFrame(pdata.get("product_runs", []))
    runs = runs[runs["shift"] == "3rd Shift"].copy()
    runs["product_family"] = runs["product"].apply(normalize_product)
    runs["oee_display"] = pd.to_numeric(runs["oee_pct"], errors="coerce") * 100
    runs["cases_produced"] = pd.to_numeric(runs["cases_produced"], errors="coerce")
    runs["downtime_minutes"] = pd.to_numeric(runs["downtime_minutes"], errors="coerce")
    runs["changeover_minutes"] = pd.to_numeric(runs["changeover_minutes"], errors="coerce")
    runs["equipment"] = runs["notes"].apply(extract_equipment)
    runs["is_trayed"] = runs["product_family"].apply(lambda x: x in IS_TRAYED)

    # Filter out non-production (line wasn't running)
    runs = runs.dropna(subset=["cases_produced", "oee_display"], how="all").copy()
    runs = runs[~((runs["cases_produced"].fillna(0) < 50) & (runs["oee_display"].fillna(0) < 1))].copy()

    meta = pdata.get("metadata", {})
    return runs, meta


def load_downtime_pareto(downtime_path):
    """Load Traksys machine data — the source of truth."""
    if not downtime_path or not os.path.exists(downtime_path):
        return None, None
    with open(downtime_path, "r", encoding="utf-8") as f:
        kb = json.load(f)
    reason_codes = kb.get("downtime_reason_codes", [])
    pareto = kb.get("pareto_top_10", {})
    oee_summary = kb.get("metadata", {}).get("oee_period_summary", {})
    return reason_codes, pareto, oee_summary


def aggregate_daily(runs):
    """Aggregate multiple runs of same product on same date."""
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


def build_sheet1(daily):
    """Sheet 1: Week-by-week daily performance — dense, color-coded."""
    rows = []

    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily["week_start"] = daily["date_dt"].apply(
        lambda d: (d - timedelta(days=d.weekday())).strftime("%m/%d"))
    daily["week_label"] = daily["date_dt"].apply(
        lambda d: f"Wk of {(d - timedelta(days=d.weekday())).strftime('%b %d')}")
    daily["day_name"] = daily["date_dt"].dt.day_name()
    daily = daily.sort_values("date_dt")

    current_week = None
    week_cases = []
    week_targets = []
    week_oees = []
    week_hits = 0
    week_total = 0

    for _, r in daily.iterrows():
        wk = r["week_label"]

        if wk != current_week:
            if current_week is not None and week_total > 0:
                rows.append(_week_summary(current_week, week_cases, week_targets,
                                          week_oees, week_hits, week_total))
                rows.append(_empty_row())
            current_week = wk
            week_cases = []
            week_targets = []
            week_oees = []
            week_hits = 0
            week_total = 0

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
        support = classify_support(r["equip_combined"], r["notes_combined"])

        # Equipment list as short string
        equip_short = ", ".join(r["equip_combined"][:3]) if r["equip_combined"] else ""

        week_cases.append(cases)
        week_targets.append(target)
        week_oees.append(oee)
        week_total += 1

        rows.append({
            "Week": wk,
            "Date": r["date"],
            "Day": r["day_name"][:3],
            "Product": r["product_family"],
            "Pack": pack,
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

    # Close last week
    if current_week is not None and week_total > 0:
        rows.append(_week_summary(current_week, week_cases, week_targets,
                                  week_oees, week_hits, week_total))

    # Period summary
    rows.append(_empty_row())
    all_cases = daily["total_cases"].sum()
    all_oee = daily["avg_oee"].mean()
    total_runs = len(daily)
    target_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_TARGET.get(x, 0) > 0)]
    if len(target_runs) > 0:
        hits = sum(1 for _, r in target_runs.iterrows()
                   if pd.notna(r["total_cases"]) and r["total_cases"] >= PRODUCT_TARGET.get(r["product_family"], 0))
        hit_rate = hits / len(target_runs) * 100
    else:
        hits = 0
        hit_rate = 0

    rows.append({
        "Week": "TOTAL",
        "Date": f"{daily['date'].min()} to {daily['date'].max()}",
        "Day": f"{total_runs}d",
        "Product": "",
        "Pack": "",
        "Target": "",
        "Actual": f"{all_cases:,.0f}",
        "Gap": "",
        "OEE%": f"{all_oee:.1f}",
        "Status": f"{hits}/{len(target_runs)} ({hit_rate:.0f}%)",
        "DT min": "",
        "CO min": "",
        "Equipment Hit": "",
        "Notes": "",
    })

    return pd.DataFrame(rows)


COL_KEYS = ["Week", "Date", "Day", "Product", "Pack", "Target",
            "Actual", "Gap", "OEE%", "Status", "DT min", "CO min",
            "Equipment Hit", "Notes"]


def _empty_row():
    return {k: "" for k in COL_KEYS}


def _week_summary(week_label, cases_list, target_list, oee_list, hits, total):
    total_cases = sum(c for c in cases_list if c > 0)
    avg_oee = np.mean([o for o in oee_list if o > 0]) if any(o > 0 for o in oee_list) else 0
    return {
        "Week": f">> {week_label}",
        "Date": "",
        "Day": f"{total}d",
        "Product": "",
        "Pack": "",
        "Target": "",
        "Actual": f"{total_cases:,.0f}",
        "Gap": "",
        "OEE%": f"{avg_oee:.1f}",
        "Status": f"{hits}/{total} hit",
        "DT min": "",
        "CO min": "",
        "Equipment Hit": "",
        "Notes": "",
    }


def build_sheet2(daily, runs, reason_codes=None, pareto=None, oee_summary=None):
    """Sheet 2: Data-driven support case. Numbers, not paragraphs."""
    rows = []

    all_oee = daily["avg_oee"].mean()
    std_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_PACK.get(x, "") in ("8pk", "12pk"))]
    tray_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_PACK.get(x, "") == "Trayed")]
    target_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_TARGET.get(x, 0) > 0)]

    if len(target_runs) > 0:
        hits = sum(1 for _, r in target_runs.iterrows()
                   if pd.notna(r["total_cases"]) and r["total_cases"] >= PRODUCT_TARGET.get(r["product_family"], 0))
        hit_rate = hits / len(target_runs) * 100
    else:
        hit_rate = 0
        hits = 0
    total_days = len(daily)

    # ── BOTTOM LINE ──
    _section(rows, "BOTTOM LINE")
    _row(rows, f"Hit target: {hits}/{len(target_runs)} nights ({hit_rate:.0f}%)")
    _row(rows, f"Avg OEE: {all_oee:.1f}%")
    if len(std_runs) > 0:
        _row(rows, f"Standard products: {std_runs['avg_oee'].mean():.1f}% OEE avg ({len(std_runs)} nights)")
    if len(tray_runs) > 0:
        _row(rows, f"Trayed products: {tray_runs['avg_oee'].mean():.1f}% OEE avg ({len(tray_runs)} nights)")

    best_day = daily.loc[daily["avg_oee"].idxmax()]
    worst_day = daily.loc[daily["avg_oee"].idxmin()]
    _row(rows, f"Best night: {best_day['date']} — {best_day['avg_oee']:.1f}% OEE, "
               f"{best_day['total_cases']:,.0f} cases ({best_day['product_family']})")
    _row(rows, f"Worst night: {worst_day['date']} — {worst_day['avg_oee']:.1f}% OEE, "
               f"{worst_day['total_cases']:,.0f} cases ({worst_day['product_family']})")
    _row(rows, f"The line CAN run. The best nights prove it. The question is why it doesn't every night.")
    _blank(rows)

    # ── THE MACHINE SAYS (Traksys Pareto) ──
    if reason_codes:
        _section(rows, "THE MACHINE SAYS — TRAKSYS DATA, 6 WEEKS, LINE 2")
        if oee_summary:
            avail = oee_summary.get("availability", 0) * 100
            perf = oee_summary.get("performance", 0) * 100
            qual = oee_summary.get("quality", 0) * 100
            overall = oee_summary.get("overall_oee", 0) * 100
            avail_loss = oee_summary.get("availability_loss_hrs", 0)
            prod_hrs = oee_summary.get("production_hrs", 0)
            net_hrs = oee_summary.get("net_operation_hrs", 0)
            _row(rows, f"OEE: {overall:.1f}% | Availability: {avail:.1f}% | Performance: {perf:.1f}% | Quality: {qual:.1f}%")
            _row(rows, f"Availability loss: {avail_loss:.0f} hrs out of {net_hrs:.0f} hrs scheduled — HALF the time is lost to downtime")
            _row(rows, f"Production time: {prod_hrs:.0f} hrs — and running at {perf:.0f}% speed when up")
        _blank(rows)

        _section(rows, "DOWNTIME PARETO — WHERE THE TIME GOES")
        _row(rows, "Source: Traksys event data. This is what the machine recorded, not what someone wrote down.")
        _blank(rows)

        # Consolidate caser system for display
        # Riverwood runs ALL products. Kayat only runs trayed.
        riverwood_min = 0
        kayat_tray_min = 0
        kayat_shrink_min = 0
        kayat_wrap_min = 0
        riverwood_events = 0
        kayat_tray_events = 0
        kayat_shrink_events = 0
        kayat_wrap_events = 0

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

        # Build ranked list, skipping Not Scheduled and consolidating caser
        skip_reasons = {"Not Scheduled", "Caser - Riverwood", "Tray Packer - Kayat",
                        "Shrink Tunnel - Kayat", "Case Wrapper - Kayat",
                        "Break-Lunch", "Day Code Change"}
        ranked = []

        # Riverwood first (affects all products)
        ranked.append(("Riverwood (all products)", riverwood_min, riverwood_events,
                       riverwood_min / riverwood_events if riverwood_events else 0))

        for rc in reason_codes:
            if rc["reason"] in skip_reasons:
                continue
            if rc["total_minutes"] < 20:
                continue
            ranked.append((rc["reason"], rc["total_minutes"], rc["total_occurrences"],
                           rc["total_minutes"] / rc["total_occurrences"] if rc["total_occurrences"] else 0))

        ranked.sort(key=lambda x: x[1], reverse=True)

        for i, (name, mins, events, avg) in enumerate(ranked[:8], 1):
            hrs = mins / 60
            _row(rows, f"#{i}  {name}")
            _row(rows, f"     {mins:,.0f} min ({hrs:,.0f} hrs) | {events:,} events | avg {avg:.1f} min/event")

        _blank(rows)

        # Caser system total for trayed
        _row(rows, f"TRAYED PRODUCTS — Full caser system total:")
        _row(rows, f"  Riverwood: {riverwood_min:,.0f} min | Tray Packer: {kayat_tray_min:,.0f} min | "
                   f"Shrink Tunnel: {kayat_shrink_min:,.0f} min | Wrapper: {kayat_wrap_min:,.0f} min")
        _row(rows, f"  TOTAL: {full_caser_min:,.0f} min ({full_caser_min/60:,.0f} hrs) | {full_caser_events:,} events")
        _row(rows, f"  This is why trayed OEE is {tray_runs['avg_oee'].mean():.0f}% vs "
                   f"{std_runs['avg_oee'].mean():.0f}% for standard — the extra equipment kills it."
             if len(tray_runs) > 0 and len(std_runs) > 0 else "")
        _blank(rows)

        # Unassigned callout
        for rc in reason_codes:
            if rc["reason"] == "Unassigned":
                _row(rows, f"DATA PROBLEM: 'Unassigned' = {rc['total_minutes']:,.0f} min ({rc['total_hours']:.0f} hrs) "
                           f"with NO reason code. {rc['total_occurrences']} events. "
                           f"Can't fix what you can't see.")
                break
        _blank(rows)
    else:
        # No knowledge base — fall back to notes-based analysis
        _section(rows, "TOP EQUIPMENT ISSUES — FROM OPERATOR NOTES")
        _row(rows, "(Traksys downtime file not provided — using shift report notes)")
        _blank(rows)
        equip_counts = Counter()
        for _, r in runs.dropna(subset=["notes"]).iterrows():
            for eq in r["equipment"]:
                equip_counts[eq] += 1
        for i, (eq, cnt) in enumerate(equip_counts.most_common(6), 1):
            pct = cnt / total_days * 100
            _row(rows, f"#{i}  {eq}: {cnt}/{total_days} nights ({pct:.0f}%)")
        _blank(rows)

    # ── CREW CAPABILITY ──
    _section(rows, "CREW CAPABILITY — SAME PRODUCT, DIFFERENT RESULTS")
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
            _row(rows, f"{fam} ({int(ps['n'])} nights): "
                       f"Best {ps['best']:.0f}% / Worst {ps['worst']:.0f}% / "
                       f"Spread {spread:.0f} pts / Avg {ps['avg']:.0f}%")
        max_spread_fam = prod_spread.index[(prod_spread["best"] - prod_spread["worst"]).argmax()]
        max_spread = (prod_spread.loc[max_spread_fam, "best"] -
                      prod_spread.loc[max_spread_fam, "worst"])
        _blank(rows)
        _row(rows, f"Same product, same line, same speed. {max_spread:.0f}-point spread = crew difference.")
        _row(rows, f"Find what the best nights do. Train everyone to that. Track by crew.")
    _blank(rows)

    # ── WHO DOES WHAT ──
    _section(rows, "WHO DOES WHAT — DEFINE IT")
    _row(rows, "OPERATOR: Clear jams, reset faults, clean glue, adjust labels, reposition cases")
    _row(rows, "MECHANIC: Chains, motors, belts, electrical, pneumatics, sensor replacement")
    _row(rows, "GRAY AREA (leadership decides): Curling bar swap, speed changes, sensor repositioning, guide rail adjustments")
    _blank(rows)

    # Count from notes
    valid_runs = runs.dropna(subset=["notes"])
    op_count = sum(1 for _, r in valid_runs.iterrows()
                   if any(w in str(r.get("notes", "")).lower()
                          for w in ["adjusted", "cleaned", "cleared", "reset", "grabbed"]))
    mech_count = sum(1 for _, r in valid_runs.iterrows()
                     if any(w in str(r.get("notes", "")).lower()
                            for w in ["maintenance", "mechanic"]))
    wait_count = sum(1 for _, r in valid_runs.iterrows()
                     if any(w in str(r.get("notes", "")).lower()
                            for w in ["called maintenance", "waiting for", "only one mechanic"]))

    _row(rows, f"Notes show: Operators took action {op_count}/{len(valid_runs)} shifts. "
               f"Called maintenance {mech_count} shifts. Waited for mechanic {wait_count} shifts.")
    _row(rows, f"When operators wait, the line is down. Define roles. Train first-response. Reduce wait time.")
    _blank(rows)

    # ── FIX THESE 3 THINGS ──
    _section(rows, "FIX THESE 3 THINGS")
    _row(rows, "1. CASER RELIABILITY")
    _row(rows, "   PM: chains, fiber guides, glue system. Stock spare parts AT the line. "
               "Operator first-response card for fiber jams.")
    if reason_codes:
        _row(rows, f"   Machine data: {riverwood_min:,.0f} min lost to Riverwood alone.")
    _blank(rows)

    _row(rows, "2. DEDICATED 3RD SHIFT MECHANIC")
    _row(rows, "   When the mechanic is shared across lines, 3rd shift waits. "
               "Every minute waiting = line down.")
    if wait_count > 0:
        _row(rows, f"   'Waiting for mechanic' appears in {wait_count} shift reports.")
    _blank(rows)

    _row(rows, "3. REASON CODE DISCIPLINE")
    if reason_codes:
        for rc in reason_codes:
            if rc["reason"] == "Unassigned":
                _row(rows, f"   {rc['total_minutes']:,.0f} min ({rc['total_hours']:.0f} hrs) of downtime "
                           f"has NO reason code. {rc['total_occurrences']} events with no cause recorded.")
                break
    _row(rows, "   Can't target what you can't measure. Fix reason coding first.")
    _blank(rows)

    # ── TARGET ──
    _section(rows, "TARGET")
    _row(rows, f"Current: {all_oee:.0f}% OEE, hitting target {hit_rate:.0f}% of nights")
    _row(rows, f"Goal: {min(all_oee + 5, 45):.0f}% OEE, hitting target 50%+ of nights, in 4 weeks")
    _row(rows, f"How: Fix #1 caser, add mechanic, enforce reason codes, train crews to best-night standard")
    _blank(rows)

    _row(rows, "Re-run this report in 4 weeks. The numbers will show if the needle moved.")

    return pd.DataFrame(rows)


def _section(rows, title):
    rows.append({"Section": title, "Detail": ""})

def _row(rows, detail):
    rows.append({"Section": "", "Detail": detail})

def _blank(rows):
    rows.append({"Section": "", "Detail": ""})


def build_sendable(daily, runs, reason_codes=None, oee_summary=None):
    """Build a short text block you can paste into email/Teams and walk out."""
    lines = []
    all_oee = daily["avg_oee"].mean()
    total_days = len(daily)
    std_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_PACK.get(x, "") in ("8pk", "12pk"))]
    tray_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_PACK.get(x, "") == "Trayed")]
    target_runs = daily[daily["product_family"].apply(lambda x: PRODUCT_TARGET.get(x, 0) > 0)]

    if len(target_runs) > 0:
        hits = sum(1 for _, r in target_runs.iterrows()
                   if pd.notna(r["total_cases"]) and r["total_cases"] >= PRODUCT_TARGET.get(r["product_family"], 0))
        hit_rate = hits / len(target_runs) * 100
    else:
        hits, hit_rate = 0, 0

    lines.append("Line 2 — 3rd Shift — Performance Summary")
    lines.append(f"{daily['date'].min()} to {daily['date'].max()} ({total_days} production days)")
    lines.append("")
    lines.append(f"Hit target: {hits}/{len(target_runs)} nights ({hit_rate:.0f}%)")
    lines.append(f"Avg OEE: {all_oee:.1f}%")
    if len(std_runs) > 0:
        lines.append(f"  Standard: {std_runs['avg_oee'].mean():.1f}% OEE ({len(std_runs)} nights)")
    if len(tray_runs) > 0:
        lines.append(f"  Trayed: {tray_runs['avg_oee'].mean():.1f}% OEE ({len(tray_runs)} nights)")
    lines.append("")

    if reason_codes:
        lines.append("Top downtime (Traksys machine data):")
        # Consolidate caser
        rw_min = 0
        for rc in reason_codes:
            if rc["reason"] == "Caser - Riverwood":
                rw_min = rc["total_minutes"]
                rw_hrs = rc["total_hours"]
                rw_events = rc["total_occurrences"]
                break
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


def write_report(sheet1, sheet2, output_path):
    print(f"Writing: {output_path}")

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        workbook = writer.book

        # Formats
        hdr = workbook.add_format({
            "bold": True, "bg_color": "#1B2A4A", "font_color": "white",
            "border": 1, "text_wrap": True, "valign": "vcenter", "font_size": 10
        })
        title_fmt = workbook.add_format({"bold": True, "font_size": 14, "font_color": "#1B2A4A"})
        subtitle_fmt = workbook.add_format({"italic": True, "font_size": 9, "font_color": "#666666"})
        summary_fmt = workbook.add_format({
            "bold": True, "bg_color": "#E8EAF6", "font_color": "#1B2A4A",
            "top": 1, "bottom": 1, "font_size": 10
        })
        hit_fmt = workbook.add_format({"bg_color": "#C8E6C9", "font_color": "#1B5E20", "bold": True, "font_size": 10})
        miss_fmt = workbook.add_format({"bg_color": "#FFCDD2", "font_color": "#B71C1C", "bold": True, "font_size": 10})
        close_fmt = workbook.add_format({"bg_color": "#FFF9C4", "font_color": "#F57F17", "bold": True, "font_size": 10})
        gap_neg = workbook.add_format({"font_color": "#B71C1C", "font_size": 10})
        gap_pos = workbook.add_format({"font_color": "#1B5E20", "font_size": 10})
        data_fmt = workbook.add_format({"font_size": 10, "valign": "vcenter"})
        wrap_fmt = workbook.add_format({"text_wrap": True, "valign": "top", "font_size": 10})

        # ── SHEET 1 ──
        start = 2
        sheet1.to_excel(writer, sheet_name="Week by Week", startrow=start, index=False)
        ws1 = writer.sheets["Week by Week"]

        ws1.write(0, 0, "3rd Shift Line 2 — Did We Hit Target?", title_fmt)
        ws1.write(1, 0, f"Generated {datetime.now().strftime('%Y-%m-%d')}", subtitle_fmt)

        for col_num, col_name in enumerate(sheet1.columns):
            ws1.write(start, col_num, col_name, hdr)

        widths = {
            "Week": 18, "Date": 11, "Day": 4, "Product": 24, "Pack": 6,
            "Target": 9, "Actual": 9, "Gap": 9, "OEE%": 6,
            "Status": 10, "DT min": 7, "CO min": 7,
            "Equipment Hit": 30, "Notes": 55,
        }
        for col_num, col_name in enumerate(sheet1.columns):
            ws1.set_column(col_num, col_num, widths.get(col_name, 10), data_fmt)

        status_col = list(sheet1.columns).index("Status")
        gap_col = list(sheet1.columns).index("Gap")

        for row_num in range(len(sheet1)):
            status = str(sheet1.iloc[row_num].get("Status", ""))
            week_val = str(sheet1.iloc[row_num].get("Week", ""))
            gap_val = str(sheet1.iloc[row_num].get("Gap", ""))

            if status == "HIT":
                ws1.write(row_num + start + 1, status_col, status, hit_fmt)
            elif status == "MISSED":
                ws1.write(row_num + start + 1, status_col, status, miss_fmt)
            elif status == "CLOSE":
                ws1.write(row_num + start + 1, status_col, status, close_fmt)

            if gap_val.startswith("-"):
                ws1.write(row_num + start + 1, gap_col, gap_val, gap_neg)
            elif gap_val.startswith("+"):
                ws1.write(row_num + start + 1, gap_col, gap_val, gap_pos)

            if ">>" in week_val or "TOTAL" in week_val:
                for c in range(len(sheet1.columns)):
                    val = sheet1.iloc[row_num].iloc[c]
                    ws1.write(row_num + start + 1, c, val, summary_fmt)

        ws1.freeze_panes(start + 1, 0)
        ws1.autofilter(start, 0, start + len(sheet1), len(sheet1.columns) - 1)

        # ── SHEET 2 ──
        sheet2.to_excel(writer, sheet_name="The Data Says", startrow=2, index=False)
        ws2 = writer.sheets["The Data Says"]

        ws2.write(0, 0, "3rd Shift Line 2 — The Data Says", title_fmt)
        ws2.write(1, 0, f"Source: Traksys machine data + operator shift reports | {datetime.now().strftime('%Y-%m-%d')}", subtitle_fmt)

        for col_num, col_name in enumerate(sheet2.columns):
            ws2.write(2, col_num, col_name, hdr)

        ws2.set_column(0, 0, 30)
        ws2.set_column(1, 1, 100)
        ws2.hide_gridlines(2)

        # Section header format
        section_fmt = workbook.add_format({
            "bold": True, "font_size": 12, "font_color": "#1B2A4A",
            "bottom": 2, "bottom_color": "#1B2A4A"
        })
        number_fmt = workbook.add_format({"font_size": 10, "font_color": "#333333"})
        bold_detail = workbook.add_format({"bold": True, "font_size": 10, "font_color": "#B71C1C"})

        section_labels = {
            "BOTTOM LINE", "THE MACHINE SAYS — TRAKSYS DATA, 6 WEEKS, LINE 2",
            "DOWNTIME PARETO — WHERE THE TIME GOES",
            "CREW CAPABILITY — SAME PRODUCT, DIFFERENT RESULTS",
            "WHO DOES WHAT — DEFINE IT", "FIX THESE 3 THINGS", "TARGET",
            "TOP EQUIPMENT ISSUES — FROM OPERATOR NOTES",
        }

        for row_num in range(len(sheet2)):
            val = str(sheet2.iloc[row_num].get("Section", ""))
            detail = str(sheet2.iloc[row_num].get("Detail", ""))

            if val in section_labels:
                ws2.write(row_num + 3, 0, val, section_fmt)

            # Bold the numbered fix items
            if detail.startswith(("1. ", "2. ", "3. ")):
                ws2.write(row_num + 3, 1, detail, bold_detail)
            elif detail.startswith("#"):
                ws2.write(row_num + 3, 1, detail, workbook.add_format(
                    {"bold": True, "font_size": 10, "font_color": "#1B2A4A"}))
            elif len(detail) > 50:
                ws2.write(row_num + 3, 1, detail, wrap_fmt)
                ws2.set_row(row_num + 3, max(15, min(45, len(detail) // 4)))

        ws2.activate()

    print(f"Done: {output_path}")


def main():
    args = sys.argv[1:]
    product_file = None
    downtime_file = None

    i = 0
    while i < len(args):
        if args[i] == "--product" and i + 1 < len(args):
            product_file = args[i + 1]
            i += 2
        elif args[i] == "--downtime" and i + 1 < len(args):
            downtime_file = args[i + 1]
            i += 2
        elif not args[i].startswith("-"):
            if not product_file:
                product_file = args[i]
            elif not downtime_file:
                downtime_file = args[i]
            i += 1
        else:
            i += 1

    if not product_file:
        print("Usage: python third_shift_targets.py --product product_data.json --downtime knowledge_base.json")
        sys.exit(1)

    if not os.path.exists(product_file):
        print(f"Error: {product_file} not found")
        sys.exit(1)

    runs, meta = load_product_data(product_file)
    daily = aggregate_daily(runs)

    reason_codes, pareto, oee_summary = None, None, None
    if downtime_file and os.path.exists(downtime_file):
        reason_codes, pareto, oee_summary = load_downtime_pareto(downtime_file)
        print(f"Loaded Traksys data: {len(reason_codes)} reason codes")
    else:
        print("No Traksys downtime file — using operator notes only")

    print(f"Loaded {len(runs)} runs -> {len(daily)} production days")
    print(f"Period: {daily['date'].min()} to {daily['date'].max()}")

    sheet1 = build_sheet1(daily)
    sheet2 = build_sheet2(daily, runs, reason_codes, pareto, oee_summary)

    output_dir = os.path.dirname(os.path.abspath(product_file))
    base_name = f"3rd_Shift_Targets_{datetime.now().strftime('%Y%m%d')}.xlsx"
    output_path = os.path.join(output_dir, base_name)
    if os.path.exists(output_path):
        try:
            with open(output_path, "a"):
                pass
        except PermissionError:
            base_name = f"3rd_Shift_Targets_{datetime.now().strftime('%Y%m%d')}_v3.xlsx"
            output_path = os.path.join(output_dir, base_name)
            print(f"  (File locked, writing to {base_name})")

    write_report(sheet1, sheet2, output_path)

    # ── Build sendable summary ──
    summary = build_sendable(daily, runs, reason_codes, oee_summary)
    summary_path = output_path.replace(".xlsx", "_EMAIL.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary)

    print(f"\nReport: {output_path}")
    print(f"Email text: {summary_path}")
    print(f"\n--- COPY BELOW THIS LINE ---\n")
    print(summary)
    print(f"--- END ---")


if __name__ == "__main__":
    main()
