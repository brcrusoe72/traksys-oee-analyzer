"""
3rd Shift Line 2 — Standalone Analysis Report
================================================
Generates a polished, presentation-ready Excel report
focused entirely on 3rd shift performance on Line 2.

Usage:
  python third_shift_report.py oee_export.xlsx --downtime knowledge_base.json --product product_data.json
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
    _smart_rename, _coerce_numerics, _derive_columns, _aggregate_oee,
    _resolve_sheets, EXPECTED_SHEETS, excel_date_to_datetime,
)
from shared import (
    EXCLUDE_REASONS, EQUIPMENT_KEYWORDS, classify_fault,
    PRODUCT_NORMALIZE, normalize_product,
    PRODUCT_RATED_SPEED, PRODUCT_PACK_TYPE,
)

# Default shift names — auto-detected from data in build_report()
SHIFT_3RD = "3rd (11p-7a)"
SHIFT_2ND = "2nd (3p-11p)"
SHIFT_1ST = "1st (7a-3p)"
LINE_NAME = "Line 2 - Flex (Labeling)"

# Equipment names to scan for in operator notes
# NOTE: Riverwood, Kayat (tray packer, shrink tunnel, wrapper), and caser
# are all part of the same casing system.
EQUIPMENT_SCAN = {
    "Caser System (Riverwood/Kayat)": [
        "riverwood", "caser", "tray packer", "kayat",
        "shrink tunnel", "shrink wrapper", "plastic drive bar",
        "double-wrapped", "open flaps", "fiber jam", "fiber mispick",
    ],
    "Bear Labeler": ["bear labeler", "labeler a", "labeler b", "label machine", "flappers", "shiners", "shinner"],
    "Palletizer (PAI)": ["palletizer"],
    "Ryson Spiral": ["ryson", "spiral"],
    "Depal (Whallon)": ["depal"],
    "Conveyors": ["conveyor", "conveyers", "overhead conveyor", "accumulation table"],
    "X-Ray (Inspec)": ["x-ray", "x ray"],
    "Diagraph Printer": ["diagraph", "print and apply"],
    "Laser Printer": ["laser jet", "laser printer"],
    "Double Stacker": ["double stacker", "case stacker"],
}


def extract_equipment_mentions(notes):
    """Scan operator notes for equipment names. Returns list of equipment mentioned."""
    if not notes or pd.isna(notes):
        return []
    text = notes.lower()
    found = []
    for equip_name, keywords in EQUIPMENT_SCAN.items():
        if any(kw in text for kw in keywords):
            found.append(equip_name)
    return found


def _detect_shift(actual_shifts, pattern):
    """Find the actual shift name matching a pattern like '3rd'."""
    for s in actual_shifts:
        if pattern in s.lower():
            return s
    return None


def load_data(oee_path, dt_path=None, product_path=None):
    # Use fuzzy sheet matching from analyze.py (supports renamed/variant sheets)
    from analyze import load_oee_data
    hourly, shift_summary, overall, hour_avg = load_oee_data(oee_path)

    downtime = None
    if dt_path and os.path.exists(dt_path):
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

    product_data = None
    if product_path and os.path.exists(product_path):
        with open(product_path, "r", encoding="utf-8") as f:
            pdata = json.load(f)

        runs = pd.DataFrame(pdata.get("product_runs", []))
        if len(runs) > 0:
            # Filter to 3rd shift only
            runs = runs[runs["shift"] == "3rd Shift"].copy()
            runs["product_family"] = runs["product"].apply(normalize_product)
            # OEE is decimal in JSON (0.521 = 52.1%)
            runs["oee_display"] = pd.to_numeric(runs["oee_pct"], errors="coerce") * 100
            runs["cases_produced"] = pd.to_numeric(runs["cases_produced"], errors="coerce")
            runs["downtime_minutes"] = pd.to_numeric(runs["downtime_minutes"], errors="coerce")
            runs["changeover_minutes"] = pd.to_numeric(runs["changeover_minutes"], errors="coerce")
            # Extract equipment mentions from notes
            runs["equipment_mentioned"] = runs["notes"].apply(extract_equipment_mentions)

        products_ref = pd.DataFrame(pdata.get("products", []))
        changeovers = pd.DataFrame(pdata.get("changeovers", []))
        meta = pdata.get("metadata", {})

        product_data = {
            "runs": runs,
            "products_ref": products_ref,
            "changeovers": changeovers,
            "meta": meta,
        }

    return hourly, shift_summary, overall, hour_avg, downtime, product_data


def build_report(hourly, shift_summary, overall, hour_avg, downtime, product_data):
    # Auto-detect shift names from data
    actual_shifts = hourly["shift"].unique().tolist()
    shift_3rd = _detect_shift(actual_shifts, "3rd") or SHIFT_3RD
    shift_2nd = _detect_shift(actual_shifts, "2nd") or SHIFT_2ND
    shift_1st = _detect_shift(actual_shifts, "1st") or SHIFT_1ST

    # Slice data
    h3 = hourly[hourly["shift"] == shift_3rd].copy()
    h2 = hourly[hourly["shift"] == shift_2nd].copy()
    h1 = hourly[hourly["shift"] == shift_1st].copy()
    ss3 = shift_summary[shift_summary["shift"] == shift_3rd].copy()
    ha3 = hour_avg[hour_avg["shift"] == shift_3rd].copy()
    ha2 = hour_avg[hour_avg["shift"] == shift_2nd].copy()

    date_min = hourly["date"].min().strftime("%B %d, %Y")
    date_max = hourly["date"].max().strftime("%B %d, %Y")
    n_days = h3["date_str"].nunique()

    # Plant averages (production-weighted)
    plant_avail, plant_perf, plant_qual, plant_oee = _aggregate_oee(hourly)
    plant_cph = hourly["total_cases"].sum() / hourly["total_hours"].sum()

    # 3rd shift metrics (production-weighted)
    s3_avail, s3_perf, s3_qual, s3_oee = _aggregate_oee(h3)
    s3_cph = overall[overall["shift"] == shift_3rd]["cases_per_hour"].values[0]
    s3_cases = h3["total_cases"].sum()
    s3_hours = h3["total_hours"].sum()

    # 2nd shift metrics (production-weighted, benchmark)
    s2_avail, s2_perf, s2_qual, s2_oee = _aggregate_oee(h2)
    s2_cph = overall[overall["shift"] == shift_2nd]["cases_per_hour"].values[0]

    # 1st shift (production-weighted)
    _s1_avail, _s1_perf, _s1_qual, s1_oee = _aggregate_oee(h1)
    s1_cph = overall[overall["shift"] == shift_1st]["cases_per_hour"].values[0]

    # Good hours benchmark
    good_hours = h3[h3["total_hours"] >= 0.5]
    target_cph = good_hours["cases_per_hour"].quantile(0.90)
    h3["cases_gap"] = (target_cph - h3["cases_per_hour"]).clip(lower=0) * h3["total_hours"]

    has_downtime = downtime is not None and len(downtime.get("reasons_df", [])) > 0
    has_product = product_data is not None and len(product_data.get("runs", [])) > 0

    sheets = {}

    # Determine primary loss driver (used in overview and elsewhere)
    avail_loss = (1 - s3_avail) * 100
    perf_loss = (1 - s3_perf) * 100
    if perf_loss > avail_loss:
        primary = "PERFORMANCE"
        primary_detail = (f"When the line IS running, it's only hitting {s3_perf:.0%} of rated speed. "
                          f"2nd shift runs at {s2_perf:.0%}. The line is up but slow — "
                          f"micro stops, speed losses, and cycle time gaps are eating output.")
    else:
        primary = "AVAILABILITY"
        primary_detail = (f"The line is down {avail_loss:.0f}% of the time. "
                          f"2nd shift keeps it running {s2_avail:.0%} of their shift. "
                          f"Breakdowns, changeovers, and material waits are the gap.")

    # Product summary for overview (compute early so we can reference it)
    product_insight_lines = []
    if has_product:
        runs = product_data["runs"]
        valid_runs = runs.dropna(subset=["oee_display"])
        if len(valid_runs) > 0:
            by_family = valid_runs.groupby("product_family").agg(
                avg_oee=("oee_display", "mean"),
                n_runs=("oee_display", "count"),
                total_cases=("cases_produced", "sum"),
            ).sort_values("avg_oee")
            worst_prod = by_family.index[0]
            best_prod = by_family.index[-1]

            # Standard vs trayed
            valid_runs_copy = valid_runs.copy()
            valid_runs_copy["pack_type"] = valid_runs_copy["product_family"].map(PRODUCT_PACK_TYPE).fillna("Unknown")
            std_oee = valid_runs_copy[valid_runs_copy["pack_type"].str.startswith("Standard")]["oee_display"].mean()
            tray_oee = valid_runs_copy[valid_runs_copy["pack_type"].str.startswith("Trayed")]["oee_display"].mean()

            product_insight_lines.append(
                f"PRODUCT MIX MATTERS: Standard products avg {std_oee:.1f}% OEE vs trayed products at {tray_oee:.1f}% OEE.")
            product_insight_lines.append(
                f"Best product: {best_prod} ({by_family.loc[best_prod, 'avg_oee']:.1f}% OEE). "
                f"Worst: {worst_prod} ({by_family.loc[worst_prod, 'avg_oee']:.1f}% OEE).")

    # =================================================================
    # SHEET 1: OVERVIEW
    # =================================================================
    overview = []
    overview.append({"": "3RD SHIFT PERFORMANCE ANALYSIS", " ": ""})
    overview.append({"": f"{LINE_NAME}", " ": ""})
    overview.append({"": f"OEE Period: {date_min} — {date_max} ({n_days} shift-days)", " ": ""})
    if has_product:
        pmeta = product_data.get("meta", {})
        overview.append({"": f"Product Period: {pmeta.get('shift_report_date_range', 'see product tabs')}", " ": ""})
    overview.append({"": "", " ": ""})
    overview.append({"": "THE BOTTOM LINE", " ": ""})
    overview.append({"": f"3rd shift is running at {s3_oee:.1f}% OEE — {s2_oee - s3_oee:.1f} points behind 2nd shift.",
                     " ": ""})
    overview.append({"": f"That gap costs {(s2_cph - s3_cph) * s3_hours / n_days:,.0f} cases every night.",
                     " ": ""})
    if product_insight_lines:
        overview.append({"": "", " ": ""})
        for line in product_insight_lines:
            overview.append({"": line, " ": ""})

    overview.append({"": "", " ": ""})
    overview.append({"": f"PRIMARY LOSS DRIVER: {primary}", " ": ""})
    overview.append({"": primary_detail, " ": ""})
    overview.append({"": "", " ": ""})
    overview.append({"": "HOW TO READ THIS REPORT", " ": ""})
    overview.append({"": "Tab 2 — Scorecard: 3rd shift numbers at a glance, compared to plant average and 2nd shift", " ": ""})
    overview.append({"": "Tab 3 — Hour by Hour: when during the shift OEE drops and why", " ": ""})
    overview.append({"": "Tab 4 — Day by Day: the 30-day trend — getting better or worse?", " ": ""})
    overview.append({"": "Tab 5 — Worst Hours: the specific hours that collapsed, with what caused each one", " ": ""})
    overview.append({"": "Tab 6 — vs 2nd Shift: side-by-side comparison — what does 2nd do differently?", " ": ""})
    if has_downtime:
        overview.append({"": "Tab 7 — Downtime Causes: Pareto of what's stopping the line", " ": ""})
        overview.append({"": "Tab 8 — Fault Owners: who owns each category of loss (maintenance, operators, CI, planning)", " ": ""})
    if has_product:
        n = 7 if not has_downtime else 9
        overview.append({"": f"Tab {n} — Product Scorecard: OEE by product — which products run well and which don't", " ": ""})
        overview.append({"": f"Tab {n+1} — Every Run: every 3rd shift run with operator notes — the story behind the numbers", " ": ""})
        overview.append({"": f"Tab {n+2} — Std vs Trayed: standard packs vs trayed — do we have a format problem?", " ": ""})
        overview.append({"": f"Tab {n+3} — Equipment x Product: which machines break down on which products", " ": ""})
    overview.append({"": "Last Tab — Recommended Actions: step-by-step plan to close the gap", " ": ""})

    sheets["Overview"] = pd.DataFrame(overview)

    # =================================================================
    # SHEET 2: SCORECARD
    # =================================================================
    sc = []
    sc.append({"Metric": "3RD SHIFT SCORECARD", "3rd Shift": "", "Plant Avg": "", "2nd Shift (Best)": "", "Gap vs 2nd": ""})
    sc.append({"Metric": "", "3rd Shift": "", "Plant Avg": "", "2nd Shift (Best)": "", "Gap vs 2nd": ""})

    def sc_row(metric, val3, valp, val2, gap, fmt=""):
        return {"Metric": metric, "3rd Shift": val3, "Plant Avg": valp, "2nd Shift (Best)": val2, "Gap vs 2nd": gap}

    sc.append(sc_row("OEE", f"{s3_oee:.1f}%", f"{plant_oee:.1f}%", f"{s2_oee:.1f}%",
                      f"-{s2_oee - s3_oee:.1f} pts"))
    sc.append(sc_row("Availability", f"{s3_avail:.1%}", f"{plant_avail:.1%}", f"{s2_avail:.1%}",
                      f"-{(s2_avail - s3_avail)*100:.1f} pts"))
    sc.append(sc_row("Performance", f"{s3_perf:.1%}", f"{plant_perf:.1%}", f"{s2_perf:.1%}",
                      f"-{(s2_perf - s3_perf)*100:.1f} pts"))
    sc.append(sc_row("Quality", f"{s3_qual:.1%}", f"{plant_qual:.1%}", f"{s2_qual:.1%}",
                      f"-{(s2_qual - s3_qual)*100:.1f} pts"))
    sc.append({"Metric": "", "3rd Shift": "", "Plant Avg": "", "2nd Shift (Best)": "", "Gap vs 2nd": ""})
    sc.append(sc_row("Cases/Hour", f"{s3_cph:,.0f}", f"{plant_cph:,.0f}", f"{s2_cph:,.0f}",
                      f"-{s2_cph - s3_cph:,.0f} CPH"))
    sc.append(sc_row("Total Cases", f"{s3_cases:,.0f}", "", "", ""))
    sc.append(sc_row("Production Hours", f"{s3_hours:,.1f}", "", "", ""))
    sc.append(sc_row("Shift-Days", f"{n_days}", "", "", ""))

    sc.append({"Metric": "", "3rd Shift": "", "Plant Avg": "", "2nd Shift (Best)": "", "Gap vs 2nd": ""})
    sc.append({"Metric": "WHERE IS OEE LOST?", "3rd Shift": "Loss %", "Plant Avg": "Share of Total Loss",
               "2nd Shift (Best)": "", "Gap vs 2nd": ""})

    total_loss = avail_loss + perf_loss + (1 - s3_qual) * 100
    sc.append(sc_row("Availability Loss", f"{avail_loss:.1f}%",
                      f"{avail_loss/total_loss*100:.0f}% of total loss", "", "Line not running"))
    sc.append(sc_row("Performance Loss", f"{perf_loss:.1f}%",
                      f"{perf_loss/total_loss*100:.0f}% of total loss", "", "Running slow"))
    sc.append(sc_row("Quality Loss", f"{(1-s3_qual)*100:.1f}%",
                      f"{(1-s3_qual)*100/total_loss*100:.0f}% of total loss", "", "Rejected product"))

    sc.append({"Metric": "", "3rd Shift": "", "Plant Avg": "", "2nd Shift (Best)": "", "Gap vs 2nd": ""})
    sc.append({"Metric": "CONSISTENCY", "3rd Shift": "", "Plant Avg": "", "2nd Shift (Best)": "", "Gap vs 2nd": ""})

    std3 = h3["oee_pct"].std()
    std2 = h2["oee_pct"].std()
    below20 = (h3["oee_pct"] < 20).sum()
    above50 = (h3["oee_pct"] > 50).sum()
    total_hrs = len(h3)

    sc.append(sc_row("OEE Std Deviation", f"{std3:.1f}", "", f"{std2:.1f}",
                      "Lower = more consistent"))
    sc.append(sc_row("Hours below 20% OEE", f"{below20} of {total_hrs} ({below20/total_hrs*100:.0f}%)",
                      "", "", "Near-zero production hours"))
    sc.append(sc_row("Hours above 50% OEE", f"{above50} of {total_hrs} ({above50/total_hrs*100:.0f}%)",
                      "", "", "Good hours — the line CAN run"))

    sheets["Scorecard"] = pd.DataFrame(sc)

    # =================================================================
    # SHEET 3: HOUR BY HOUR
    # =================================================================
    hbh = []
    hbh.append({"Hour": "HOUR-BY-HOUR PATTERN — 3RD SHIFT", "OEE %": "", "Cases/Hr": "",
                "Availability": "", "Performance": "", "Insight": ""})
    hbh.append({"Hour": "When during the shift is OEE dropping?", "OEE %": "", "Cases/Hr": "",
                "Availability": "", "Performance": "", "Insight": ""})
    hbh.append({"Hour": "", "OEE %": "", "Cases/Hr": "", "Availability": "", "Performance": "", "Insight": ""})

    h3_hourly_avg = (
        h3.groupby("shift_hour")
        .agg(avg_oee=("oee_pct", "mean"), avg_cph=("cases_per_hour", "mean"),
             avg_avail=("availability", "mean"), avg_perf=("performance", "mean"),
             n=("oee_pct", "count"))
        .reset_index()
        .sort_values("shift_hour")
    )
    # Add time_block label from shift_hour
    h3_hourly_avg["time_block"] = h3_hourly_avg["shift_hour"].apply(
        lambda h: f"{int(h)}:00" if pd.notna(h) else ""
    )

    best_hr = h3_hourly_avg.loc[h3_hourly_avg["avg_oee"].idxmax()]
    worst_hr = h3_hourly_avg.loc[h3_hourly_avg["avg_oee"].idxmin()]
    min_hour = h3_hourly_avg["shift_hour"].min()

    for _, row in h3_hourly_avg.iterrows():
        hr_num = int(row["shift_hour"])
        insight = ""
        if hr_num == min_hour:
            rest_avg = h3_hourly_avg[h3_hourly_avg["shift_hour"] != min_hour]["avg_oee"].mean()
            gap = rest_avg - row["avg_oee"]
            if gap > 2:
                insight = f"Startup: {gap:.0f} pts below rest of shift"
        if row["avg_oee"] == best_hr["avg_oee"]:
            insight = "BEST HOUR"
        if row["avg_oee"] == worst_hr["avg_oee"]:
            insight = "WORST HOUR"
        if row["avg_avail"] < 0.50:
            insight += " | Line down >50% of this hour" if insight else "Line down >50% of this hour"
        if row["avg_perf"] < 0.50:
            insight += " | Speed below 50%" if insight else "Speed below 50%"

        hbh.append({
            "Hour": f"Hour {hr_num} ({row['time_block']})",
            "OEE %": f"{row['avg_oee']:.1f}%",
            "Cases/Hr": f"{row['avg_cph']:,.0f}",
            "Availability": f"{row['avg_avail']:.0%}",
            "Performance": f"{row['avg_perf']:.0%}",
            "Insight": insight,
        })

    hbh.append({"Hour": "", "OEE %": "", "Cases/Hr": "", "Availability": "", "Performance": "", "Insight": ""})
    hbh.append({"Hour": "SAME HOURS — 2ND SHIFT COMPARISON", "OEE %": "", "Cases/Hr": "",
                "Availability": "", "Performance": "", "Insight": ""})

    ha2_sorted = ha2.sort_values("shift_hour") if "shift_hour" in ha2.columns else ha2
    for _, row in ha2_sorted.iterrows():
        hr_num = int(row["shift_hour"])
        h3_match = h3_hourly_avg[h3_hourly_avg["shift_hour"] == hr_num]
        gap = ""
        if len(h3_match) > 0:
            diff = row["oee_pct"] - h3_match.iloc[0]["avg_oee"]
            gap = f"2nd is +{diff:.1f} pts" if diff > 0 else f"3rd is +{abs(diff):.1f} pts"

        hour_label = row.get("time_block", f"{hr_num}:00") or f"{hr_num}:00"
        has_cph_ha2 = "cases_per_hour" in ha2.columns and ha2["cases_per_hour"].sum() > 0
        cph_val = f"{row['cases_per_hour']:,.0f}" if has_cph_ha2 else ""
        avail_val = f"{row['availability']:.0%}" if "availability" in ha2.columns else ""
        perf_val = f"{row['performance']:.0%}" if "performance" in ha2.columns else ""

        hbh.append({
            "Hour": f"Hour {hr_num} ({hour_label})",
            "OEE %": f"{row['oee_pct']:.1f}%",
            "Cases/Hr": cph_val,
            "Availability": avail_val,
            "Performance": perf_val,
            "Insight": gap,
        })

    sheets["Hour by Hour"] = pd.DataFrame(hbh)

    # =================================================================
    # SHEET 4: DAY BY DAY
    # =================================================================
    dbd = []
    ss3_sorted = ss3.sort_values("date_str")
    ss2 = shift_summary[shift_summary["shift"] == shift_2nd].copy()

    # If we have product data, build a date->product lookup
    date_product_map = {}
    if has_product:
        for _, prow in product_data["runs"].iterrows():
            d = prow["date"]
            pf = prow["product_family"]
            if d in date_product_map:
                date_product_map[d] += f", {pf}"
            else:
                date_product_map[d] = pf

    for _, row in ss3_sorted.iterrows():
        date = row["date_str"]
        dow = pd.Timestamp(date).day_name()

        s2_day = ss2[ss2["date_str"] == date]
        s2_oee_day = f"{s2_day.iloc[0]['oee_pct']:.1f}%" if len(s2_day) > 0 else ""

        flag = ""
        if row["oee_pct"] < 25:
            flag = "CRITICAL"
        elif row["oee_pct"] < 30:
            flag = "Poor"
        elif row["oee_pct"] > 45:
            flag = "Good"

        # Look up product for this date
        prod_running = date_product_map.get(date, "")

        entry = {
            "Date": date,
            "Day": dow,
            "OEE %": f"{row['oee_pct']:.1f}%",
            "Cases/Hr": f"{row['cases_per_hour']:,.0f}",
            "Total Cases": f"{row['total_cases']:,.0f}",
            "2nd Shift OEE": s2_oee_day,
            "Status": flag,
        }
        if has_product:
            entry["Product Running"] = prod_running
        dbd.append(entry)

    # Trend analysis
    if len(ss3_sorted) >= 6:
        first_half = ss3_sorted.head(len(ss3_sorted) // 2)["oee_pct"].mean()
        second_half = ss3_sorted.tail(len(ss3_sorted) // 2)["oee_pct"].mean()
        if second_half > first_half + 1:
            trend = f"IMPROVING: first half avg {first_half:.1f}% -> second half {second_half:.1f}%"
        elif second_half < first_half - 1:
            trend = f"DECLINING: first half avg {first_half:.1f}% -> second half {second_half:.1f}%"
        else:
            trend = f"FLAT: first half avg {first_half:.1f}%, second half {second_half:.1f}%"
        empty = {"Date": "", "Day": "", "OEE %": "", "Cases/Hr": "", "Total Cases": "",
                 "2nd Shift OEE": "", "Status": ""}
        if has_product:
            empty["Product Running"] = ""
        dbd.append(empty)
        trend_row = {"Date": "TREND", "Day": trend, "OEE %": "", "Cases/Hr": "", "Total Cases": "",
                     "2nd Shift OEE": "", "Status": ""}
        if has_product:
            trend_row["Product Running"] = ""
        dbd.append(trend_row)

    # Best and worst days
    best_day = ss3_sorted.loc[ss3_sorted["oee_pct"].idxmax()]
    worst_day = ss3_sorted.loc[ss3_sorted["oee_pct"].idxmin()]
    for label, day in [("BEST DAY", best_day), ("WORST DAY", worst_day)]:
        entry = {"Date": label,
                 "Day": f"{day['date_str']} ({pd.Timestamp(day['date_str']).day_name()})",
                 "OEE %": f"{day['oee_pct']:.1f}%", "Cases/Hr": f"{day['cases_per_hour']:,.0f}",
                 "Total Cases": f"{day['total_cases']:,.0f}", "2nd Shift OEE": "", "Status": ""}
        if has_product:
            entry["Product Running"] = date_product_map.get(day["date_str"], "")
        dbd.append(entry)

    # Day of week summary
    empty = {"Date": "", "Day": "", "OEE %": "", "Cases/Hr": "", "Total Cases": "",
             "2nd Shift OEE": "", "Status": ""}
    if has_product:
        empty["Product Running"] = ""
    dbd.append(empty)
    header = {"Date": "DAY OF WEEK AVG", "Day": "", "OEE %": "", "Cases/Hr": "", "Total Cases": "",
              "2nd Shift OEE": "", "Status": ""}
    if has_product:
        header["Product Running"] = ""
    dbd.append(header)

    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dow3 = h3.groupby("day_of_week").agg(
        avg_oee=("oee_pct", "mean"), avg_cph=("cases_per_hour", "mean"),
        total_cases=("total_cases", "sum"), n=("oee_pct", "count")
    ).reindex(dow_order).dropna(how="all")

    for day_name, drow in dow3.iterrows():
        flag = ""
        if drow["avg_oee"] == dow3["avg_oee"].min() and dow3["avg_oee"].max() - dow3["avg_oee"].min() > 3:
            flag = "WORST DAY"
        if drow["avg_oee"] == dow3["avg_oee"].max() and dow3["avg_oee"].max() - dow3["avg_oee"].min() > 3:
            flag = "BEST DAY"
        entry = {
            "Date": day_name, "Day": f"{int(drow['n'])} hours",
            "OEE %": f"{drow['avg_oee']:.1f}%", "Cases/Hr": f"{drow['avg_cph']:,.0f}",
            "Total Cases": f"{drow['total_cases']:,.0f}", "2nd Shift OEE": "", "Status": flag
        }
        if has_product:
            entry["Product Running"] = ""
        dbd.append(entry)

    sheets["Day by Day"] = pd.DataFrame(dbd)

    # =================================================================
    # SHEET 5: WORST HOURS
    # =================================================================
    wh = []
    shift_worst = h3[h3["total_hours"] >= 0.5].nsmallest(20, "oee_pct")

    for _, row in shift_worst.iterrows():
        a = row["availability"]
        p = row["performance"]
        q = row["quality"]

        if a < 0.20:
            what = f"Line down most of the hour (Avail {a:.0%})"
        elif a < 0.50:
            what = f"Major stoppage — line up only {a:.0%} of the hour"
        elif p < 0.30:
            what = f"Line was up ({a:.0%}) but crawling at {p:.0%} speed"
        elif p < 0.50:
            what = f"Speed loss — {p:.0%} of rated speed despite {a:.0%} uptime"
        elif a < 0.70 and p < 0.70:
            what = f"Both: line down {(1-a)*100:.0f}% of hour AND slow when running ({p:.0%} speed)"
        elif q < 0.95:
            what = f"Quality issue — {q:.1%} first pass"
        else:
            what = f"Avail {a:.0%} / Perf {p:.0%} — multiple small losses"

        wh.append({
            "Date": row["date_str"],
            "Day": row["day_of_week"],
            "Time": row["time_block"],
            "OEE %": round(row["oee_pct"], 1),
            "Cases/Hr": round(row["cases_per_hour"], 0),
            "Avail %": round(a * 100, 1),
            "Perf %": round(p * 100, 1),
            "Qual %": round(q * 100, 1),
            "What Happened": what,
        })

    sheets["Worst Hours"] = pd.DataFrame(wh)

    # =================================================================
    # SHEET 6: VS 2ND SHIFT
    # =================================================================
    vs = []
    vs.append({"Metric": "3RD SHIFT vs 2ND SHIFT — WHERE IS THE GAP?", "3rd Shift": "", "2nd Shift": "", "Difference": "", "What This Means": ""})
    vs.append({"Metric": "", "3rd Shift": "", "2nd Shift": "", "Difference": "", "What This Means": ""})

    oee_gap = s2_oee - s3_oee
    avail_gap = (s2_avail - s3_avail) * 100
    perf_gap = (s2_perf - s3_perf) * 100
    cph_gap = s2_cph - s3_cph

    vs.append({"Metric": "OEE", "3rd Shift": f"{s3_oee:.1f}%", "2nd Shift": f"{s2_oee:.1f}%",
               "Difference": f"-{oee_gap:.1f} pts", "What This Means": f"3rd produces {cph_gap:,.0f} fewer cases/hr"})
    vs.append({"Metric": "Availability", "3rd Shift": f"{s3_avail:.1%}", "2nd Shift": f"{s2_avail:.1%}",
               "Difference": f"-{avail_gap:.1f} pts",
               "What This Means": "Line stops more on 3rd" if avail_gap > 2 else "Similar uptime"})
    vs.append({"Metric": "Performance", "3rd Shift": f"{s3_perf:.1%}", "2nd Shift": f"{s2_perf:.1%}",
               "Difference": f"-{perf_gap:.1f} pts",
               "What This Means": "Line runs slower on 3rd" if perf_gap > 2 else "Similar speed"})
    vs.append({"Metric": "Cases/Hour", "3rd Shift": f"{s3_cph:,.0f}", "2nd Shift": f"{s2_cph:,.0f}",
               "Difference": f"-{cph_gap:,.0f}", "What This Means": f"{cph_gap:,.0f} cases/hr left on the table"})

    vs.append({"Metric": "", "3rd Shift": "", "2nd Shift": "", "Difference": "", "What This Means": ""})
    vs.append({"Metric": "HOUR-BY-HOUR GAP", "3rd Shift": "3rd OEE", "2nd Shift": "2nd OEE",
               "Difference": "Gap", "What This Means": ""})

    # Compare all hours present in 3rd shift data with 2nd shift
    for _, h3_row in h3_hourly_avg.iterrows():
        hr_num = int(h3_row["shift_hour"])
        h2_hr = ha2[ha2["shift_hour"] == hr_num]
        if len(h2_hr) > 0:
            h3_val = h3_row["avg_oee"]
            h2_val = h2_hr.iloc[0]["oee_pct"]
            gap = h2_val - h3_val
            tb = h3_row["time_block"]
            note = ""
            if gap > 10:
                note = "BIG GAP — investigate this hour"
            elif gap < 0:
                note = "3rd shift WINS this hour"
            vs.append({"Metric": f"Hour {hr_num} ({tb})", "3rd Shift": f"{h3_val:.1f}%",
                        "2nd Shift": f"{h2_val:.1f}%", "Difference": f"{'-' if gap > 0 else '+'}{abs(gap):.1f} pts",
                        "What This Means": note})

    vs.append({"Metric": "", "3rd Shift": "", "2nd Shift": "", "Difference": "", "What This Means": ""})
    vs.append({"Metric": "CONSISTENCY", "3rd Shift": "", "2nd Shift": "", "Difference": "", "What This Means": ""})
    vs.append({"Metric": "Std Deviation", "3rd Shift": f"{std3:.1f}", "2nd Shift": f"{std2:.1f}",
               "Difference": "", "What This Means": "Higher = more variation hour to hour"})

    below20_3 = (h3["oee_pct"] < 20).sum()
    below20_2 = (h2["oee_pct"] < 20).sum()
    vs.append({"Metric": "Hours < 20% OEE", "3rd Shift": str(below20_3), "2nd Shift": str(below20_2),
               "Difference": "", "What This Means": "Collapse hours — near-zero production"})

    above50_3 = (h3["oee_pct"] > 50).sum()
    above50_2 = (h2["oee_pct"] > 50).sum()
    vs.append({"Metric": "Hours > 50% OEE", "3rd Shift": str(above50_3), "2nd Shift": str(above50_2),
               "Difference": "", "What This Means": "Good production hours"})

    vs.append({"Metric": "", "3rd Shift": "", "2nd Shift": "", "Difference": "", "What This Means": ""})
    if perf_gap > avail_gap:
        vs.append({"Metric": "VERDICT", "3rd Shift": "", "2nd Shift": "", "Difference": "",
                    "What This Means": f"The biggest gap is PERFORMANCE (-{perf_gap:.1f} pts). "
                    f"3rd shift keeps the line running about as well as 2nd, but it runs SLOWER. "
                    f"Focus: micro stops, speed settings, operator response time."})
    else:
        vs.append({"Metric": "VERDICT", "3rd Shift": "", "2nd Shift": "", "Difference": "",
                    "What This Means": f"The biggest gap is AVAILABILITY (-{avail_gap:.1f} pts). "
                    f"3rd shift has more downtime than 2nd. "
                    f"Focus: changeover discipline, breakdown response, material staging."})

    sheets["vs 2nd Shift"] = pd.DataFrame(vs)

    # =================================================================
    # SHEET 7: DOWNTIME CAUSES (if available)
    # =================================================================
    if has_downtime:
        reasons_df = downtime["reasons_df"].copy()
        actionable = reasons_df[~reasons_df["reason"].isin(EXCLUDE_REASONS)].copy()
        actionable = actionable.sort_values("total_minutes", ascending=False).reset_index(drop=True)

        total_min = actionable["total_minutes"].sum()
        actionable["pct"] = (actionable["total_minutes"] / total_min * 100).round(1)
        actionable["cum_pct"] = actionable["pct"].cumsum().round(1)
        actionable["avg_min"] = (actionable["total_minutes"] / actionable["total_occurrences"]).round(1)
        actionable["fault_type"] = actionable["reason"].apply(classify_fault)

        pareto = actionable[["reason", "fault_type", "total_occurrences", "total_minutes",
                              "total_hours", "avg_min", "pct", "cum_pct"]].copy()
        pareto.columns = ["Cause", "Fault Type", "Events", "Total Minutes", "Total Hours",
                          "Avg Min/Event", "% of Total", "Cumulative %"]

        sheets["Downtime Causes"] = pareto

    # =================================================================
    # SHEET 8: FAULT OWNERS
    # =================================================================
    if has_downtime:
        reasons_df = downtime["reasons_df"].copy()
        reasons_df["fault_type"] = reasons_df["reason"].apply(classify_fault)

        fault_sum = (
            reasons_df.groupby("fault_type")
            .agg(events=("total_occurrences", "sum"), minutes=("total_minutes", "sum"),
                 hours=("total_hours", "sum"), n_codes=("reason", "count"))
            .sort_values("minutes", ascending=False)
            .reset_index()
        )
        grand = fault_sum["minutes"].sum()
        fault_sum["pct"] = (fault_sum["minutes"] / grand * 100).round(1)

        ownership = {
            "Equipment / Mechanical": "Maintenance / Reliability team",
            "Micro Stops": "Engineering + Operators — sensor tuning, line adjustments, guide rails",
            "Process / Changeover": "CI / Operations — SMED, standard work, pre-staging",
            "Scheduled / Non-Production": "Planning / Management — optimize schedule, reduce non-production windows",
            "Data Gap (uncoded)": "Supervisors — enforce reason code entry, simplify code tree",
            "Other / Unclassified": "Review and reclassify these reason codes",
        }
        fault_sum["owner"] = fault_sum["fault_type"].map(ownership).fillna("TBD")

        what_to_ask = {
            "Equipment / Mechanical": "Are PMs current? What parts keep failing? Is there a pattern by shift/day?",
            "Micro Stops": "Where on the line do short stops happen most? What sensor or transfer point?",
            "Process / Changeover": "How long is the average changeover? What steps take the longest?",
            "Scheduled / Non-Production": "Can non-production windows be reduced or shifted to lower-demand periods?",
            "Data Gap (uncoded)": "Why aren't operators coding these? Is the reason code list too long or confusing?",
            "Other / Unclassified": "These need to be reviewed and categorized properly.",
        }
        fault_sum["question_to_ask"] = fault_sum["fault_type"].map(what_to_ask).fillna("")

        fo = fault_sum[["fault_type", "n_codes", "events", "hours", "pct", "owner", "question_to_ask"]].copy()
        fo.columns = ["Fault Category", "# Codes", "Events", "Hours", "% of All Downtime",
                       "Who Owns This", "Question to Ask"]

        sheets["Fault Owners"] = fo

    # =================================================================
    # SHEET: PRODUCT SCORECARD (if product data available)
    # =================================================================
    if has_product:
        runs = product_data["runs"]
        valid = runs.dropna(subset=["oee_display"]).copy()

        by_fam = (
            valid.groupby("product_family")
            .agg(
                n_runs=("oee_display", "count"),
                avg_oee=("oee_display", "mean"),
                min_oee=("oee_display", "min"),
                max_oee=("oee_display", "max"),
                total_cases=("cases_produced", "sum"),
                avg_cases=("cases_produced", "mean"),
                total_dt=("downtime_minutes", "sum"),
                avg_dt=("downtime_minutes", "mean"),
                total_co=("changeover_minutes", "sum"),
            )
            .sort_values("avg_oee")
            .reset_index()
        )

        psc = []
        psc.append({"Product": "3RD SHIFT — OEE BY PRODUCT", "Pack Type": "",
                     "Rated Speed": "", "Runs": "", "Avg OEE": "", "Best Run": "",
                     "Worst Run": "", "Total Cases": "", "Avg Cases/Run": "",
                     "Avg Downtime": "", "Status": ""})
        psc.append({"Product": "Sorted worst to best. Which products does 3rd shift struggle with?",
                     "Pack Type": "", "Rated Speed": "", "Runs": "", "Avg OEE": "",
                     "Best Run": "", "Worst Run": "", "Total Cases": "",
                     "Avg Cases/Run": "", "Avg Downtime": "", "Status": ""})
        psc.append({"Product": "", "Pack Type": "", "Rated Speed": "", "Runs": "",
                     "Avg OEE": "", "Best Run": "", "Worst Run": "", "Total Cases": "",
                     "Avg Cases/Run": "", "Avg Downtime": "", "Status": ""})

        for _, frow in by_fam.iterrows():
            fam = frow["product_family"]
            rated = PRODUCT_RATED_SPEED.get(fam, "")
            pack = PRODUCT_PACK_TYPE.get(fam, "")

            status = ""
            if frow["avg_oee"] < 20:
                status = "CRITICAL"
            elif frow["avg_oee"] < 30:
                status = "Problem"
            elif frow["avg_oee"] > 45:
                status = "Solid"

            dt_str = f"{frow['avg_dt']:.0f} min" if pd.notna(frow["avg_dt"]) else "—"

            psc.append({
                "Product": fam,
                "Pack Type": pack,
                "Rated Speed": f"{rated} cpm" if rated else "",
                "Runs": int(frow["n_runs"]),
                "Avg OEE": f"{frow['avg_oee']:.1f}%",
                "Best Run": f"{frow['max_oee']:.1f}%",
                "Worst Run": f"{frow['min_oee']:.1f}%",
                "Total Cases": f"{frow['total_cases']:,.0f}" if pd.notna(frow["total_cases"]) else "—",
                "Avg Cases/Run": f"{frow['avg_cases']:,.0f}" if pd.notna(frow["avg_cases"]) else "—",
                "Avg Downtime": dt_str,
                "Status": status,
            })

        # Summary row
        psc.append({"Product": "", "Pack Type": "", "Rated Speed": "", "Runs": "",
                     "Avg OEE": "", "Best Run": "", "Worst Run": "", "Total Cases": "",
                     "Avg Cases/Run": "", "Avg Downtime": "", "Status": ""})
        psc.append({
            "Product": "ALL PRODUCTS",
            "Pack Type": "",
            "Rated Speed": "",
            "Runs": int(by_fam["n_runs"].sum()),
            "Avg OEE": f"{valid['oee_display'].mean():.1f}%",
            "Best Run": f"{valid['oee_display'].max():.1f}%",
            "Worst Run": f"{valid['oee_display'].min():.1f}%",
            "Total Cases": f"{valid['cases_produced'].sum():,.0f}",
            "Avg Cases/Run": f"{valid['cases_produced'].mean():,.0f}",
            "Avg Downtime": "",
            "Status": "",
        })

        # Key insight
        psc.append({"Product": "", "Pack Type": "", "Rated Speed": "", "Runs": "",
                     "Avg OEE": "", "Best Run": "", "Worst Run": "", "Total Cases": "",
                     "Avg Cases/Run": "", "Avg Downtime": "", "Status": ""})

        trayed = by_fam[by_fam["product_family"].apply(lambda x: PRODUCT_PACK_TYPE.get(x, "")).str.contains("Trayed")]
        standard = by_fam[~by_fam["product_family"].apply(lambda x: PRODUCT_PACK_TYPE.get(x, "")).str.contains("Trayed")]
        if len(trayed) > 0 and len(standard) > 0:
            t_valid = valid[valid["product_family"].apply(lambda x: "Trayed" in PRODUCT_PACK_TYPE.get(x, ""))]
            s_valid = valid[valid["product_family"].apply(lambda x: "Standard" in PRODUCT_PACK_TYPE.get(x, ""))]
            t_avg = t_valid["oee_display"].mean() if len(t_valid) > 0 else 0
            s_avg = s_valid["oee_display"].mean() if len(s_valid) > 0 else 0
            psc.append({
                "Product": "KEY FINDING",
                "Pack Type": f"Standard products avg {s_avg:.1f}% OEE vs trayed at {t_avg:.1f}%. "
                             f"Trayed products run at lower speeds (572-720 cpm vs 1200) AND have more stoppages. "
                             f"When 3rd shift runs peaches or pears, expect OEE below 30%.",
                "Rated Speed": "", "Runs": "", "Avg OEE": "", "Best Run": "",
                "Worst Run": "", "Total Cases": "", "Avg Cases/Run": "",
                "Avg Downtime": "", "Status": "",
            })

        sheets["Product Scorecard"] = pd.DataFrame(psc)

    # =================================================================
    # SHEET: EVERY RUN (product detail with operator notes)
    # =================================================================
    if has_product:
        runs = product_data["runs"]
        valid = runs.copy()
        valid = valid.sort_values("date")

        detail = []
        for _, r in valid.iterrows():
            oee_str = f"{r['oee_display']:.1f}%" if pd.notna(r["oee_display"]) else "—"
            cases_str = f"{r['cases_produced']:,.0f}" if pd.notna(r["cases_produced"]) else "—"
            dt_str = f"{r['downtime_minutes']:.0f}" if pd.notna(r["downtime_minutes"]) else "—"
            co_str = f"{r['changeover_minutes']:.0f}" if pd.notna(r["changeover_minutes"]) else "—"

            status = ""
            if pd.notna(r["oee_display"]):
                if r["oee_display"] < 20:
                    status = "CRITICAL"
                elif r["oee_display"] < 30:
                    status = "Poor"
                elif r["oee_display"] > 45:
                    status = "Good"

            notes = r.get("notes", "") or ""
            # Truncate very long notes for display
            if len(notes) > 300:
                notes = notes[:297] + "..."

            detail.append({
                "Date": r["date"],
                "Product": r["product_family"],
                "OEE %": oee_str,
                "Cases": cases_str,
                "Downtime (min)": dt_str,
                "Changeover (min)": co_str,
                "Equipment Mentioned": ", ".join(r["equipment_mentioned"]) if r["equipment_mentioned"] else "—",
                "Operator Notes": notes,
                "Status": status,
            })

        sheets["Every Run"] = pd.DataFrame(detail)

    # =================================================================
    # SHEET: STANDARD vs TRAYED
    # =================================================================
    if has_product:
        runs = product_data["runs"]
        valid = runs.dropna(subset=["oee_display"]).copy()
        valid["pack_type"] = valid["product_family"].map(PRODUCT_PACK_TYPE).fillna("Unknown")

        std_runs = valid[valid["pack_type"].str.startswith("Standard")]
        tray_runs = valid[valid["pack_type"].str.startswith("Trayed")]

        svt = []
        svt.append({"Metric": "STANDARD vs TRAYED PRODUCTS — 3RD SHIFT",
                     "Standard (8pk/12pk)": "", "Trayed (6/4)": "", "Gap": "",
                     "What This Means": ""})
        svt.append({"Metric": "Do trayed products (peaches, pears, trayed corn) drag down 3rd shift OEE?",
                     "Standard (8pk/12pk)": "", "Trayed (6/4)": "", "Gap": "",
                     "What This Means": ""})
        svt.append({"Metric": "", "Standard (8pk/12pk)": "", "Trayed (6/4)": "",
                     "Gap": "", "What This Means": ""})

        s_avg = std_runs["oee_display"].mean() if len(std_runs) > 0 else 0
        t_avg = tray_runs["oee_display"].mean() if len(tray_runs) > 0 else 0
        s_cases = std_runs["cases_produced"].sum() if len(std_runs) > 0 else 0
        t_cases = tray_runs["cases_produced"].sum() if len(tray_runs) > 0 else 0
        s_count = len(std_runs)
        t_count = len(tray_runs)

        svt.append({"Metric": "Average OEE",
                     "Standard (8pk/12pk)": f"{s_avg:.1f}%",
                     "Trayed (6/4)": f"{t_avg:.1f}%",
                     "Gap": f"{s_avg - t_avg:.1f} pts",
                     "What This Means": "Trayed products perform dramatically worse" if s_avg - t_avg > 10 else ""})
        svt.append({"Metric": "# Runs",
                     "Standard (8pk/12pk)": str(s_count),
                     "Trayed (6/4)": str(t_count),
                     "Gap": "",
                     "What This Means": f"Trayed = {t_count/(s_count+t_count)*100:.0f}% of all runs" if s_count + t_count > 0 else ""})
        svt.append({"Metric": "Total Cases",
                     "Standard (8pk/12pk)": f"{s_cases:,.0f}",
                     "Trayed (6/4)": f"{t_cases:,.0f}",
                     "Gap": "",
                     "What This Means": ""})
        svt.append({"Metric": "Rated Speed",
                     "Standard (8pk/12pk)": "1,200 cpm",
                     "Trayed (6/4)": "572-720 cpm",
                     "Gap": "~2x speed difference",
                     "What This Means": "Trayed inherently runs slower — but OEE should still be achievable"})

        # Best/worst run in each category
        if len(std_runs) > 0:
            s_best = std_runs.loc[std_runs["oee_display"].idxmax()]
            s_worst = std_runs.loc[std_runs["oee_display"].idxmin()]
            svt.append({"Metric": "", "Standard (8pk/12pk)": "", "Trayed (6/4)": "", "Gap": "", "What This Means": ""})
            svt.append({"Metric": "Best Standard Run",
                         "Standard (8pk/12pk)": f"{s_best['product_family']} on {s_best['date']}",
                         "Trayed (6/4)": "",
                         "Gap": f"{s_best['oee_display']:.1f}% OEE",
                         "What This Means": f"{s_best['cases_produced']:,.0f} cases" if pd.notna(s_best["cases_produced"]) else ""})
            svt.append({"Metric": "Worst Standard Run",
                         "Standard (8pk/12pk)": f"{s_worst['product_family']} on {s_worst['date']}",
                         "Trayed (6/4)": "",
                         "Gap": f"{s_worst['oee_display']:.1f}% OEE",
                         "What This Means": f"{s_worst['cases_produced']:,.0f} cases" if pd.notna(s_worst["cases_produced"]) else ""})

        if len(tray_runs) > 0:
            t_best = tray_runs.loc[tray_runs["oee_display"].idxmax()]
            t_worst = tray_runs.loc[tray_runs["oee_display"].idxmin()]
            svt.append({"Metric": "Best Trayed Run",
                         "Standard (8pk/12pk)": "",
                         "Trayed (6/4)": f"{t_best['product_family']} on {t_best['date']}",
                         "Gap": f"{t_best['oee_display']:.1f}% OEE",
                         "What This Means": f"{t_best['cases_produced']:,.0f} cases" if pd.notna(t_best["cases_produced"]) else ""})
            svt.append({"Metric": "Worst Trayed Run",
                         "Standard (8pk/12pk)": "",
                         "Trayed (6/4)": f"{t_worst['product_family']} on {t_worst['date']}",
                         "Gap": f"{t_worst['oee_display']:.1f}% OEE",
                         "What This Means": f"{t_worst['cases_produced']:,.0f} cases" if pd.notna(t_worst["cases_produced"]) else ""})

        # Products in each category
        svt.append({"Metric": "", "Standard (8pk/12pk)": "", "Trayed (6/4)": "", "Gap": "", "What This Means": ""})
        svt.append({"Metric": "PRODUCTS IN EACH CATEGORY", "Standard (8pk/12pk)": "", "Trayed (6/4)": "", "Gap": "", "What This Means": ""})

        std_prods = std_runs.groupby("product_family")["oee_display"].mean().sort_values()
        tray_prods = tray_runs.groupby("product_family")["oee_display"].mean().sort_values()

        all_prods = list(std_prods.items()) + [(None, None)] * max(0, len(tray_prods) - len(std_prods))
        all_tray = list(tray_prods.items()) + [(None, None)] * max(0, len(std_prods) - len(tray_prods))

        for i in range(max(len(std_prods), len(tray_prods))):
            sp = all_prods[i] if i < len(all_prods) and all_prods[i][0] else ("", "")
            tp = all_tray[i] if i < len(all_tray) and all_tray[i][0] else ("", "")
            svt.append({
                "Metric": "",
                "Standard (8pk/12pk)": f"{sp[0]} ({sp[1]:.1f}% OEE)" if sp[0] else "",
                "Trayed (6/4)": f"{tp[0]} ({tp[1]:.1f}% OEE)" if tp[0] else "",
                "Gap": "",
                "What This Means": "",
            })

        # Verdict
        svt.append({"Metric": "", "Standard (8pk/12pk)": "", "Trayed (6/4)": "", "Gap": "", "What This Means": ""})
        if s_avg - t_avg > 15:
            verdict = (f"YES — trayed products are a major OEE drag. At {t_avg:.1f}% avg OEE vs {s_avg:.1f}% for standard, "
                       f"every trayed run pulls the shift average down by ~{s_avg - t_avg:.0f} points. "
                       f"Two actions: (1) build trayed-specific setup standards and (2) schedule trayed runs "
                       f"when your best operators are available.")
        elif s_avg - t_avg > 5:
            verdict = (f"Trayed products run {s_avg - t_avg:.1f} pts below standard. "
                       f"The gap is real but manageable. Focus on tray packer and shrink wrapper setup for trayed runs.")
        else:
            verdict = "Product format doesn't seem to be a major factor. Focus on equipment reliability instead."
        svt.append({"Metric": "VERDICT", "Standard (8pk/12pk)": "", "Trayed (6/4)": "",
                     "Gap": "", "What This Means": verdict})

        sheets["Std vs Trayed"] = pd.DataFrame(svt)

    # =================================================================
    # SHEET: EQUIPMENT x PRODUCT
    # =================================================================
    if has_product:
        runs = product_data["runs"]
        valid = runs.dropna(subset=["oee_display"]).copy()

        # Build equipment x product matrix from operator notes
        equip_prod = defaultdict(lambda: defaultdict(int))
        equip_total = Counter()
        for _, r in valid.iterrows():
            for eq in r["equipment_mentioned"]:
                equip_prod[eq][r["product_family"]] += 1
                equip_total[eq] += 1

        # Sort equipment by total mentions
        sorted_equip = sorted(equip_total.items(), key=lambda x: -x[1])
        all_prods_in_data = sorted(valid["product_family"].unique())

        exb = []
        exb.append({"Equipment": "EQUIPMENT ISSUES BY PRODUCT — 3RD SHIFT",
                     "Total Mentions": "", "Top Product": "",
                     "Mentions on Top Product": "", "Pattern": ""})
        exb.append({"Equipment": "Which machines break down most, and does it depend on what product is running?",
                     "Total Mentions": "", "Top Product": "",
                     "Mentions on Top Product": "", "Pattern": ""})
        exb.append({"Equipment": "", "Total Mentions": "", "Top Product": "",
                     "Mentions on Top Product": "", "Pattern": ""})

        for eq, count in sorted_equip:
            prod_counts = equip_prod[eq]
            top_prod = max(prod_counts.items(), key=lambda x: x[1])
            top_pct = top_prod[1] / count * 100

            pattern = ""
            if top_pct > 60 and count >= 3:
                pattern = f"Concentrated on {top_prod[0]} — investigate product-specific setup"
            elif count >= 5:
                n_prods = len(prod_counts)
                pattern = f"Happens across {n_prods} products — systemic equipment issue"
            elif count >= 2:
                pattern = "Monitor — not enough data yet for a pattern"

            exb.append({
                "Equipment": eq,
                "Total Mentions": count,
                "Top Product": top_prod[0],
                "Mentions on Top Product": f"{top_prod[1]} ({top_pct:.0f}%)",
                "Pattern": pattern,
            })

        # Add detail breakdown
        exb.append({"Equipment": "", "Total Mentions": "", "Top Product": "",
                     "Mentions on Top Product": "", "Pattern": ""})
        exb.append({"Equipment": "DETAIL — MENTIONS PER PRODUCT", "Total Mentions": "",
                     "Top Product": "", "Mentions on Top Product": "", "Pattern": ""})

        for eq, count in sorted_equip[:8]:  # Top 8 equipment
            prod_counts = equip_prod[eq]
            breakdown = ", ".join(f"{p}: {c}" for p, c in
                                  sorted(prod_counts.items(), key=lambda x: -x[1]))
            exb.append({
                "Equipment": f"  {eq}",
                "Total Mentions": count,
                "Top Product": breakdown,
                "Mentions on Top Product": "",
                "Pattern": "",
            })

        # Key insight
        exb.append({"Equipment": "", "Total Mentions": "", "Top Product": "",
                     "Mentions on Top Product": "", "Pattern": ""})
        if sorted_equip:
            top_eq = sorted_equip[0][0]
            top_count = sorted_equip[0][1]
            exb.append({
                "Equipment": "KEY FINDING",
                "Total Mentions": "",
                "Top Product": f"{top_eq} is mentioned in {top_count} of {len(valid)} runs "
                               f"({top_count/len(valid)*100:.0f}%). "
                               f"This is the #1 equipment reliability issue on 3rd shift. "
                               f"Start root cause analysis here.",
                "Mentions on Top Product": "",
                "Pattern": "",
            })

        sheets["Equipment x Product"] = pd.DataFrame(exb)

    # =================================================================
    # LAST SHEET: RECOMMENDED ACTIONS
    # =================================================================
    actions = []
    p = 1

    if has_downtime:
        top = actionable.iloc[0]
        actions.append({
            "Priority": p, "Area": f"#{1} Equipment Loss",
            "Problem": f"{top['reason']}: {top['total_hours']:.0f} hours / {int(top['total_occurrences'])} events ({top['pct']:.0f}% of all actionable downtime)",
            "Step 1": f"Pull {top['reason']} events from Traksys for the last 2 weeks. Sort by duration — find the 10 longest stops.",
            "Step 2": f"Walk the line during the next {top['reason']} failure. Document: what broke, how long to diagnose, wait for parts, repair, restart.",
            "Step 3": "Run a 5-Why with maintenance on the top 3 failure modes. Separate root cause from symptom.",
            "Step 4": "Build countermeasures: PM task, spare parts kit at the line, operator first-response SOP. Assign owners + dates.",
            "Step 5": f"Track weekly: {top['reason']} hours per week. Target: 50% reduction in 6 weeks.",
        })
        p += 1

    if perf_gap > avail_gap:
        actions.append({
            "Priority": p, "Area": "Performance Gap (3rd vs 2nd)",
            "Problem": f"3rd shift runs {perf_gap:.1f} performance points below 2nd — the line is up but slow",
            "Step 1": "Compare speed settings between shifts. Is 3rd running at a lower rate? Check HMI setpoints.",
            "Step 2": "Count micro stops per hour on 3rd vs 2nd. Are operators clearing jams slower?",
            "Step 3": "Check if 3rd shift has newer operators who may be running conservative speeds.",
            "Step 4": "Document 2nd shift's best practices: how fast do they clear jams? How do they handle product changes?",
            "Step 5": "Build a 'speed standard' card for each product. Post at the line. Train 3rd shift operators.",
        })
        p += 1
    else:
        actions.append({
            "Priority": p, "Area": "Availability Gap (3rd vs 2nd)",
            "Problem": f"3rd shift has {avail_gap:.1f} pts worse availability than 2nd — more downtime",
            "Step 1": "Compare changeover durations by shift. Is 3rd taking longer? Time with a stopwatch.",
            "Step 2": "Check material staging: is 3rd waiting for materials that 2nd had pre-staged?",
            "Step 3": "Review breakdown response: how quickly does 3rd shift get a mechanic vs 2nd?",
            "Step 4": "Build a shift startup + changeover checklist from 2nd shift best practices.",
            "Step 5": "Pilot the checklist on 3rd for 2 weeks. Track availability daily.",
        })
        p += 1

    # Product-specific actions
    if has_product:
        runs = product_data["runs"]
        valid = runs.dropna(subset=["oee_display"]).copy()
        valid["pack_type"] = valid["product_family"].map(PRODUCT_PACK_TYPE).fillna("Unknown")
        tray_runs = valid[valid["pack_type"].str.startswith("Trayed")]
        std_runs = valid[valid["pack_type"].str.startswith("Standard")]

        if len(tray_runs) > 0 and len(std_runs) > 0:
            t_avg = tray_runs["oee_display"].mean()
            s_avg = std_runs["oee_display"].mean()
            if s_avg - t_avg > 10:
                actions.append({
                    "Priority": p, "Area": "Trayed Product Setup",
                    "Problem": f"Trayed products avg {t_avg:.1f}% OEE vs {s_avg:.1f}% for standard — "
                               f"{s_avg - t_avg:.0f} point gap, dragging 3rd shift average down",
                    "Step 1": "Document the tray packer setup procedure. Time each step. Where does time go?",
                    "Step 2": "Compare tray packer settings between a good run and a bad run. What changed?",
                    "Step 3": "Build a trayed-product setup card: tray packer settings, shrink wrapper temps, conveyor speeds.",
                    "Step 4": "Train 3rd shift operators on the setup card. Run a supervised trayed changeover.",
                    "Step 5": f"Target: trayed OEE above {min(t_avg + 10, 35):.0f}% within 4 weeks.",
                })
                p += 1

        # Top equipment from notes
        equip_total = Counter()
        for _, r in valid.iterrows():
            for eq in r["equipment_mentioned"]:
                equip_total[eq] += 1
        if equip_total:
            top_eq, top_count = equip_total.most_common(1)[0]
            total_runs = len(valid)
            if top_count >= 5:
                actions.append({
                    "Priority": p, "Area": f"{top_eq} Reliability",
                    "Problem": f"{top_eq} appears in operator notes {top_count} of {total_runs} runs ({top_count/total_runs*100:.0f}%) — chronic issue",
                    "Step 1": f"Pull PM history for {top_eq}. When was last PM? What was done?",
                    "Step 2": f"Review the operator notes mentioning {top_eq}. Group by failure mode (jams, faults, settings).",
                    "Step 3": "Identify the top 2 failure modes. Get maintenance to do root cause analysis.",
                    "Step 4": "Build operator first-response guide: what to check before calling maintenance.",
                    "Step 5": f"Track: {top_eq} mentions per week in shift reports. Target: 50% reduction.",
                })
                p += 1

    if has_downtime:
        unassigned = downtime["reasons_df"][downtime["reasons_df"]["reason"].isin(["Unassigned", "Unknown"])]
        if len(unassigned) > 0 and unassigned["total_hours"].sum() > 5:
            uh = unassigned["total_hours"].sum()
            ue = int(unassigned["total_occurrences"].sum())
            actions.append({
                "Priority": p, "Area": "Data Discipline",
                "Problem": f"{uh:.0f} hours / {ue} events have no reason code — can't fix what we can't name",
                "Step 1": "Review the Traksys reason code tree. Count how many codes exist. If >30, simplify.",
                "Step 2": "Pick the 15 most common actual causes. Make those the primary codes.",
                "Step 3": "At shift start, tell supervisors: 'Every stop gets a code. If you're not sure, pick the closest one and add a note.'",
                "Step 4": "Pull unassigned events weekly. Review with shift leads. Retroactively assign codes.",
                "Step 5": f"Target: Unassigned below 5% of events (currently {ue} events — that's too many to ignore).",
            })
            p += 1

    # Startup — use minimum hour in shift (clock hours), not hardcoded 1
    _min_hr = h3_hourly_avg["shift_hour"].min() if len(h3_hourly_avg) > 0 else None
    first_hr_oee = h3_hourly_avg[h3_hourly_avg["shift_hour"] == _min_hr]["avg_oee"].values if _min_hr is not None else np.array([])
    rest_avg = h3_hourly_avg[h3_hourly_avg["shift_hour"] != _min_hr]["avg_oee"].mean() if _min_hr is not None else 0
    if len(first_hr_oee) > 0:
        startup_gap = rest_avg - first_hr_oee[0]
        if startup_gap > 3:
            actions.append({
                "Priority": p, "Area": "Startup Loss",
                "Problem": f"First hour of 3rd shift averages {first_hr_oee[0]:.1f}% OEE vs {rest_avg:.1f}% for the rest — {startup_gap:.0f} point gap",
                "Step 1": "Time the shift start: from bell to first good case. How many minutes?",
                "Step 2": "Document what takes the time: passdown? Machine warmup? Material not staged? Operator late?",
                "Step 3": "Build a startup checklist: materials pre-staged, machine verified, passdown done in <10 min.",
                "Step 4": "Consider a 15-min overlap with 2nd shift so the line doesn't stop during handoff.",
                "Step 5": f"Target: first-hour OEE above {rest_avg - 3:.0f}% within 3 weeks.",
            })
            p += 1

    # Closing
    actions.append({
        "Priority": p, "Area": "Measurement & Follow-Up",
        "Problem": f"Current 3rd shift OEE: {s3_oee:.1f}% — Target: {min(s3_oee + 5, s2_oee):.1f}%",
        "Step 1": "Pick the top 2 actions above. Don't try to fix everything — focus beats breadth.",
        "Step 2": "Assign a single owner for each action. Not a team — one person accountable.",
        "Step 3": "Review progress weekly: pull Traksys data, check the specific metrics.",
        "Step 4": "Re-run this analysis in 4 weeks with fresh data. Did the needle move?",
        "Step 5": f"If 3rd shift gains 5 OEE points, that recovers ~{5 * s3_hours / n_days / 100 * s3_cph:,.0f} cases per night.",
    })

    sheets["Recommended Actions"] = pd.DataFrame(actions)

    return sheets


def write_report(sheets, output_path):
    print(f"Writing: {output_path}")

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        workbook = writer.book

        # Formats
        hdr = workbook.add_format({
            "bold": True, "bg_color": "#1B2A4A", "font_color": "white",
            "border": 1, "text_wrap": True, "valign": "vcenter", "font_size": 11
        })
        title = workbook.add_format({"bold": True, "font_size": 16, "font_color": "#1B2A4A"})
        subtitle = workbook.add_format({"italic": True, "font_size": 10, "font_color": "#666666"})
        section = workbook.add_format({
            "bold": True, "font_size": 12, "font_color": "#1B2A4A",
            "bottom": 2, "bottom_color": "#1B2A4A"
        })
        good_fmt = workbook.add_format({"bg_color": "#E8F5E9", "font_color": "#2E7D32"})
        bad_fmt = workbook.add_format({"bg_color": "#FFEBEE", "font_color": "#C62828"})
        warn_fmt = workbook.add_format({"bg_color": "#FFF8E1", "font_color": "#F57F17"})

        tab_order = [
            "Overview", "Scorecard", "Hour by Hour", "Day by Day",
            "Worst Hours", "vs 2nd Shift",
            "Downtime Causes", "Fault Owners",
            "Product Scorecard", "Every Run", "Std vs Trayed", "Equipment x Product",
            "Recommended Actions",
        ]

        for sheet_name in tab_order:
            if sheet_name not in sheets:
                continue

            df = sheets[sheet_name]
            safe = sheet_name[:31]
            start_row = 2
            df.to_excel(writer, sheet_name=safe, startrow=start_row, index=False)
            ws = writer.sheets[safe]

            # Title
            ws.write(0, 0, f"3rd Shift — {sheet_name}", title)
            ws.write(1, 0, f"Line 2 Flex | Generated {datetime.now().strftime('%Y-%m-%d')}", subtitle)

            # Headers
            for col_num, col_name in enumerate(df.columns):
                ws.write(start_row, col_num, col_name, hdr)

            # Auto-width
            for col_num, col_name in enumerate(df.columns):
                max_len = max(
                    df[col_name].astype(str).map(len).max() if len(df) > 0 else 0,
                    len(str(col_name))
                )
                ws.set_column(col_num, col_num, min(max_len + 4, 65))

            # --- Sheet-specific formatting ---
            if sheet_name == "Overview":
                ws.set_column(0, 0, 100)
                ws.hide_gridlines(2)
                for row_num in range(len(df)):
                    val = str(df.iloc[row_num].iloc[0])
                    if any(s in val for s in ["THE BOTTOM LINE", "HOW TO READ THIS REPORT",
                                               "PRIMARY LOSS DRIVER", "PRODUCT MIX MATTERS"]):
                        ws.write(row_num + start_row + 1, 0, val, section)

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
                if "Product Running" in df.columns:
                    prod_col = list(df.columns).index("Product Running")
                    ws.set_column(prod_col, prod_col, 35)
                for row_num in range(len(df)):
                    status = str(df.iloc[row_num].get("Status", ""))
                    if status == "CRITICAL":
                        ws.write(row_num + start_row + 1, status_col, status, bad_fmt)
                    elif status == "Poor":
                        ws.write(row_num + start_row + 1, status_col, status, warn_fmt)
                    elif status == "Good":
                        ws.write(row_num + start_row + 1, status_col, status, good_fmt)

            if sheet_name == "Worst Hours":
                ws.set_column(8, 8, 55)
                if "OEE %" in df.columns:
                    col_idx = list(df.columns).index("OEE %")
                    ws.conditional_format(start_row + 1, col_idx, start_row + len(df), col_idx, {
                        "type": "3_color_scale",
                        "min_color": "#F8696B", "mid_color": "#FFEB84", "max_color": "#63BE7B",
                    })

            if sheet_name == "vs 2nd Shift":
                ws.set_column(0, 0, 25)
                ws.set_column(4, 4, 70)

            if sheet_name == "Downtime Causes":
                if "Total Minutes" in df.columns:
                    col_idx = list(df.columns).index("Total Minutes")
                    ws.conditional_format(start_row + 1, col_idx, start_row + len(df), col_idx, {
                        "type": "3_color_scale",
                        "min_color": "#63BE7B", "mid_color": "#FFEB84", "max_color": "#F8696B",
                    })

            if sheet_name == "Fault Owners":
                ws.set_column(5, 5, 55)
                ws.set_column(6, 6, 60)

            if sheet_name == "Product Scorecard":
                ws.set_column(0, 0, 28)    # Product
                ws.set_column(1, 1, 18)    # Pack Type
                ws.set_column(10, 10, 12)  # Status
                # Color the status column
                if "Status" in df.columns:
                    s_col = list(df.columns).index("Status")
                    for row_num in range(len(df)):
                        status = str(df.iloc[row_num].get("Status", ""))
                        if status == "CRITICAL":
                            ws.write(row_num + start_row + 1, s_col, status, bad_fmt)
                        elif status == "Problem":
                            ws.write(row_num + start_row + 1, s_col, status, warn_fmt)
                        elif status == "Solid":
                            ws.write(row_num + start_row + 1, s_col, status, good_fmt)
                # KEY FINDING row spans full width
                if "Pack Type" in df.columns:
                    pt_col = list(df.columns).index("Pack Type")
                    for row_num in range(len(df)):
                        prod_val = str(df.iloc[row_num].get("Product", ""))
                        if prod_val == "KEY FINDING":
                            ws.write(row_num + start_row + 1, 0, prod_val, section)
                            # Make the Pack Type cell wider for this row
                            ws.set_column(pt_col, pt_col, 85)

            if sheet_name == "Every Run":
                ws.set_column(0, 0, 12)    # Date
                ws.set_column(1, 1, 28)    # Product
                ws.set_column(6, 6, 40)    # Equipment Mentioned
                ws.set_column(7, 7, 80)    # Operator Notes
                ws.set_column(8, 8, 12)    # Status
                # Color status
                if "Status" in df.columns:
                    s_col = list(df.columns).index("Status")
                    for row_num in range(len(df)):
                        status = str(df.iloc[row_num].get("Status", ""))
                        if status == "CRITICAL":
                            ws.write(row_num + start_row + 1, s_col, status, bad_fmt)
                        elif status == "Poor":
                            ws.write(row_num + start_row + 1, s_col, status, warn_fmt)
                        elif status == "Good":
                            ws.write(row_num + start_row + 1, s_col, status, good_fmt)

            if sheet_name == "Std vs Trayed":
                ws.set_column(0, 0, 25)
                ws.set_column(1, 2, 30)
                ws.set_column(3, 3, 22)
                ws.set_column(4, 4, 70)
                # Style VERDICT row
                for row_num in range(len(df)):
                    val = str(df.iloc[row_num].get("Metric", ""))
                    if val == "VERDICT":
                        ws.write(row_num + start_row + 1, 0, val, section)

            if sheet_name == "Equipment x Product":
                ws.set_column(0, 0, 25)
                ws.set_column(2, 2, 30)  # Top Product
                ws.set_column(4, 4, 60)  # Pattern
                # Style KEY FINDING
                for row_num in range(len(df)):
                    val = str(df.iloc[row_num].get("Equipment", ""))
                    if val == "KEY FINDING":
                        ws.write(row_num + start_row + 1, 0, val, section)
                        tp_col = list(df.columns).index("Top Product")
                        ws.set_column(tp_col, tp_col, 85)

            if sheet_name == "Recommended Actions":
                ws.set_column(1, 1, 25)   # Area
                ws.set_column(2, 2, 70)   # Problem
                ws.set_column(3, 7, 60)   # Steps

        # Activate Overview
        if "Overview" in sheets:
            writer.sheets["Overview"].activate()

    print(f"Done! Open: {output_path}")


def main():
    args = sys.argv[1:]
    oee_file = None
    downtime_file = None
    product_file = None

    i = 0
    while i < len(args):
        if args[i] == "--downtime" and i + 1 < len(args):
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
        oee_file = os.path.join(os.path.dirname(__file__), "..",
                                "shift_oee_cases_by_day_shift_with_shift_totals.xlsx")

    oee_file = os.path.abspath(oee_file)
    if not os.path.exists(oee_file):
        print(f"Error: OEE file not found: {oee_file}")
        sys.exit(1)

    hourly, shift_summary, overall, hour_avg, downtime, product_data = load_data(
        oee_file, downtime_file, product_file)
    sheets = build_report(hourly, shift_summary, overall, hour_avg, downtime, product_data)

    output_dir = os.path.dirname(oee_file)
    output_path = os.path.join(output_dir, f"Line2_3rd_Shift_Analysis_{datetime.now().strftime('%Y%m%d')}.xlsx")

    write_report(sheets, output_path)

    # Console preview
    print("\n" + "=" * 60)
    print("3RD SHIFT — LINE 2 FLEX — REPORT SUMMARY")
    print("=" * 60)
    sc = sheets["Scorecard"]
    for _, row in sc.iterrows():
        m = row.get("Metric", "")
        if m and m not in ["", "3RD SHIFT SCORECARD", "WHERE IS OEE LOST?", "CONSISTENCY"]:
            v3 = row.get("3rd Shift", "")
            g = row.get("Gap vs 2nd", "")
            if v3:
                gap_str = f"  ({g})" if g else ""
                print(f"  {m}: {v3}{gap_str}")

    if "Product Scorecard" in sheets:
        print("\nPRODUCT PERFORMANCE (3rd Shift):")
        ps = sheets["Product Scorecard"]
        for _, row in ps.iterrows():
            prod = row.get("Product", "")
            oee = row.get("Avg OEE", "")
            status = row.get("Status", "")
            if prod and oee and prod not in ["", "3RD SHIFT — OEE BY PRODUCT", "ALL PRODUCTS", "KEY FINDING"] \
               and not prod.startswith("Sorted"):
                flag = f"  [{status}]" if status else ""
                print(f"  {prod}: {oee}{flag}")

    if "Recommended Actions" in sheets:
        print("\nRECOMMENDED ACTIONS:")
        for _, row in sheets["Recommended Actions"].iterrows():
            print(f"  #{row['Priority']} [{row['Area']}]: {str(row['Problem'])[:80]}...")

    print(f"\nFull report: {output_path}")


if __name__ == "__main__":
    main()
