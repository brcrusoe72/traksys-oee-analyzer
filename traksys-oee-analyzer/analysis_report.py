"""
Analysis Report — 2-Page PDF from up to 6 Analysis Excel Files
==============================================================
Reads the output workbooks produced by analyze.py (the 5-sheet Excel
files) and consolidates them into a tight, 2-page PDF summary.

Page 1: Scorecard (KPIs, shift grid, loss breakdown, daily OEE trend)
Page 2: Root Cause & Actions (downtime Pareto, shift narratives, IDS items)

Usage:
  python analysis_report.py file1.xlsx file2.xlsx ...          # up to 6 files
  python analysis_report.py file1.xlsx file2.xlsx -o report.pdf
"""

import sys
import os
from datetime import datetime

import pandas as pd
import numpy as np
from fpdf import FPDF

# ---------------------------------------------------------------------------
# Color palette — matches the navy/slate aesthetic of the Excel reports
# ---------------------------------------------------------------------------
NAVY = (27, 42, 74)       # #1B2A4A — headers, titles
WHITE = (255, 255, 255)
LIGHT_GRAY = (245, 245, 245)
MID_GRAY = (200, 200, 200)
DARK_TEXT = (51, 51, 51)
RED = (231, 76, 60)       # #E74C3C — bad / availability
ORANGE = (243, 156, 18)   # #F39C12 — warning / performance
GREEN = (46, 204, 113)    # #2ECC71 — good
BLUE = (52, 152, 219)     # #3498DB — quality


def _oee_color(oee_pct):
    """Return (R,G,B) for OEE value -- red/orange/green."""
    if oee_pct >= 50:
        return GREEN
    if oee_pct >= 35:
        return ORANGE
    return RED


def _sanitize_text(text):
    """Replace Unicode characters that built-in PDF fonts can't render."""
    return (
        str(text)
        .replace("\u2014", "--")   # em dash
        .replace("\u2013", "-")    # en dash
        .replace("\u2018", "'")    # left single quote
        .replace("\u2019", "'")    # right single quote
        .replace("\u201c", '"')    # left double quote
        .replace("\u201d", '"')    # right double quote
        .replace("\u2026", "...")  # ellipsis
        .replace("\u00b7", "*")    # middle dot
        .replace("\u03c3", "s")    # sigma
        .replace("\u2265", ">=")   # >=
        .replace("\u2264", "<=")   # <=
        .replace("\u00b1", "+/-")  # plus-minus
    )


def _safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_str(val, default=""):
    """Convert to string, returning default for None/NaN."""
    if val is None:
        return default
    try:
        import math
        if isinstance(val, float) and math.isnan(val):
            return default
    except (TypeError, ValueError):
        pass
    s = str(val)
    return default if s.lower() == "nan" else s


# ---------------------------------------------------------------------------
# Excel Reader — extract structured data from analysis workbooks
# ---------------------------------------------------------------------------
def read_analysis_workbook(path):
    """Read an analysis workbook produced by analyze.py.

    Returns a dict with:
      - kpis: list of {Metric, Value}
      - shift_comparison: DataFrame
      - loss_breakdown: DataFrame
      - daily_trend: DataFrame
      - shift_narratives: dict of {shift_name: narrative_text}
      - focus_items: DataFrame (What to Focus On)
      - shift_raw: dict of {shift_name: raw_metrics_dict}
      - shift_downtime_causes: dict of {shift_name: DataFrame}
      - source_file: basename
    """
    xls = pd.ExcelFile(path)
    result = {"source_file": os.path.basename(path)}

    # --- Plant Summary ---
    if "Plant Summary" in xls.sheet_names:
        ps = pd.read_excel(path, sheet_name="Plant Summary", header=None)
        # Find KPI table — look for "Metric" / "Value" headers
        kpis = []
        shift_comp = pd.DataFrame()
        loss_df = pd.DataFrame()
        daily_df = pd.DataFrame()

        # Scan for section markers
        rows = ps.values.tolist()
        i = 0
        while i < len(rows):
            cell0 = str(rows[i][0]).strip() if pd.notna(rows[i][0]) else ""

            if cell0 == "Plant KPIs" and i + 1 < len(rows):
                # Next row is header, then data
                j = i + 1
                if j < len(rows) and str(rows[j][0]).strip() == "Metric":
                    j += 1
                    while j < len(rows) and pd.notna(rows[j][0]) and str(rows[j][0]).strip() not in ("", "Shift Comparison", "Loss Breakdown", "Daily Trend"):
                        kpis.append({"Metric": str(rows[j][0]).strip(), "Value": str(rows[j][1]).strip() if pd.notna(rows[j][1]) else ""})
                        j += 1
                    i = j
                    continue

            if cell0 == "Shift Comparison" and i + 1 < len(rows):
                j = i + 1
                # Read header row
                header = [str(c).strip() for c in rows[j] if pd.notna(c)]
                j += 1
                data_rows = []
                while j < len(rows) and pd.notna(rows[j][0]) and str(rows[j][0]).strip() not in ("", "Loss Breakdown", "Daily Trend", "Plant KPIs"):
                    data_rows.append(rows[j][:len(header)])
                    j += 1
                if header and data_rows:
                    shift_comp = pd.DataFrame(data_rows, columns=header)
                i = j
                continue

            if cell0 == "Loss Breakdown by Shift" and i + 1 < len(rows):
                j = i + 1
                header = [str(c).strip() for c in rows[j] if pd.notna(c)]
                j += 1
                data_rows = []
                while j < len(rows) and pd.notna(rows[j][0]) and str(rows[j][0]).strip() not in ("", "Shift Comparison", "Daily Trend", "Plant KPIs"):
                    data_rows.append(rows[j][:len(header)])
                    j += 1
                if header and data_rows:
                    loss_df = pd.DataFrame(data_rows, columns=header)
                i = j
                continue

            if cell0 == "Daily Trend" and i + 1 < len(rows):
                j = i + 1
                header = [str(c).strip() for c in rows[j] if pd.notna(c)]
                j += 1
                data_rows = []
                while j < len(rows) and pd.notna(rows[j][0]) and str(rows[j][0]).strip() not in ("", "Shift Comparison", "Loss Breakdown", "Plant KPIs"):
                    data_rows.append(rows[j][:len(header)])
                    j += 1
                if header and data_rows:
                    daily_df = pd.DataFrame(data_rows, columns=header)
                i = j
                continue

            i += 1

        result["kpis"] = kpis
        result["shift_comparison"] = shift_comp
        result["loss_breakdown"] = loss_df
        result["daily_trend"] = daily_df

    # --- Per-shift sheets: extract narrative + downtime causes ---
    shift_narratives = {}
    shift_downtime = {}
    for sname in ["1st Shift", "2nd Shift", "3rd Shift"]:
        if sname in xls.sheet_names:
            ss = pd.read_excel(path, sheet_name=sname, header=None)
            rows = ss.values.tolist()
            # Narrative is in merged cells row 3-9 typically (row index 2-8)
            narrative = ""
            for r in range(2, min(10, len(rows))):
                cell = rows[r][0] if pd.notna(rows[r][0]) else ""
                cell = str(cell).strip()
                if len(cell) > 40:  # Narrative text is long
                    narrative = cell
                    break

            shift_narratives[sname] = narrative

            # Find downtime causes table
            causes_df = pd.DataFrame()
            for r in range(len(rows)):
                cell = str(rows[r][0]).strip() if pd.notna(rows[r][0]) else ""
                if "Downtime Causes" in cell and r + 1 < len(rows):
                    header = [str(c).strip() for c in rows[r + 1] if pd.notna(c)]
                    j = r + 2
                    data_rows = []
                    while j < len(rows) and pd.notna(rows[j][0]):
                        val0 = str(rows[j][0]).strip()
                        if val0 in ("", "Hour-by-Hour Detail", "Dead Hours",
                                    "Worst Hours", "Loss Breakdown", "Scorecard",
                                    "Loss Type"):
                            break
                        data_rows.append(rows[j][:len(header)])
                        j += 1
                    if header and data_rows:
                        causes_df = pd.DataFrame(data_rows, columns=header)
                    break
            shift_downtime[sname] = causes_df

    result["shift_narratives"] = shift_narratives
    result["shift_downtime_causes"] = shift_downtime

    # --- What to Focus On ---
    if "What to Focus On" in xls.sheet_names:
        focus = pd.read_excel(path, sheet_name="What to Focus On", header=2)
        result["focus_items"] = focus
    else:
        result["focus_items"] = pd.DataFrame()

    xls.close()
    return result


# ---------------------------------------------------------------------------
# Consolidator — merge data from multiple workbooks
# ---------------------------------------------------------------------------
def consolidate(workbooks):
    """Merge data from multiple analysis workbook dicts into one consolidated view.

    Returns a dict with:
      - date_range: str
      - n_files: int
      - kpis: dict of {metric: value}
      - shift_grid: list of dicts (all shift rows across all files)
      - loss_grid: list of dicts
      - daily_trend: list of dicts
      - downtime_pareto: list of {Cause, Total Min, Events, Pct} (top 10)
      - shift_narratives: dict of {shift: [narratives]}
      - ids_items: list of {Priority, Finding, TheWork, Steps}  (top 3)
      - source_files: list of str
    """
    all_shift_rows = []
    all_loss_rows = []
    all_daily_rows = []
    all_downtime = []
    all_narratives = {}
    all_focus = []
    all_kpis = {}
    source_files = []
    dates_seen = set()

    for wb in workbooks:
        source_files.append(wb["source_file"])

        # KPIs — collect all, we'll aggregate later
        for kpi in wb.get("kpis", []):
            metric = kpi["Metric"]
            if metric not in all_kpis:
                all_kpis[metric] = []
            all_kpis[metric].append(kpi["Value"])

        # Shift comparison rows — prefer Top Issue from Excel columns (per-date-shift),
        # fall back to cross-referencing shift_downtime_causes (old behavior).
        sc = wb.get("shift_comparison", pd.DataFrame())
        shift_dt = wb.get("shift_downtime_causes", {})
        if len(sc) > 0:
            for _, row in sc.iterrows():
                r = {}
                for col in sc.columns:
                    r[col] = row[col]
                r["_source"] = wb["source_file"]
                # Use Top Issue/Min from Excel if present (new format)
                if "Top Issue" not in r or not r.get("Top Issue"):
                    shift_name = str(row.get("Shift", ""))
                    causes_df = shift_dt.get(shift_name, pd.DataFrame())
                    if len(causes_df) > 0:
                        r["Top Issue"] = str(causes_df.iloc[0, 0])
                        r["Top Issue Min"] = _safe_float(
                            causes_df.iloc[0, 2]) if len(causes_df.columns) > 2 else 0
                all_shift_rows.append(r)
                if "Date" in r and pd.notna(r["Date"]):
                    dates_seen.add(str(r["Date"]))

        # Loss breakdown — attach top reason codes from same workbook
        lb = wb.get("loss_breakdown", pd.DataFrame())
        if len(lb) > 0:
            for _, row in lb.iterrows():
                r = {col: row[col] for col in lb.columns}
                shift_name = str(row.get("Shift", ""))
                causes_df = shift_dt.get(shift_name, pd.DataFrame())
                if len(causes_df) > 0:
                    r["Top Issue"] = str(causes_df.iloc[0, 0])
                    r["Top Issue Min"] = _safe_float(
                        causes_df.iloc[0, 2]) if len(causes_df.columns) > 2 else 0
                    if len(causes_df) > 1:
                        r["Issue #2"] = str(causes_df.iloc[1, 0])
                        r["Issue #2 Min"] = _safe_float(
                            causes_df.iloc[1, 2]) if len(causes_df.columns) > 2 else 0
                all_loss_rows.append(r)

        # Daily trend
        dt = wb.get("daily_trend", pd.DataFrame())
        if len(dt) > 0:
            for _, row in dt.iterrows():
                r = {col: row[col] for col in dt.columns}
                all_daily_rows.append(r)
                if "Date" in r and pd.notna(r["Date"]):
                    dates_seen.add(str(r["Date"]))

        # Downtime causes (merge from all shift sheets)
        for sname, causes_df in wb.get("shift_downtime_causes", {}).items():
            if len(causes_df) > 0:
                for _, row in causes_df.iterrows():
                    cause = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                    total_min = _safe_float(row.iloc[1]) if len(row) > 1 else 0
                    events = _safe_float(row.iloc[2]) if len(row) > 2 else 0
                    if cause and total_min > 0:
                        all_downtime.append({
                            "Cause": cause,
                            "Total Min": total_min,
                            "Events": events,
                        })

        # Narratives
        for sname, narrative in wb.get("shift_narratives", {}).items():
            if narrative:
                if sname not in all_narratives:
                    all_narratives[sname] = []
                all_narratives[sname].append(narrative)

        # Focus items
        fi = wb.get("focus_items", pd.DataFrame())
        if len(fi) > 0:
            for _, row in fi.iterrows():
                all_focus.append(row.to_dict())

    # --- Aggregate downtime Pareto ---
    downtime_pareto = []
    if all_downtime:
        dt_df = pd.DataFrame(all_downtime)
        agg = dt_df.groupby("Cause").agg(
            total_min=("Total Min", "sum"),
            events=("Events", "sum"),
        ).sort_values("total_min", ascending=False).head(10).reset_index()
        grand_total = agg["total_min"].sum()
        for _, row in agg.iterrows():
            pct = row["total_min"] / grand_total * 100 if grand_total > 0 else 0
            downtime_pareto.append({
                "Cause": row["Cause"],
                "Total Min": row["total_min"],
                "Events": int(row["events"]),
                "Pct": round(pct, 1),
            })

    # --- Aggregate KPIs (take most meaningful values) ---
    agg_kpis = {}
    for metric, values in all_kpis.items():
        if metric == "Overall OEE":
            # Extract numeric OEE values and average them
            nums = []
            for v in values:
                try:
                    nums.append(float(str(v).replace("%", "")))
                except ValueError:
                    pass
            if nums:
                agg_kpis[metric] = f"{sum(nums) / len(nums):.1f}%"
        elif metric == "Total Cases":
            nums = []
            for v in values:
                try:
                    nums.append(float(str(v).replace(",", "")))
                except ValueError:
                    pass
            if nums:
                agg_kpis[metric] = f"{sum(nums):,.0f}"
        elif metric == "Top Downtime Cause":
            # Use the one that appears most or has most hours
            agg_kpis[metric] = values[0] if values else "N/A"
        else:
            # Use the last value (most recent) for other KPIs
            agg_kpis[metric] = values[-1] if values else ""

    # --- Deduplicate and sort focus items by priority ---
    seen_findings = set()
    unique_focus = []
    for item in all_focus:
        finding = item.get("Finding", "")
        if finding and finding not in seen_findings:
            seen_findings.add(finding)
            unique_focus.append(item)
    # Sort by priority, take top 3
    unique_focus.sort(key=lambda x: _safe_float(x.get("Priority", 99)))
    ids_items = unique_focus[:3]

    # --- Date range ---
    sorted_dates = sorted(dates_seen)
    if sorted_dates:
        date_range = f"{sorted_dates[0]} to {sorted_dates[-1]}"
    else:
        date_range = "Unknown"

    return {
        "date_range": date_range,
        "n_files": len(workbooks),
        "n_days": len(dates_seen),
        "kpis": agg_kpis,
        "shift_grid": all_shift_rows,
        "loss_grid": all_loss_rows,
        "daily_trend": all_daily_rows,
        "downtime_pareto": downtime_pareto,
        "shift_narratives": all_narratives,
        "ids_items": ids_items,
        "source_files": source_files,
    }


# ---------------------------------------------------------------------------
# Fresh summary / action generators — always equipment-first, data-driven
# ---------------------------------------------------------------------------
def _build_fresh_summaries(shift_grid, pareto):
    """Generate equipment-first shift summaries from consolidated data."""
    summaries = {}
    if not shift_grid:
        return summaries
    df = pd.DataFrame(shift_grid)
    for shift_name in ["1st Shift", "2nd Shift", "3rd Shift"]:
        sdf = df[df["Shift"] == shift_name]
        if len(sdf) == 0:
            continue
        n_days = sdf["Date"].nunique()
        avg_oee = _safe_float(sdf["OEE %"].mean())
        total_cases = _safe_float(sdf["Cases"].sum())
        avg_cph = _safe_float(sdf["CPH"].mean())
        avg_pct = _safe_float(sdf["% of Target"].mean())

        # Find this shift's #1 issue by total minutes
        issue_df = sdf[sdf["Top Issue"].apply(lambda x: bool(_safe_str(x)))]
        top_issue = ""
        top_min = 0
        agg = None
        if len(issue_df) > 0:
            agg = issue_df.groupby("Top Issue")["Top Issue Min"].sum().sort_values(ascending=False)
            top_issue = _safe_str(agg.index[0])
            top_min = _safe_float(agg.iloc[0])

        # Paragraph 1: what happened
        parts = [f"{shift_name} averaged {avg_oee:.1f}% OEE across {n_days} day(s), "
                 f"producing {total_cases:,.0f} cases ({avg_cph:,.0f} CPH, "
                 f"{avg_pct:.0f}% of target)."]

        # Paragraph 2: lead with specific equipment issue
        if top_issue:
            parts.append(f"#1 issue: {top_issue} -- {top_min:,.0f} min total.")
            # Add runner-up if exists
            if agg is not None and len(agg) > 1:
                r2 = _safe_str(agg.index[1])
                r2_min = _safe_float(agg.iloc[1])
                parts.append(f"Also: {r2} ({r2_min:,.0f} min).")

        summaries[shift_name] = " ".join(parts)
    return summaries


def _build_fresh_actions(shift_grid, pareto):
    """Generate data-driven action items from consolidated data."""
    actions = []
    for i, item in enumerate(pareto[:3]):
        cause = _safe_str(item.get("Cause", ""))
        total_min = _safe_float(item.get("Total Min", 0))
        events = int(_safe_float(item.get("Events", 0)))
        pct = _safe_float(item.get("Pct", 0))
        if not cause:
            continue
        actions.append({
            "Priority": i + 1,
            "Finding": f"#{i+1} loss: {cause} -- {total_min:,.0f} min / {events} events ({pct:.1f}% of downtime)",
            "The Work": f"Target 50% reduction in {cause}. Pull event logs, identify patterns by shift/time/product. "
                        f"5-why the top events, implement countermeasures, track weekly.",
        })
    return actions


# ---------------------------------------------------------------------------
# PDF Builder
# ---------------------------------------------------------------------------
class AnalysisReport(FPDF):
    """Flowing PDF analysis report with 0.5-inch margins."""

    def __init__(self):
        super().__init__(orientation="L", unit="mm", format="letter")
        self.set_margins(12.7, 12.7, 12.7)  # 0.5 inches
        self.set_auto_page_break(auto=True, margin=12.7)
        self._col_navy = NAVY
        self._generated = datetime.now().strftime("%Y-%m-%d %H:%M")

    # --- Text sanitization for built-in fonts ---
    def cell(self, w=0, h=None, text="", **kwargs):
        return super().cell(w, h, _sanitize_text(text), **kwargs)

    def multi_cell(self, w, h=None, text="", **kwargs):
        return super().multi_cell(w, h, _sanitize_text(text), **kwargs)

    # --- Header / Footer ---
    def header(self):
        pass  # We draw our own headers

    def footer(self):
        self.set_y(-8)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(*MID_GRAY)
        super().cell(0, 4, _sanitize_text(f"Generated {self._generated}  |  Numbers from the machine, not opinions"), align="C")

    # --- Helpers ---
    def _section_header(self, text, y=None, font_size=9, w=None):
        """Navy section divider."""
        if y is not None:
            self.set_y(y)
        self.set_font("Helvetica", "B", font_size)
        self.set_text_color(*NAVY)
        self.set_draw_color(*NAVY)
        if w is None:
            w = self.epw
        self.cell(w, 4, f"  {text}", border="B", new_x="LMARGIN", new_y="NEXT")
        self.ln(0.5)

    def _table_header(self, widths, headers, h=5, font_size=7):
        """Draw table header row."""
        self.set_font("Helvetica", "B", font_size)
        self.set_fill_color(*NAVY)
        self.set_text_color(*WHITE)
        for w, hdr in zip(widths, headers):
            self.cell(w, h, f" {hdr}", border=1, fill=True)
        self.ln()
        self.set_text_color(*DARK_TEXT)

    def _table_row(self, widths, values, highlight_col=None, highlight_color=None,
                   fill=False, h=4.5, font_size=7):
        """Draw a single data row."""
        self.set_font("Helvetica", "", font_size)
        if fill:
            self.set_fill_color(*LIGHT_GRAY)
        for i, (w, v) in enumerate(zip(widths, values)):
            do_fill = fill
            if highlight_col is not None and i == highlight_col and highlight_color:
                self.set_fill_color(*highlight_color)
                do_fill = True
            self.cell(w, h, f" {v}", border=1, fill=do_fill)
            if highlight_col is not None and i == highlight_col and fill:
                self.set_fill_color(*LIGHT_GRAY)
            elif highlight_col is not None and i == highlight_col:
                self.set_fill_color(*WHITE)
        self.ln()

    def _needs_break(self, h):
        """True if adding *h* mm would overflow past the bottom margin."""
        return self.get_y() + h > self.h - self.b_margin

    def _ensure_space(self, h):
        """Start a new page if less than *h* mm of usable space remains."""
        if self._needs_break(h):
            self.add_page()

    # ------------------------------------------------------------------
    # Flowing report — content breaks to new pages as needed
    # ------------------------------------------------------------------
    def build_page1(self, data):
        self.add_page()

        # --- Gather all data ---
        kpis = data.get("kpis", {})
        shift_rows = data.get("shift_grid", [])
        daily_rows = data.get("daily_trend", [])
        pareto = data.get("downtime_pareto", [])[:10]
        narratives = data.get("shift_narratives", {})
        ids_items = data.get("ids_items", [])

        # --- Fixed sizing for clean, readable layout ---
        ROW_H = 5.0
        HDR_H = 5.5
        FONT_TBL = 7.0
        FONT_HDR = 7.5
        FONT_SEC = 9.0
        GAP = 3.0

        # === TITLE BAR ===
        y0 = self.get_y()
        self.set_fill_color(*NAVY)
        self.rect(self.l_margin, y0, self.epw, 10, style="F")
        self.set_xy(self.l_margin + 2, y0 + 0.5)
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(*WHITE)
        self.cell(0, 5, "Analysis Report -- OEE Analysis")
        self.set_font("Helvetica", "", 8)
        self.set_xy(self.l_margin + 2, y0 + 5)
        self.cell(0, 5, f"{data['date_range']}  |  {data['n_days']} day(s)  |  {data['n_files']} file(s)")
        self.set_text_color(*DARK_TEXT)
        self.set_y(y0 + 12)

        # === KPI CARDS ===
        card_kpis = [
            ("Overall OEE", kpis.get("Overall OEE", "N/A")),
            ("OEE Gap to 50%", kpis.get("OEE Gap to 50% Target", "N/A")),
            ("Total Cases", kpis.get("Total Cases", "N/A")),
            ("Cases vs Target", kpis.get("Cases vs Target (Plant Std)", "N/A")),
            ("Utilization", kpis.get("Utilization", "N/A")),
            ("Top Downtime", kpis.get("Top Downtime Cause", "N/A")),
        ]
        card_w = self.epw / len(card_kpis)
        y_kpi = self.get_y()
        for i, (label, value) in enumerate(card_kpis):
            x = self.l_margin + i * card_w
            self.set_fill_color(*LIGHT_GRAY)
            self.rect(x + 0.5, y_kpi, card_w - 1, 11, style="F")
            self.set_xy(x + 1, y_kpi + 0.5)
            self.set_font("Helvetica", "", 5.5)
            self.set_text_color(*MID_GRAY)
            self.cell(card_w - 2, 3, label, align="C")
            self.set_xy(x + 1, y_kpi + 3.5)
            self.set_font("Helvetica", "B", 8)
            self.set_text_color(*NAVY)
            display_val = str(value)
            if len(display_val) > 22:
                display_val = display_val[:20] + ".."
            self.cell(card_w - 2, 5, display_val, align="C")
        self.set_text_color(*DARK_TEXT)
        self.set_y(y_kpi + 13)

        # === SHIFT PERFORMANCE TABLE (full width) ===
        self._section_header("Shift Performance", font_size=FONT_SEC)
        if shift_rows:
            widths = [22, 18, 16, 14, 20, 14, 14, 46, 16]
            headers = ["Date", "Shift", "Product", "OEE%", "Cases", "CPH", "%Tgt", "Top Issue", "Min"]
            scale = self.epw / sum(widths)
            widths = [w * scale for w in widths]
            self._table_header(widths, headers, h=HDR_H, font_size=FONT_HDR)
            for idx, row in enumerate(shift_rows):
                if self._needs_break(ROW_H):
                    self.add_page()
                    self._table_header(widths, headers, h=HDR_H, font_size=FONT_HDR)
                oee_val = _safe_float(row.get("OEE %", 0))
                issue_min = _safe_float(row.get("Top Issue Min", 0))
                values = [
                    str(row.get("Date", ""))[:10],
                    str(row.get("Shift", "")),
                    _safe_str(row.get("Product", ""))[:20],
                    f"{oee_val:.1f}",
                    f"{_safe_float(row.get('Cases', 0)):,.0f}",
                    f"{_safe_float(row.get('CPH', 0)):,.0f}",
                    f"{_safe_float(row.get('% of Target', 0)):.1f}",
                    _safe_str(row.get("Top Issue", ""))[:36],
                    f"{issue_min:,.0f}" if issue_min else "",
                ]
                self._table_row(widths, values, highlight_col=3,
                                highlight_color=_oee_color(oee_val),
                                fill=(idx % 2 == 1), h=ROW_H, font_size=FONT_TBL)

        self.ln(GAP)

        # === DAILY OEE TREND TABLE (full width) ===
        self._ensure_space(HDR_H + ROW_H + 6)
        self._section_header("Daily OEE Trend", font_size=FONT_SEC)
        if daily_rows:
            widths = [28, 22, 28, 24, 24, 22]
            headers = ["Date", "OEE%", "Cases", "CPH", "Tgt CPH", "%Tgt"]
            scale = self.epw / sum(widths)
            widths = [w * scale for w in widths]
            self._table_header(widths, headers, h=HDR_H, font_size=FONT_HDR)
            for idx, row in enumerate(daily_rows):
                if self._needs_break(ROW_H):
                    self.add_page()
                    self._table_header(widths, headers, h=HDR_H, font_size=FONT_HDR)
                oee_val = _safe_float(row.get("OEE %", 0))
                values = [
                    str(row.get("Date", ""))[:10],
                    f"{oee_val:.1f}",
                    f"{_safe_float(row.get('Actual Cases', row.get('Cases', 0))):,.0f}",
                    f"{_safe_float(row.get('Cases/Hr', row.get('CPH', 0))):,.0f}",
                    f"{_safe_float(row.get('Target CPH', 0)):,.0f}",
                    f"{_safe_float(row.get('% of Target', row.get('% Tgt', 0))):.1f}",
                ]
                self._table_row(widths, values, highlight_col=1,
                                highlight_color=_oee_color(oee_val),
                                fill=(idx % 2 == 1), h=ROW_H, font_size=FONT_TBL)

        self.ln(GAP)

        # === TOP DOWNTIME CAUSES (full width) ===
        self._ensure_space(HDR_H + ROW_H + 6)
        self._section_header("Top Downtime Causes", font_size=FONT_SEC)
        if pareto:
            widths = [60, 24, 20, 18]
            headers = ["Cause", "Minutes", "Events", "% Tot"]
            scale = self.epw / sum(widths)
            widths = [w * scale for w in widths]
            self._table_header(widths, headers, h=HDR_H, font_size=FONT_HDR)
            for idx, item in enumerate(pareto):
                if self._needs_break(ROW_H):
                    self.add_page()
                    self._table_header(widths, headers, h=HDR_H, font_size=FONT_HDR)
                values = [
                    str(item["Cause"])[:45],
                    f"{item['Total Min']:,.0f}",
                    f"{item['Events']:,}",
                    f"{item['Pct']:.1f}%",
                ]
                self._table_row(widths, values, fill=(idx % 2 == 1),
                                h=ROW_H, font_size=FONT_TBL)

        self.ln(GAP)

        # === SHIFT SUMMARIES (fresh from consolidated data) ===
        fresh_summaries = _build_fresh_summaries(shift_rows, pareto)
        # Append photo context from Excel narratives to fresh summaries
        for sname in ["1st Shift", "2nd Shift", "3rd Shift"]:
            narr_list = narratives.get(sname, [])
            for narr_text in narr_list:
                if "from context photos" in narr_text.lower():
                    # Extract the photo line(s) from the narrative
                    for line in narr_text.split("\n"):
                        stripped = line.strip()
                        if stripped.lower().startswith("**from context photos"):
                            # Remove markdown bold markers for PDF
                            clean = stripped.replace("**", "")
                            if sname in fresh_summaries:
                                fresh_summaries[sname] += " " + clean
                            break
        active_shifts = [s for s in ["1st Shift", "2nd Shift", "3rd Shift"]
                         if fresh_summaries.get(s) or narratives.get(s)]
        if active_shifts:
            self._ensure_space(15)
            self._section_header("Shift Summaries", font_size=FONT_SEC)
            for sname in active_shifts:
                self._ensure_space(10)
                self.set_font("Helvetica", "B", FONT_TBL)
                self.set_text_color(*NAVY)
                self.cell(20, 4, f"{sname}:", new_x="END")
                self.set_font("Helvetica", "", FONT_TBL)
                self.set_text_color(*DARK_TEXT)
                summary = fresh_summaries.get(sname, "")
                if not summary and narratives.get(sname):
                    summary = narratives[sname][-1]
                self.multi_cell(self.epw - 22, 3.5, summary)
                self.ln(1)

        self.ln(GAP)

        # === IDS ACTION ITEMS (fresh from consolidated data) ===
        fresh_actions = _build_fresh_actions(shift_rows, pareto)
        display_actions = fresh_actions if fresh_actions else ids_items
        # Include photo findings from focus items if they exist — they
        # have unique context (whiteboards, work orders) that machine
        # data can't capture, so they should always surface.
        photo_items = [item for item in ids_items
                       if "photo" in str(item.get("Finding", "")).lower()]
        if photo_items and fresh_actions:
            for pi in photo_items:
                pi["Priority"] = len(display_actions) + 1
                display_actions.append(pi)
        if display_actions:
            self._ensure_space(15)
            self._section_header("IDS -- Action Items", font_size=FONT_SEC)
            for idx, item in enumerate(display_actions):
                self._ensure_space(12)
                priority = item.get("Priority", idx + 1)
                finding = str(item.get("Finding", ""))
                the_work = str(item.get("The Work", ""))

                self.set_fill_color(*NAVY)
                self.set_text_color(*WHITE)
                self.set_font("Helvetica", "B", 7)
                self.cell(6, 5, f" #{priority}", fill=True)
                self.set_text_color(*NAVY)
                self.set_font("Helvetica", "B", 7)
                self.cell(self.epw - 8, 5, f"  {finding[:120]}")
                self.ln()
                self.set_x(self.l_margin + 6)
                self.set_text_color(*DARK_TEXT)
                self.set_font("Helvetica", "", 6.5)
                work_text = the_work[:400] if len(the_work) > 400 else the_work
                self.multi_cell(self.epw - 8, 3.5, work_text)
                self.ln(1)
        else:
            self._ensure_space(10)
            self._section_header("IDS -- Action Items", font_size=FONT_SEC)
            self.set_font("Helvetica", "I", 7)
            self.cell(0, 4, "No focus items found.")
            self.ln()

        # === SOURCE FILES ===
        self.ln(1)
        self.set_font("Helvetica", "I", 6)
        self.set_text_color(*MID_GRAY)
        files_str = ", ".join(data.get("source_files", []))
        self.cell(0, 3, f"Sources: {files_str}")

    # ------------------------------------------------------------------
    # Page 2: kept as no-op — all content fits on page 1
    # ------------------------------------------------------------------
    def build_page2(self, data):
        pass


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def generate_analysis_report(excel_paths, output_path=None):
    """Generate a 2-page analysis PDF from up to 6 analysis Excel files.

    Args:
        excel_paths: list of paths to analysis workbooks (max 6)
        output_path: optional output PDF path (auto-generated if None)

    Returns:
        output_path: path to the generated PDF
    """
    if len(excel_paths) > 6:
        print(f"Warning: Maximum 6 files. Using first 6 of {len(excel_paths)}.")
        excel_paths = excel_paths[:6]

    # Read all workbooks
    workbooks = []
    for p in excel_paths:
        if not os.path.exists(p):
            print(f"Warning: File not found, skipping: {p}")
            continue
        try:
            wb = read_analysis_workbook(p)
            workbooks.append(wb)
            print(f"  Loaded: {os.path.basename(p)}")
        except Exception as e:
            print(f"  Error reading {os.path.basename(p)}: {e}")

    if not workbooks:
        raise ValueError("No valid analysis workbooks found.")

    # Consolidate
    data = consolidate(workbooks)
    print(f"\n  Consolidated: {data['n_files']} file(s), {data['n_days']} day(s), {data['date_range']}")

    # Build PDF
    pdf = AnalysisReport()
    pdf.build_page1(data)
    pdf.build_page2(data)

    # Output path
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_path = f"Analysis_Report_{timestamp}.pdf"

    pdf.output(output_path)
    print(f"\n  Analysis Report: {output_path}")
    return output_path


def generate_analysis_report_bytes(excel_paths):
    """Generate analysis PDF and return as bytes (for Streamlit download).

    Args:
        excel_paths: list of paths to analysis workbooks (max 6)

    Returns:
        tuple of (pdf_bytes, consolidated_data_dict)
    """
    if len(excel_paths) > 6:
        excel_paths = excel_paths[:6]

    workbooks = []
    for p in excel_paths:
        if not os.path.exists(p):
            continue
        try:
            wb = read_analysis_workbook(p)
            workbooks.append(wb)
        except Exception:
            pass

    if not workbooks:
        raise ValueError("No valid analysis workbooks found.")

    data = consolidate(workbooks)

    pdf = AnalysisReport()
    pdf.build_page1(data)
    pdf.build_page2(data)

    return bytes(pdf.output()), data


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    args = sys.argv[1:]
    output_path = None
    excel_paths = []

    i = 0
    while i < len(args):
        if args[i] in ("-o", "--output") and i + 1 < len(args):
            output_path = args[i + 1]
            i += 2
        elif not args[i].startswith("-"):
            excel_paths.append(args[i])
            i += 1
        else:
            i += 1

    if not excel_paths:
        print("Usage: python analysis_report.py file1.xlsx [file2.xlsx ...] [-o output.pdf]")
        print("  Reads up to 6 analysis Excel files and generates a 2-page analysis PDF.")
        sys.exit(1)

    generate_analysis_report(excel_paths, output_path)


if __name__ == "__main__":
    main()
