"""
Traksys OEE Analyzer — Web Interface
=====================================
Upload your Traksys OEE export, get back a formatted analysis workbook.

Supports both:
  - Raw Traksys exports (OEE Period Detail + Event Summary)
  - Pre-processed OEE workbooks (DayShiftHour format)

Usage:
  streamlit run streamlit_app.py
"""

import streamlit as st
import tempfile
import shutil
import os
import sys
from datetime import datetime

# Ensure sibling modules are importable when run from repo root (Streamlit Cloud)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from analyze import load_oee_data, load_downtime_data, analyze, write_excel, _aggregate_oee
from parse_traksys import parse_oee_period_detail, parse_event_summary, detect_file_type
from shared import SHIFT_HOURS

st.set_page_config(
    page_title="Traksys OEE Analyzer",
    page_icon="📊",
    layout="wide",
)

st.title("Traksys OEE Analyzer")
st.markdown("Upload your OEE export. Get back a formatted analysis workbook with loss breakdowns and prioritized actions.")

# --- File uploads ---
col1, col2 = st.columns(2)

with col1:
    oee_files = st.file_uploader(
        "OEE Data (Excel) — upload one or more",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
        help="Traksys 'OEE Period Detail' export OR processed workbook with DayShiftHour sheets",
    )

with col2:
    downtime_files = st.file_uploader(
        "Downtime Data (Excel or JSON) — optional",
        type=["json", "xlsx", "xls"],
        accept_multiple_files=True,
        help="Event Summary (.xlsx), Shift Passdown (.xlsx), or knowledge base (.json)",
    )

context_files = st.file_uploader(
    "Additional Context — optional (up to 6 photos or Excel files)",
    type=["png", "jpg", "jpeg", "xlsx", "xls"],
    accept_multiple_files=True,
    help="Shift photos, work orders, passdown sheets — anything that adds context to the analysis",
)
if context_files and len(context_files) > 6:
    st.warning("Maximum 6 context files. Only the first 6 will be used.")
    context_files = context_files[:6]

output_format = st.radio(
    "Output format",
    options=["Excel (.xlsx)", "PDF Report (.pdf)", "Both"],
    index=0,
    horizontal=True,
    help="Excel gives you the full multi-sheet workbook. PDF gives a 1-page summary report.",
)

def _build_overall(hourly):
    """Build per-shift aggregate from hourly data."""
    rows = []
    for shift_name in hourly["shift"].unique():
        sh = hourly[hourly["shift"] == shift_name]
        _a, _p, _q, oee = _aggregate_oee(sh)
        total_hrs = float(sh["total_hours"].sum())
        rows.append({
            "shift": shift_name,
            "total_hours": total_hrs,
            "good_cases": float(sh["good_cases"].sum()) if "good_cases" in sh.columns else 0,
            "bad_cases": float(sh["bad_cases"].sum()) if "bad_cases" in sh.columns else 0,
            "total_cases": float(sh["total_cases"].sum()),
            "oee_pct": oee,
            "cases_per_hour": float(sh["total_cases"].sum()) / (sh["date_str"].nunique() * SHIFT_HOURS) if sh["date_str"].nunique() > 0 else 0,
        })
    return pd.DataFrame(rows)


def _build_hour_avg(hourly):
    """Build per-shift-hour aggregate from hourly data."""
    rows = []
    for (shift, hour), grp in hourly.groupby(["shift", "shift_hour"]):
        _a, _p, _q, oee = _aggregate_oee(grp)
        total_hrs = float(grp["total_hours"].sum())
        n_hr_days = grp["date_str"].nunique()
        rows.append({
            "shift": shift, "shift_hour": hour,
            "oee_pct": oee,
            "availability": _a, "performance": _p,
            "cases_per_hour": float(grp["total_cases"].sum()) / max(n_hr_days, 1),
            "total_hours": total_hrs,
        })
    return pd.DataFrame(rows)


def _merge_downtime_dicts(dt_list):
    """Merge multiple downtime dicts into one, re-aggregating reasons."""
    if not dt_list:
        return None
    merged = dt_list[0].copy()
    for extra in dt_list[1:]:
        merged["events_df"] = pd.concat(
            [merged["events_df"], extra["events_df"]], ignore_index=True)
        merged["reasons_df"] = pd.concat(
            [merged["reasons_df"], extra["reasons_df"]], ignore_index=True)
        sr_extra = extra.get("shift_reasons_df", pd.DataFrame())
        if len(sr_extra) > 0:
            merged["shift_reasons_df"] = pd.concat(
                [merged.get("shift_reasons_df", pd.DataFrame()), sr_extra],
                ignore_index=True)
    # Re-aggregate reasons
    if len(merged["reasons_df"]) > 0:
        merged["reasons_df"] = (
            merged["reasons_df"]
            .groupby("reason", as_index=False)
            .agg({"total_minutes": "sum", "total_occurrences": "sum", "total_hours": "sum"})
            .sort_values("total_minutes", ascending=False)
            .reset_index(drop=True)
        )
    sr = merged.get("shift_reasons_df", pd.DataFrame())
    if len(sr) > 0:
        merged["shift_reasons_df"] = (
            sr.groupby(["shift", "reason"], as_index=False)
            .agg({"total_minutes": "sum", "count": "sum"})
            .sort_values(["shift", "total_minutes"], ascending=[True, False])
            .reset_index(drop=True)
        )
    return merged


# --- Analyze ---
if oee_files:
    if len(oee_files) > 1:
        st.info(f"{len(oee_files)} OEE file(s): {', '.join(f.name for f in oee_files)}")
    if st.button("Analyze", type="primary", use_container_width=True):
        with st.spinner("Running analysis..."):
            # Write uploaded files to temp directory
            tmp_dir = tempfile.mkdtemp()
            try:
                # Load all OEE files
                all_hourly, all_shift_summary = [], []
                for oee_file in oee_files:
                    oee_path = os.path.join(tmp_dir, oee_file.name)
                    with open(oee_path, "wb") as f:
                        f.write(oee_file.getbuffer())

                    file_type = detect_file_type(oee_path)
                    if file_type == "oee_period_detail":
                        st.info(f"Detected: {oee_file.name} — Traksys OEE Period Detail")
                        h, ss, _ov, _ha = parse_oee_period_detail(oee_path)
                    else:
                        h, ss, _ov, _ha = load_oee_data(oee_path)
                        # Ensure line column exists for old-format files
                        if "line" not in h.columns:
                            h["line"] = "All"
                    all_hourly.append(h)
                    all_shift_summary.append(ss)

                # Merge OEE data (dedup overlapping date ranges per line)
                hourly = pd.concat(all_hourly, ignore_index=True)
                if "line" not in hourly.columns:
                    hourly["line"] = "All"
                hourly["line"] = hourly["line"].fillna("All")
                hourly = hourly.drop_duplicates(
                    subset=["date_str", "shift", "shift_hour", "line"], keep="first")
                shift_summary = pd.concat(all_shift_summary, ignore_index=True)
                shift_summary = shift_summary.drop_duplicates(
                    subset=["shift_date", "shift"], keep="first")

                # Load all downtime / event data files, tagged by line
                # dt_by_line: {line_name: [list of downtime dicts]}
                dt_by_line = {}
                if downtime_files:
                    from parse_passdown import parse_passdown
                    for dt_file in downtime_files:
                        dt_path = os.path.join(tmp_dir, dt_file.name)
                        with open(dt_path, "wb") as f:
                            f.write(dt_file.getbuffer())
                        try:
                            if dt_file.name.lower().endswith(".json"):
                                dt_data = load_downtime_data(dt_path)
                                line_key = dt_data.get("line") or "All"
                                dt_by_line.setdefault(line_key, []).append(dt_data)
                            else:
                                dt_type = detect_file_type(dt_path)
                                if dt_type == "event_summary":
                                    dt_data = parse_event_summary(dt_path)
                                    line_key = dt_data.get("line") or "All"
                                    st.info(f"Detected: {dt_file.name} — Event Summary ({line_key})")
                                    dt_by_line.setdefault(line_key, []).append(dt_data)
                                elif dt_type == "passdown":
                                    st.info(f"Detected: {dt_file.name} — Shift Passdown")
                                    dt_data = parse_passdown(dt_path)
                                    line_key = dt_data.get("line") or "All"
                                    dt_by_line.setdefault(line_key, []).append(dt_data)
                                else:
                                    st.warning(f"Unrecognized downtime format: {dt_file.name}")
                        except Exception as e:
                            st.warning(f"Could not load {dt_file.name}: {e}")

                # Process context files (photos + additional Excel)
                context_photos = []
                if context_files:
                    from parse_passdown import parse_passdown, detect_passdown
                    for cf in context_files:
                        cf_path = os.path.join(tmp_dir, cf.name)
                        with open(cf_path, "wb") as f:
                            f.write(cf.getbuffer())
                        name_lower = cf.name.lower()
                        if name_lower.endswith((".png", ".jpg", ".jpeg")):
                            context_photos.append((cf.name, cf_path))
                        elif name_lower.endswith((".xlsx", ".xls")):
                            try:
                                if detect_passdown(cf_path):
                                    extra = parse_passdown(cf_path)
                                    line_key = extra.get("line") or "All"
                                    dt_by_line.setdefault(line_key, []).append(extra)
                                    st.info(f"Context: {cf.name} — Shift Passdown ({len(extra['events_df'])} events)")
                                else:
                                    st.info(f"Context: {cf.name} — uploaded (not a recognized format)")
                            except Exception as e:
                                st.warning(f"Could not parse {cf.name}: {e}")

                # Analyze context photos via OpenAI Vision
                photo_display_results = []
                if context_photos:
                    try:
                        from photo_analysis import get_openai_api_key, analyze_photos
                        api_key = get_openai_api_key()
                        if api_key:
                            data_shifts = list(hourly["shift"].unique())
                            photo_dt, photo_display_results = analyze_photos(
                                context_photos, api_key, data_shifts=data_shifts)
                            if photo_dt:
                                dt_by_line.setdefault("All", []).append(photo_dt)
                                n_issues = len(photo_dt["events_df"])
                                st.info(f"Photo analysis: extracted {n_issues} issue(s) from {len(context_photos)} photo(s)")
                    except Exception as photo_err:
                        st.warning(f"Photo analysis failed (non-blocking): {photo_err}")

                # Determine lines present in the data
                lines = sorted(hourly["line"].unique())
                multi_line = len(lines) > 1 or (len(lines) == 1 and lines[0] != "All")

                dates = sorted(hourly["date_str"].unique())
                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                want_excel = output_format in ("Excel (.xlsx)", "Both")
                want_pdf = output_format in ("PDF Report (.pdf)", "Both")

                if multi_line:
                    st.success(f"Analyzing {len(dates)} day(s) across {len(lines)} lines: {', '.join(lines)}")
                else:
                    st.success(f"Analyzing {len(dates)} day(s): {', '.join(dates)}")

                # --- Per-line analysis loop ---
                for line_idx, line_name in enumerate(lines):
                    line_hourly = hourly[hourly["line"] == line_name].copy()
                    if len(line_hourly) == 0:
                        continue

                    # Build per-line shift summary from hourly
                    line_ss = shift_summary.copy()  # will be rebuilt from hourly below
                    line_overall = _build_overall(line_hourly)
                    line_hour_avg = _build_hour_avg(line_hourly)

                    # Rebuild shift summary from this line's hourly data
                    line_hourly["_is_prod"] = (line_hourly["availability"] > 0) | (line_hourly["total_cases"] > 0)
                    line_hourly["_prod_hours"] = line_hourly["total_hours"] * line_hourly["_is_prod"]
                    line_hourly["_w_oee"] = line_hourly["oee_pct"] * line_hourly["_prod_hours"]
                    ss_agg = (
                        line_hourly.groupby(["shift_date", "shift"])
                        .agg(
                            total_cases=("total_cases", "sum"),
                            total_hours=("total_hours", "sum"),
                            _prod_hours=("_prod_hours", "sum"),
                            _w_oee=("_w_oee", "sum"),
                            hour_blocks=("intervals", "sum") if "intervals" in line_hourly.columns else ("total_hours", "count"),
                        )
                        .reset_index()
                    )
                    ss_agg["oee_pct"] = (ss_agg["_w_oee"] / ss_agg["_prod_hours"].replace(0, float("nan"))).fillna(0)
                    ss_agg.drop(columns=["_w_oee", "_prod_hours"], inplace=True)
                    ss_agg["cases_per_hour"] = ss_agg["total_cases"] / ss_agg["total_hours"].replace(0, float("nan"))
                    ss_agg["date"] = pd.to_datetime(ss_agg["shift_date"])
                    ss_agg["date_str"] = ss_agg["date"].dt.strftime("%Y-%m-%d")
                    line_ss = ss_agg
                    line_hourly.drop(columns=["_is_prod", "_prod_hours", "_w_oee"], inplace=True, errors="ignore")

                    # Match downtime to this line (avoid double-counting "All" when line IS "All")
                    line_dt_list = dt_by_line.get(line_name, [])
                    if line_name != "All":
                        line_dt_list = line_dt_list + dt_by_line.get("All", [])
                    line_downtime = _merge_downtime_dicts(line_dt_list) if line_dt_list else None

                    # Build output filename
                    basename = os.path.splitext(oee_files[0].name)[0]
                    if len(oee_files) > 1:
                        basename += f"_+{len(oee_files) - 1}"
                    suffix = "_FULL_ANALYSIS" if line_downtime else "_ANALYSIS"
                    line_tag = f"_{line_name.replace(' ', '')}" if multi_line else ""
                    output_name = f"{basename}{line_tag}{suffix}_{timestamp}.xlsx"
                    output_path = os.path.join(tmp_dir, output_name)

                    results = analyze(line_hourly, line_ss, line_overall, line_hour_avg, line_downtime)

                    # Inject photo findings into shift narratives so they appear
                    # in the Excel output regardless of downtime pipeline matching
                    if photo_display_results:
                        from photo_analysis import build_photo_narrative
                        photo_narrative = build_photo_narrative(photo_display_results)
                        if photo_narrative:
                            for shift_key in ["1st Shift", "2nd Shift", "3rd Shift"]:
                                shift_data = results.get(shift_key)
                                if isinstance(shift_data, dict) and "narrative" in shift_data:
                                    shift_data["narrative"] += photo_narrative

                    write_excel(results, output_path)

                    # Display results under a line header
                    if multi_line:
                        st.subheader(line_name)

                    if want_excel:
                        with open(output_path, "rb") as f:
                            excel_bytes = f.read()
                        st.download_button(
                            label=f"Download {output_name}",
                            data=excel_bytes,
                            file_name=output_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            key=f"excel_dl_{line_idx}",
                        )

                    if want_pdf:
                        try:
                            from analysis_report import generate_analysis_report_bytes
                            pdf_bytes, _report_data = generate_analysis_report_bytes([output_path])
                            if isinstance(pdf_bytes, bytearray):
                                pdf_bytes = bytes(pdf_bytes)
                            pdf_name = output_name.replace(".xlsx", ".pdf")
                            st.download_button(
                                label=f"Download {pdf_name}",
                                data=pdf_bytes,
                                file_name=pdf_name,
                                mime="application/pdf",
                                use_container_width=True,
                                key=f"pdf_dl_{line_idx}",
                            )
                        except Exception as pdf_err:
                            st.warning(f"PDF generation failed: {pdf_err}")
                            if not want_excel:
                                with open(output_path, "rb") as f:
                                    excel_bytes = f.read()
                                st.download_button(
                                    label=f"Download {output_name} (Excel fallback)",
                                    data=excel_bytes,
                                    file_name=output_name,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    use_container_width=True,
                                    key=f"excel_fallback_dl_{line_idx}",
                                )

                    # Quick summary metrics from Plant Summary KPIs
                    plant_data = results.get("Plant Summary")
                    if isinstance(plant_data, dict):
                        kpis = plant_data.get("kpis", pd.DataFrame())
                        if len(kpis) > 0:
                            mcols = st.columns(min(4, len(kpis)))
                            for i, (_, row) in enumerate(kpis.head(4).iterrows()):
                                mcols[i].metric(str(row["Metric"]), str(row["Value"]))

                    # Per-shift narrative in expandable sections
                    for shift_name in ["1st Shift", "2nd Shift", "3rd Shift"]:
                        shift_data = results.get(shift_name)
                        if isinstance(shift_data, dict):
                            raw = shift_data.get("raw", {})
                            label = f"{line_name} — {shift_name}" if multi_line else shift_name
                            with st.expander(f"{label} — {raw.get('oee', 0):.1f}% OEE"):
                                st.markdown(shift_data.get("narrative", ""))

                    # Top 3 actions
                    focus_df = results.get("What to Focus On")
                    if focus_df is not None:
                        for _, row in focus_df.head(3).iterrows():
                            st.markdown(f"**#{row['Priority']}:** {row['Finding']}")

                    st.caption(f"Sheets: {', '.join(results.keys())}")

                    if multi_line:
                        st.divider()

                # Display context photos with AI results
                if context_photos:
                    with st.expander(f"Context Photos ({len(context_photos)})", expanded=True):
                        # Build lookup from display results
                        photo_findings = {name: findings for name, findings in photo_display_results}
                        if not photo_findings:
                            from photo_analysis import get_openai_api_key
                            if not get_openai_api_key():
                                st.caption("Set `OPENAI_API_KEY` in environment or Streamlit secrets to enable photo analysis.")
                            else:
                                st.caption("Photos displayed for reference.")
                        for pname, ppath in context_photos:
                            st.image(ppath, caption=pname, use_container_width=True)
                            findings = photo_findings.get(pname)
                            if findings and "error" not in findings:
                                ptype = findings.get("photo_type", "unknown")
                                conf = findings.get("confidence", "?")
                                st.caption(f"Type: {ptype} | Confidence: {conf}")
                                for issue in findings.get("issues", []):
                                    dur = issue.get("duration_minutes")
                                    dur_str = f" ({dur} min)" if dur else ""
                                    sev = issue.get("severity", "")
                                    sev_badge = {"high": " :red[HIGH]", "medium": " :orange[MED]", "low": ""}.get(sev, "")
                                    st.markdown(f"- **{issue.get('equipment', '?')}**: {issue.get('description', '')}{dur_str}{sev_badge}")
                                for note in findings.get("shift_notes", []):
                                    st.markdown(f"- *Shift note:* {note}")
                                for note in findings.get("production_notes", []):
                                    st.markdown(f"- *Production:* {note}")
                                raw = findings.get("raw_text", "")
                                if raw:
                                    with st.expander("Raw text", expanded=False):
                                        st.text(raw)
                            elif findings and "error" in findings:
                                st.caption(f"Analysis error: {findings['error']}")
                            st.markdown("---")

            except ValueError as e:
                err_msg = str(e)
                if "worksheet" in err_msg.lower() or "sheet" in err_msg.lower():
                    st.error("**Sheet mismatch** — your Excel file doesn't have the expected sheet names.")
                    st.info(
                        "The analyzer expects these sheets in your Traksys OEE export:\n\n"
                        "| Sheet | Columns |\n"
                        "|---|---|\n"
                        "| **DayShiftHour** | 14 columns — Date, Shift, StartTime, Hour, DurationHours, ProductCode, Job, GoodCases, BadCases, TotalCases, Availability, Performance, Quality, OEE |\n"
                        "| **DayShift_Summary** | 7 columns — Date, Shift, Hours, GoodCases, BadCases, TotalCases, AvgOEE |\n"
                        "| **Shift_Summary** | 6 columns — Shift, Hours, GoodCases, BadCases, TotalCases, AvgOEE |\n"
                        "| **ShiftHour_Summary** | 5 columns — Shift, Hour, AvgAvailability, AvgPerformance, AvgOEE |\n\n"
                        "**Fix options:**\n"
                        "1. Rename your sheets to match the names above\n"
                        "2. Check that you're uploading the correct Traksys OEE export file"
                    )
                    st.code(err_msg, language=None)
                else:
                    st.error(f"Analysis failed: {e}")
                    st.exception(e)
            except Exception as e:
                st.error(f"Analysis failed: {e}")
                st.exception(e)
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)
else:
    st.info("Upload a Traksys OEE export (.xlsx) to get started.")

# --- Footer ---
st.markdown("---")
st.caption("Built by Brian Crusoe | Numbers from the machine, not opinions")
