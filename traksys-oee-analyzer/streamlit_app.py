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
                    all_hourly.append(h)
                    all_shift_summary.append(ss)

                # Merge OEE data (dedup overlapping date ranges across files)
                hourly = pd.concat(all_hourly, ignore_index=True)
                hourly = hourly.drop_duplicates(subset=["date_str", "shift", "shift_hour"], keep="first")
                shift_summary = pd.concat(all_shift_summary, ignore_index=True)
                shift_summary = shift_summary.drop_duplicates(subset=["shift_date", "shift"], keep="first")

                # Rebuild overall (per-shift aggregate) from merged hourly
                overall_rows = []
                for shift_name in hourly["shift"].unique():
                    sh = hourly[hourly["shift"] == shift_name]
                    _a, _p, _q, oee = _aggregate_oee(sh)
                    total_hrs = float(sh["total_hours"].sum())
                    overall_rows.append({
                        "shift": shift_name,
                        "total_hours": total_hrs,
                        "good_cases": float(sh["good_cases"].sum()) if "good_cases" in sh.columns else 0,
                        "bad_cases": float(sh["bad_cases"].sum()) if "bad_cases" in sh.columns else 0,
                        "total_cases": float(sh["total_cases"].sum()),
                        "oee_pct": oee,
                        "cases_per_hour": float(sh["total_cases"].sum()) / (sh["date_str"].nunique() * SHIFT_HOURS) if sh["date_str"].nunique() > 0 else 0,
                    })
                overall = pd.DataFrame(overall_rows)

                # Rebuild hour_avg (per-shift-hour aggregate) from merged hourly
                hour_avg_rows = []
                for (shift, hour), grp in hourly.groupby(["shift", "shift_hour"]):
                    _a, _p, _q, oee = _aggregate_oee(grp)
                    total_hrs = float(grp["total_hours"].sum())
                    n_hr_days = grp["date_str"].nunique()
                    hour_avg_rows.append({
                        "shift": shift, "shift_hour": hour,
                        "oee_pct": oee,
                        "availability": _a, "performance": _p,
                        "cases_per_hour": float(grp["total_cases"].sum()) / max(n_hr_days, 1),
                        "total_hours": total_hrs,
                    })
                hour_avg = pd.DataFrame(hour_avg_rows)

                # Load all downtime / event data files
                downtime = None
                if downtime_files:
                    from parse_passdown import parse_passdown
                    dt_list = []
                    for dt_file in downtime_files:
                        dt_path = os.path.join(tmp_dir, dt_file.name)
                        with open(dt_path, "wb") as f:
                            f.write(dt_file.getbuffer())
                        try:
                            if dt_file.name.lower().endswith(".json"):
                                dt_list.append(load_downtime_data(dt_path))
                            else:
                                dt_type = detect_file_type(dt_path)
                                if dt_type == "event_summary":
                                    st.info(f"Detected: {dt_file.name} — Event Summary")
                                    dt_list.append(parse_event_summary(dt_path))
                                elif dt_type == "passdown":
                                    st.info(f"Detected: {dt_file.name} — Shift Passdown")
                                    dt_list.append(parse_passdown(dt_path))
                                else:
                                    st.warning(f"Unrecognized downtime format: {dt_file.name}")
                        except Exception as e:
                            st.warning(f"Could not load {dt_file.name}: {e}")

                    if dt_list:
                        downtime = dt_list[0]
                        for extra in dt_list[1:]:
                            downtime["events_df"] = pd.concat(
                                [downtime["events_df"], extra["events_df"]], ignore_index=True)
                            downtime["reasons_df"] = pd.concat(
                                [downtime["reasons_df"], extra["reasons_df"]], ignore_index=True)
                            sr_extra = extra.get("shift_reasons_df", pd.DataFrame())
                            if len(sr_extra) > 0:
                                downtime["shift_reasons_df"] = pd.concat(
                                    [downtime.get("shift_reasons_df", pd.DataFrame()), sr_extra],
                                    ignore_index=True)
                        # Re-aggregate reasons after merging
                        if len(downtime["reasons_df"]) > 0:
                            downtime["reasons_df"] = (
                                downtime["reasons_df"]
                                .groupby("reason", as_index=False)
                                .agg({"total_minutes": "sum", "total_occurrences": "sum", "total_hours": "sum"})
                                .sort_values("total_minutes", ascending=False)
                                .reset_index(drop=True)
                            )
                        sr = downtime.get("shift_reasons_df", pd.DataFrame())
                        if len(sr) > 0:
                            downtime["shift_reasons_df"] = (
                                sr.groupby(["shift", "reason"], as_index=False)
                                .agg({"total_minutes": "sum", "count": "sum"})
                                .sort_values(["shift", "total_minutes"], ascending=[True, False])
                                .reset_index(drop=True)
                            )

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
                                    if downtime is None:
                                        downtime = extra
                                        st.info(f"Context: {cf.name} — Shift Passdown ({len(extra['events_df'])} events)")
                                    else:
                                        # Merge events into existing downtime
                                        import pandas as _pd
                                        downtime["events_df"] = _pd.concat(
                                            [downtime["events_df"], extra["events_df"]], ignore_index=True)
                                        downtime["reasons_df"] = _pd.concat(
                                            [downtime["reasons_df"], extra["reasons_df"]], ignore_index=True)
                                        if len(extra.get("shift_reasons_df", _pd.DataFrame())) > 0:
                                            downtime["shift_reasons_df"] = _pd.concat(
                                                [downtime["shift_reasons_df"], extra["shift_reasons_df"]],
                                                ignore_index=True)
                                        st.info(f"Context: {cf.name} — merged {len(extra['events_df'])} passdown events")
                                else:
                                    st.info(f"Context: {cf.name} — uploaded (not a recognized format)")
                            except Exception as e:
                                st.warning(f"Could not parse {cf.name}: {e}")

                # Build output filename
                dates = sorted(hourly["date_str"].unique())
                basename = os.path.splitext(oee_files[0].name)[0]
                if len(oee_files) > 1:
                    basename += f"_+{len(oee_files) - 1}"
                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                suffix = "_FULL_ANALYSIS" if downtime else "_ANALYSIS"

                st.success(f"Analyzing {len(dates)} day(s): {', '.join(dates)}")

                results = analyze(hourly, shift_summary, overall, hour_avg, downtime)

                output_name = f"{basename}{suffix}_{timestamp}.xlsx"
                output_path = os.path.join(tmp_dir, output_name)
                write_excel(results, output_path)

                want_excel = output_format in ("Excel (.xlsx)", "Both")
                want_pdf = output_format in ("PDF Report (.pdf)", "Both")

                if want_excel:
                    with open(output_path, "rb") as f:
                        excel_bytes = f.read()
                    st.download_button(
                        label=f"Download {output_name}",
                        data=excel_bytes,
                        file_name=output_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key="analyze_excel_dl",
                    )

                if want_pdf:
                    try:
                        from analysis_report import generate_analysis_report_bytes
                        pdf_bytes, _report_data = generate_analysis_report_bytes([output_path])
                        if isinstance(pdf_bytes, bytearray):
                            pdf_bytes = bytes(pdf_bytes)
                        pdf_name = f"{basename}{suffix}_{timestamp}.pdf"
                        st.download_button(
                            label=f"Download {pdf_name}",
                            data=pdf_bytes,
                            file_name=pdf_name,
                            mime="application/pdf",
                            use_container_width=True,
                            key="analyze_pdf_dl",
                        )
                    except Exception as pdf_err:
                        st.warning(f"PDF generation failed: {pdf_err}")
                        if not want_excel:
                            # Fallback: offer Excel if PDF was the only choice
                            with open(output_path, "rb") as f:
                                excel_bytes = f.read()
                            st.download_button(
                                label=f"Download {output_name} (Excel fallback)",
                                data=excel_bytes,
                                file_name=output_name,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True,
                                key="analyze_excel_fallback_dl",
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
                        with st.expander(f"{shift_name} — {raw.get('oee', 0):.1f}% OEE"):
                            st.markdown(shift_data.get("narrative", ""))

                # Top 3 actions
                focus_df = results.get("What to Focus On")
                if focus_df is not None:
                    for _, row in focus_df.head(3).iterrows():
                        st.markdown(f"**#{row['Priority']}:** {row['Finding']}")

                # Display context photos
                if context_photos:
                    with st.expander(f"Context Photos ({len(context_photos)})", expanded=True):
                        st.caption("Photos are displayed for reference. Automated photo analysis is not yet available.")
                        photo_cols = st.columns(min(3, len(context_photos)))
                        for i, (pname, ppath) in enumerate(context_photos):
                            with photo_cols[i % 3]:
                                st.image(ppath, caption=pname, use_container_width=True)

                st.caption(f"Sheets: {', '.join(results.keys())}")

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
