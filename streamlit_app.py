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
from datetime import datetime

import pandas as pd

from analyze import load_oee_data, load_downtime_data, analyze, write_excel
from parse_traksys import parse_oee_period_detail, parse_event_summary, detect_file_type
from oee_history import save_run, load_trends
from eos_report import generate_eos_report_bytes

st.set_page_config(
    page_title="Traksys OEE Analyzer",
    page_icon="📊",
    layout="wide",
)

st.title("Traksys OEE Analyzer")
st.markdown("Upload your OEE export. Get back a formatted analysis workbook with shift deep dives, loss breakdowns, and prioritized actions.")

# --- Tab navigation ---
tab_analyze, tab_eos, tab_history = st.tabs(["Analyze", "EOS Report", "Plant History"])

# =====================================================================
# TAB 1: ANALYZE (original functionality)
# =====================================================================
with tab_analyze:
    # --- File uploads ---
    col1, col2 = st.columns(2)

    with col1:
        oee_file = st.file_uploader(
            "OEE Data (Excel)",
            type=["xlsx", "xls"],
            help="Traksys 'OEE Period Detail' export OR processed workbook with DayShiftHour sheets",
        )

    with col2:
        downtime_file = st.file_uploader(
            "Downtime Data (Excel or JSON) — optional",
            type=["json", "xlsx", "xls"],
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

    # --- Analyze ---
    if oee_file is not None:
        if st.button("Analyze", type="primary", use_container_width=True):
            with st.spinner("Running analysis..."):
                # Write uploaded files to temp directory
                tmp_dir = tempfile.mkdtemp()
                try:
                    oee_path = os.path.join(tmp_dir, oee_file.name)
                    with open(oee_path, "wb") as f:
                        f.write(oee_file.getbuffer())

                    # Detect OEE file format and load accordingly
                    file_type = detect_file_type(oee_path)
                    if file_type == "oee_period_detail":
                        st.info("Detected: Traksys OEE Period Detail export")
                        hourly, shift_summary, overall, hour_avg = parse_oee_period_detail(oee_path)
                    else:
                        hourly, shift_summary, overall, hour_avg = load_oee_data(oee_path)

                    # Load downtime / event data
                    downtime = None
                    if downtime_file is not None:
                        dt_path = os.path.join(tmp_dir, downtime_file.name)
                        with open(dt_path, "wb") as f:
                            f.write(downtime_file.getbuffer())
                        try:
                            if downtime_file.name.lower().endswith(".json"):
                                downtime = load_downtime_data(dt_path)
                            else:
                                dt_type = detect_file_type(dt_path)
                                if dt_type == "event_summary":
                                    st.info("Detected: Traksys Event Summary export")
                                    downtime = parse_event_summary(dt_path)
                                elif dt_type == "passdown":
                                    from parse_passdown import parse_passdown
                                    st.info("Detected: Shift Passdown Report")
                                    downtime = parse_passdown(dt_path)
                                else:
                                    st.warning("Unrecognized downtime file format")
                        except Exception as e:
                            st.warning(f"Could not load downtime data: {e}")

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

                    # Single file per analysis run
                    dates = sorted(hourly["date_str"].unique())
                    basename = os.path.splitext(oee_file.name)[0]
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                    suffix = "_FULL_ANALYSIS" if downtime else "_ANALYSIS"

                    st.success(f"Analyzing {len(dates)} day(s): {', '.join(dates)}")

                    results = analyze(hourly, shift_summary, overall, hour_avg, downtime)

                    output_name = f"{basename}{suffix}_{timestamp}.xlsx"
                    output_path = os.path.join(tmp_dir, output_name)
                    write_excel(results, output_path)

                    # Save to history
                    try:
                        save_run(results, hourly, shift_summary, overall, downtime)
                    except Exception:
                        pass

                    with open(output_path, "rb") as f:
                        output_bytes = f.read()

                    st.download_button(
                        label=f"Download {output_name}",
                        data=output_bytes,
                        file_name=output_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
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

# =====================================================================
# TAB 2: EOS MEETING REPORT
# =====================================================================
with tab_eos:
    st.subheader("EOS Meeting Report")
    st.markdown(
        "Upload up to **6 analysis Excel files** (the output from Analyze tab) "
        "to generate a **2-page PDF** with consolidated KPIs, shift performance, "
        "root cause analysis, and prioritized action items for your Level 10 meeting."
    )

    eos_files = st.file_uploader(
        "Analysis Excel Files (up to 6)",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
        help="Upload the _ANALYSIS or _FULL_ANALYSIS Excel files produced by the Analyze tab",
        key="eos_uploader",
    )
    if eos_files and len(eos_files) > 6:
        st.warning("Maximum 6 files. Only the first 6 will be used.")
        eos_files = eos_files[:6]

    if eos_files:
        st.info(f"{len(eos_files)} file(s) uploaded: {', '.join(f.name for f in eos_files)}")

        if st.button("Generate EOS Report", type="primary", use_container_width=True):
            with st.spinner("Building 2-page EOS PDF..."):
                tmp_dir = tempfile.mkdtemp()
                try:
                    # Write uploaded files to temp directory
                    tmp_paths = []
                    for ef in eos_files:
                        ef_path = os.path.join(tmp_dir, ef.name)
                        with open(ef_path, "wb") as f:
                            f.write(ef.getbuffer())
                        tmp_paths.append(ef_path)

                    pdf_bytes, eos_data = generate_eos_report_bytes(tmp_paths)
                    if isinstance(pdf_bytes, bytearray):
                        pdf_bytes = bytes(pdf_bytes)

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                    pdf_name = f"EOS_Report_{timestamp}.pdf"

                    st.download_button(
                        label=f"Download {pdf_name}",
                        data=pdf_bytes,
                        file_name=pdf_name,
                        mime="application/pdf",
                        use_container_width=True,
                    )

                    st.success(
                        f"EOS Report generated: {eos_data['n_files']} file(s), "
                        f"{eos_data['n_days']} day(s), {eos_data['date_range']}"
                    )

                    # Show consolidated KPIs
                    kpis = eos_data.get("kpis", {})
                    if kpis:
                        kpi_items = list(kpis.items())
                        mcols = st.columns(min(4, len(kpi_items)))
                        for i, (metric, value) in enumerate(kpi_items[:4]):
                            mcols[i].metric(metric, str(value))

                    # Show top 3 IDS items
                    ids_items = eos_data.get("ids_items", [])
                    if ids_items:
                        st.subheader("Top IDS Items")
                        for item in ids_items:
                            st.markdown(f"**#{item.get('Priority', '?')}:** {item.get('Finding', '')}")

                except Exception as e:
                    st.error(f"EOS report generation failed: {e}")
                    st.exception(e)
                finally:
                    shutil.rmtree(tmp_dir, ignore_errors=True)
    else:
        st.info("Upload analysis Excel files to generate your EOS meeting report.")

# =====================================================================
# TAB 3: PLANT HISTORY (SPC + Gardener Intelligence)
# =====================================================================
with tab_history:
    trends = load_trends()

    if trends is None:
        st.info("No history yet. Run an analysis on the Analyze tab to start building your trend data.")
    else:
        runs = pd.DataFrame(trends["runs"])
        shifts = pd.DataFrame(trends.get("shifts", []))
        downtime_hist = pd.DataFrame(trends.get("downtime", []))
        spc = trends.get("spc", {})
        wow = trends.get("week_over_week")
        dt_classes = trends.get("downtime_classifications", [])
        shift_trends = trends.get("shift_trends", {})
        determinations = trends.get("determinations", [])

        n_runs = trends["total_runs"]
        dupes = trends.get("duplicates_removed", 0)
        total_days = int(runs["n_days"].sum()) if len(runs) > 0 else 0
        latest_oee = float(runs.iloc[-1]["avg_oee"]) if len(runs) > 0 else 0

        # --- Overview metrics ---
        st.subheader("Overview")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Runs Analyzed", n_runs)
        c2.metric("Total Days Covered", total_days)
        c3.metric("Latest OEE", f"{latest_oee:.1f}%")
        if wow:
            delta_str = f"{wow['oee_delta']:+.1f} pts"
            c4.metric("vs Previous Run", f"{latest_oee:.1f}%", delta=delta_str)
        else:
            c4.metric("Control Status", "Building..." if n_runs < 3 else "Active")

        if dupes > 0:
            st.caption(f"{dupes} duplicate run(s) removed (same date range re-analyzed)")

        # --- Determinations (the intelligence layer) ---
        if determinations:
            st.subheader("Determinations")
            st.markdown("*Auto-generated findings from SPC analysis, trend tests, and pattern detection.*")
            for finding in determinations:
                if "CHRONIC" in finding:
                    st.markdown(f"- :red[**{finding}**]")
                elif "EMERGING" in finding:
                    st.markdown(f"- :orange[**{finding}**]")
                elif "below" in finding.lower() and ("control limit" in finding.lower() or "decline" in finding.lower()):
                    st.markdown(f"- :red[{finding}]")
                elif "improving" in finding.lower() or "above" in finding.lower():
                    st.markdown(f"- :green[{finding}]")
                else:
                    st.markdown(f"- {finding}")

        # --- OEE Control Chart (SPC) ---
        if len(runs) > 0:
            st.subheader("OEE Control Chart")
            oee_chart = runs[["date_from", "avg_oee"]].copy()
            oee_chart = oee_chart.rename(columns={"date_from": "Period", "avg_oee": "OEE %"})
            oee_chart = oee_chart.set_index("Period")

            if spc:
                oee_chart["Mean"] = spc["mean"]
                oee_chart["UCL (+3σ)"] = spc["ucl"]
                oee_chart["LCL (-3σ)"] = spc["lcl"]

            if n_runs >= 7:
                oee_chart["7-Run Avg"] = oee_chart["OEE %"].rolling(7, min_periods=1).mean()

            st.line_chart(oee_chart)

            if spc:
                st.caption(
                    f"Control limits: UCL={spc['ucl']:.1f}% | Mean={spc['mean']:.1f}% | "
                    f"LCL={spc['lcl']:.1f}% | σ={spc['sigma']:.2f}")

        # --- A / P / Q Breakdown ---
        if n_runs > 1 and len(runs) > 1:
            st.subheader("Availability / Performance / Quality")
            apq_chart = runs[["date_from", "avg_availability", "avg_performance", "avg_quality"]].copy()
            apq_chart = apq_chart.rename(columns={
                "date_from": "Period",
                "avg_availability": "Availability %",
                "avg_performance": "Performance %",
                "avg_quality": "Quality %",
            })
            apq_chart = apq_chart.set_index("Period")
            st.line_chart(apq_chart)

        # --- Shift Trends ---
        if shift_trends:
            st.subheader("Shift Trends")
            shift_rows = []
            for sname, sdata in shift_trends.items():
                icon = {"improving": "+", "declining": "-", "stable": "="}
                shift_rows.append({
                    "Shift": sname,
                    "Current OEE": f"{sdata['current_oee']:.1f}%",
                    "4-Run Avg": f"{sdata['4run_avg']:.1f}%",
                    "Direction": sdata["direction"].title(),
                    "Below Plant Mean": f"{sdata['runs_below_plant_mean']}/{sdata['total_runs']} runs",
                })
            st.dataframe(pd.DataFrame(shift_rows), use_container_width=True, hide_index=True)

            # Shift OEE over time chart
            if len(shifts) > 0 and n_runs > 1:
                shift_pivot = shifts.pivot_table(
                    index="date_from", columns="shift", values="oee_pct", aggfunc="first"
                )
                shift_pivot.index.name = "Period"
                st.line_chart(shift_pivot)

        # --- Downtime Intelligence ---
        if dt_classes:
            st.subheader("Downtime Intelligence")
            dt_display = []
            for d in dt_classes[:10]:
                status_label = d["status"].upper()
                dt_display.append({
                    "Cause": d["cause"],
                    "Status": status_label,
                    "Appearances": f"{d['appearances']}/{n_runs} runs",
                    "Streak": f"{d['current_streak']} consecutive",
                    "Total Minutes": f"{d['total_minutes']:,.0f}",
                    "Times #1": d["times_rank1"],
                })
            st.dataframe(pd.DataFrame(dt_display), use_container_width=True, hide_index=True)

            # Bar chart of top causes
            if len(downtime_hist) > 0:
                agg_dt = (
                    downtime_hist.groupby("cause")["minutes"]
                    .sum().sort_values(ascending=False).head(7)
                )
                agg_dt.index.name = "Cause"
                st.bar_chart(agg_dt)

        # --- Run Log ---
        st.subheader("Run Log")
        if len(runs) > 0:
            display_runs = runs[["run_id", "date_from", "date_to", "n_days",
                                 "avg_oee", "avg_cph", "total_cases", "cases_lost"]].copy()
            display_runs.columns = ["Run", "From", "To", "Days", "OEE %", "CPH", "Cases", "Cases Lost"]
            display_runs["Run"] = display_runs["Run"].astype(str).str[:19]
            st.dataframe(display_runs, use_container_width=True, hide_index=True)

# --- Footer ---
st.markdown("---")
st.caption("Built by Brian Crusoe | Numbers from the machine, not opinions")
