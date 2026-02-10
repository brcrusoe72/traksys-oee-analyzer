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
from third_shift_report import (
    load_data as load_3rd_data,
    build_report as build_3rd_report,
    write_report as write_3rd_report,
)
from third_shift_targets import (
    load_product_data,
    load_downtime_pareto,
    aggregate_daily,
    build_sheet1,
    build_sheet2,
    write_report as write_targets_report,
    build_sendable,
)
from shared import PRODUCT_TARGET

st.set_page_config(
    page_title="Traksys OEE Analyzer",
    page_icon="📊",
    layout="wide",
)

st.title("Traksys OEE Analyzer")
st.markdown("Upload your OEE export. Get back a formatted analysis workbook with shift deep dives, loss breakdowns, and prioritized actions.")

# --- Tab navigation ---
tab_analyze, tab_3rd_shift, tab_history = st.tabs(["Analyze", "3rd Shift Report", "Plant History"])

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
            help="Traksys 'Event Summary' export (.xlsx) or knowledge base (.json)",
        )

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
                                else:
                                    st.warning("Unrecognized downtime file format")
                        except Exception as e:
                            st.warning(f"Could not load downtime data: {e}")

                    # Run analysis
                    results = analyze(hourly, shift_summary, overall, hour_avg, downtime)

                    # Write output
                    basename = os.path.splitext(oee_file.name)[0]
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                    suffix = "_FULL_ANALYSIS" if downtime else "_ANALYSIS"
                    output_name = f"{basename}{suffix}_{timestamp}.xlsx"
                    output_path = os.path.join(tmp_dir, output_name)
                    write_excel(results, output_path)

                    # Save to history
                    try:
                        save_run(results, hourly, shift_summary, overall, downtime)
                    except Exception:
                        pass  # history save should never block the main workflow

                    # Read back for download
                    with open(output_path, "rb") as f:
                        output_bytes = f.read()

                    st.success(f"Analysis complete — {len(results)} sheets generated")

                    # Download button
                    st.download_button(
                        label=f"Download {output_name}",
                        data=output_bytes,
                        file_name=output_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )

                    # --- Quick summary ---
                    st.markdown("---")
                    st.subheader("Quick Summary")

                    exec_df = results.get("Executive Summary")
                    if exec_df is not None:
                        metrics = exec_df[exec_df["Metric"].astype(str).str.strip() != ""]
                        cols = st.columns(min(4, len(metrics)))
                        for i, (_, row) in enumerate(metrics.iterrows()):
                            if i < len(cols):
                                cols[i % len(cols)].metric(str(row["Metric"]), str(row["Value"]))

                    # Fault summary
                    fault_df = results.get("Fault Summary")
                    if fault_df is not None:
                        st.subheader("Fault Classification")
                        st.dataframe(
                            fault_df[["Fault Category", "Total Hours", "% of All Downtime", "Who Owns This"]],
                            use_container_width=True,
                            hide_index=True,
                        )

                    # Downtime Pareto
                    pareto_df = results.get("Downtime Pareto")
                    if pareto_df is not None:
                        st.subheader("Top Downtime Causes")
                        display_cols = [c for c in ["Cause", "Fault Type", "Total Minutes", "Events", "% of Total", "Cumulative %"] if c in pareto_df.columns]
                        st.dataframe(
                            pareto_df[display_cols].head(10),
                            use_container_width=True,
                            hide_index=True,
                        )

                    # Top actions
                    focus_df = results.get("What to Focus On")
                    if focus_df is not None:
                        st.subheader("Top Actions")
                        for _, row in focus_df.head(5).iterrows():
                            st.markdown(f"**#{row['Priority']}:** {row['Finding']}")
                            st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;Step 1: {row['Step 1']}")

                    # Sheet list
                    st.markdown("---")
                    st.caption(f"Sheets in output: {', '.join(results.keys())}")

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
# TAB 2: 3RD SHIFT REPORT
# =====================================================================
with tab_3rd_shift:
    st.markdown("Generate 3rd shift deep-dive reports from your OEE and product data.")

    # --- Report type selector ---
    report_type = st.radio(
        "Report type",
        ["3rd Shift Analysis", "Target Tracker"],
        horizontal=True,
        help="**3rd Shift Analysis**: Full shift deep-dive from OEE export. "
             "**Target Tracker**: Daily target tracking + email text from product JSON.",
    )

    if report_type == "3rd Shift Analysis":
        # --- 3rd Shift Analysis: needs OEE file (required), downtime + product JSON (optional) ---
        st.markdown("Upload OEE export (required). Optionally add downtime KB and product data for a richer report.")
        c1, c2, c3 = st.columns(3)
        with c1:
            tsr_oee_file = st.file_uploader(
                "OEE Data (Excel)",
                type=["xlsx", "xls"],
                key="tsr_oee",
                help="Same Traksys OEE export used in the Analyze tab",
            )
        with c2:
            tsr_dt_file = st.file_uploader(
                "Downtime KB (JSON) — optional",
                type=["json"],
                key="tsr_dt",
                help="rochelle_production_knowledge_base.json",
            )
        with c3:
            tsr_prod_file = st.file_uploader(
                "Product Data (JSON) — optional",
                type=["json"],
                key="tsr_prod",
                help="rochelle_product_data.json",
            )

        if tsr_oee_file is not None:
            if st.button("Generate 3rd Shift Analysis", type="primary", use_container_width=True):
                with st.spinner("Building 3rd shift report..."):
                    tmp_dir = tempfile.mkdtemp()

                    # Write OEE file
                    oee_path = os.path.join(tmp_dir, tsr_oee_file.name)
                    with open(oee_path, "wb") as f:
                        f.write(tsr_oee_file.getbuffer())

                    # Write optional files
                    dt_path = None
                    if tsr_dt_file is not None:
                        dt_path = os.path.join(tmp_dir, tsr_dt_file.name)
                        with open(dt_path, "wb") as f:
                            f.write(tsr_dt_file.getbuffer())

                    prod_path = None
                    if tsr_prod_file is not None:
                        prod_path = os.path.join(tmp_dir, tsr_prod_file.name)
                        with open(prod_path, "wb") as f:
                            f.write(tsr_prod_file.getbuffer())

                    try:
                        hourly, shift_summary, overall, hour_avg, downtime, product_data = load_3rd_data(
                            oee_path, dt_path, prod_path)
                        sheets = build_3rd_report(hourly, shift_summary, overall, hour_avg, downtime, product_data)

                        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                        output_name = f"Line2_3rd_Shift_Analysis_{timestamp}.xlsx"
                        output_path = os.path.join(tmp_dir, output_name)
                        write_3rd_report(sheets, output_path)

                        with open(output_path, "rb") as f:
                            output_bytes = f.read()

                        st.success(f"3rd Shift Analysis complete — {len(sheets)} sheets generated")
                        st.download_button(
                            label=f"Download {output_name}",
                            data=output_bytes,
                            file_name=output_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                        )

                        # --- Quick preview: Scorecard ---
                        st.markdown("---")
                        st.subheader("Scorecard Preview")
                        sc = sheets.get("Scorecard")
                        if sc is not None:
                            display_cols = [c for c in sc.columns if c != ""]
                            key_rows = sc[sc["Metric"].astype(str).str.strip() != ""]
                            # Show key metrics as cards
                            metric_names = ["OEE %", "Cases per Hour", "Availability %", "Performance %", "Quality %"]
                            found = key_rows[key_rows["Metric"].isin(metric_names)]
                            if len(found) > 0:
                                mcols = st.columns(min(5, len(found)))
                                for i, (_, row) in enumerate(found.iterrows()):
                                    if i < len(mcols):
                                        val = row.get("3rd Shift", "")
                                        gap = row.get("Gap vs 2nd", "")
                                        mcols[i].metric(
                                            str(row["Metric"]),
                                            str(val),
                                            delta=str(gap) if gap else None,
                                        )

                        # --- Quick preview: Recommended Actions top 3 ---
                        actions_df = sheets.get("Recommended Actions")
                        if actions_df is not None and len(actions_df) > 0:
                            st.subheader("Top Recommended Actions")
                            for _, row in actions_df.head(3).iterrows():
                                st.markdown(f"**#{row['Priority']} [{row['Area']}]:** {row['Problem']}")
                                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;{row['Step 1']}")

                        st.markdown("---")
                        st.caption(f"Sheets: {', '.join(sheets.keys())}")

                    except Exception as e:
                        st.error(f"3rd Shift Analysis failed: {e}")
                        st.exception(e)
                    finally:
                        shutil.rmtree(tmp_dir, ignore_errors=True)
        else:
            st.info("Upload a Traksys OEE export (.xlsx) to generate the 3rd shift analysis.")

    else:  # Target Tracker
        # --- Target Tracker: needs Product JSON (required), downtime KB (optional) ---
        st.markdown("Upload product data (required). Optionally add downtime KB for machine-data Pareto.")
        c1, c2 = st.columns(2)
        with c1:
            tt_prod_file = st.file_uploader(
                "Product Data (JSON)",
                type=["json"],
                key="tt_prod",
                help="rochelle_product_data.json — contains 3rd shift product runs",
            )
        with c2:
            tt_dt_file = st.file_uploader(
                "Downtime KB (JSON) — optional",
                type=["json"],
                key="tt_dt",
                help="rochelle_production_knowledge_base.json",
            )

        if tt_prod_file is not None:
            if st.button("Generate Target Tracker", type="primary", use_container_width=True):
                with st.spinner("Building target tracker..."):
                    tmp_dir = tempfile.mkdtemp()

                    prod_path = os.path.join(tmp_dir, tt_prod_file.name)
                    with open(prod_path, "wb") as f:
                        f.write(tt_prod_file.getbuffer())

                    dt_path = None
                    if tt_dt_file is not None:
                        dt_path = os.path.join(tmp_dir, tt_dt_file.name)
                        with open(dt_path, "wb") as f:
                            f.write(tt_dt_file.getbuffer())

                    try:
                        runs_data, meta = load_product_data(prod_path)
                        daily = aggregate_daily(runs_data)

                        reason_codes, pareto, oee_summary = None, None, None
                        if dt_path:
                            reason_codes, pareto, oee_summary = load_downtime_pareto(dt_path)

                        sheet1 = build_sheet1(daily)
                        sheet2 = build_sheet2(daily, runs_data, reason_codes, pareto, oee_summary)

                        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                        output_name = f"3rd_Shift_Targets_{timestamp}.xlsx"
                        output_path = os.path.join(tmp_dir, output_name)
                        write_targets_report(sheet1, sheet2, output_path)

                        with open(output_path, "rb") as f:
                            output_bytes = f.read()

                        st.success("Target Tracker generated")
                        st.download_button(
                            label=f"Download {output_name}",
                            data=output_bytes,
                            file_name=output_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                        )

                        # --- Quick preview: Hit rate ---
                        st.markdown("---")
                        st.subheader("Quick Summary")

                        target_runs = daily[daily["product_family"].apply(
                            lambda x: PRODUCT_TARGET.get(x, 0) > 0)]
                        if len(target_runs) > 0:
                            hits = sum(
                                1 for _, r in target_runs.iterrows()
                                if pd.notna(r["total_cases"])
                                and r["total_cases"] >= PRODUCT_TARGET.get(r["product_family"], 0)
                            )
                            hit_rate = hits / len(target_runs) * 100
                            avg_oee = daily["avg_oee"].mean()
                            mc1, mc2, mc3 = st.columns(3)
                            mc1.metric("Hit Rate", f"{hits}/{len(target_runs)} ({hit_rate:.0f}%)")
                            mc2.metric("Avg OEE", f"{avg_oee:.1f}%")
                            mc3.metric("Production Days", len(daily))

                        # --- Email text preview ---
                        email_text = build_sendable(daily, runs_data, reason_codes, oee_summary)
                        st.subheader("Email Text (copy & send)")
                        st.code(email_text, language=None)

                    except Exception as e:
                        st.error(f"Target Tracker failed: {e}")
                        st.exception(e)
                    finally:
                        shutil.rmtree(tmp_dir, ignore_errors=True)
        else:
            st.info("Upload rochelle_product_data.json to generate the target tracker.")

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
