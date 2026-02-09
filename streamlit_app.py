"""
Traksys OEE Analyzer — Web Interface
=====================================
Upload your Traksys OEE export, get back a formatted analysis workbook.

Usage:
  streamlit run streamlit_app.py
"""

import streamlit as st
import tempfile
import os
from datetime import datetime

from analyze import load_oee_data, load_downtime_data, analyze, write_excel

st.set_page_config(
    page_title="Traksys OEE Analyzer",
    page_icon="📊",
    layout="wide",
)

st.title("Traksys OEE Analyzer")
st.markdown("Upload your OEE export. Get back a formatted analysis workbook with shift deep dives, loss breakdowns, and prioritized actions.")

# --- File uploads ---
col1, col2 = st.columns(2)

with col1:
    oee_file = st.file_uploader(
        "OEE Export (Excel)",
        type=["xlsx", "xls"],
        help="Traksys OEE export with DayShiftHour, DayShift_Summary, Shift_Summary, ShiftHour_Summary sheets",
    )

with col2:
    downtime_file = st.file_uploader(
        "Downtime Knowledge Base (JSON) — optional",
        type=["json"],
        help="JSON with downtime_reason_codes, pareto_top_10, etc.",
    )

# --- Analyze ---
if oee_file is not None:
    if st.button("Analyze", type="primary", use_container_width=True):
        with st.spinner("Running analysis..."):
            # Write uploaded files to temp directory
            tmp_dir = tempfile.mkdtemp()
            oee_path = os.path.join(tmp_dir, oee_file.name)
            with open(oee_path, "wb") as f:
                f.write(oee_file.getbuffer())

            downtime = None
            if downtime_file is not None:
                dt_path = os.path.join(tmp_dir, downtime_file.name)
                with open(dt_path, "wb") as f:
                    f.write(downtime_file.getbuffer())
                try:
                    downtime = load_downtime_data(dt_path)
                except Exception as e:
                    st.warning(f"Could not load downtime data: {e}")

            # Run analysis
            try:
                hourly, shift_summary, overall, hour_avg = load_oee_data(oee_path)
                results = analyze(hourly, shift_summary, overall, hour_avg, downtime)

                # Write output
                basename = os.path.splitext(oee_file.name)[0]
                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                suffix = "_FULL_ANALYSIS" if downtime else "_ANALYSIS"
                output_name = f"{basename}{suffix}_{timestamp}.xlsx"
                output_path = os.path.join(tmp_dir, output_name)
                write_excel(results, output_path)

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
else:
    st.info("Upload a Traksys OEE export (.xlsx) to get started.")

# --- Footer ---
st.markdown("---")
st.caption("Built by Brian Crusoe | Numbers from the machine, not opinions")
