"""
Traksys OEE Analyzer — Web Interface
=====================================
Upload your Traksys OEE export, get back a formatted analysis workbook.

Supports both:
  - Raw Traksys exports (OEE Period Detail + Event Summary)
  - Pre-processed OEE workbooks (DayShiftHour format)

Tabs:
  - Daily Analysis: single-day analysis (original functionality)
  - Trend Analysis: multi-report SPC and trend detection

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
import numpy as np
import altair as alt

from analyze import load_oee_data, load_downtime_data, analyze, write_excel, _aggregate_oee
from parse_traksys import parse_oee_period_detail, parse_event_summary, detect_file_type
from shared import SHIFT_HOURS, load_standards_reference
from analysis_report import read_analysis_workbook
from oee_history import (
    _shewhart_limits, _nelson_rules, _trend_test,
    _classify_downtime, _analyze_shifts,
)
from operations_intelligence import (
    score_action_items,
    build_shift_handoff_packet,
    detect_trend_anomalies,
)

st.set_page_config(
    page_title="Traksys OEE Analyzer",
    page_icon="📊",
    layout="wide",
)

st.title("Traksys OEE Analyzer")
st.markdown("Upload your OEE export. Get back a formatted analysis workbook with loss breakdowns and prioritized actions.")


# ---------------------------------------------------------------------------
# Helper functions (pure Python, no Streamlit calls)
# ---------------------------------------------------------------------------
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


def _parse_pct_val(val):
    """Parse '29.5%' or '85.3%' to float."""
    s = str(val).strip().rstrip("%")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_num_val(val):
    """Parse '1,234' or '1234.5' to float."""
    s = str(val).strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _safe_float_val(val, default=0.0):
    """Safe float conversion."""
    try:
        v = float(val)
        return default if np.isnan(v) else v
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_daily, tab_trend = st.tabs(["Daily Analysis", "Trend Analysis"])


# ===================================================================
# TAB 1: Daily Analysis (original functionality)
# ===================================================================
with tab_daily:
    with st.expander("Standards reference (line/product targets)", expanded=False):
        try:
            standards_df = load_standards_reference()
            st.caption("Reference table used for dropdown/default target context (8-hour shift basis).")
            st.dataframe(standards_df, use_container_width=True, hide_index=True)
            st.download_button(
                "Download standards reference (.csv)",
                standards_df.to_csv(index=False),
                file_name="standards_reference.csv",
                mime="text/csv",
            )
        except Exception as e:
            st.warning(f"Could not load standards reference: {e}")

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
                                st.info(f"Analyzing {len(context_photos)} photo(s) with AI vision...")
                                data_shifts = list(hourly["shift"].unique())
                                photo_dt, photo_display_results = analyze_photos(
                                    context_photos, api_key, data_shifts=data_shifts)
                                # Surface any per-photo errors
                                for pname, findings in photo_display_results:
                                    if findings and "error" in findings:
                                        st.warning(f"Photo `{pname}`: {findings['error']}")
                                if photo_dt:
                                    dt_by_line.setdefault("All", []).append(photo_dt)
                                    n_issues = len(photo_dt["events_df"])
                                    st.success(f"Photo analysis: extracted {n_issues} issue(s) from {len(context_photos)} photo(s)")
                                else:
                                    st.info("Photo analysis: no equipment issues extracted from photos.")
                            else:
                                st.warning(
                                    "**Photo analysis skipped** — no OpenAI API key found. "
                                    "Set `OPENAI_API_KEY` in [Streamlit secrets](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management) "
                                    "or as an environment variable to enable AI photo analysis."
                                )
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

                        results = analyze(line_hourly, line_ss, line_overall, line_hour_avg, line_downtime,
                                          photo_findings=photo_display_results or None)

                        # Inject photo findings into shift narratives — shift-specific
                        # so each shift only sees issues the AI assigned to that shift
                        # (plus unassigned issues that could apply to any shift).
                        if photo_display_results:
                            from photo_analysis import build_photo_narrative
                            _shift_prefixes = {"1st Shift": "1st", "2nd Shift": "2nd", "3rd Shift": "3rd"}
                            for shift_key in ["1st Shift", "2nd Shift", "3rd Shift"]:
                                shift_data = results.get(shift_key)
                                if isinstance(shift_data, dict) and "narrative" in shift_data:
                                    prefix = _shift_prefixes.get(shift_key, "")
                                    photo_narrative = build_photo_narrative(
                                        photo_display_results, shift_filter=prefix)
                                    if photo_narrative:
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


# ===================================================================
# TAB 2: Trend Analysis (multi-report SPC + trends)
# ===================================================================
with tab_trend:
    st.subheader("Multi-Report Trend Analysis")
    st.markdown(
        "Upload previously generated analysis workbooks to see OEE trends, "
        "SPC signals, and chronic vs emerging downtime classifications over time."
    )

    trend_files = st.file_uploader(
        "Upload Analysis Workbooks (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=True,
        key="trend_uploader",
        help="Upload the _ANALYSIS or _FULL_ANALYSIS workbooks generated by the Daily Analysis tab",
    )

    if trend_files and len(trend_files) >= 2:
        # Parse all uploaded workbooks
        workbooks = []
        trend_tmp = tempfile.mkdtemp()
        try:
            for tf in trend_files:
                tf_path = os.path.join(trend_tmp, tf.name)
                with open(tf_path, "wb") as f:
                    f.write(tf.getbuffer())
                try:
                    wb = read_analysis_workbook(tf_path)
                    workbooks.append(wb)
                except Exception as e:
                    st.warning(f"Could not parse {tf.name}: {e}")

            if len(workbooks) < 2:
                st.warning("Need at least 2 valid workbooks for trend analysis.")
            else:
                # -------------------------------------------------------
                # Build SPC-compatible DataFrames from parsed workbooks
                # -------------------------------------------------------
                runs_rows = []
                shift_rows = []
                dt_rows = []

                for i, wb in enumerate(workbooks):
                    run_id = f"wb_{i}"

                    # Extract dates from daily_trend
                    dt_df = wb.get("daily_trend", pd.DataFrame())
                    date_from = ""
                    date_to = ""
                    if len(dt_df) > 0 and "Date" in dt_df.columns:
                        dates_parsed = pd.to_datetime(dt_df["Date"], errors="coerce").dropna()
                        if len(dates_parsed) > 0:
                            date_from = dates_parsed.min().strftime("%Y-%m-%d")
                            date_to = dates_parsed.max().strftime("%Y-%m-%d")

                    # Fall back to source filename for date if daily_trend empty
                    if not date_from:
                        date_from = f"report_{i}"
                        date_to = date_from

                    # Extract KPIs
                    kpis_lookup = {k["Metric"]: k["Value"] for k in wb.get("kpis", [])}
                    avg_oee = _parse_pct_val(kpis_lookup.get("Overall OEE", "0"))
                    avg_avail = _parse_pct_val(kpis_lookup.get("Average Availability", "0"))
                    avg_perf = _parse_pct_val(kpis_lookup.get("Average Performance", "0"))
                    avg_qual = _parse_pct_val(kpis_lookup.get("Average Quality", "0"))
                    total_cases = _parse_num_val(kpis_lookup.get("Total Cases", "0"))

                    # CPH from shift comparison average
                    sc = wb.get("shift_comparison", pd.DataFrame())
                    avg_cph = 0.0
                    if len(sc) > 0 and "CPH" in sc.columns:
                        cph_vals = pd.to_numeric(sc["CPH"], errors="coerce").dropna()
                        if len(cph_vals) > 0:
                            avg_cph = float(cph_vals.mean())

                    runs_rows.append({
                        "run_id": run_id,
                        "date_from": date_from,
                        "date_to": date_to,
                        "avg_oee": avg_oee,
                        "avg_availability": avg_avail,
                        "avg_performance": avg_perf,
                        "avg_quality": avg_qual,
                        "avg_cph": avg_cph,
                        "total_cases": total_cases,
                    })

                    # Shift comparison → one row per shift (averaged across dates)
                    if len(sc) > 0 and "Shift" in sc.columns:
                        for shift_name in sc["Shift"].unique():
                            sdf = sc[sc["Shift"] == shift_name]
                            oee_vals = pd.to_numeric(sdf.get("OEE %", pd.Series(dtype=float)), errors="coerce")
                            cph_vals = pd.to_numeric(sdf.get("CPH", pd.Series(dtype=float)), errors="coerce")
                            cases_vals = pd.to_numeric(sdf.get("Cases", pd.Series(dtype=float)), errors="coerce")

                            # Primary loss from downtime causes
                            primary_loss = ""
                            causes_df = wb.get("shift_downtime_causes", {}).get(str(shift_name), pd.DataFrame())
                            if len(causes_df) > 0 and pd.notna(causes_df.iloc[0, 0]):
                                primary_loss = str(causes_df.iloc[0, 0])

                            shift_rows.append({
                                "run_id": run_id,
                                "date_from": date_from,
                                "shift": str(shift_name),
                                "oee_pct": _safe_float_val(oee_vals.mean()),
                                "cases_per_hour": _safe_float_val(cph_vals.mean()),
                                "total_cases": _safe_float_val(cases_vals.sum()),
                                "primary_loss": primary_loss,
                            })

                    # Downtime causes → aggregate across all shifts per workbook
                    wb_causes = []
                    for sname, causes_df in wb.get("shift_downtime_causes", {}).items():
                        if len(causes_df) > 0:
                            for _, row in causes_df.iterrows():
                                cause = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                                minutes = _safe_float_val(row.iloc[1]) if len(row) > 1 else 0
                                if cause and minutes > 0:
                                    wb_causes.append({"cause": cause, "minutes": minutes})

                    # Deduplicate causes within this workbook (same cause from multiple shifts)
                    if wb_causes:
                        wb_causes_df = pd.DataFrame(wb_causes)
                        wb_agg = wb_causes_df.groupby("cause", as_index=False)["minutes"].sum()
                        total_min = wb_agg["minutes"].sum()
                        for _, row in wb_agg.iterrows():
                            pct = row["minutes"] / total_min * 100 if total_min > 0 else 0
                            dt_rows.append({
                                "run_id": run_id,
                                "date_from": date_from,
                                "cause": row["cause"],
                                "minutes": row["minutes"],
                                "pct_of_total": round(pct, 1),
                            })

                runs_df = pd.DataFrame(runs_rows)
                shifts_df = pd.DataFrame(shift_rows) if shift_rows else pd.DataFrame()
                downtime_df = pd.DataFrame(dt_rows) if dt_rows else pd.DataFrame()

                # Sort by date
                runs_df = runs_df.sort_values("date_from").reset_index(drop=True)

                # Summary header
                n_reports = len(workbooks)
                date_range = f"{runs_df['date_from'].iloc[0]} to {runs_df['date_from'].iloc[-1]}"
                st.success(f"Loaded {n_reports} reports | Date range: {date_range}")

                # Initialize for use in Section E
                dt_classes = []
                shift_trends = {}

                # -------------------------------------------------------
                # Section A: OEE Trend with SPC
                # -------------------------------------------------------
                st.markdown("### OEE Trend")

                n_runs = len(runs_df)
                latest_oee = runs_df.iloc[-1]["avg_oee"]

                if n_runs >= 3:
                    mean, ucl, lcl, sigma = _shewhart_limits(runs_df["avg_oee"])
                    trend_result = _trend_test(runs_df["avg_oee"])

                    # Parse trend direction for display
                    if trend_result and "improving" in trend_result.lower():
                        trend_dir = "Improving"
                    elif trend_result and "declining" in trend_result.lower():
                        trend_dir = "Declining"
                    else:
                        trend_dir = "Flat"

                    wow_delta = round(runs_df.iloc[-1]["avg_oee"] - runs_df.iloc[-2]["avg_oee"], 1)

                    # Metric cards
                    mcols = st.columns(4)
                    mcols[0].metric("Latest OEE", f"{latest_oee:.1f}%")
                    mcols[1].metric("Process Mean", f"{mean:.1f}%")
                    mcols[2].metric("Trend", trend_dir)
                    mcols[3].metric("Last Delta", f"{wow_delta:+.1f} pts")

                    # Altair chart: OEE line + SPC control limits
                    chart_df = runs_df[["date_from", "avg_oee"]].copy()
                    chart_df["date_from"] = pd.to_datetime(chart_df["date_from"], errors="coerce")

                    oee_line = alt.Chart(chart_df).mark_line(
                        point=alt.OverlayMarkDef(size=60), color="#1B2A4A"
                    ).encode(
                        x=alt.X("date_from:T", title="Date"),
                        y=alt.Y("avg_oee:Q", title="OEE %",
                                scale=alt.Scale(domain=[
                                    max(0, lcl - 5),
                                    min(100, ucl + 5)
                                ])),
                        tooltip=[
                            alt.Tooltip("date_from:T", title="Date"),
                            alt.Tooltip("avg_oee:Q", title="OEE %", format=".1f"),
                        ],
                    )

                    # Reference lines for mean, UCL, LCL
                    rules_df = pd.DataFrame([
                        {"y": mean, "label": f"Mean: {mean:.1f}%", "color": "green"},
                        {"y": ucl, "label": f"UCL: {ucl:.1f}%", "color": "red"},
                        {"y": lcl, "label": f"LCL: {lcl:.1f}%", "color": "red"},
                    ])
                    mean_line = alt.Chart(pd.DataFrame({"y": [mean]})).mark_rule(
                        color="green", strokeWidth=1.5
                    ).encode(y="y:Q")
                    ucl_line = alt.Chart(pd.DataFrame({"y": [ucl]})).mark_rule(
                        color="red", strokeDash=[5, 5], strokeWidth=1
                    ).encode(y="y:Q")
                    lcl_line = alt.Chart(pd.DataFrame({"y": [lcl]})).mark_rule(
                        color="red", strokeDash=[5, 5], strokeWidth=1
                    ).encode(y="y:Q")

                    spc_chart = (oee_line + mean_line + ucl_line + lcl_line).properties(
                        height=350
                    )
                    st.altair_chart(spc_chart, use_container_width=True)

                    st.caption(
                        f"Green line = process mean ({mean:.1f}%). "
                        f"Red dashed = control limits (UCL {ucl:.1f}%, LCL {lcl:.1f}%). "
                        f"Sigma = {sigma:.2f}"
                    )
                else:
                    # Fewer than 3 points — basic trend only
                    st.info("Need at least 3 reports for SPC analysis. Showing basic trend.")
                    mcols = st.columns(2)
                    mcols[0].metric("Latest OEE", f"{latest_oee:.1f}%")
                    if n_runs >= 2:
                        delta = round(runs_df.iloc[-1]["avg_oee"] - runs_df.iloc[-2]["avg_oee"], 1)
                        mcols[1].metric("Last Delta", f"{delta:+.1f} pts")

                    chart_df = runs_df[["date_from", "avg_oee"]].copy()
                    chart_df["date_from"] = pd.to_datetime(chart_df["date_from"], errors="coerce")
                    basic_chart = alt.Chart(chart_df).mark_line(
                        point=True, color="#1B2A4A"
                    ).encode(
                        x=alt.X("date_from:T", title="Date"),
                        y=alt.Y("avg_oee:Q", title="OEE %"),
                    ).properties(height=300)
                    st.altair_chart(basic_chart, use_container_width=True)

                st.divider()

                # -------------------------------------------------------
                # Section B: Shift Performance Trends
                # -------------------------------------------------------
                if len(shifts_df) > 0:
                    st.markdown("### Shift Performance")

                    plant_mean = float(runs_df["avg_oee"].mean())
                    shift_trends = _analyze_shifts(runs_df, shifts_df, plant_mean)

                    # Shift metric cards
                    if shift_trends:
                        scols = st.columns(len(shift_trends))
                        for idx, (sname, sdata) in enumerate(shift_trends.items()):
                            delta_text = f"{sdata['direction']} | 4-run avg {sdata['4run_avg']:.1f}%"
                            scols[idx].metric(sname, f"{sdata['current_oee']:.1f}%", delta=delta_text)

                    # Multi-line Altair chart: per-shift OEE over time
                    shifts_chart = shifts_df.merge(
                        runs_df[["run_id", "date_from"]], on="run_id"
                    )
                    shifts_chart["date_from"] = pd.to_datetime(shifts_chart["date_from"], errors="coerce")

                    shift_colors = ["#1B2A4A", "#E74C3C", "#3498DB"]
                    shift_line = alt.Chart(shifts_chart).mark_line(point=True).encode(
                        x=alt.X("date_from:T", title="Date"),
                        y=alt.Y("oee_pct:Q", title="OEE %", scale=alt.Scale(zero=False)),
                        color=alt.Color("shift:N", title="Shift",
                                        scale=alt.Scale(range=shift_colors)),
                        tooltip=[
                            alt.Tooltip("date_from:T", title="Date"),
                            alt.Tooltip("shift:N", title="Shift"),
                            alt.Tooltip("oee_pct:Q", title="OEE %", format=".1f"),
                        ],
                    ).properties(height=300)

                    # Plant mean reference line
                    plant_mean_line = alt.Chart(pd.DataFrame({"y": [plant_mean]})).mark_rule(
                        color="gray", strokeDash=[3, 3]
                    ).encode(y="y:Q")

                    st.altair_chart(shift_line + plant_mean_line, use_container_width=True)
                    st.caption(f"Gray dashed line = plant mean ({plant_mean:.1f}%)")

                    # Flag underperformers
                    for sname, sdata in shift_trends.items():
                        if sdata["runs_below_plant_mean"] >= sdata["total_runs"] * 0.8 and sdata["total_runs"] >= 3:
                            st.warning(
                                f"**{sname}**: below plant mean in "
                                f"{sdata['runs_below_plant_mean']}/{sdata['total_runs']} reports"
                            )
                        if sdata["direction"] == "declining" and sdata["total_runs"] >= 3:
                            st.warning(
                                f"**{sname}**: declining 3 consecutive reports "
                                f"(current {sdata['current_oee']:.1f}%, "
                                f"4-run avg {sdata['4run_avg']:.1f}%)"
                            )

                    st.divider()

                # -------------------------------------------------------
                # Section C: Downtime Pareto (aggregated + classified)
                # -------------------------------------------------------
                if len(downtime_df) > 0:
                    st.markdown("### Downtime Pareto (Aggregated)")

                    dt_classes = _classify_downtime(runs_df, downtime_df)

                    if dt_classes:
                        dt_display = pd.DataFrame(dt_classes[:10])

                        # Horizontal bar chart with classification colors
                        pareto_chart = alt.Chart(dt_display).mark_bar().encode(
                            x=alt.X("total_minutes:Q", title="Total Minutes"),
                            y=alt.Y("cause:N", sort="-x", title=""),
                            color=alt.Color(
                                "status:N",
                                title="Classification",
                                scale=alt.Scale(
                                    domain=["chronic", "emerging", "intermittent"],
                                    range=["#E74C3C", "#F39C12", "#3498DB"],
                                ),
                            ),
                            tooltip=[
                                alt.Tooltip("cause:N", title="Cause"),
                                alt.Tooltip("total_minutes:Q", title="Minutes", format=",.0f"),
                                alt.Tooltip("status:N", title="Classification"),
                                alt.Tooltip("appearances:Q", title="Reports"),
                                alt.Tooltip("pct_runs:Q", title="% of Reports"),
                            ],
                        ).properties(height=min(400, 40 * len(dt_display)))

                        st.altair_chart(pareto_chart, use_container_width=True)

                        # Detail table
                        for item in dt_classes[:10]:
                            status_upper = item["status"].upper()
                            badge = {"CHRONIC": "[CHRONIC]", "EMERGING": "[EMERGING]", "INTERMITTENT": "[INTERMITTENT]"}.get(status_upper, "")
                            st.markdown(
                                f"**{item['cause']}** -- {badge} | "
                                f"{item['total_minutes']:,.0f} min | "
                                f"In {item['appearances']}/{n_runs} reports ({item['pct_runs']:.0f}%) | "
                                f"Streak: {item['current_streak']}"
                                + (f" | Rank #1 in {item['times_rank1']} reports" if item["times_rank1"] > 0 else "")
                            )
                    else:
                        st.info("Not enough data to classify downtime patterns (need 2+ reports).")

                    st.divider()

                # -------------------------------------------------------
                # Section D: SPC Findings
                # -------------------------------------------------------
                st.markdown("### SPC Findings")

                if n_runs >= 3:
                    nelson = _nelson_rules(runs_df)
                    trend_result = _trend_test(runs_df["avg_oee"])

                    if nelson:
                        for finding in nelson:
                            low = finding.lower()
                            if "below" in low or "declining" in low or "broke" in low or "downward" in low:
                                st.warning(finding)
                            else:
                                st.info(finding)
                    else:
                        st.info("No Nelson Rules violations detected -- process is in statistical control.")

                    if trend_result:
                        st.markdown(f"**Trend test:** {trend_result}")

                    # Week-over-week
                    if n_runs >= 2:
                        latest = runs_df.iloc[-1]
                        previous = runs_df.iloc[-2]
                        oee_delta = round(latest["avg_oee"] - previous["avg_oee"], 1)
                        direction = "up" if oee_delta > 0 else "down" if oee_delta < 0 else "flat"
                        st.markdown(
                            f"**Week-over-week:** OEE {direction} {abs(oee_delta):.1f} pts "
                            f"({previous['avg_oee']:.1f}% -> {latest['avg_oee']:.1f}%)"
                        )
                else:
                    st.info("Need at least 3 reports for SPC analysis.")

                anomaly_flags = detect_trend_anomalies(runs_df, dt_classes)
                if anomaly_flags:
                    st.markdown("### Anomaly Alerts")
                    for flag in anomaly_flags:
                        st.warning(flag)

                st.divider()

                # -------------------------------------------------------
                # Section E: Action Items (synthesized from trend data)
                # -------------------------------------------------------
                st.markdown("### Action Items")

                # Build smart action items from trend-level analysis
                smart_items = []
                try:
                    from db import build_smart_action_items
                    smart_items = build_smart_action_items(
                        dt_classes, runs_df, shift_trends=shift_trends
                    )
                except Exception:
                    pass

                if smart_items:
                    smart_items = score_action_items(smart_items)
                    # Check if database is enhancing the items
                    try:
                        from db import is_connected
                        if is_connected():
                            st.caption("Equipment knowledge and baselines active")
                    except Exception:
                        pass

                    for item in smart_items:
                        score = item.get("priority_score", 0)
                        st.markdown(f"**#{item['priority']} (score {score:.1f}):** {item['finding']}")
                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;*{item['action']}*")

                    if shift_trends:
                        worst_shift = min(
                            shift_trends.items(),
                            key=lambda kv: kv[1].get("current_oee", 999),
                        )[0]
                        period_label = f"{runs_df.iloc[-1]['date_from']} to {runs_df.iloc[-1]['date_to']}"
                        handoff_txt = build_shift_handoff_packet(
                            worst_shift,
                            period_label,
                            [it.get("finding", "") for it in smart_items[:3]],
                            smart_items[:3],
                        )
                        st.download_button(
                            "Download shift handoff packet (.txt)",
                            data=handoff_txt,
                            file_name=f"{worst_shift.replace(' ', '_').lower()}_handoff.txt",
                            mime="text/plain",
                        )
                else:
                    # Fallback: deduplicated focus items from workbooks
                    all_focus = []
                    seen_findings = set()
                    for wb in workbooks:
                        fi = wb.get("focus_items", pd.DataFrame())
                        if len(fi) > 0:
                            for _, row in fi.iterrows():
                                finding = str(row.get("Finding", ""))
                                if finding and finding != "nan" and finding not in seen_findings:
                                    seen_findings.add(finding)
                                    all_focus.append(row.to_dict())

                    if all_focus:
                        all_focus.sort(key=lambda x: _safe_float_val(x.get("Priority", 99)))
                        for item in all_focus[:10]:
                            priority = item.get("Priority", "")
                            finding = str(item.get("Finding", ""))
                            if priority:
                                st.markdown(f"**#{int(_safe_float_val(priority))}:** {finding}")
                            else:
                                st.markdown(f"- {finding}")
                    else:
                        st.info("No action items found in uploaded workbooks.")

        except Exception as e:
            st.error(f"Trend analysis failed: {e}")
            st.exception(e)
        finally:
            shutil.rmtree(trend_tmp, ignore_errors=True)

    elif trend_files and len(trend_files) == 1:
        st.info("Upload at least 2 analysis workbooks to see trends. A single workbook only shows a snapshot.")
    else:
        st.info(
            "Upload 2 or more analysis workbooks (.xlsx) generated by the Daily Analysis tab "
            "to see OEE trends, SPC signals, and downtime classifications over time."
        )


# --- Footer ---
st.markdown("---")
st.caption("Built by Brian Crusoe | Numbers from the machine, not opinions")
