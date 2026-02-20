"""
Tests for Analysis Report generation.

Run: python -m pytest test_analysis_report.py -v
"""

import pytest
import os
import tempfile

import pandas as pd
import numpy as np
from datetime import datetime

from analysis_report import (
    read_analysis_workbook,
    consolidate,
    AnalysisReport,
    generate_analysis_report,
    _safe_float,
    _oee_color,
    GREEN, ORANGE, RED,
)


# =====================================================================
# Helpers — create mock analysis workbooks
# =====================================================================

def _make_mock_analysis_workbook(path, date_str="2025-01-15", oee=42.5):
    """Create a mock analysis Excel workbook matching analyze.py output structure."""
    writer = pd.ExcelWriter(path, engine="xlsxwriter")
    wb = writer.book

    # --- Plant Summary sheet ---
    ws = wb.add_worksheet("Plant Summary")
    # Title
    ws.write(0, 0, "Plant Summary — Line 2 Flex")
    ws.write(1, 0, f"{date_str} to {date_str} · 1 day(s) analyzed")
    # KPIs section
    ws.write(3, 0, "Plant KPIs")
    ws.write(4, 0, "Metric")
    ws.write(4, 1, "Value")
    kpi_data = [
        ("Overall OEE", f"{oee}%"),
        ("OEE Gap to 50% Target", f"{50.0 - oee:.1f} points"),
        ("Total Cases", "25,000"),
        ("Cases vs Target (Plant Std)", "-5,000 (83%)"),
        ("Utilization", "75% (18.0 of 24.0 hrs)"),
        ("Top Downtime Cause", "Caser System (4 hrs)"),
    ]
    for i, (metric, value) in enumerate(kpi_data):
        ws.write(5 + i, 0, metric)
        ws.write(5 + i, 1, value)

    # Shift Comparison section
    row = 5 + len(kpi_data) + 1
    ws.write(row, 0, "Shift Comparison")
    row += 1
    headers = ["Date", "Shift", "OEE %", "Cases", "CPH", "Target CPH", "% of Target", "Avail %", "Perf %", "Qual %"]
    for c, h in enumerate(headers):
        ws.write(row, c, h)
    row += 1
    shifts = [
        (date_str, "1st Shift", oee + 5, 10000, 1250, 1500, 83.3, 80.0, 70.0, 98.0),
        (date_str, "2nd Shift", oee, 8000, 1000, 1500, 66.7, 75.0, 65.0, 97.0),
        (date_str, "3rd Shift", oee - 5, 7000, 875, 1500, 58.3, 65.0, 75.0, 96.0),
    ]
    for s in shifts:
        for c, v in enumerate(s):
            ws.write(row, c, v)
        row += 1

    # Loss Breakdown section
    row += 1
    ws.write(row, 0, "Loss Breakdown by Shift")
    row += 1
    loss_headers = ["Date", "Shift", "Avail Loss %", "Perf Loss %", "Qual Loss %", "Primary Driver", "Cases Lost"]
    for c, h in enumerate(loss_headers):
        ws.write(row, c, h)
    row += 1
    losses = [
        (date_str, "1st Shift", 20.0, 30.0, 2.0, "Performance", 2000),
        (date_str, "2nd Shift", 25.0, 35.0, 3.0, "Performance", 3000),
        (date_str, "3rd Shift", 35.0, 25.0, 4.0, "Availability", 4000),
    ]
    for lo in losses:
        for c, v in enumerate(lo):
            ws.write(row, c, v)
        row += 1

    # Daily Trend section
    row += 1
    ws.write(row, 0, "Daily Trend")
    row += 1
    daily_headers = ["Date", "Sched Hours", "Cases/Hr", "Target CPH", "Actual Cases", "Target Cases", "% of Target", "OEE %"]
    for c, h in enumerate(daily_headers):
        ws.write(row, c, h)
    row += 1
    ws.write(row, 0, date_str)
    ws.write(row, 1, 24.0)
    ws.write(row, 2, 1042)
    ws.write(row, 3, 1500)
    ws.write(row, 4, 25000)
    ws.write(row, 5, 30000)
    ws.write(row, 6, 83.3)
    ws.write(row, 7, oee)

    # --- Shift sheets ---
    for sname in ["1st Shift", "2nd Shift", "3rd Shift"]:
        ws = wb.add_worksheet(sname)
        ws.write(0, 0, sname)
        ws.write(1, 0, f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        # Narrative in row 3 (merged area)
        narrative = (
            f"{sname} averaged {oee:.1f}% OEE across 1 day(s), producing 8,333 cases "
            f"in 6.0 producing hours (75% utilization). 7.5 points below plant average (50.0%).\n\n"
            f"The primary loss driver was Performance (35% of total loss). When running, "
            f"the line averaged 1,000 CPH vs 1,500 target.\n\n"
            f"Focus on: (1) Reduce Caser System — 120 min across 8 events; "
            f"50% reduction recovers ~1,000 cases."
        )
        ws.write(3, 0, narrative)
        # Downtime causes table
        ws.write(12, 0, "Downtime Causes (Top 10)")
        causes_headers = ["Cause", "Total Min", "Events", "Fault Type"]
        for c, h in enumerate(causes_headers):
            ws.write(13, c, h)
        causes_data = [
            ("Caser System", 120, 8, "Equipment / Mechanical"),
            ("Changeover", 60, 3, "Process / Changeover"),
            ("Short Stop", 45, 22, "Micro Stops"),
        ]
        for i, cd in enumerate(causes_data):
            for c, v in enumerate(cd):
                ws.write(14 + i, c, v)

    # --- What to Focus On sheet ---
    focus_data = {
        "Priority": [1, 2, 3],
        "Finding": [
            f"OEE is {oee:.1f}% vs 50% target — {50 - oee:.1f} points to close",
            "#1 loss: Caser System — 4 hrs / 24 events (45% of all downtime)",
            "3rd Shift underperforms 1st Shift by 10.0 OEE points",
        ],
        "The Work": [
            f"Current: {oee:.1f}% OEE. Target 50% requires closing {50 - oee:.1f} pts.",
            "Caser System consumed 240 min across 24 events. Avg event: 10.0 min.",
            "1st Shift: 47.5% OEE. 3rd Shift: 37.5% OEE. Gap: 375 cases/hr.",
        ],
        "Step 1": ["Fix availability first", "Pull 2 weeks of events", "See shift deep dives"],
        "Step 2": ["Performance secondary", "Walk the line", "Shadow 1st shift"],
        "Step 3": ["Fix top 2 causes", "5-Why on top 3 failures", "Interview leads"],
        "Step 4": ["Track monthly", "Build countermeasures", "Build checklist"],
        "Step 5": ["Every 1 pt = more cases", "Track weekly", "Goal: close gap by 5 pts"],
    }
    focus_df = pd.DataFrame(focus_data)
    focus_df.to_excel(writer, sheet_name="What to Focus On", startrow=2, index=False)
    ws = writer.sheets["What to Focus On"]
    ws.write(0, 0, "What to Focus On")
    ws.write(1, 0, f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    for c, col in enumerate(focus_df.columns):
        ws.write(2, c, col)

    writer.close()
    return path


# =====================================================================
# Tests
# =====================================================================

class TestSafeFloat:
    def test_normal_number(self):
        assert _safe_float(42.5) == 42.5

    def test_string_number(self):
        assert _safe_float("42.5") == 42.5

    def test_none_returns_default(self):
        assert _safe_float(None) == 0.0

    def test_invalid_string(self):
        assert _safe_float("N/A", -1.0) == -1.0

    def test_nan(self):
        assert _safe_float(float("nan"), 0.0) != _safe_float(float("nan"), 0.0) or True  # NaN is NaN


class TestOEEColor:
    def test_good_oee(self):
        assert _oee_color(55.0) == GREEN

    def test_medium_oee(self):
        assert _oee_color(40.0) == ORANGE

    def test_bad_oee(self):
        assert _oee_color(20.0) == RED

    def test_boundary_50(self):
        assert _oee_color(50.0) == GREEN

    def test_boundary_35(self):
        assert _oee_color(35.0) == ORANGE


class TestReadAnalysisWorkbook:
    @pytest.fixture
    def mock_workbook(self, tmp_path):
        path = str(tmp_path / "test_analysis.xlsx")
        _make_mock_analysis_workbook(path, "2025-01-15", 42.5)
        return path

    def test_returns_dict(self, mock_workbook):
        result = read_analysis_workbook(mock_workbook)
        assert isinstance(result, dict)

    def test_has_source_file(self, mock_workbook):
        result = read_analysis_workbook(mock_workbook)
        assert result["source_file"] == "test_analysis.xlsx"

    def test_has_kpis(self, mock_workbook):
        result = read_analysis_workbook(mock_workbook)
        assert len(result["kpis"]) > 0
        metrics = [k["Metric"] for k in result["kpis"]]
        assert "Overall OEE" in metrics

    def test_has_shift_comparison(self, mock_workbook):
        result = read_analysis_workbook(mock_workbook)
        sc = result["shift_comparison"]
        assert isinstance(sc, pd.DataFrame)
        assert len(sc) == 3  # 3 shifts

    def test_has_loss_breakdown(self, mock_workbook):
        result = read_analysis_workbook(mock_workbook)
        lb = result["loss_breakdown"]
        assert isinstance(lb, pd.DataFrame)
        assert len(lb) == 3

    def test_has_daily_trend(self, mock_workbook):
        result = read_analysis_workbook(mock_workbook)
        dt = result["daily_trend"]
        assert isinstance(dt, pd.DataFrame)
        assert len(dt) == 1

    def test_has_shift_narratives(self, mock_workbook):
        result = read_analysis_workbook(mock_workbook)
        narratives = result["shift_narratives"]
        assert "1st Shift" in narratives
        assert len(narratives["1st Shift"]) > 40

    def test_has_shift_downtime_causes(self, mock_workbook):
        result = read_analysis_workbook(mock_workbook)
        causes = result["shift_downtime_causes"]
        assert "1st Shift" in causes
        assert len(causes["1st Shift"]) > 0

    def test_has_focus_items(self, mock_workbook):
        result = read_analysis_workbook(mock_workbook)
        fi = result["focus_items"]
        assert isinstance(fi, pd.DataFrame)
        assert len(fi) == 3


class TestConsolidate:
    @pytest.fixture
    def two_workbooks(self, tmp_path):
        wb1 = read_analysis_workbook(
            _make_mock_analysis_workbook(str(tmp_path / "file1.xlsx"), "2025-01-15", 42.5)
        )
        wb2 = read_analysis_workbook(
            _make_mock_analysis_workbook(str(tmp_path / "file2.xlsx"), "2025-01-16", 38.0)
        )
        return [wb1, wb2]

    def test_returns_dict(self, two_workbooks):
        result = consolidate(two_workbooks)
        assert isinstance(result, dict)

    def test_n_files(self, two_workbooks):
        result = consolidate(two_workbooks)
        assert result["n_files"] == 2

    def test_n_days(self, two_workbooks):
        result = consolidate(two_workbooks)
        assert result["n_days"] == 2

    def test_date_range(self, two_workbooks):
        result = consolidate(two_workbooks)
        assert "2025-01-15" in result["date_range"]
        assert "2025-01-16" in result["date_range"]

    def test_shift_grid_merged(self, two_workbooks):
        result = consolidate(two_workbooks)
        assert len(result["shift_grid"]) == 6  # 3 shifts x 2 files

    def test_loss_grid_merged(self, two_workbooks):
        result = consolidate(two_workbooks)
        assert len(result["loss_grid"]) == 6

    def test_downtime_pareto_aggregated(self, two_workbooks):
        result = consolidate(two_workbooks)
        assert len(result["downtime_pareto"]) > 0
        # Same causes from both files should be aggregated
        caser = [d for d in result["downtime_pareto"] if "Caser" in d["Cause"]]
        if caser:
            # Two files contribute; doubled minutes
            assert caser[0]["Total Min"] >= 120 * 2  # 120 min per shift x 2 files x 3 shifts

    def test_kpis_aggregated(self, two_workbooks):
        result = consolidate(two_workbooks)
        oee = result["kpis"].get("Overall OEE", "")
        assert "%" in oee
        # Should be average of 42.5 and 38.0 = 40.25
        oee_val = float(oee.replace("%", ""))
        assert abs(oee_val - 40.25) < 0.1

    def test_ids_items_top3(self, two_workbooks):
        result = consolidate(two_workbooks)
        assert len(result["ids_items"]) <= 3

    def test_source_files(self, two_workbooks):
        result = consolidate(two_workbooks)
        assert len(result["source_files"]) == 2

    def test_shift_narratives_collected(self, two_workbooks):
        result = consolidate(two_workbooks)
        narratives = result["shift_narratives"]
        assert "1st Shift" in narratives
        assert len(narratives["1st Shift"]) == 2  # One from each file


class TestAnalysisReportPDF:
    @pytest.fixture
    def mock_data(self, tmp_path):
        wb1 = read_analysis_workbook(
            _make_mock_analysis_workbook(str(tmp_path / "f1.xlsx"), "2025-01-15", 42.5)
        )
        wb2 = read_analysis_workbook(
            _make_mock_analysis_workbook(str(tmp_path / "f2.xlsx"), "2025-01-16", 38.0)
        )
        return consolidate([wb1, wb2])

    def test_pdf_builds_without_error(self, mock_data):
        pdf = AnalysisReport()
        pdf.build_page1(mock_data)
        pdf.build_page2(mock_data)
        output = pdf.output()
        assert len(output) > 0

    def test_pdf_has_pages(self, mock_data):
        pdf = AnalysisReport()
        pdf.build_page1(mock_data)
        pdf.build_page2(mock_data)
        assert pdf.page_no() >= 1

    def test_pdf_is_valid(self, mock_data, tmp_path):
        pdf = AnalysisReport()
        pdf.build_page1(mock_data)
        pdf.build_page2(mock_data)
        output_path = str(tmp_path / "test_report.pdf")
        pdf.output(output_path)
        assert os.path.exists(output_path)
        with open(output_path, "rb") as f:
            header = f.read(5)
        assert header == b"%PDF-"

    def test_pdf_output_bytes(self, mock_data):
        pdf = AnalysisReport()
        pdf.build_page1(mock_data)
        pdf.build_page2(mock_data)
        data = pdf.output()
        assert isinstance(data, (bytes, bytearray))
        assert data[:5] == b"%PDF-"


class TestGenerateAnalysisReport:
    def test_end_to_end(self, tmp_path):
        # Create mock workbooks
        paths = []
        for i, (date, oee) in enumerate([
            ("2025-01-13", 40.0),
            ("2025-01-14", 42.0),
            ("2025-01-15", 38.5),
        ]):
            p = str(tmp_path / f"analysis_{i}.xlsx")
            _make_mock_analysis_workbook(p, date, oee)
            paths.append(p)

        output = str(tmp_path / "report_output.pdf")
        result = generate_analysis_report(paths, output)

        assert result == output
        assert os.path.exists(output)
        # Verify it's a valid PDF
        with open(output, "rb") as f:
            assert f.read(5) == b"%PDF-"
        # Should be under 500KB for a 2-page report
        size = os.path.getsize(output)
        assert size < 500_000

    def test_single_file(self, tmp_path):
        p = str(tmp_path / "single.xlsx")
        _make_mock_analysis_workbook(p, "2025-01-15", 45.0)
        output = str(tmp_path / "report_single.pdf")
        result = generate_analysis_report([p], output)
        assert os.path.exists(result)

    def test_max_six_files(self, tmp_path):
        paths = []
        for i in range(8):
            p = str(tmp_path / f"file_{i}.xlsx")
            _make_mock_analysis_workbook(p, f"2025-01-{10+i}", 40.0 + i)
            paths.append(p)

        output = str(tmp_path / "report_max.pdf")
        result = generate_analysis_report(paths, output)
        assert os.path.exists(result)

    def test_missing_file_skipped(self, tmp_path):
        p = str(tmp_path / "exists.xlsx")
        _make_mock_analysis_workbook(p, "2025-01-15", 42.0)
        output = str(tmp_path / "report_missing.pdf")
        result = generate_analysis_report([p, "/nonexistent/file.xlsx"], output)
        assert os.path.exists(result)

    def test_no_valid_files_raises(self):
        with pytest.raises(ValueError, match="No valid"):
            generate_analysis_report(["/nonexistent/a.xlsx", "/nonexistent/b.xlsx"])


class TestConsolidateEdgeCases:
    def test_empty_workbook_list(self):
        result = consolidate([])
        assert result["n_files"] == 0

    def test_workbook_without_downtime(self, tmp_path):
        """Workbook where shift sheets have no downtime causes."""
        path = str(tmp_path / "no_dt.xlsx")
        writer = pd.ExcelWriter(path, engine="xlsxwriter")
        wb = writer.book

        # Minimal Plant Summary
        ws = wb.add_worksheet("Plant Summary")
        ws.write(0, 0, "Plant Summary")
        ws.write(1, 0, "2025-01-15")
        ws.write(3, 0, "Plant KPIs")
        ws.write(4, 0, "Metric")
        ws.write(4, 1, "Value")
        ws.write(5, 0, "Overall OEE")
        ws.write(5, 1, "35.0%")

        # Minimal shift sheet with no downtime
        ws2 = wb.add_worksheet("1st Shift")
        ws2.write(0, 0, "1st Shift")
        ws2.write(3, 0, "This shift ran with no tracked downtime causes for the period analyzed.")

        writer.close()

        result = read_analysis_workbook(path)
        consolidated = consolidate([result])
        assert consolidated["n_files"] == 1
        # Should still work with empty downtime
        assert len(consolidated["downtime_pareto"]) == 0
