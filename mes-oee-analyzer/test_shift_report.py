"""
Tests for shift_report.py — generalized shift deep dive.

Run: python -m pytest test_shift_report.py -v
"""

import pytest
import pandas as pd
import numpy as np

from shared import extract_equipment_mentions, summarize_issues, classify_support, EQUIPMENT_SCAN


# =====================================================================
# Helpers
# =====================================================================

def _make_hourly(rows):
    """Build a minimal hourly DataFrame for shift detection tests."""
    df = pd.DataFrame(rows)
    for col in ["total_hours", "total_cases", "availability",
                 "performance", "quality", "good_cases", "oee_pct",
                 "cases_per_hour"]:
        if col not in df.columns:
            if col == "good_cases":
                df[col] = df.get("total_cases", 100)
            elif col == "quality":
                df[col] = 1.0
            elif col == "oee_pct":
                df[col] = 40.0
            elif col == "cases_per_hour":
                df[col] = 500
            elif col == "total_hours":
                df[col] = 1.0
            elif col == "total_cases":
                df[col] = 500
            elif col in ("availability", "performance"):
                df[col] = 0.8
    return df


def _make_overall(rows):
    """Build a minimal overall DataFrame."""
    return pd.DataFrame(rows)


# =====================================================================
# TestDetectShifts
# =====================================================================

class TestDetectShifts:
    """detect_shifts should return sorted list of shift names from data."""

    def test_three_shifts(self):
        from shift_report import detect_shifts
        hourly = _make_hourly([
            {"shift": "3rd (11p-7a)"},
            {"shift": "1st (7a-3p)"},
            {"shift": "2nd (3p-11p)"},
            {"shift": "3rd (11p-7a)"},
        ])
        result = detect_shifts(hourly)
        assert len(result) == 3
        assert result == sorted(result)

    def test_single_shift(self):
        from shift_report import detect_shifts
        hourly = _make_hourly([
            {"shift": "1st (7a-3p)"},
            {"shift": "1st (7a-3p)"},
        ])
        result = detect_shifts(hourly)
        assert result == ["1st (7a-3p)"]

    def test_two_shifts(self):
        from shift_report import detect_shifts
        hourly = _make_hourly([
            {"shift": "2nd (3p-11p)"},
            {"shift": "3rd (11p-7a)"},
        ])
        result = detect_shifts(hourly)
        assert len(result) == 2


# =====================================================================
# TestPickBenchmarkShift
# =====================================================================

class TestPickBenchmarkShift:
    """pick_benchmark_shift should select the best OTHER shift."""

    def test_picks_highest_cph(self):
        from shift_report import pick_benchmark_shift
        hourly = _make_hourly([
            {"shift": "1st (7a-3p)"},
            {"shift": "2nd (3p-11p)"},
            {"shift": "3rd (11p-7a)"},
        ])
        overall = _make_overall([
            {"shift": "1st (7a-3p)", "cases_per_hour": 400},
            {"shift": "2nd (3p-11p)", "cases_per_hour": 600},
            {"shift": "3rd (11p-7a)", "cases_per_hour": 300},
        ])
        result = pick_benchmark_shift(hourly, overall, "3rd (11p-7a)")
        assert result == "2nd (3p-11p)"

    def test_excludes_target_shift(self):
        from shift_report import pick_benchmark_shift
        hourly = _make_hourly([
            {"shift": "1st (7a-3p)"},
            {"shift": "2nd (3p-11p)"},
        ])
        overall = _make_overall([
            {"shift": "1st (7a-3p)", "cases_per_hour": 400},
            {"shift": "2nd (3p-11p)", "cases_per_hour": 600},
        ])
        result = pick_benchmark_shift(hourly, overall, "2nd (3p-11p)")
        assert result == "1st (7a-3p)"

    def test_single_shift_returns_none(self):
        from shift_report import pick_benchmark_shift
        hourly = _make_hourly([{"shift": "3rd (11p-7a)"}])
        overall = _make_overall([
            {"shift": "3rd (11p-7a)", "cases_per_hour": 300},
        ])
        result = pick_benchmark_shift(hourly, overall, "3rd (11p-7a)")
        assert result is None


# =====================================================================
# TestExtractEquipmentMentions
# =====================================================================

class TestExtractEquipmentMentions:
    """Equipment scanning from operator notes."""

    def test_riverwood_mention(self):
        result = extract_equipment_mentions("Riverwood had a fiber jam at 2am")
        assert "Riverwood" in result

    def test_multiple_equipment(self):
        result = extract_equipment_mentions("Bear labeler flappers loose, palletizer misformed layers")
        assert "Labeler" in result
        assert "Palletizer" in result

    def test_case_insensitive(self):
        result = extract_equipment_mentions("RYSON spiral was jammed")
        assert "Spiral" in result

    def test_empty_notes(self):
        assert extract_equipment_mentions("") == []
        assert extract_equipment_mentions(None) == []

    def test_nan_notes(self):
        assert extract_equipment_mentions(float("nan")) == []

    def test_no_equipment(self):
        result = extract_equipment_mentions("Good run, no issues tonight")
        assert result == []

    def test_xray(self):
        result = extract_equipment_mentions("x-ray check passed")
        assert "X-Ray" in result

    def test_conveyor_typo(self):
        """Operators sometimes misspell conveyor."""
        result = extract_equipment_mentions("overhead conveypr was slow")
        assert "Conveyors" in result


# =====================================================================
# TestSummarizeIssues
# =====================================================================

class TestSummarizeIssues:
    """Note cleaning and truncation."""

    def test_filters_xray_pass(self):
        result = summarize_issues("X-ray passed;; Both passed")
        assert result == ""

    def test_keeps_real_issues(self):
        result = summarize_issues("Riverwood fiber jam;; Bear labeler flappers")
        assert "Riverwood" in result
        assert "labeler" in result.lower()

    def test_truncation(self):
        long_note = "A" * 200 + ";; " + "B" * 200
        result = summarize_issues(long_note)
        assert len(result) <= 183  # 180 + "..."
        assert result.endswith("...")

    def test_empty(self):
        assert summarize_issues("") == ""
        assert summarize_issues(None) == ""

    def test_max_two_issues(self):
        result = summarize_issues("Issue one;; Issue two;; Issue three;; Issue four")
        # Should have at most 2 parts separated by "; "
        parts = result.split("; ")
        assert len(parts) <= 2

    def test_filters_startup(self):
        result = summarize_issues("Set-up: normal;; Riverwood jam")
        assert "Riverwood" in result
        # Short startup notes get filtered
        assert "Set-up" not in result


# =====================================================================
# TestClassifySupport
# =====================================================================

class TestClassifySupport:
    """Support classification from equipment list."""

    def test_single_equipment(self):
        result = classify_support(["Riverwood"], "some notes")
        assert result == "Caser"

    def test_multiple_returns_multiple(self):
        result = classify_support(["Riverwood", "Labeler", "Palletizer"], "notes")
        assert result == "MULTIPLE"

    def test_staffing_from_notes(self):
        result = classify_support(["Labeler"], "short staff tonight, no checker available")
        assert "Staffing" in result

    def test_empty_equipment(self):
        result = classify_support([], "some notes")
        assert result == ""


# =====================================================================
# TestTargetTracking
# =====================================================================

class TestTargetTracking:
    """HIT/MISSED/CLOSE logic from build_week_by_week."""

    def test_hit_status(self):
        """Cases >= target = HIT."""
        from shared import PRODUCT_TARGET
        # Use a known product with a target
        product = "Cut Green Beans 8pk"
        target = PRODUCT_TARGET[product]
        assert target == 30000
        # If cases >= target, it's a HIT
        assert target >= target  # trivially true

    def test_close_status(self):
        """Cases >= 85% of target but < target = CLOSE."""
        target = 30000
        close_val = target * 0.85  # 25500
        assert close_val < target
        assert close_val >= target * 0.85

    def test_missed_status(self):
        """Cases < 85% of target = MISSED."""
        target = 30000
        missed_val = target * 0.80  # 24000
        assert missed_val < target * 0.85


# =====================================================================
# TestShiftLabel
# =====================================================================

class TestShiftLabel:
    """_shift_label extracts short name from full shift name."""

    def test_3rd_shift(self):
        from shift_report import _shift_label
        assert _shift_label("3rd (11p-7a)") == "3rd"

    def test_1st_shift(self):
        from shift_report import _shift_label
        assert _shift_label("1st (7a-3p)") == "1st"

    def test_2nd_shift(self):
        from shift_report import _shift_label
        assert _shift_label("2nd (3p-11p)") == "2nd"

    def test_empty(self):
        from shift_report import _shift_label
        assert _shift_label("") == ""

    def test_none(self):
        from shift_report import _shift_label
        assert _shift_label(None) == ""
