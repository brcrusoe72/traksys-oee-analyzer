"""
Unit tests for core OEE analysis math and parsers.

Run: python -m pytest test_core.py -v
"""

import pytest
import pandas as pd
import numpy as np

from shared import classify_fault, normalize_product, PRODUCT_NORMALIZE
from analyze import _aggregate_oee, _smart_rename, _weighted_mean, EXPECTED_SHEETS


# =====================================================================
# _aggregate_oee — production-weighted OEE math
# =====================================================================

class TestAggregateOEE:
    """Production-weighted OEE should not be a simple average of ratios."""

    def _make_df(self, rows):
        """Helper: build a DataFrame with columns matching hourly data."""
        df = pd.DataFrame(rows)
        for col in ["total_hours", "total_cases", "availability",
                     "performance", "quality", "good_cases"]:
            if col not in df.columns:
                if col == "good_cases":
                    df[col] = df["total_cases"]
                elif col == "quality":
                    df[col] = 1.0
        return df

    def test_single_row(self):
        df = self._make_df([{
            "total_hours": 1.0, "total_cases": 100,
            "availability": 0.9, "performance": 0.8, "quality": 0.95,
            "good_cases": 95,
        }])
        avail, perf, qual, oee = _aggregate_oee(df)
        assert abs(avail - 0.9) < 0.001
        assert abs(perf - 0.8) < 0.001
        assert abs(qual - 0.95) < 0.001
        assert abs(oee - 68.4) < 0.1  # 0.9 * 0.8 * 0.95 * 100

    def test_weighted_not_simple_average(self):
        """The bug: averaging per-hour ratios gives wrong answer when hours differ."""
        df = self._make_df([
            # Hour 1: full hour, low OEE
            {"total_hours": 1.0, "total_cases": 50,
             "availability": 0.5, "performance": 0.5, "quality": 1.0, "good_cases": 50},
            # Hour 2: full hour, high OEE
            {"total_hours": 1.0, "total_cases": 200,
             "availability": 1.0, "performance": 1.0, "quality": 1.0, "good_cases": 200},
        ])
        avail, perf, qual, oee = _aggregate_oee(df)

        # Simple mean would give avail = (0.5+1.0)/2 = 0.75
        # Weighted: production_time = 0.5*1 + 1.0*1 = 1.5, scheduled = 2.0
        # availability = 1.5/2.0 = 0.75 (same in this case because hours are equal)
        assert abs(avail - 0.75) < 0.001

    def test_unequal_hours_weighting(self):
        """With unequal hours, weighting matters a lot."""
        df = self._make_df([
            # Short interval (0.25 hr), bad availability
            {"total_hours": 0.25, "total_cases": 10,
             "availability": 0.2, "performance": 0.5, "quality": 1.0, "good_cases": 10},
            # Long interval (1.0 hr), good availability
            {"total_hours": 1.0, "total_cases": 200,
             "availability": 0.95, "performance": 0.9, "quality": 1.0, "good_cases": 200},
        ])
        avail, perf, qual, oee = _aggregate_oee(df)

        # Simple mean: (0.2 + 0.95) / 2 = 0.575
        # Weighted: (0.2*0.25 + 0.95*1.0) / (0.25 + 1.0) = 1.0/1.25 = 0.80
        assert abs(avail - 0.80) < 0.01
        # The weighted answer is closer to the long interval (0.95) than the bad one
        assert avail > 0.70  # Much better than simple mean of 0.575

    def test_zero_production_excluded(self):
        """Rows with zero cases or zero hours should be excluded."""
        df = self._make_df([
            {"total_hours": 1.0, "total_cases": 100,
             "availability": 0.9, "performance": 0.8, "quality": 1.0, "good_cases": 100},
            # This row should be excluded (zero cases)
            {"total_hours": 1.0, "total_cases": 0,
             "availability": 0.0, "performance": 0.0, "quality": 0.0, "good_cases": 0},
        ])
        avail, perf, qual, oee = _aggregate_oee(df)
        assert abs(avail - 0.9) < 0.001
        assert abs(perf - 0.8) < 0.001

    def test_empty_dataframe(self):
        df = pd.DataFrame({
            "total_hours": pd.Series(dtype=float),
            "total_cases": pd.Series(dtype=float),
            "availability": pd.Series(dtype=float),
            "performance": pd.Series(dtype=float),
            "quality": pd.Series(dtype=float),
            "good_cases": pd.Series(dtype=float),
        })
        avail, perf, qual, oee = _aggregate_oee(df)
        assert avail == 0.0
        assert oee == 0.0

    def test_quality_from_good_cases(self):
        """Quality = good_cases / total_cases."""
        df = self._make_df([{
            "total_hours": 1.0, "total_cases": 200,
            "availability": 1.0, "performance": 1.0,
            "quality": 0.9, "good_cases": 180,
        }])
        _, _, qual, _ = _aggregate_oee(df)
        assert abs(qual - 0.9) < 0.001  # 180/200


# =====================================================================
# classify_fault — downtime reason classification
# =====================================================================

class TestClassifyFault:
    def test_equipment_keywords(self):
        assert classify_fault("Caser - Riverwood") == "Equipment / Mechanical"
        assert classify_fault("Tray Packer - Kayat") == "Equipment / Mechanical"
        assert classify_fault("Palletizer fault") == "Equipment / Mechanical"

    def test_data_gap(self):
        assert classify_fault("Unassigned") == "Data Gap (uncoded)"
        assert classify_fault("Unknown reason") == "Data Gap (uncoded)"

    def test_scheduled(self):
        assert classify_fault("Break-Lunch") == "Scheduled / Non-Production"
        assert classify_fault("Not Scheduled") == "Scheduled / Non-Production"
        assert classify_fault("Lunch (Comida)") == "Scheduled / Non-Production"

    def test_micro_stops(self):
        assert classify_fault("Short Stop") == "Micro Stops"
        assert classify_fault("short stop - filler") == "Micro Stops"

    def test_process(self):
        assert classify_fault("Day Code Change") == "Process / Changeover"
        assert classify_fault("Changeover") == "Process / Changeover"
        assert classify_fault("CIP Cleanup") == "Process / Changeover"

    def test_dash_defaults_to_equipment(self):
        """Reason codes with dashes default to equipment."""
        assert classify_fault("Something - Brand X") == "Equipment / Mechanical"

    def test_unrecognized_no_dash(self):
        assert classify_fault("Random uncategorized thing") == "Other / Unclassified"


# =====================================================================
# normalize_product — product name cleanup
# =====================================================================

class TestNormalizeProduct:
    def test_known_mappings(self):
        assert normalize_product("DM Cut Gr Bn") == "Cut Green Beans 8pk"
        assert normalize_product("dm wk corn") == "WK Corn 12pk"
        assert normalize_product("DM Sliced Pears") == "Pears (trayed)"

    def test_case_insensitive(self):
        assert normalize_product("DM CUT GR BN") == "Cut Green Beans 8pk"
        assert normalize_product("dm cut gr bn") == "Cut Green Beans 8pk"

    def test_whitespace_handling(self):
        assert normalize_product("  dm cut gr bn  ") == "Cut Green Beans 8pk"

    def test_unknown_product_passthrough(self):
        assert normalize_product("New Product XYZ") == "New Product XYZ"

    def test_null_handling(self):
        assert normalize_product(None) == "Unknown"
        assert normalize_product(float("nan")) == "Unknown"
        assert normalize_product("") == "Unknown"


# =====================================================================
# _smart_rename — column name fuzzy matching
# =====================================================================

class TestSmartRename:
    def test_exact_match(self):
        df = pd.DataFrame({"Shift Date": [1], "Shift": ["1st"], "Shift Hour": [1]})
        expected = EXPECTED_SHEETS["DayShiftHour"]["columns"]
        result = _smart_rename(df, expected)
        assert "shift_date" in result.columns

    def test_case_insensitive_match(self):
        df = pd.DataFrame({"shift date": [1], "SHIFT": ["1st"], "shift hour": [1]})
        expected = EXPECTED_SHEETS["DayShiftHour"]["columns"]
        result = _smart_rename(df, expected)
        assert "shift_date" in result.columns

    def test_header_name_matching(self):
        """_smart_rename uses _HEADER_TO_INTERNAL for flexible header matching."""
        df = pd.DataFrame({
            "Date": [1], "Shift": ["1st"], "Hour": [1],
            "Duration Hours": [1.0], "Total Cases": [100],
            "OEE (%)": [50], "Availability": [0.9],
        })
        expected = EXPECTED_SHEETS["DayShiftHour"]["columns"]
        result = _smart_rename(df, expected)
        assert "shift_date" in result.columns
        assert "shift_hour" in result.columns
        assert "oee_pct" in result.columns


# =====================================================================
# _weighted_mean — helper for production-weighted averages
# =====================================================================

class TestWeightedMean:
    def test_basic_weighted_mean(self):
        values = pd.Series([10.0, 20.0])
        weights = pd.Series([1.0, 3.0])
        result = _weighted_mean(values, weights)
        assert abs(result - 17.5) < 0.001  # (10*1 + 20*3) / (1+3) = 70/4

    def test_zero_weights_excluded(self):
        values = pd.Series([10.0, 999.0, 20.0])
        weights = pd.Series([1.0, 0.0, 1.0])
        result = _weighted_mean(values, weights)
        assert abs(result - 15.0) < 0.001  # 999 excluded

    def test_all_zero_weights(self):
        values = pd.Series([10.0, 20.0])
        weights = pd.Series([0.0, 0.0])
        result = _weighted_mean(values, weights)
        assert result == 0.0
