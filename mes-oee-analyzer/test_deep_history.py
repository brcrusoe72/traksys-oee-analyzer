"""
Unit tests for deep history save/load/dedup/analytics.

Run: python -m pytest test_deep_history.py -v
"""

import json
import os
import tempfile
import shutil

import pytest
import pandas as pd
import numpy as np

import oee_history as oh


# =====================================================================
# Helpers — build mock inputs for save_run()
# =====================================================================

def _make_hourly(rows):
    """Build a minimal hourly DataFrame matching analyze.py output."""
    df = pd.DataFrame(rows)
    defaults = {
        "total_hours": 1.0, "total_cases": 100, "good_cases": 95,
        "bad_cases": 5, "availability": 0.9, "performance": 0.8,
        "quality": 0.99, "oee_pct": 71.3, "cases_per_hour": 100,
        "product_code": "8PK",
    }
    for col, val in defaults.items():
        if col not in df.columns:
            df[col] = val
    if "date" not in df.columns:
        df["date"] = pd.Timestamp("2026-02-06")
    if "date_str" not in df.columns:
        df["date_str"] = df["date"].apply(
            lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x))
    if "shift" not in df.columns:
        df["shift"] = "3rd"
    if "shift_hour" not in df.columns:
        df["shift_hour"] = range(1, len(df) + 1)
    if "day_of_week" not in df.columns:
        df["day_of_week"] = df["date"].apply(
            lambda x: x.strftime("%A") if hasattr(x, "strftime") else "Thursday")
    return df


def _make_shift_summary(rows):
    """Build a minimal shift_summary DataFrame."""
    df = pd.DataFrame(rows)
    defaults = {
        "total_hours": 8.0, "total_cases": 800, "good_cases": 780,
        "bad_cases": 20, "oee_pct": 55.0, "cases_per_hour": 100,
    }
    for col, val in defaults.items():
        if col not in df.columns:
            df[col] = val
    if "date" not in df.columns:
        df["date"] = pd.Timestamp("2026-02-06")
    if "date_str" not in df.columns:
        df["date_str"] = df["date"].apply(
            lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x))
    if "shift" not in df.columns:
        df["shift"] = "3rd"
    return df


def _make_overall():
    """Build a minimal overall DataFrame (one row per shift)."""
    return pd.DataFrame([
        {"shift": "3rd Shift", "oee_pct": 55.0, "total_cases": 800,
         "cases_per_hour": 100, "total_hours": 8.0, "good_cases": 780},
    ])


def _make_results():
    """Build a minimal results dict with Plant Summary KPIs."""
    kpis = pd.DataFrame([
        {"Metric": "Overall OEE", "Value": "55.0%"},
        {"Metric": "Average Availability", "Value": "85.0%"},
        {"Metric": "Average Performance", "Value": "75.0%"},
        {"Metric": "Average Quality", "Value": "99.0%"},
        {"Metric": "Est. Cases Lost vs Benchmark", "Value": "100"},
        {"Metric": "Utilization", "Value": "70.0%"},
    ])
    return {"Plant Summary": {"kpis": kpis}}


@pytest.fixture(autouse=True)
def _isolate_files(tmp_path, monkeypatch):
    """Redirect all history files to a temp directory for each test."""
    monkeypatch.setattr(oh, "HISTORY_FILE", str(tmp_path / "history.jsonl"))
    monkeypatch.setattr(oh, "TRENDS_FILE", str(tmp_path / "plant_trends.json"))
    monkeypatch.setattr(oh, "HOURLY_FILE", str(tmp_path / "hourly_history.jsonl"))
    monkeypatch.setattr(oh, "SHIFT_DAILY_FILE", str(tmp_path / "shift_daily_history.jsonl"))
    yield


# =====================================================================
# TestSaveRunDeepHistory
# =====================================================================

class TestSaveRunDeepHistory:
    """Hourly/shift-daily files created, schemas correct, dead hours counted."""

    def test_hourly_file_created(self):
        hourly = _make_hourly([
            {"shift_hour": 1, "total_cases": 100},
            {"shift_hour": 2, "total_cases": 0},
            {"shift_hour": 3, "total_cases": 200},
        ])
        shift_summary = _make_shift_summary([{}])
        oh.save_run(_make_results(), hourly, shift_summary, _make_overall())

        assert os.path.exists(oh.HOURLY_FILE)
        with open(oh.HOURLY_FILE, "r") as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 3

    def test_hourly_schema(self):
        hourly = _make_hourly([{"shift_hour": 1}])
        oh.save_run(_make_results(), hourly, _make_shift_summary([{}]), _make_overall())

        with open(oh.HOURLY_FILE, "r") as f:
            rec = json.loads(f.readline())

        expected_keys = {"run_id", "date", "dow", "shift", "hour", "hours",
                         "cases", "good", "avail", "perf", "qual", "oee", "cph", "product"}
        assert set(rec.keys()) == expected_keys

    def test_shift_daily_file_created(self):
        hourly = _make_hourly([{"shift_hour": 1}])
        shift_summary = _make_shift_summary([{}])
        oh.save_run(_make_results(), hourly, shift_summary, _make_overall())

        assert os.path.exists(oh.SHIFT_DAILY_FILE)
        with open(oh.SHIFT_DAILY_FILE, "r") as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 1

    def test_shift_daily_schema(self):
        hourly = _make_hourly([{"shift_hour": 1}])
        oh.save_run(_make_results(), hourly, _make_shift_summary([{}]), _make_overall())

        with open(oh.SHIFT_DAILY_FILE, "r") as f:
            rec = json.loads(f.readline())

        expected_keys = {"run_id", "date", "dow", "shift", "hours",
                         "cases", "good", "oee", "cph", "dead"}
        assert set(rec.keys()) == expected_keys

    def test_dead_hour_count(self):
        """Dead hours (zero-production) should be counted in shift-daily records."""
        hourly = _make_hourly([
            {"shift_hour": 1, "total_cases": 100},
            {"shift_hour": 2, "total_cases": 0},
            {"shift_hour": 3, "total_cases": 0},
            {"shift_hour": 4, "total_cases": 50},
        ])
        shift_summary = _make_shift_summary([{}])
        oh.save_run(_make_results(), hourly, shift_summary, _make_overall())

        with open(oh.SHIFT_DAILY_FILE, "r") as f:
            rec = json.loads(f.readline())
        assert rec["dead"] == 2

    def test_shift_names_normalized(self):
        """Raw shift names like '3rd (11p-7a)' should map to '3rd Shift'."""
        hourly = _make_hourly([{"shift": "3rd (11p-7a)", "shift_hour": 1}])
        shift_summary = _make_shift_summary([{"shift": "3rd (11p-7a)"}])
        oh.save_run(_make_results(), hourly, shift_summary, _make_overall())

        with open(oh.HOURLY_FILE, "r") as f:
            h_rec = json.loads(f.readline())
        with open(oh.SHIFT_DAILY_FILE, "r") as f:
            sd_rec = json.loads(f.readline())

        assert h_rec["shift"] == "3rd Shift"
        assert sd_rec["shift"] == "3rd Shift"

    def test_short_shift_names_normalized(self):
        """Short shift names like '1st' should map to '1st Shift'."""
        hourly = _make_hourly([{"shift": "1st", "shift_hour": 1}])
        shift_summary = _make_shift_summary([{"shift": "1st"}])
        oh.save_run(_make_results(), hourly, shift_summary, _make_overall())

        with open(oh.HOURLY_FILE, "r") as f:
            h_rec = json.loads(f.readline())
        assert h_rec["shift"] == "1st Shift"


# =====================================================================
# TestLoadDeepHistory
# =====================================================================

class TestLoadDeepHistory:
    """Loader returns None for empty/missing files, DataFrame otherwise."""

    def test_hourly_empty_returns_none(self):
        assert oh.load_hourly_history() is None

    def test_shift_daily_empty_returns_none(self):
        assert oh.load_shift_daily_history() is None

    def test_hourly_loads_populated(self):
        hourly = _make_hourly([
            {"shift_hour": 1}, {"shift_hour": 2},
        ])
        oh.save_run(_make_results(), hourly, _make_shift_summary([{}]), _make_overall())

        df = oh.load_hourly_history()
        assert df is not None
        assert len(df) == 2
        assert "oee" in df.columns
        assert "shift" in df.columns

    def test_shift_daily_loads_populated(self):
        hourly = _make_hourly([{"shift_hour": 1}])
        oh.save_run(_make_results(), hourly, _make_shift_summary([{}]), _make_overall())

        df = oh.load_shift_daily_history()
        assert df is not None
        assert len(df) == 1
        assert "dead" in df.columns


# =====================================================================
# TestDeepHistoryDeduplication
# =====================================================================

class TestDeepHistoryDeduplication:
    """Re-analysis doesn't duplicate; latest run wins; invalid rows removed."""

    def test_no_duplicate_after_reanalysis(self):
        """Running save_run twice for same date → tend_garden deduplicates."""
        hourly = _make_hourly([
            {"shift_hour": 1, "oee_pct": 50.0},
            {"shift_hour": 2, "oee_pct": 60.0},
        ])
        shift_summary = _make_shift_summary([{"oee_pct": 55.0}])

        oh.save_run(_make_results(), hourly, shift_summary, _make_overall())
        oh.save_run(_make_results(), hourly, shift_summary, _make_overall())

        trends = oh.load_trends()
        assert trends is not None
        # Run-level dedup
        assert trends["total_runs"] == 1

        # Deep history dedup: hourly should have 2 rows (not 4)
        deep = trends.get("deep_history", {})
        hod = deep.get("hour_of_day", [])
        assert len(hod) == 2  # hours 1 and 2
        # Ingest should short-circuit true duplicates (no new raw run row).
        with open(oh.HISTORY_FILE, "r", encoding="utf-8") as f:
            raw = [json.loads(l) for l in f if l.strip()]
        assert len(raw) == 1

    def test_latest_run_wins(self):
        """Second save_run with different OEE should overwrite during tend_garden dedup."""
        hourly_v1 = _make_hourly([{"shift_hour": 1, "oee_pct": 40.0, "total_cases": 80}])
        hourly_v2 = _make_hourly([{"shift_hour": 1, "oee_pct": 70.0, "total_cases": 150}])
        shift_summary = _make_shift_summary([{}])

        oh.save_run(_make_results(), hourly_v1, shift_summary, _make_overall())
        oh.save_run(_make_results(), hourly_v2, shift_summary, _make_overall())

        trends = oh.load_trends()
        deep = trends.get("deep_history", {})
        hod = deep.get("hour_of_day", [])
        assert len(hod) == 1
        # The latest run (70.0) should win
        assert hod[0]["avg_oee"] == 70.0
        # Same period with changed data should create a new revision.
        with open(oh.HISTORY_FILE, "r", encoding="utf-8") as f:
            raw = [json.loads(l) for l in f if l.strip()]
        # Compaction keeps only latest row for the period.
        assert len(raw) == 1
        assert raw[0].get("revision") == 2
        assert bool(raw[0].get("supersedes_run_id"))

    def test_invalid_run_rows_removed(self):
        """Rows with run_ids not in valid_ids should be filtered out by tend_garden."""
        hourly = _make_hourly([{"shift_hour": 1}])
        shift_summary = _make_shift_summary([{}])

        # Save a valid run
        oh.save_run(_make_results(), hourly, shift_summary, _make_overall())

        # Manually inject a row with a fake run_id into hourly history
        fake_rec = {
            "run_id": "FAKE_ID", "date": "2026-01-01", "dow": "Wednesday",
            "shift": "3rd Shift", "hour": 1, "hours": 1.0, "cases": 999,
            "good": 999, "avail": 1.0, "perf": 1.0, "qual": 1.0,
            "oee": 99.9, "cph": 999, "product": "TEST",
        }
        with open(oh.HOURLY_FILE, "a") as f:
            f.write(json.dumps(fake_rec) + "\n")

        # Re-tend
        trends = oh.tend_garden()
        deep = trends.get("deep_history", {})
        hod = deep.get("hour_of_day", [])
        # Only the valid run's hour should remain
        assert len(hod) == 1
        assert hod[0]["avg_oee"] != 99.9

    def test_duplicate_ingest_returns_duplicate_status(self):
        hourly = _make_hourly([{"shift_hour": 1, "oee_pct": 55.0}])
        shift_summary = _make_shift_summary([{"oee_pct": 55.0}])

        first = oh.save_run(_make_results(), hourly, shift_summary, _make_overall())
        second = oh.save_run(_make_results(), hourly, shift_summary, _make_overall())

        assert second.get("ingest_status") == "duplicate_ignored"
        assert second.get("duplicate_of_run_id") == first.get("run_id")
        with open(oh.HISTORY_FILE, "r", encoding="utf-8") as f:
            raw = [json.loads(l) for l in f if l.strip()]
        assert len(raw) == 1

    def test_learning_ledger_contains_revision_and_fingerprint(self):
        hourly = _make_hourly([{"shift_hour": 1, "oee_pct": 55.0}])
        shift_summary = _make_shift_summary([{"oee_pct": 55.0}])
        oh.save_run(_make_results(), hourly, shift_summary, _make_overall())

        ledger = oh.load_learning_ledger(limit=20)
        assert ledger is not None
        assert len(ledger) == 1
        row = ledger.iloc[0]
        assert int(row["revision"]) == 1
        assert isinstance(row["dataset_fingerprint_short"], str)
        assert len(row["dataset_fingerprint_short"]) > 0


# =====================================================================
# TestDeepHistoryAnalytics
# =====================================================================

class TestDeepHistoryAnalytics:
    """Aggregation math, hotspot detection, consistency calculations."""

    def _save_multi_day(self):
        """Helper: save runs across multiple days and shifts for rich analytics."""
        dates = [pd.Timestamp("2026-02-02"), pd.Timestamp("2026-02-03"),
                 pd.Timestamp("2026-02-04")]
        shifts = ["1st", "3rd"]

        hourly_rows = []
        sd_rows = []
        for d in dates:
            for s in shifts:
                for h in range(1, 5):
                    cases = 0 if (h == 1 and s == "3rd") else 100 + h * 10
                    hourly_rows.append({
                        "date": d, "shift": s, "shift_hour": h,
                        "total_cases": cases, "good_cases": cases,
                        "oee_pct": 0.0 if cases == 0 else 50.0 + h * 5,
                        "cases_per_hour": cases,
                        "availability": 0.0 if cases == 0 else 0.9,
                        "performance": 0.0 if cases == 0 else 0.8,
                        "quality": 1.0,
                    })
                sd_rows.append({
                    "date": d, "shift": s,
                    "total_hours": 4.0, "total_cases": 400,
                    "good_cases": 390, "oee_pct": 60.0 if s == "1st" else 45.0,
                    "cases_per_hour": 100,
                })

        hourly = _make_hourly(hourly_rows)
        shift_summary = _make_shift_summary(sd_rows)
        oh.save_run(_make_results(), hourly, shift_summary, _make_overall())

    def test_hour_of_day_aggregation(self):
        self._save_multi_day()
        trends = oh.load_trends()
        deep = trends.get("deep_history", {})
        hod = deep.get("hour_of_day", [])
        assert len(hod) > 0
        # Each record should have these keys
        for rec in hod:
            assert "hour" in rec
            assert "avg_oee" in rec
            assert "avg_cph" in rec
            assert "dead_pct" in rec

    def test_day_of_week_aggregation(self):
        self._save_multi_day()
        trends = oh.load_trends()
        deep = trends.get("deep_history", {})
        dow = deep.get("day_of_week", [])
        assert len(dow) > 0
        for rec in dow:
            assert "dow" in rec
            assert "avg_oee" in rec
            assert "avg_cph" in rec
            assert "total_dead" in rec

    def test_dead_hour_hotspot(self):
        """Hour 1 of 3rd shift always has 0 cases → should be a dead hotspot."""
        self._save_multi_day()
        trends = oh.load_trends()
        deep = trends.get("deep_history", {})
        hod = deep.get("hour_of_day", [])
        # Hour 1 should have nonzero dead_pct (3rd shift always dead at hour 1)
        hour1 = [h for h in hod if h["hour"] == 1]
        assert len(hour1) == 1
        assert hour1[0]["dead_pct"] > 0

    def test_shift_consistency(self):
        self._save_multi_day()
        trends = oh.load_trends()
        deep = trends.get("deep_history", {})
        cons = deep.get("shift_consistency", [])
        assert len(cons) > 0
        for rec in cons:
            assert "shift" in rec
            assert "std_dev" in rec
            assert "cv_pct" in rec
            assert "min_oee" in rec
            assert "max_oee" in rec
            assert "range" in rec
            assert rec["range"] == round(rec["max_oee"] - rec["min_oee"], 1)

    def test_shift_gap_trend(self):
        self._save_multi_day()
        trends = oh.load_trends()
        deep = trends.get("deep_history", {})
        gap = deep.get("shift_gap_trend", [])
        assert len(gap) > 0
        for rec in gap:
            assert "date" in rec
            assert "shift" in rec
            assert "oee" in rec
            assert "rolling_7d" in rec

    def test_hour_of_day_by_shift(self):
        self._save_multi_day()
        trends = oh.load_trends()
        deep = trends.get("deep_history", {})
        hod_s = deep.get("hour_of_day_by_shift", [])
        assert len(hod_s) > 0
        shifts_in_data = {rec["shift"] for rec in hod_s}
        assert len(shifts_in_data) >= 2


# =====================================================================
# TestNormalizeShift
# =====================================================================

class TestNormalizeShift:
    """Unit tests for _normalize_shift helper."""

    def test_short_forms(self):
        assert oh._normalize_shift("1st") == "1st Shift"
        assert oh._normalize_shift("2nd") == "2nd Shift"
        assert oh._normalize_shift("3rd") == "3rd Shift"

    def test_long_forms(self):
        assert oh._normalize_shift("3rd (11p-7a)") == "3rd Shift"
        assert oh._normalize_shift("1st (7a-3p)") == "1st Shift"

    def test_already_normalized(self):
        assert oh._normalize_shift("1st Shift") == "1st Shift"

    def test_unknown_passthrough(self):
        assert oh._normalize_shift("Day Shift") == "Day Shift"
