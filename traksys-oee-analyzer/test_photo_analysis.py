"""
Tests for photo_analysis.py — pure logic only, no OpenAI API calls.
"""

import pytest
import pandas as pd

from photo_analysis import _map_to_equipment_scan, findings_to_downtime_dict, _image_media_type


class TestMapToEquipmentScan:
    def test_direct_match(self):
        assert _map_to_equipment_scan("Riverwood") == "Riverwood"

    def test_direct_match_case_insensitive(self):
        assert _map_to_equipment_scan("riverwood") == "Riverwood"
        assert _map_to_equipment_scan("PALLETIZER") == "Palletizer"

    def test_keyword_match(self):
        assert _map_to_equipment_scan("caser") == "Riverwood"
        assert _map_to_equipment_scan("shrink tunnel") == "Kayat (Tray/Shrink/Wrap)"
        assert _map_to_equipment_scan("bear labeler") == "Labeler"

    def test_unknown_returns_original(self):
        assert _map_to_equipment_scan("mystery machine") == "mystery machine"

    def test_none_returns_none(self):
        assert _map_to_equipment_scan(None) is None

    def test_empty_string_returns_none(self):
        assert _map_to_equipment_scan("") is None


class TestFindingsToDowntimeDict:
    def test_single_issue(self):
        findings = [{
            "issues": [{
                "equipment": "Riverwood",
                "description": "fiber jam",
                "duration_minutes": 30,
                "shift": "1st Shift",
                "severity": "high",
            }],
            "production_notes": [],
            "shift_notes": [],
            "raw_text": "",
        }]
        result = findings_to_downtime_dict(findings, ["photo1.jpg"])
        assert result is not None
        assert len(result["events_df"]) == 1
        assert result["events_df"].iloc[0]["reason"] == "Riverwood: fiber jam"
        assert result["events_df"].iloc[0]["duration_minutes"] == 30.0

    def test_no_issues_returns_none(self):
        findings = [{"issues": [], "production_notes": [], "shift_notes": [], "raw_text": ""}]
        result = findings_to_downtime_dict(findings, ["photo1.jpg"])
        assert result is None

    def test_error_findings_skipped(self):
        findings = [{"error": "Could not parse", "issues": [], "production_notes": [], "shift_notes": []}]
        result = findings_to_downtime_dict(findings, ["photo1.jpg"])
        assert result is None

    def test_multiple_photos_merged(self):
        f1 = {"issues": [{"equipment": "Riverwood", "description": "jam", "duration_minutes": 10, "shift": "1st Shift", "severity": "high"}]}
        f2 = {"issues": [{"equipment": "Labeler", "description": "loose labels", "duration_minutes": 15, "shift": "2nd Shift", "severity": "medium"}]}
        result = findings_to_downtime_dict([f1, f2], ["p1.jpg", "p2.jpg"])
        assert result is not None
        assert len(result["events_df"]) == 2
        assert len(result["reasons_df"]) == 2

    def test_output_has_required_keys(self):
        findings = [{"issues": [{"equipment": "Palletizer", "description": "misformed layers", "duration_minutes": 5, "shift": "", "severity": "low"}]}]
        result = findings_to_downtime_dict(findings, ["photo.png"])
        assert result is not None
        required_keys = {"events_df", "reasons_df", "shift_reasons_df", "pareto_df", "findings"}
        assert required_keys.issubset(set(result.keys()))

    def test_no_duration_defaults_to_zero(self):
        findings = [{"issues": [{"equipment": "Depal", "description": "suction cup issue", "duration_minutes": None, "shift": "3rd Shift", "severity": "medium"}]}]
        result = findings_to_downtime_dict(findings, ["photo.jpg"])
        assert result is not None
        assert result["events_df"].iloc[0]["duration_minutes"] == 0.0

    def test_keyword_equipment_mapping_in_findings(self):
        """Equipment names from AI get mapped to canonical EQUIPMENT_SCAN keys."""
        findings = [{"issues": [{"equipment": "caser", "description": "stuck", "duration_minutes": 5, "shift": "", "severity": "low"}]}]
        result = findings_to_downtime_dict(findings, ["photo.jpg"])
        assert result is not None
        assert result["events_df"].iloc[0]["reason"] == "Riverwood: stuck"

    def test_shift_reasons_built_when_shift_present(self):
        findings = [{"issues": [
            {"equipment": "Riverwood", "description": "jam", "duration_minutes": 10, "shift": "1st Shift", "severity": "high"},
            {"equipment": "Riverwood", "description": "another jam", "duration_minutes": 5, "shift": "1st Shift", "severity": "medium"},
        ]}]
        result = findings_to_downtime_dict(findings, ["photo.jpg"])
        assert result is not None
        assert len(result["shift_reasons_df"]) > 0


class TestImageMediaType:
    def test_png(self):
        assert _image_media_type("photo.png") == "image/png"

    def test_jpg(self):
        assert _image_media_type("photo.jpg") == "image/jpeg"

    def test_jpeg(self):
        assert _image_media_type("photo.jpeg") == "image/jpeg"

    def test_unknown_defaults_to_jpeg(self):
        assert _image_media_type("photo.bmp") == "image/jpeg"
