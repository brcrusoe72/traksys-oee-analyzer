"""
Tests for photo_analysis.py — pure logic only, no OpenAI API calls.
"""

import pytest
import pandas as pd

from photo_analysis import (
    _map_to_equipment_scan, findings_to_downtime_dict, _image_media_type,
    _match_shift_to_data, build_photo_narrative, _DEFAULT_DURATION_MIN,
    _retune_create_kwargs_for_param_error, analyze_photos,
)


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

    def test_no_duration_defaults_to_estimated(self):
        findings = [{"issues": [{"equipment": "Depal", "description": "suction cup issue", "duration_minutes": None, "shift": "3rd Shift", "severity": "medium"}]}]
        result = findings_to_downtime_dict(findings, ["photo.jpg"])
        assert result is not None
        assert result["events_df"].iloc[0]["duration_minutes"] == _DEFAULT_DURATION_MIN

    def test_zero_duration_defaults_to_estimated(self):
        findings = [{"issues": [{"equipment": "Depal", "description": "issue", "duration_minutes": 0, "shift": "", "severity": "low"}]}]
        result = findings_to_downtime_dict(findings, ["photo.jpg"])
        assert result is not None
        assert result["events_df"].iloc[0]["duration_minutes"] == _DEFAULT_DURATION_MIN

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


    def test_shift_matched_to_data_format(self):
        """Shift names from AI get mapped to actual data shift names."""
        data_shifts = ["1st (7a-3p)", "2nd (3p-11p)", "3rd (11p-7a)"]
        findings = [{"issues": [{"equipment": "Riverwood", "description": "jam", "duration_minutes": 20, "shift": "1st Shift", "severity": "high"}]}]
        result = findings_to_downtime_dict(findings, ["photo.jpg"], data_shifts=data_shifts)
        assert result is not None
        assert result["events_df"].iloc[0]["shift"] == "1st (7a-3p)"


class TestMatchShiftToData:
    def test_exact_prefix_match(self):
        assert _match_shift_to_data("1st Shift", ["1st (7a-3p)", "2nd (3p-11p)"]) == "1st (7a-3p)"

    def test_already_matching(self):
        assert _match_shift_to_data("1st Shift", ["1st Shift", "2nd Shift"]) == "1st Shift"

    def test_no_match_returns_original(self):
        assert _match_shift_to_data("1st Shift", ["A Shift", "B Shift"]) == "1st Shift"

    def test_empty_shift_returns_empty(self):
        assert _match_shift_to_data("", ["1st Shift"]) == ""

    def test_none_data_shifts(self):
        assert _match_shift_to_data("1st Shift", None) == "1st Shift"


class TestBuildPhotoNarrative:
    def test_returns_narrative_with_issues(self):
        results = [("photo.jpg", {
            "issues": [{"equipment": "Riverwood", "description": "fiber jam", "duration_minutes": 30}],
            "shift_notes": ["running low on fiber"],
            "production_notes": [],
        })]
        text = build_photo_narrative(results)
        assert "Riverwood" in text
        assert "fiber jam" in text
        assert "running low on fiber" in text

    def test_returns_empty_for_no_findings(self):
        results = [("photo.jpg", {"issues": [], "shift_notes": [], "production_notes": []})]
        assert build_photo_narrative(results) == ""

    def test_skips_error_findings(self):
        results = [("photo.jpg", {"error": "fail", "issues": [], "shift_notes": [], "production_notes": []})]
        assert build_photo_narrative(results) == ""


class TestImageMediaType:
    def test_png(self):
        assert _image_media_type("photo.png") == "image/png"

    def test_jpg(self):
        assert _image_media_type("photo.jpg") == "image/jpeg"

    def test_jpeg(self):
        assert _image_media_type("photo.jpeg") == "image/jpeg"

    def test_unknown_defaults_to_jpeg(self):
        assert _image_media_type("photo.bmp") == "image/jpeg"


class TestOpenAIParamRetuning:
    def test_retunes_max_tokens_to_max_completion_tokens(self):
        kwargs = {"model": "gpt-5-mini", "messages": [], "max_tokens": 2000}
        err = ("Error code: 400 - {'error': {'message': "
               "\"Unsupported parameter: 'max_tokens' is not supported with this model. "
               "Use 'max_completion_tokens' instead.\"}}")
        changed = _retune_create_kwargs_for_param_error(kwargs, err, is_reasoning=True)
        assert changed is True
        assert "max_tokens" not in kwargs
        assert kwargs["max_completion_tokens"] == 2000

    def test_retunes_max_completion_tokens_to_max_tokens_when_server_says_so(self):
        kwargs = {"model": "legacy-model", "messages": [], "max_completion_tokens": 1200, "temperature": 0.1}
        err = ("Error code: 400 - {'error': {'message': "
               "\"Unsupported parameter: 'max_completion_tokens' is not supported with this model. "
               "Use 'max_tokens' instead.\"}}")
        changed = _retune_create_kwargs_for_param_error(kwargs, err, is_reasoning=False)
        assert changed is True
        assert "max_completion_tokens" not in kwargs
        assert kwargs["max_tokens"] == 1200

    def test_non_parameter_error_does_not_rewrite(self):
        kwargs = {"model": "gpt-5-mini", "messages": [], "max_completion_tokens": 2000}
        changed = _retune_create_kwargs_for_param_error(kwargs, "timeout happened", is_reasoning=True)
        assert changed is False
        assert kwargs["max_completion_tokens"] == 2000


class TestAnalyzePhotosDefaults:
    def test_default_primary_and_fallback_models(self, monkeypatch):
        calls = []

        def fake_analyze_photo(filepath, api_key, model_name=None):
            calls.append(model_name)
            if model_name == "gpt-5-mini":
                return {"error": "empty", "issues": [], "shift_notes": [], "production_notes": [], "raw_text": ""}
            return {"issues": [{"equipment": "Riverwood", "description": "jam", "duration_minutes": 5, "shift": ""}],
                    "shift_notes": [], "production_notes": [], "raw_text": ""}

        monkeypatch.delenv("OPENAI_VISION_MODEL", raising=False)
        monkeypatch.delenv("OPENAI_VISION_FALLBACK_MODEL", raising=False)
        monkeypatch.setattr("photo_analysis.analyze_photo", fake_analyze_photo)

        dt, display = analyze_photos([("p1.png", "C:\\fake\\p1.png")], "test-key")

        assert calls == ["gpt-5-mini", "gpt-5.1"]
        assert dt is not None
        assert len(display) == 1
