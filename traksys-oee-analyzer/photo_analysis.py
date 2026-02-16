"""
Photo Analysis via OpenAI Vision API
=====================================
Extracts equipment issues, shift notes, and production data from context
photos (whiteboards, work orders, handwritten notes) using GPT vision.

Results are converted to standard downtime dict format so they plug into
the existing analysis pipeline via _merge_downtime_dicts().
"""

import base64
import json
import os
import re

import pandas as pd

from shared import EQUIPMENT_SCAN, classify_fault

# Default duration (minutes) for photo-extracted issues when the AI
# doesn't report a specific duration.  15 min is a conservative estimate
# that ensures photo issues are visible in Pareto rankings.
_DEFAULT_DURATION_MIN = 15.0

# Map display shift names ("1st Shift") to common data-format prefixes
# so we can match against whatever format the hourly data uses.
_SHIFT_PREFIXES = {
    "1st Shift": "1st",
    "2nd Shift": "2nd",
    "3rd Shift": "3rd",
}


def _image_media_type(filepath):
    """Return MIME media type for an image file."""
    ext = os.path.splitext(filepath)[1].lower()
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
    }.get(ext, "image/jpeg")


def get_openai_api_key():
    """Resolve OpenAI API key from environment, Streamlit secrets, or Windows registry."""
    key = os.environ.get("OPENAI_API_KEY")
    if key:
        return key
    try:
        import streamlit as st
        key = st.secrets.get("OPENAI_API_KEY")
        if key:
            return key
    except Exception:
        pass
    # Fallback: Windows User/Machine env vars (not always in os.environ, e.g. Git Bash)
    if os.name == "nt":
        try:
            import subprocess
            result = subprocess.run(
                ["powershell.exe", "-Command",
                 "[System.Environment]::GetEnvironmentVariable('OPENAI_API_KEY', 'User')"],
                capture_output=True, text=True, timeout=5)
            key = result.stdout.strip()
            if key:
                return key
        except Exception:
            pass
    return None


def _map_to_equipment_scan(name):
    """Map AI-extracted equipment name to canonical EQUIPMENT_SCAN key.

    Returns canonical name (e.g. "Riverwood") or the original name if no match.
    """
    if not name:
        return None
    lower = name.lower().strip()
    # Direct key match (case-insensitive)
    for equip_name in EQUIPMENT_SCAN:
        if equip_name.lower() == lower:
            return equip_name
    # Keyword match
    for equip_name, keywords in EQUIPMENT_SCAN.items():
        if any(kw in lower for kw in keywords):
            return equip_name
    return name


def _match_shift_to_data(ai_shift, data_shifts):
    """Map AI shift name ('1st Shift') to the actual shift name in the data.

    data_shifts is a list of shift names from hourly['shift'].unique().
    Returns the matching data shift name, or the original ai_shift if no match.
    """
    if not ai_shift or not data_shifts:
        return ai_shift or ""
    prefix = _SHIFT_PREFIXES.get(ai_shift, ai_shift.split()[0] if ai_shift else "")
    for ds in data_shifts:
        if ds.lower().startswith(prefix.lower()):
            return ds
    return ai_shift


def build_photo_narrative(display_results):
    """Build a short narrative paragraph from photo analysis results.

    Returns a string suitable for appending to shift narratives, or ""
    if there's nothing useful to report.
    """
    all_issues = []
    all_notes = []
    for pname, findings in display_results:
        if not findings or "error" in findings:
            continue
        for issue in findings.get("issues", []):
            equip = _map_to_equipment_scan(issue.get("equipment", "")) or issue.get("equipment", "?")
            desc = issue.get("description", "")
            dur = issue.get("duration_minutes")
            dur_str = f" ({dur} min)" if dur else ""
            all_issues.append(f"{equip}: {desc}{dur_str}")
        for note in findings.get("shift_notes", []):
            all_notes.append(note)
        for note in findings.get("production_notes", []):
            all_notes.append(note)

    if not all_issues and not all_notes:
        return ""

    parts = []
    if all_issues:
        parts.append("**From context photos:** " + "; ".join(all_issues[:5]) + ".")
    if all_notes:
        parts.append("Photo notes: " + "; ".join(all_notes[:3]) + ".")
    return "\n\n" + " ".join(parts)


def _build_prompt():
    """Build the structured extraction prompt with known equipment list."""
    equipment_list = ", ".join(sorted(EQUIPMENT_SCAN.keys()))
    return f"""You are analyzing a photo from a food manufacturing plant (canning/packaging).
This could be a whiteboard, work order, handwritten shift notes, or equipment status board.

Known equipment at this plant: {equipment_list}

Extract the following as JSON (no markdown fences):
{{
  "photo_type": "whiteboard|work_order|shift_notes|equipment_status|other",
  "confidence": "high|medium|low",
  "issues": [
    {{
      "equipment": "equipment name from the photo",
      "description": "what happened",
      "duration_minutes": number or null,
      "shift": "1st Shift|2nd Shift|3rd Shift" or null,
      "severity": "high|medium|low"
    }}
  ],
  "production_notes": ["any production-related notes"],
  "shift_notes": ["any shift handoff or general notes"],
  "raw_text": "full transcription of all readable text"
}}

Rules:
- Only include issues you can actually read from the photo
- Map equipment names to the known list when possible
- Duration in minutes if mentioned, null otherwise
- If you cannot read the photo clearly, return empty issues and explain in raw_text
- Do NOT fabricate issues not visible in the photo"""


def analyze_photo(filepath, api_key, model_name=None):
    """Send one image to GPT vision and return parsed findings.

    Returns dict with keys: photo_type, confidence, issues, production_notes,
    shift_notes, raw_text.  Returns {"error": "..."} on failure.
    """
    from openai import OpenAI

    with open(filepath, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")

    media_type = _image_media_type(filepath)
    client = OpenAI(api_key=api_key)

    model_name = model_name or os.environ.get("OPENAI_VISION_MODEL", "gpt-5-mini")
    messages = [{
        "role": "user",
        "content": [
            {"type": "text", "text": _build_prompt()},
            {"type": "image_url", "image_url": {
                "url": f"data:{media_type};base64,{b64}",
            }},
        ],
    }]

    # Modern OpenAI models use max_completion_tokens; older ones use max_tokens.
    # Try max_completion_tokens first, fall back to max_tokens on error.
    create_kwargs = {
        "model": model_name,
        "messages": messages,
        "temperature": 0.1,
        "max_completion_tokens": 1500,
    }

    try:
        try:
            resp = client.chat.completions.create(**create_kwargs)
        except Exception as e:
            err = str(e).lower()
            # Retry with swapped token parameter if the API rejects the current one.
            if "max_completion_tokens" in err or "unsupported_parameter" in err:
                create_kwargs.pop("max_completion_tokens", None)
                create_kwargs["max_tokens"] = 1500
                resp = client.chat.completions.create(**create_kwargs)
            elif "max_tokens" in err:
                create_kwargs.pop("max_tokens", None)
                create_kwargs["max_completion_tokens"] = 1500
                resp = client.chat.completions.create(**create_kwargs)
            else:
                raise
        raw = resp.choices[0].message.content.strip()
        # Strip markdown fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"error": f"Could not parse response: {raw[:200]}",
                "raw_text": raw, "issues": [], "production_notes": [],
                "shift_notes": []}
    except Exception as e:
        return {"error": str(e), "issues": [], "production_notes": [],
                "shift_notes": [], "raw_text": ""}


def findings_to_downtime_dict(findings_list, photo_names, data_shifts=None):
    """Convert extracted issues into standard downtime dict format.

    Parameters
    ----------
    findings_list : list of dict
        One findings dict per photo (from analyze_photo).
    photo_names : list of str
        Corresponding photo filenames.
    data_shifts : list of str, optional
        Shift names from the hourly data (e.g. ["1st Shift", "1st (7a-3p)"]).
        Used to match AI-provided shift names to the data format.

    Returns
    -------
    dict or None
        Standard downtime dict with events_df, reasons_df, shift_reasons_df,
        or None if no actionable issues found.
    """
    events = []
    for findings, pname in zip(findings_list, photo_names):
        if "error" in findings and not findings.get("issues"):
            continue
        for issue in findings.get("issues", []):
            equip_raw = issue.get("equipment", "")
            equip = _map_to_equipment_scan(equip_raw) or equip_raw
            desc = issue.get("description", "")
            reason = f"{equip}: {desc}" if equip and desc else (equip or desc or "Unknown (photo)")
            raw_dur = issue.get("duration_minutes")
            duration = float(raw_dur) if raw_dur else _DEFAULT_DURATION_MIN
            ai_shift = issue.get("shift") or ""
            shift = _match_shift_to_data(ai_shift, data_shifts) if data_shifts else ai_shift

            events.append({
                "reason": reason,
                "start_time": pd.NaT,
                "end_time": pd.NaT,
                "shift": shift,
                "oee_type": "Availability Loss",
                "duration_minutes": duration,
                "source": f"photo:{pname}",
            })

    if not events:
        return None

    events_df = pd.DataFrame(events)
    core_cols = ["reason", "start_time", "end_time", "shift", "oee_type", "duration_minutes"]
    for c in core_cols:
        if c not in events_df.columns:
            events_df[c] = pd.NaT if "time" in c else ""

    # Build reasons_df
    reasons_agg = (
        events_df.groupby("reason")
        .agg(
            total_minutes=("duration_minutes", "sum"),
            total_occurrences=("duration_minutes", "count"),
        )
        .reset_index()
        .sort_values("total_minutes", ascending=False)
    )
    reasons_agg["total_hours"] = (reasons_agg["total_minutes"] / 60).round(1)
    reasons_agg["total_minutes"] = reasons_agg["total_minutes"].round(1)

    # Build shift_reasons_df
    shift_reasons_df = pd.DataFrame()
    if events_df["shift"].str.strip().ne("").any():
        shift_events = events_df[events_df["shift"].str.strip() != ""]
        shift_reasons_df = (
            shift_events.groupby(["shift", "reason"])
            .agg(
                total_minutes=("duration_minutes", "sum"),
                count=("duration_minutes", "count"),
            )
            .reset_index()
            .sort_values(["shift", "total_minutes"], ascending=[True, False])
        )

    return {
        "reasons_df": reasons_agg,
        "events_df": events_df,
        "shift_reasons_df": shift_reasons_df,
        "pareto_df": pd.DataFrame(),
        "findings": [],
        "shift_samples": [],
        "event_samples": [],
        "meta": {"source": "photo_analysis"},
        "oee_summary": {},
        "pareto_raw": pd.DataFrame(),
    }


def analyze_photos(photo_list, api_key, data_shifts=None):
    """Analyze all photos and return (downtime_dict, display_results).

    Parameters
    ----------
    photo_list : list of (name, filepath) tuples
    api_key : str
    data_shifts : list of str, optional
        Shift names from the hourly data for shift name matching.

    Returns
    -------
    (dict or None, list of (name, findings_dict))
    """
    findings_list = []
    photo_names = []
    display_results = []
    primary_model = os.environ.get("OPENAI_VISION_MODEL", "gpt-5-mini")
    fallback_model = os.environ.get("OPENAI_VISION_FALLBACK_MODEL", "gpt-5")
    enable_fallback = os.environ.get("OPENAI_VISION_ENABLE_FALLBACK", "1") != "0"

    for pname, ppath in photo_list:
        findings = analyze_photo(ppath, api_key, model_name=primary_model)

        # If mini returns little/no signal, retry once with a stronger model.
        if enable_fallback and fallback_model and fallback_model != primary_model:
            no_signal = (
                isinstance(findings, dict)
                and "error" not in findings
                and not findings.get("issues")
                and not findings.get("shift_notes")
                and not findings.get("production_notes")
            )
            if no_signal:
                retry = analyze_photo(ppath, api_key, model_name=fallback_model)
                # Use retry only when it improves signal or succeeds where primary errored.
                if (
                    isinstance(retry, dict)
                    and (
                        ("error" not in retry and (retry.get("issues") or retry.get("shift_notes") or retry.get("production_notes")))
                        or ("error" in findings and "error" not in retry)
                    )
                ):
                    findings = retry

        findings_list.append(findings)
        photo_names.append(pname)
        display_results.append((pname, findings))

    dt_dict = findings_to_downtime_dict(findings_list, photo_names, data_shifts)
    return dt_dict, display_results
