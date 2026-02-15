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


def _image_media_type(filepath):
    """Return MIME media type for an image file."""
    ext = os.path.splitext(filepath)[1].lower()
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
    }.get(ext, "image/jpeg")


def get_openai_api_key():
    """Resolve OpenAI API key from environment or Streamlit secrets."""
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


def analyze_photo(filepath, api_key):
    """Send one image to GPT vision and return parsed findings.

    Returns dict with keys: photo_type, confidence, issues, production_notes,
    shift_notes, raw_text.  Returns {"error": "..."} on failure.
    """
    from openai import OpenAI

    with open(filepath, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")

    media_type = _image_media_type(filepath)
    client = OpenAI(api_key=api_key)

    try:
        resp = client.chat.completions.create(
            model="gpt-5-mini",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": _build_prompt()},
                    {"type": "image_url", "image_url": {
                        "url": f"data:{media_type};base64,{b64}",
                    }},
                ],
            }],
            temperature=0.1,
            max_tokens=1500,
        )
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


def findings_to_downtime_dict(findings_list, photo_names):
    """Convert extracted issues into standard downtime dict format.

    Parameters
    ----------
    findings_list : list of dict
        One findings dict per photo (from analyze_photo).
    photo_names : list of str
        Corresponding photo filenames.

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
            duration = float(issue.get("duration_minutes") or 0)
            shift = issue.get("shift") or ""

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


def analyze_photos(photo_list, api_key):
    """Analyze all photos and return (downtime_dict, display_results).

    Parameters
    ----------
    photo_list : list of (name, filepath) tuples
    api_key : str

    Returns
    -------
    (dict or None, list of (name, findings_dict))
    """
    findings_list = []
    photo_names = []
    display_results = []

    for pname, ppath in photo_list:
        findings = analyze_photo(ppath, api_key)
        findings_list.append(findings)
        photo_names.append(pname)
        display_results.append((pname, findings))

    dt_dict = findings_to_downtime_dict(findings_list, photo_names)
    return dt_dict, display_results
