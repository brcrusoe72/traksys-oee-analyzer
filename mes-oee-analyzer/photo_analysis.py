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


def build_photo_narrative(display_results, shift_filter=None):
    """Build a short narrative paragraph from photo analysis results.

    Parameters
    ----------
    display_results : list of (name, findings_dict) tuples
    shift_filter : str, optional
        If provided, only include issues whose AI-assigned shift matches
        this prefix (e.g. "1st", "2nd", "3rd").  Issues with no shift
        assigned are always included.

    Returns a string suitable for appending to shift narratives, or ""
    if there's nothing useful to report.
    """
    all_issues = []
    all_notes = []
    for pname, findings in display_results:
        if not findings or "error" in findings:
            continue
        for issue in findings.get("issues", []):
            # Filter by shift if requested
            if shift_filter:
                issue_shift = issue.get("shift", "") or ""
                prefix = _SHIFT_PREFIXES.get(issue_shift, issue_shift.split()[0] if issue_shift else "")
                if prefix and prefix.lower() != shift_filter.lower():
                    continue  # skip issues assigned to a different shift
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


def _extract_json(text):
    """Extract a JSON object from model output that may contain extra text.

    Handles: bare JSON, markdown fences, preamble text before JSON, and
    reasoning model outputs that wrap JSON in prose.
    Raises json.JSONDecodeError if no valid JSON object can be found.
    """
    # 1. Try the raw text directly.
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 2. Strip markdown code fences (```json ... ``` or ``` ... ```).
    fenced = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text)
    if fenced:
        try:
            return json.loads(fenced.group(1))
        except json.JSONDecodeError:
            pass

    # 3. Find the first { ... } block (greedy match for outermost braces).
    brace_start = text.find("{")
    if brace_start != -1:
        # Walk forward to find matching closing brace.
        depth = 0
        for i in range(brace_start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    candidate = text[brace_start:i + 1]
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        break

    # Nothing worked — raise so caller can handle it.
    raise json.JSONDecodeError("No valid JSON object found", text, 0)


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


def _retune_create_kwargs_for_param_error(create_kwargs, error_text, is_reasoning):
    """Retune OpenAI request kwargs for unsupported-parameter errors.

    Returns True if kwargs were changed and a retry should be attempted.
    Returns False when the error is not a recognized parameter-compatibility case.
    """
    err = (error_text or "")
    err_lower = err.lower()
    if not (
        "unsupported_parameter" in err_lower
        or "unsupported parameter" in err_lower
        or "unsupported_value" in err_lower
        or "unsupported value" in err_lower
    ):
        return False

    # Keep the existing token budget when swapping token parameter names.
    token_budget = (
        create_kwargs.get("max_completion_tokens")
        or create_kwargs.get("max_tokens")
        or (8000 if is_reasoning else 2000)
    )

    unsupported = None
    suggested = None
    m_unsupported = re.search(r"Unsupported parameter:\s*'([^']+)'", err, re.IGNORECASE)
    if m_unsupported:
        unsupported = m_unsupported.group(1)
    m_suggested = re.search(r"Use\s*'([^']+)'\s*instead", err, re.IGNORECASE)
    if m_suggested:
        suggested = m_suggested.group(1)

    changed = False
    if unsupported and unsupported in create_kwargs:
        create_kwargs.pop(unsupported, None)
        changed = True

    # Select token parameter using explicit server guidance first.
    if suggested in ("max_completion_tokens", "max_tokens"):
        create_kwargs.pop("max_completion_tokens", None)
        create_kwargs.pop("max_tokens", None)
        create_kwargs[suggested] = token_budget
        changed = True
    elif unsupported in ("max_completion_tokens", "max_tokens"):
        preferred = "max_completion_tokens" if is_reasoning else "max_completion_tokens"
        create_kwargs.pop("max_completion_tokens", None)
        create_kwargs.pop("max_tokens", None)
        create_kwargs[preferred] = token_budget
        changed = True

    # Reasoning families generally reject sampling controls.
    if is_reasoning:
        for param in ("temperature", "top_p", "presence_penalty", "frequency_penalty"):
            if param in create_kwargs:
                create_kwargs.pop(param, None)
                changed = True
    else:
        if "reasoning_effort" in create_kwargs:
            create_kwargs.pop("reasoning_effort", None)
            changed = True

    return changed


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

    # Defaults are environment-configurable. Keep primary/fallback in analyze_photos().
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

    # Reasoning models (o1, o3, gpt-5) don't support temperature, top_p, or
    # max_tokens, and need a larger token budget for hidden reasoning tokens.
    _reasoning_prefixes = ("o1", "o3", "gpt-5")
    is_reasoning = model_name.lower().startswith(_reasoning_prefixes)

    create_kwargs = {
        "model": model_name,
        "messages": messages,
    }
    if is_reasoning:
        create_kwargs["max_completion_tokens"] = 8000
        create_kwargs["reasoning_effort"] = "low"
    else:
        create_kwargs["max_completion_tokens"] = 2000
        create_kwargs["temperature"] = 0.1

    try:
        resp = None
        last_exc = None
        for _ in range(3):
            try:
                resp = client.chat.completions.create(**create_kwargs)
                break
            except Exception as e:
                last_exc = e
                if not _retune_create_kwargs_for_param_error(create_kwargs, str(e), is_reasoning):
                    raise
        if resp is None and last_exc is not None:
            raise last_exc
        content = resp.choices[0].message.content
        if not content or not content.strip():
            finish = getattr(resp.choices[0], "finish_reason", "unknown")
            return {"error": f"Empty response from model (finish_reason={finish})",
                    "issues": [], "production_notes": [], "shift_notes": [],
                    "raw_text": ""}
        raw = content.strip()
        return _extract_json(raw)
    except json.JSONDecodeError:
        return {"error": f"Could not parse response: {raw[:300]}",
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
    fallback_model = os.environ.get("OPENAI_VISION_FALLBACK_MODEL", "gpt-5.1")
    enable_fallback = os.environ.get("OPENAI_VISION_ENABLE_FALLBACK", "1") != "0"

    for pname, ppath in photo_list:
        findings = analyze_photo(ppath, api_key, model_name=primary_model)

        # If primary returns no signal or an error, retry with fallback model.
        if enable_fallback and fallback_model and fallback_model != primary_model:
            no_signal = (
                isinstance(findings, dict)
                and (
                    "error" in findings
                    or (
                        not findings.get("issues")
                        and not findings.get("shift_notes")
                        and not findings.get("production_notes")
                    )
                )
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
