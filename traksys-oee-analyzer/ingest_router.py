"""Source-agnostic ingestion router for Daily Analysis uploads."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import pandas as pd

from analyze import load_downtime_data, load_oee_data
from parse_mes import detect_file_type, parse_event_summary, parse_oee_period_detail


@dataclass
class IngestMeta:
    detected_mode: str
    detected_sources: list[str] = field(default_factory=list)
    parser_chain: list[str] = field(default_factory=list)
    confidence: float = 1.0
    info_messages: list[str] = field(default_factory=list)
    warning_messages: list[str] = field(default_factory=list)

    def to_record(self) -> dict:
        return {
            "detected_mode": self.detected_mode,
            "detected_sources": list(self.detected_sources),
            "parser_chain": list(self.parser_chain),
            "confidence": float(self.confidence),
            "warning_count": len(self.warning_messages),
        }


@dataclass
class IngestBundle:
    hourly: pd.DataFrame
    shift_summary: pd.DataFrame
    downtime_by_line: dict
    context_photos: list[tuple[str, str]]
    meta: IngestMeta


def _write_uploaded_file(uploaded_file, tmp_dir: str) -> str:
    path = os.path.join(tmp_dir, uploaded_file.name)
    with open(path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return path


def ingest_uploaded_inputs(oee_files, downtime_files, context_files, tmp_dir: str) -> IngestBundle:
    """Parse uploaded inputs and return canonical dataframes plus ingest metadata."""
    all_hourly: list[pd.DataFrame] = []
    all_shift_summary: list[pd.DataFrame] = []
    dt_by_line: dict[str, list[dict]] = {}
    context_photos: list[tuple[str, str]] = []

    detected_sources: set[str] = set()
    parser_chain: list[str] = []
    info_messages: list[str] = []
    warning_messages: list[str] = []

    # OEE files (structured)
    for oee_file in oee_files or []:
        oee_path = _write_uploaded_file(oee_file, tmp_dir)
        file_type = detect_file_type(oee_path)
        if file_type == "oee_period_detail":
            info_messages.append(f"Detected: {oee_file.name} - OEE Period Detail")
            h, ss, _ov, _ha = parse_oee_period_detail(oee_path)
            detected_sources.add("oee_period_detail")
            parser_chain.append("parse_mes.parse_oee_period_detail")
        else:
            h, ss, _ov, _ha = load_oee_data(oee_path)
            detected_sources.add("oee_workbook")
            parser_chain.append("analyze.load_oee_data")

        if "line" not in h.columns:
            h["line"] = "All"
        all_hourly.append(h)
        all_shift_summary.append(ss)

    if not all_hourly:
        raise ValueError("No valid OEE data parsed from uploaded files.")

    hourly = pd.concat(all_hourly, ignore_index=True)
    if "line" not in hourly.columns:
        hourly["line"] = "All"
    hourly["line"] = hourly["line"].fillna("All")
    hourly = hourly.drop_duplicates(subset=["date_str", "shift", "shift_hour", "line"], keep="first")

    shift_summary = pd.concat(all_shift_summary, ignore_index=True)
    shift_summary = shift_summary.drop_duplicates(subset=["shift_date", "shift"], keep="first")

    # Downtime files (structured)
    for dt_file in downtime_files or []:
        dt_path = _write_uploaded_file(dt_file, tmp_dir)
        try:
            if dt_file.name.lower().endswith(".json"):
                dt_data = load_downtime_data(dt_path)
                line_key = dt_data.get("line") or "All"
                dt_by_line.setdefault(line_key, []).append(dt_data)
                detected_sources.add("downtime_json")
                parser_chain.append("analyze.load_downtime_data")
            else:
                dt_type = detect_file_type(dt_path)
                if dt_type == "event_summary":
                    dt_data = parse_event_summary(dt_path)
                    line_key = dt_data.get("line") or "All"
                    info_messages.append(f"Detected: {dt_file.name} - Event Summary ({line_key})")
                    dt_by_line.setdefault(line_key, []).append(dt_data)
                    detected_sources.add("event_summary")
                    parser_chain.append("parse_mes.parse_event_summary")
                elif dt_type == "passdown":
                    from parse_passdown import parse_passdown

                    dt_data = parse_passdown(dt_path)
                    line_key = dt_data.get("line") or "All"
                    info_messages.append(f"Detected: {dt_file.name} - Shift Passdown")
                    dt_by_line.setdefault(line_key, []).append(dt_data)
                    detected_sources.add("passdown")
                    parser_chain.append("parse_passdown.parse_passdown")
                else:
                    warning_messages.append(f"Unrecognized downtime format: {dt_file.name}")
        except Exception as e:
            warning_messages.append(f"Could not load {dt_file.name}: {e}")

    # Context files (mixed: structured + unstructured)
    if context_files:
        from parse_passdown import detect_passdown, parse_passdown

        for cf in context_files:
            cf_path = _write_uploaded_file(cf, tmp_dir)
            name_lower = cf.name.lower()
            if name_lower.endswith((".png", ".jpg", ".jpeg")):
                context_photos.append((cf.name, cf_path))
                detected_sources.add("photo_context")
                continue

            if name_lower.endswith((".xlsx", ".xls")):
                try:
                    if detect_passdown(cf_path):
                        extra = parse_passdown(cf_path)
                        line_key = extra.get("line") or "All"
                        dt_by_line.setdefault(line_key, []).append(extra)
                        info_messages.append(
                            f"Context: {cf.name} - Shift Passdown ({len(extra.get('events_df', []))} events)"
                        )
                        detected_sources.add("passdown_context")
                        parser_chain.append("parse_passdown.parse_passdown")
                    else:
                        info_messages.append(f"Context: {cf.name} - uploaded (not a recognized structured format)")
                        detected_sources.add("context_excel_unstructured")
                except Exception as e:
                    warning_messages.append(f"Could not parse {cf.name}: {e}")

    has_structured = len(all_hourly) > 0 or bool(dt_by_line)
    has_unstructured = len(context_photos) > 0
    if has_structured and has_unstructured:
        detected_mode = "mixed"
    elif has_structured:
        detected_mode = "structured"
    else:
        detected_mode = "unstructured"

    confidence = 1.0 if detected_mode == "structured" else (0.9 if detected_mode == "mixed" else 0.6)
    if warning_messages:
        confidence = max(0.5, confidence - min(0.3, 0.05 * len(warning_messages)))

    meta = IngestMeta(
        detected_mode=detected_mode,
        detected_sources=sorted(detected_sources),
        parser_chain=parser_chain,
        confidence=round(confidence, 2),
        info_messages=info_messages,
        warning_messages=warning_messages,
    )

    return IngestBundle(
        hourly=hourly,
        shift_summary=shift_summary,
        downtime_by_line=dt_by_line,
        context_photos=context_photos,
        meta=meta,
    )
