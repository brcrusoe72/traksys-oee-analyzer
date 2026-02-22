"""Multi-format parser and tool-enabled agent for operations datasets."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from parse_mes import detect_file_type, parse_event_summary, parse_oee_period_detail
from parse_passdown import parse_passdown


SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".csv", ".json", ".txt", ".log", ".md"}
LINE_RE = re.compile(r"(?:^|[_\-\s])l(?:ine)?\s*([0-9]+)(?:[_\-\s]|$)", re.IGNORECASE)


@dataclass
class ParsedArtifact:
    path: str
    parser: str
    kind: str
    frames: dict[str, pd.DataFrame] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)
    text: str = ""

    def summary(self) -> dict[str, Any]:
        rows_by_frame = {name: int(len(df)) for name, df in self.frames.items()}
        cols_by_frame = {name: list(df.columns) for name, df in self.frames.items()}
        return {
            "path": self.path,
            "kind": self.kind,
            "parser": self.parser,
            "rows_by_frame": rows_by_frame,
            "cols_by_frame": cols_by_frame,
            "meta": dict(self.meta),
            "text_chars": len(self.text or ""),
        }


class VigilDataParser:
    """Parse mixed-format operations data into a common artifact object."""

    def parse_file(self, path: str | Path) -> ParsedArtifact:
        p = Path(path).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"File not found: {p}")
        if not p.is_file():
            raise ValueError(f"Expected a file path, got directory: {p}")

        ext = p.suffix.lower()
        if ext in {".xlsx", ".xls"}:
            return self._parse_excel(p)
        if ext == ".csv":
            df = pd.read_csv(p)
            return ParsedArtifact(path=str(p), parser="pandas.read_csv", kind="csv", frames={"data": df})
        if ext == ".json":
            return self._parse_json(p)
        if ext in {".txt", ".log", ".md"}:
            text = p.read_text(encoding="utf-8", errors="replace")
            return ParsedArtifact(
                path=str(p),
                parser="text_reader",
                kind=ext.lstrip("."),
                text=text,
                meta={"line_count": text.count("\n") + (1 if text else 0)},
            )
        raise ValueError(f"Unsupported file extension: {ext}")

    def _parse_excel(self, path: Path) -> ParsedArtifact:
        first_sheet_df = pd.read_excel(path, sheet_name=0)
        first_cols = {str(c).strip() for c in first_sheet_df.columns}

        if self._looks_like_event_overview(first_cols):
            return self._parse_event_overview(path, first_sheet_df)
        if self._looks_like_oee_overview(first_cols):
            return self._parse_oee_overview(path, first_sheet_df)

        detected = detect_file_type(str(path))
        if detected == "oee_period_detail":
            hourly, shift_summary, overall, hour_avg = parse_oee_period_detail(str(path))
            return ParsedArtifact(
                path=str(path),
                parser="parse_mes.parse_oee_period_detail",
                kind="oee_period_detail",
                frames={
                    "hourly": hourly,
                    "shift_summary": shift_summary,
                    "overall": overall,
                    "hour_avg": hour_avg,
                },
            )

        if detected == "event_summary":
            parsed = parse_event_summary(str(path))
            return ParsedArtifact(
                path=str(path),
                parser="parse_mes.parse_event_summary",
                kind="event_summary",
                frames={
                    "reasons_df": parsed.get("reasons_df", pd.DataFrame()),
                    "events_df": parsed.get("events_df", pd.DataFrame()),
                    "shift_reasons_df": parsed.get("shift_reasons_df", pd.DataFrame()),
                },
                meta={"line": parsed.get("line")},
            )

        if detected == "passdown":
            parsed = parse_passdown(str(path))
            return ParsedArtifact(
                path=str(path),
                parser="parse_passdown.parse_passdown",
                kind="passdown",
                frames={
                    "reasons_df": parsed.get("reasons_df", pd.DataFrame()),
                    "events_df": parsed.get("events_df", pd.DataFrame()),
                    "shift_reasons_df": parsed.get("shift_reasons_df", pd.DataFrame()),
                },
            )

        sheets = pd.read_excel(path, sheet_name=None)
        norm_sheets = {name: df for name, df in sheets.items() if isinstance(df, pd.DataFrame)}
        return ParsedArtifact(
            path=str(path),
            parser="pandas.read_excel",
            kind="excel_generic",
            frames=norm_sheets,
            meta={"sheet_names": list(norm_sheets)},
        )

    @staticmethod
    def _looks_like_event_overview(columns: set[str]) -> bool:
        required = {"EventID", "StartDateTimeOffset", "DurationSeconds"}
        return required.issubset(columns)

    @staticmethod
    def _looks_like_oee_overview(columns: set[str]) -> bool:
        required = {"GroupLabel", "OeeDecimal", "IntervalSeconds"}
        return required.issubset(columns)

    @staticmethod
    def _shift_for_timestamp(ts: datetime | pd.Timestamp | None) -> str:
        if ts is None or pd.isna(ts):
            return ""
        hour = int(ts.hour)
        if 7 <= hour < 15:
            return "1st Shift"
        if 15 <= hour < 23:
            return "2nd Shift"
        return "3rd Shift"

    @staticmethod
    def _extract_line_from_name(name: str) -> str:
        m = LINE_RE.search(name)
        if not m:
            return ""
        return f"Line {m.group(1)}"

    def _parse_event_overview(self, path: Path, df: pd.DataFrame) -> ParsedArtifact:
        work = df.copy()
        work["StartDateTimeOffset"] = pd.to_datetime(work.get("StartDateTimeOffset"), errors="coerce")
        work["EndDateTimeOffset"] = pd.to_datetime(work.get("EndDateTimeOffset"), errors="coerce")
        work["DurationSeconds"] = pd.to_numeric(work.get("DurationSeconds"), errors="coerce").fillna(0)
        work["EventCategoryName"] = work.get("EventCategoryName", "").fillna("")
        work["EventDefinitionName"] = work.get("EventDefinitionName", "").fillna("")
        work["OeeEventTypeName"] = work.get("OeeEventTypeName", "").fillna("")
        work["SystemName"] = work.get("SystemName", "").fillna("")

        # Drop header-style metadata rows that are embedded in the export.
        work = work[work["StartDateTimeOffset"].notna()].copy()
        work = work[work["DurationSeconds"] >= 0].copy()

        work["duration_minutes"] = (work["DurationSeconds"] / 60.0).round(1)
        work["reason"] = work["EventCategoryName"].astype(str).str.strip()
        work.loc[work["reason"] == "", "reason"] = work["EventDefinitionName"].astype(str).str.strip()
        work["shift"] = work["StartDateTimeOffset"].apply(self._shift_for_timestamp)

        events_df = pd.DataFrame(
            {
                "reason": work["reason"],
                "start_time": work["StartDateTimeOffset"],
                "end_time": work["EndDateTimeOffset"],
                "shift": work["shift"],
                "oee_type": work["OeeEventTypeName"],
                "duration_minutes": work["duration_minutes"],
                "system_name": work["SystemName"],
                "event_definition": work["EventDefinitionName"],
            }
        )
        events_df = events_df[events_df["reason"].astype(str).str.strip() != ""].copy()

        reasons_df = (
            events_df.groupby("reason", as_index=False)
            .agg(
                total_minutes=("duration_minutes", "sum"),
                total_occurrences=("duration_minutes", "count"),
            )
            .sort_values("total_minutes", ascending=False)
        )
        reasons_df["total_hours"] = (reasons_df["total_minutes"] / 60.0).round(1)
        reasons_df["total_minutes"] = reasons_df["total_minutes"].round(1)

        shift_reasons_df = (
            events_df.groupby(["shift", "reason"], as_index=False)
            .agg(total_minutes=("duration_minutes", "sum"), count=("duration_minutes", "count"))
            .sort_values(["shift", "total_minutes"], ascending=[True, False])
        )

        return ParsedArtifact(
            path=str(path),
            parser="vigil_event_overview_parser",
            kind="event_overview",
            frames={
                "events_df": events_df,
                "reasons_df": reasons_df,
                "shift_reasons_df": shift_reasons_df,
            },
            meta={"source_format": "event_overview"},
        )

    def _parse_oee_overview(self, path: Path, df: pd.DataFrame) -> ParsedArtifact:
        work = df.copy()
        work["GroupValue"] = pd.to_datetime(work.get("GroupValue"), errors="coerce")
        work["OeeDecimal"] = pd.to_numeric(work.get("OeeDecimal"), errors="coerce")
        work["AvailabilityDecimal"] = pd.to_numeric(work.get("AvailabilityDecimal"), errors="coerce")
        work["PerformanceDecimal"] = pd.to_numeric(work.get("PerformanceDecimal"), errors="coerce")
        work["QualityDecimal"] = pd.to_numeric(work.get("QualityDecimal"), errors="coerce")
        work["TotalDisplayUnits"] = pd.to_numeric(work.get("TotalDisplayUnits"), errors="coerce")
        work["GroupLabel"] = work.get("GroupLabel", "").astype(str)

        work = work[work["GroupValue"].notna()].copy()
        work = work[work["OeeDecimal"].notna()].copy()
        work["oee_pct"] = work["OeeDecimal"] * 100.0
        line = self._extract_line_from_name(path.stem)

        summary_df = pd.DataFrame(
            {
                "timestamp": work["GroupValue"],
                "group_label": work["GroupLabel"],
                "line": line or "Unknown",
                "oee_pct": work["oee_pct"],
                "availability_pct": work["AvailabilityDecimal"] * 100.0,
                "performance_pct": work["PerformanceDecimal"] * 100.0,
                "quality_pct": work["QualityDecimal"] * 100.0,
                "total_display_units": work["TotalDisplayUnits"],
            }
        )

        return ParsedArtifact(
            path=str(path),
            parser="vigil_oee_overview_parser",
            kind="oee_overview",
            frames={"summary_df": summary_df},
            meta={"source_format": "oee_overview", "line": line},
        )

    def _parse_json(self, path: Path) -> ParsedArtifact:
        data = json.loads(path.read_text(encoding="utf-8"))
        frames: dict[str, pd.DataFrame] = {}
        meta: dict[str, Any] = {"json_type": type(data).__name__}

        if isinstance(data, list):
            frames["data"] = pd.json_normalize(data)
            meta["item_count"] = len(data)
        elif isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list):
                    frames[key] = pd.json_normalize(value)
            if not frames:
                frames["data"] = pd.json_normalize(data)
            meta["keys"] = sorted(list(data.keys()))
        else:
            frames["data"] = pd.DataFrame({"value": [data]})

        return ParsedArtifact(path=str(path), parser="json.loads+pandas.json_normalize", kind="json", frames=frames, meta=meta)


class VigilToolAgent:
    """Simple tool-using agent for local data parsing and querying."""

    def __init__(self, parser: VigilDataParser | None = None):
        self.parser = parser or VigilDataParser()
        self._cache: dict[str, ParsedArtifact] = {}

    def tool_scan_directory(self, directory: str | Path) -> dict[str, Any]:
        root = Path(directory).expanduser().resolve()
        if not root.exists() or not root.is_dir():
            raise ValueError(f"Invalid directory: {root}")

        files = [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS]
        by_ext: dict[str, int] = {}
        for p in files:
            by_ext[p.suffix.lower()] = by_ext.get(p.suffix.lower(), 0) + 1

        return {
            "directory": str(root),
            "file_count": len(files),
            "by_extension": dict(sorted(by_ext.items())),
            "files": [str(p) for p in sorted(files)],
        }

    def tool_parse_file(self, file_path: str | Path) -> dict[str, Any]:
        artifact = self.parser.parse_file(file_path)
        self._cache[artifact.path] = artifact
        return artifact.summary()

    def tool_summarize_dataset(self, directory: str | Path) -> dict[str, Any]:
        scan = self.tool_scan_directory(directory)
        kinds: dict[str, int] = {}
        rows_by_kind: dict[str, int] = {}
        errors: list[dict[str, str]] = []

        for file_path in scan["files"]:
            try:
                parsed = self.parser.parse_file(file_path)
                self._cache[parsed.path] = parsed
                kinds[parsed.kind] = kinds.get(parsed.kind, 0) + 1
                total_rows = sum(len(df) for df in parsed.frames.values())
                rows_by_kind[parsed.kind] = rows_by_kind.get(parsed.kind, 0) + int(total_rows)
            except Exception as exc:  # pragma: no cover - guarded by tests
                errors.append({"path": file_path, "error": str(exc)})

        return {
            "scan": scan,
            "kinds": dict(sorted(kinds.items())),
            "rows_by_kind": dict(sorted(rows_by_kind.items())),
            "parsed_files": len(scan["files"]) - len(errors),
            "errors": errors,
        }

    def tool_query(self, question: str, directory: str | Path) -> dict[str, Any]:
        q = question.lower().strip()
        if not q:
            return {"question": question, "answer": "Empty question."}

        summary = self.tool_summarize_dataset(directory)
        cache = list(self._cache.values())

        if "top downtime" in q or ("downtime" in q and "cause" in q):
            reasons = []
            for artifact in cache:
                if "reasons_df" in artifact.frames:
                    rdf = artifact.frames["reasons_df"]
                    if {"reason", "total_minutes"}.issubset(rdf.columns):
                        reasons.append(rdf[["reason", "total_minutes"]])
            if reasons:
                merged = pd.concat(reasons, ignore_index=True)
                top = (
                    merged.groupby("reason", as_index=False)["total_minutes"]
                    .sum()
                    .sort_values("total_minutes", ascending=False)
                    .head(10)
                )
                return {
                    "question": question,
                    "answer": "Top downtime causes found.",
                    "top_downtime_causes": top.to_dict(orient="records"),
                }
            return {"question": question, "answer": "No downtime cause table found in parsed files."}

        if "oee" in q and ("best" in q or "worst" in q or "line" in q):
            records = []
            for artifact in cache:
                if artifact.kind == "oee_period_detail" and "hourly" in artifact.frames:
                    h = artifact.frames["hourly"]
                    needed = {"line", "oee_pct"}
                    if needed.issubset(h.columns):
                        records.append(h[list(needed)])
                if artifact.kind == "oee_overview" and "summary_df" in artifact.frames:
                    s = artifact.frames["summary_df"]
                    needed = {"line", "oee_pct"}
                    if needed.issubset(s.columns):
                        records.append(s[list(needed)])
            if records:
                merged = pd.concat(records, ignore_index=True)
                line_oee = merged.groupby("line", as_index=False)["oee_pct"].mean().sort_values("oee_pct")
                best = line_oee.tail(1).to_dict(orient="records")[0]
                worst = line_oee.head(1).to_dict(orient="records")[0]
                return {
                    "question": question,
                    "answer": "Computed line-level OEE ranking.",
                    "best_line": best,
                    "worst_line": worst,
                }
            return {"question": question, "answer": "No OEE-by-line records found."}

        return {
            "question": question,
            "answer": (
                "Parsed dataset summary available. Supported query intents: "
                "'top downtime causes' and 'best/worst OEE line'."
            ),
            "dataset_summary": summary,
        }

    def run(self, instruction: str, directory: str | Path) -> dict[str, Any]:
        """Route a plain-language instruction to an internal tool."""
        i = instruction.lower().strip()
        if any(k in i for k in ["scan", "list files", "inventory"]):
            return {"tool": "scan_directory", "result": self.tool_scan_directory(directory)}
        if any(k in i for k in ["summary", "summarize", "overview"]):
            return {"tool": "summarize_dataset", "result": self.tool_summarize_dataset(directory)}
        if any(k in i for k in ["query", "ask", "top downtime", "oee"]):
            return {"tool": "query", "result": self.tool_query(instruction, directory)}
        return {"tool": "summarize_dataset", "result": self.tool_summarize_dataset(directory)}
