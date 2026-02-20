"""Canonical dataframe validation/coercion for analysis boundary."""

from __future__ import annotations

import pandas as pd


_REQUIRED_HOURLY = [
    "date_str",
    "shift",
    "shift_hour",
    "total_hours",
    "total_cases",
    "availability",
    "performance",
    "quality",
    "oee_pct",
    "line",
]

_NUMERIC_HOURLY = [
    "shift_hour",
    "total_hours",
    "total_cases",
    "availability",
    "performance",
    "quality",
    "oee_pct",
    "good_cases",
    "bad_cases",
    "intervals",
]


def validate_and_coerce_ingest_frames(hourly: pd.DataFrame, shift_summary: pd.DataFrame):
    """Ensure ingest outputs satisfy the analyzer's canonical frame contract."""
    warnings: list[str] = []
    h = hourly.copy()
    ss = shift_summary.copy()

    if "line" not in h.columns:
        h["line"] = "All"
        warnings.append("Hourly input missing `line`; defaulted to 'All'.")
    h["line"] = h["line"].fillna("All").astype(str).str.strip().replace("", "All")

    if "date_str" not in h.columns:
        if "date" in h.columns:
            h["date_str"] = pd.to_datetime(h["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            warnings.append("Hourly input missing `date_str`; derived from `date`.")
        elif "shift_date" in h.columns:
            h["date_str"] = pd.to_datetime(h["shift_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            warnings.append("Hourly input missing `date_str`; derived from `shift_date`.")

    if "date" not in h.columns and "date_str" in h.columns:
        h["date"] = pd.to_datetime(h["date_str"], errors="coerce")
        warnings.append("Hourly input missing `date`; derived from `date_str`.")
    elif "date" in h.columns:
        h["date"] = pd.to_datetime(h["date"], errors="coerce")

    if "good_cases" not in h.columns:
        h["good_cases"] = h.get("total_cases", 0)
    if "bad_cases" not in h.columns:
        h["bad_cases"] = 0
    if "intervals" not in h.columns:
        h["intervals"] = 1

    if "shift" in h.columns:
        h["shift"] = h["shift"].fillna("").astype(str).str.strip()

    for col in _NUMERIC_HOURLY:
        if col in h.columns:
            h[col] = pd.to_numeric(h[col], errors="coerce").fillna(0)

    # Normalize percentages if source gave 0-100 scale for A/P/Q.
    for pct_col in ["availability", "performance", "quality"]:
        if pct_col in h.columns:
            vmax = float(h[pct_col].max()) if len(h[pct_col]) > 0 else 0
            if vmax > 1.5:
                h[pct_col] = h[pct_col] / 100.0
                warnings.append(f"`{pct_col}` appeared to be 0-100 scale; normalized to 0-1.")

    if "oee_pct" in h.columns:
        vmax = float(h["oee_pct"].max()) if len(h["oee_pct"]) > 0 else 0
        if 0 < vmax <= 1.5:
            h["oee_pct"] = h["oee_pct"] * 100.0
            warnings.append("`oee_pct` appeared to be 0-1 scale; normalized to 0-100.")

    missing = [c for c in _REQUIRED_HOURLY if c not in h.columns]
    if missing:
        raise ValueError(f"Ingest output missing required hourly columns: {', '.join(missing)}")

    h = h[h["date_str"].notna() & (h["date_str"].astype(str).str.len() > 0)]
    h = h[h["shift"].astype(str).str.len() > 0]
    h = h.drop_duplicates(subset=["date_str", "shift", "shift_hour", "line"], keep="first").reset_index(drop=True)

    if len(h) == 0:
        raise ValueError("Ingest output has no valid hourly rows after canonical validation.")

    # Keep shift_summary minimally aligned if present.
    if len(ss) > 0:
        if "shift_date" in ss.columns:
            ss["shift_date"] = pd.to_datetime(ss["shift_date"], errors="coerce")
        if "shift" in ss.columns:
            ss["shift"] = ss["shift"].fillna("").astype(str).str.strip()

    return h, ss, warnings
