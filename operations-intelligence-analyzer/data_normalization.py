"""Shared dataframe normalization utilities for analysis ingestion."""

from datetime import datetime
import re
import pandas as pd


# Maps normalized header names found in user uploads to internal column names.
HEADER_TO_INTERNAL = {
    # Date / time columns
    "date": "shift_date",
    "shiftdate": "shift_date",
    "shift": "shift",
    "hour": "shift_hour",
    "shifthour": "shift_hour",
    "starttime": "time_block",
    "timeblock": "time_block",
    "blockstart": "block_start",
    "blockend": "block_end",
    # Volume / duration
    "hours": "total_hours",
    "durationhours": "total_hours",
    "totalhours": "total_hours",
    "productcode": "product_code",
    "job": "job",
    "goodcases": "good_cases",
    "badcases": "bad_cases",
    "totalcases": "total_cases",
    "casesperhour": "cases_per_hour",
    "cases/hr": "cases_per_hour",
    "cph": "cases_per_hour",
    # OEE metrics
    "oee": "oee_pct",
    "oeepct": "oee_pct",
    "oee(%)": "oee_pct",
    "avgoee": "oee_pct",
    "availability": "availability",
    "avgavailability": "availability",
    "performance": "performance",
    "avgperformance": "performance",
    "quality": "quality",
    # Counts
    "intervals": "intervals",
    "nintervals": "n_intervals",
    "hourblocks": "hour_blocks",
}

NUMERIC_COLUMNS = {
    "shift_hour", "total_hours", "total_cases", "cases_per_hour",
    "oee_pct", "availability", "performance", "quality",
    "good_cases", "bad_cases", "intervals", "n_intervals", "hour_blocks",
}


def _collapse_duplicate_columns(df):
    """Merge duplicate-named columns by taking the first non-null value per row."""
    if not df.columns.duplicated().any():
        return df

    out = pd.DataFrame(index=df.index)
    seen = set()
    for col in df.columns:
        if col in seen:
            continue
        seen.add(col)
        data = df.loc[:, df.columns == col]
        if isinstance(data, pd.DataFrame) and data.shape[1] > 1:
            out[col] = data.bfill(axis=1).iloc[:, 0]
        else:
            out[col] = data.iloc[:, 0] if isinstance(data, pd.DataFrame) else data
    return out


def normalize_col(name):
    """Normalize a column header for fuzzy matching."""
    s = str(name).lower().strip()
    return re.sub(r"[^a-z0-9]+", "", s)


def smart_rename(df, expected_columns):
    """Rename DataFrame columns using header-name matching, falling back to positional."""
    header_map = {}
    claimed = set()
    for col in df.columns:
        norm = normalize_col(col)
        if norm in HEADER_TO_INTERNAL:
            internal = HEADER_TO_INTERNAL[norm]
            if internal not in claimed:
                header_map[col] = internal
                claimed.add(internal)

    expected_set = set(expected_columns)
    matched = claimed & expected_set

    if len(matched) >= max(2, len(expected_set) * 0.3):
        return df.rename(columns=header_map)

    if len(df.columns) == len(expected_columns):
        df.columns = expected_columns
        return df

    if header_map:
        return df.rename(columns=header_map)

    raise ValueError(
        f"Cannot map columns: expected {len(expected_columns)} columns "
        f"({', '.join(expected_columns[:5])}...), "
        f"got {len(df.columns)} columns ({', '.join(str(c) for c in df.columns[:5])}...)"
    )


def coerce_numerics(df):
    """Ensure columns that should be numeric are actually numeric."""
    df = _collapse_duplicate_columns(df)
    for col in df.columns:
        if col in NUMERIC_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def derive_columns(df):
    """Compute missing derived columns from available data."""
    df = _collapse_duplicate_columns(df)
    if "total_cases" not in df.columns and "good_cases" in df.columns:
        if "bad_cases" in df.columns:
            df["total_cases"] = df["good_cases"] + df["bad_cases"]
        else:
            df["total_cases"] = df["good_cases"]

    if "cases_per_hour" not in df.columns:
        if "total_cases" in df.columns and "total_hours" in df.columns:
            mask = df["total_hours"] > 0
            df["cases_per_hour"] = 0.0
            df.loc[mask, "cases_per_hour"] = (
                df.loc[mask, "total_cases"] / df.loc[mask, "total_hours"]
            )
        else:
            df["cases_per_hour"] = 0.0

    if "oee_pct" in df.columns:
        oee_vals = pd.to_numeric(df["oee_pct"], errors="coerce").dropna()
        if len(oee_vals) > 0 and oee_vals.max() <= 1.0:
            df["oee_pct"] = pd.to_numeric(df["oee_pct"], errors="coerce") * 100

    if "time_block" in df.columns:
        sample = df["time_block"].dropna().head(5)
        if len(sample) > 0:
            first = sample.iloc[0]
            if isinstance(first, (pd.Timestamp, datetime)):
                df["time_block"] = df["time_block"].apply(
                    lambda x: x.strftime("%H:%M") if isinstance(x, (pd.Timestamp, datetime)) else str(x)
                )

    if "time_block" not in df.columns:
        if "shift_hour" in df.columns:
            df["time_block"] = df["shift_hour"].apply(
                lambda h: f"{int(h)}:00" if pd.notna(h) else ""
            )
        else:
            df["time_block"] = ""

    return df
