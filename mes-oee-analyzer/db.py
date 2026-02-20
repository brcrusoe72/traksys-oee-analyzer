"""
Supabase Database Layer for MES OEE Analyzer
=================================================
Equipment knowledge, downtime baselines, and persistent run history.

All functions gracefully return None/empty when no database is configured.
The app works fine without Supabase — the database is an enhancement.

Connection: set SUPABASE_URL and SUPABASE_KEY as environment variables
or in Streamlit secrets (.streamlit/secrets.toml or Cloud dashboard).
"""

import os
from datetime import datetime

import pandas as pd

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
_client = None
_equipment_cache = None
_equipment_cache_ts = None
_CACHE_TTL = 300  # 5 minutes


def get_client():
    """Get Supabase client. Returns None if not configured."""
    global _client
    if _client is not None:
        return _client

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")

    # Try Streamlit secrets
    try:
        import streamlit as st
        url = url or st.secrets.get("SUPABASE_URL", "")
        key = key or st.secrets.get("SUPABASE_KEY", "")
    except Exception:
        pass

    if not url or not key:
        return None

    try:
        from supabase import create_client
        _client = create_client(url, key)
        return _client
    except Exception:
        return None


def is_connected():
    """Check if database is available."""
    return get_client() is not None


# ---------------------------------------------------------------------------
# Equipment Knowledge
# ---------------------------------------------------------------------------
def get_all_equipment():
    """Fetch all equipment entries. Cached for 5 minutes.
    Returns list of dicts or empty list."""
    global _equipment_cache, _equipment_cache_ts

    # Check cache
    if _equipment_cache is not None and _equipment_cache_ts is not None:
        age = (datetime.now() - _equipment_cache_ts).total_seconds()
        if age < _CACHE_TTL:
            return _equipment_cache

    client = get_client()
    if client is None:
        return []

    try:
        resp = client.table("equipment").select("*").execute()
        _equipment_cache = resp.data or []
        _equipment_cache_ts = datetime.now()
        return _equipment_cache
    except Exception:
        return []


def get_equipment_for_cause(cause_text):
    """Match a downtime cause string to the best equipment entry.

    Scores each equipment row by how many of its cause_keywords appear
    in the cause text. Returns the best match dict, or None.
    """
    all_equip = get_all_equipment()
    if not all_equip:
        return None

    cause_lower = cause_text.lower()
    best_match = None
    best_score = 0

    for equip in all_equip:
        keywords = equip.get("cause_keywords", [])
        if not keywords:
            continue
        score = sum(1 for kw in keywords if kw.lower() in cause_lower)
        if score > best_score:
            best_score = score
            best_match = equip

    return best_match if best_score > 0 else None


# ---------------------------------------------------------------------------
# Downtime Baselines
# ---------------------------------------------------------------------------
def get_all_baselines():
    """Fetch all downtime baselines. Returns list of dicts or empty list."""
    client = get_client()
    if client is None:
        return []

    try:
        resp = client.table("downtime_baselines").select("*").execute()
        return resp.data or []
    except Exception:
        return []


def get_baseline(cause):
    """Get baseline for a specific cause. Returns dict or None."""
    client = get_client()
    if client is None:
        return None

    try:
        resp = (
            client.table("downtime_baselines")
            .select("*")
            .eq("cause", cause)
            .limit(1)
            .execute()
        )
        return resp.data[0] if resp.data else None
    except Exception:
        return None


def upsert_baseline(cause, avg_minutes, std_minutes=0, min_minutes=0,
                    max_minutes=0, n_events=0):
    """Insert or update a downtime baseline."""
    client = get_client()
    if client is None:
        return False

    try:
        client.table("downtime_baselines").upsert({
            "cause": cause,
            "avg_minutes": round(float(avg_minutes), 1),
            "std_minutes": round(float(std_minutes), 1),
            "min_minutes": round(float(min_minutes), 1),
            "max_minutes": round(float(max_minutes), 1),
            "n_events": int(n_events),
            "last_updated": datetime.now().isoformat(),
        }, on_conflict="cause").execute()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Run History
# ---------------------------------------------------------------------------
def save_run_to_db(record):
    """Save a run record to run_history. Returns True/False."""
    client = get_client()
    if client is None:
        return False

    try:
        client.table("run_history").upsert({
            "run_id": record["run_id"],
            "date_from": record["date_from"],
            "date_to": record["date_to"],
            "n_days": record.get("n_days", 1),
            "avg_oee": record.get("avg_oee"),
            "avg_availability": record.get("avg_availability"),
            "avg_performance": record.get("avg_performance"),
            "avg_quality": record.get("avg_quality"),
            "utilization": record.get("utilization"),
            "avg_cph": record.get("avg_cph"),
            "total_cases": record.get("total_cases"),
            "total_hours": record.get("total_hours"),
            "cases_lost": record.get("cases_lost"),
            "shifts": record.get("shifts", []),
            "top_downtime": record.get("top_downtime", []),
        }, on_conflict="run_id").execute()
        return True
    except Exception:
        return False


def load_runs_from_db():
    """Load all runs from run_history. Returns DataFrame or None."""
    client = get_client()
    if client is None:
        return None

    try:
        resp = (
            client.table("run_history")
            .select("*")
            .order("date_from")
            .execute()
        )
        if not resp.data:
            return None
        return pd.DataFrame(resp.data)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Smart Action Item Synthesis
# ---------------------------------------------------------------------------
def build_smart_action_items(dt_classes, runs_df, shift_trends=None):
    """Build equipment-aware, baseline-aware action items from trend data.

    Uses:
      - dt_classes: output of _classify_downtime() — chronic/emerging/intermittent
      - runs_df: DataFrame with run-level OEE data
      - shift_trends: output of _analyze_shifts() — per-shift direction/stats
      - Equipment knowledge from database (if configured)
      - Downtime baselines from database (if configured)

    Returns list of dicts with keys: priority, finding, action.
    Always returns at least deterministic items even without DB.
    """
    items = []
    n_runs = len(runs_df)

    # --- Downtime-based items ---
    for rank, dt in enumerate(dt_classes[:5]):
        cause = dt["cause"]
        status = dt["status"]
        total_min = dt["total_minutes"]
        appearances = dt["appearances"]
        streak = dt["current_streak"]
        times_rank1 = dt.get("times_rank1", 0)
        avg_per_report = total_min / max(appearances, 1)

        # Look up equipment knowledge + baseline
        equip = get_equipment_for_cause(cause)
        baseline = get_baseline(cause)

        # --- Build finding ---
        finding_parts = [f"{cause}: {status.upper()}"]
        finding_parts.append(f"{total_min:,.0f} min across {appearances}/{n_runs} reports")

        if times_rank1 > 0:
            finding_parts.append(f"#1 loss in {times_rank1} reports")

        # Baseline comparison
        if baseline and baseline.get("avg_minutes", 0) > 0:
            bl_avg = baseline["avg_minutes"]
            bl_std = baseline.get("std_minutes", 0)
            if bl_std > 0 and avg_per_report > bl_avg + bl_std:
                finding_parts.append(
                    f"avg {avg_per_report:.0f} min/report vs {bl_avg:.0f} min baseline -- above normal"
                )

        # Expected repair time comparison
        if equip:
            exp_min = equip.get("expected_repair_hrs_min")
            exp_max = equip.get("expected_repair_hrs_max")
            if exp_min and exp_max:
                avg_hrs = avg_per_report / 60
                if avg_hrs > exp_max:
                    finding_parts.append(
                        f"expected {exp_min}-{exp_max} hrs, actual avg {avg_hrs:.1f} hrs -- needs investigation"
                    )
                else:
                    finding_parts.append(f"expected {exp_min}-{exp_max} hrs, actual avg {avg_hrs:.1f} hrs")

        finding = " | ".join(finding_parts)

        # --- Build action ---
        action_parts = []

        # Equipment-specific fixes
        if equip:
            fixes = equip.get("common_fixes", [])
            if fixes:
                action_parts.append(f"Known fixes: {', '.join(fixes[:3])}.")
            machine = equip.get("machine", "")
            failure = equip.get("failure_mode", "")
            if machine and failure:
                action_parts.append(f"[{machine} / {failure}]")
            elif machine:
                action_parts.append(f"[{machine}]")

        # Classification-specific guidance
        if status == "chronic":
            action_parts.append(
                "Target 50% reduction. Pull event logs, 5-why top events by shift/time/product, "
                "implement countermeasures, track weekly."
            )
        elif status == "emerging":
            action_parts.append(
                f"Appeared last {streak} consecutive reports -- emerging pattern. "
                "Investigate what changed recently."
            )
        else:
            action_parts.append("Intermittent -- monitor, address if pattern develops.")

        action = " ".join(action_parts)

        items.append({
            "priority": rank + 1,
            "finding": finding,
            "action": action,
        })

    # --- Shift-level items ---
    if shift_trends:
        for sname, sdata in shift_trends.items():
            if sdata.get("direction") == "declining" and sdata.get("total_runs", 0) >= 3:
                items.append({
                    "priority": len(items) + 1,
                    "finding": (
                        f"{sname}: declining 3 consecutive reports "
                        f"(current {sdata['current_oee']:.1f}%, "
                        f"4-run avg {sdata['4run_avg']:.1f}%)"
                    ),
                    "action": "Investigate what changed -- staffing, product mix, equipment condition.",
                })
            if (sdata.get("runs_below_plant_mean", 0) >=
                    sdata.get("total_runs", 0) * 0.8 and sdata.get("total_runs", 0) >= 3):
                items.append({
                    "priority": len(items) + 1,
                    "finding": (
                        f"{sname}: below plant mean in "
                        f"{sdata['runs_below_plant_mean']}/{sdata['total_runs']} reports"
                    ),
                    "action": "Consistent underperformer -- compare practices with best-performing shift.",
                })

    return items
