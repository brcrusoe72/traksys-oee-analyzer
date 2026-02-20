"""Operational intelligence helpers for prioritization, handoff packets, and anomaly flags."""

from __future__ import annotations

from datetime import datetime

import pandas as pd


def score_action_items(items):
    """Rank action items by expected-value score.

    Score model:
      opportunity_score = estimated_cases_lost (or inferred from finding text)
      confidence_score = confidence (0-1, default 0.7)
      effort_penalty = effort (1-5, default 3)
      score = opportunity_score * confidence_score / effort_penalty
    """
    scored = []
    for item in items:
        opp = float(item.get("estimated_cases_lost", 0) or 0)
        confidence = float(item.get("confidence", 0.7) or 0.7)
        effort = max(float(item.get("effort", 3) or 3), 1.0)
        score = opp * confidence / effort
        enriched = dict(item)
        enriched["priority_score"] = round(score, 1)
        scored.append(enriched)

    scored.sort(key=lambda x: x.get("priority_score", 0), reverse=True)
    for idx, item in enumerate(scored, start=1):
        item["priority"] = idx
    return scored


def build_shift_handoff_packet(shift_name, period_label, top_losses, actions):
    """Build a text handoff packet for shift-to-shift execution."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        "SHIFT HANDOFF PACKET",
        f"Generated: {now}",
        f"Shift: {shift_name}",
        f"Period: {period_label}",
        "",
        "Top Losses:",
    ]
    for i, loss in enumerate(top_losses[:3], start=1):
        lines.append(f"{i}. {loss}")

    lines.append("")
    lines.append("Containment / Next Actions:")
    for i, action in enumerate(actions[:3], start=1):
        finding = action.get("finding", "")
        steps = action.get("action", "")
        lines.append(f"{i}. {finding}")
        lines.append(f"   - Action: {steps}")
        lines.append("   - Owner: __________________")
        lines.append("   - Due: ____________________")

    lines.append("")
    lines.append("Verification:")
    lines.append("- Confirm first-hour performance after handoff.")
    lines.append("- Confirm top-loss countermeasure is active.")
    return "\n".join(lines)


def detect_trend_anomalies(runs_df, dt_classes):
    """Detect high-signal trend anomalies for operations alerts."""
    anomalies = []
    if runs_df is None or len(runs_df) < 3:
        return anomalies

    oee = pd.to_numeric(runs_df["avg_oee"], errors="coerce").dropna()
    if len(oee) >= 3:
        baseline = float(oee.iloc[:-1].mean()) if len(oee) > 1 else float(oee.mean())
        latest = float(oee.iloc[-1])
        delta = latest - baseline
        if delta <= -3.0:
            anomalies.append(
                f"Plant OEE alert: latest run is {abs(delta):.1f} pts below trailing baseline ({latest:.1f}% vs {baseline:.1f}%)."
            )

    for dt in dt_classes[:5]:
        if dt.get("status") == "emerging" and dt.get("current_streak", 0) >= 2:
            anomalies.append(
                f"Emerging downtime alert: {dt['cause']} appeared in {dt['current_streak']} consecutive runs."
            )
        if dt.get("status") == "chronic" and dt.get("times_rank1", 0) >= 2:
            anomalies.append(
                f"Chronic downtime alert: {dt['cause']} ranked #1 in {dt['times_rank1']} runs."
            )

    return anomalies
