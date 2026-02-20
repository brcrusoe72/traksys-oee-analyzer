import pandas as pd

from operations_intelligence import (
    score_action_items,
    build_shift_handoff_packet,
    detect_trend_anomalies,
)


def test_score_action_items_orders_by_expected_value():
    items = [
        {"finding": "A", "action": "x", "estimated_cases_lost": 1000, "confidence": 0.9, "effort": 3},
        {"finding": "B", "action": "x", "estimated_cases_lost": 600, "confidence": 0.8, "effort": 1},
    ]
    scored = score_action_items(items)
    assert scored[0]["finding"] == "B"
    assert scored[0]["priority"] == 1


def test_build_shift_handoff_packet_contains_required_sections():
    txt = build_shift_handoff_packet(
        "3rd Shift",
        "2026-02-01 to 2026-02-07",
        ["Loss 1", "Loss 2"],
        [{"finding": "Loss 1", "action": "Do thing"}],
    )
    assert "SHIFT HANDOFF PACKET" in txt
    assert "Top Losses" in txt
    assert "Owner:" in txt


def test_detect_trend_anomalies_flags_oee_drop_and_emerging():
    runs = pd.DataFrame({"avg_oee": [78.0, 77.5, 72.0]})
    dt = [
        {"cause": "Caser - Riverwood", "status": "emerging", "current_streak": 2},
    ]
    flags = detect_trend_anomalies(runs, dt)
    assert any("Plant OEE alert" in f for f in flags)
    assert any("Emerging downtime alert" in f for f in flags)
