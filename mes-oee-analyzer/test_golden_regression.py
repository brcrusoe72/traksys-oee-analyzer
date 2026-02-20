import json
from pathlib import Path

import pandas as pd

from analyze import _aggregate_oee


def test_golden_weighted_summary_regression():
    fixture_dir = Path(__file__).parent / "tests" / "fixtures"
    df = pd.read_csv(fixture_dir / "golden_hourly.csv")
    expected = json.loads((fixture_dir / "golden_summary.txt").read_text())

    avail, perf, qual, oee = _aggregate_oee(df)

    assert round(avail, 2) == expected["availability"]
    assert round(perf, 2) == expected["performance"]
    assert round(qual, 2) == expected["quality"]
    assert round(oee, 2) == expected["oee"]
