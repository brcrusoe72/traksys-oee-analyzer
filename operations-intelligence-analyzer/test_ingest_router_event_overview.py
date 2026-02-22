from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ingest_router import ingest_uploaded_inputs


@dataclass
class _UploadStub:
    name: str
    _bytes: bytes

    def getbuffer(self):
        return self._bytes


def _make_upload(path: Path) -> _UploadStub:
    return _UploadStub(name=path.name, _bytes=path.read_bytes())


def test_ingest_event_overview_downtime(tmp_path):
    # Minimal OEE workbook that existing loader can parse.
    oee_path = tmp_path / "OEE Overview_L2_hour.xlsx"
    oee_df = pd.DataFrame(
        [
            {
                "GroupValue": "2026-02-20 07:00:00",
                "GroupLabel": "2026-02-20 07:00:00",
                "OeeDecimal": 0.42,
                "AvailabilityDecimal": 0.8,
                "PerformanceDecimal": 0.7,
                "QualityDecimal": 0.95,
                "IntervalSeconds": 3600,
            }
        ]
    )
    oee_df.to_excel(oee_path, sheet_name="Data", index=False)

    # Event Overview downtime workbook (the format that previously raised "Unrecognized downtime format").
    dt_path = tmp_path / "Event OverviewL2_event.xlsx"
    dt_df = pd.DataFrame(
        [
            {
                "EventID": 1,
                "StartDateTimeOffset": "2026-02-20 07:00:00",
                "EndDateTimeOffset": "2026-02-20 07:10:00",
                "Date": "2026-02-20",
                "DurationSeconds": 600,
                "SystemName": "Line 2 - Flex",
                "EventDefinitionName": "Downtime",
                "EventCategoryName": "Hydraulics",
                "OeeEventTypeName": "Availability Loss",
            }
        ]
    )
    dt_df.to_excel(dt_path, sheet_name="Data", index=False)

    bundle = ingest_uploaded_inputs(
        oee_files=[_make_upload(oee_path)],
        downtime_files=[_make_upload(dt_path)],
        context_files=[],
        tmp_dir=str(tmp_path),
    )

    assert not any("Unrecognized downtime format" in w for w in bundle.meta.warning_messages)
    assert "Line 2" in bundle.downtime_by_line
    assert len(bundle.downtime_by_line["Line 2"]) == 1
    dt_data = bundle.downtime_by_line["Line 2"][0]
    assert "reasons_df" in dt_data
    assert len(dt_data["reasons_df"]) == 1
