from shared import load_standards_reference


def test_standards_reference_file_loads_with_expected_columns():
    df = load_standards_reference()
    assert len(df) >= 10
    assert {"line", "product", "target_cases_8h", "cases_per_pallet", "pcs_per_case"}.issubset(df.columns)


def test_standards_reference_contains_line2_8pack_target():
    df = load_standards_reference()
    row = df[(df["line"] == "Line 2") & (df["product"] == "8 Pack")]
    assert len(row) == 1
    assert int(row.iloc[0]["target_cases_8h"]) == 30000
