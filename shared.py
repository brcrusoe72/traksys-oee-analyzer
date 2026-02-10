"""
Shared constants and utilities for Traksys OEE Analyzer
========================================================
Single source of truth for product normalization, fault classification,
equipment keywords, and related helpers used across analyze.py,
third_shift_report.py, and third_shift_targets.py.
"""

import pandas as pd

# ---------------------------------------------------------------------------
# Exclude from actionable downtime analysis
# ---------------------------------------------------------------------------
EXCLUDE_REASONS = {"Not Scheduled", "Break-Lunch", "Lunch (Comida)", "Meetings"}

# ---------------------------------------------------------------------------
# Equipment keyword list — used to classify reason codes as equipment faults
# ---------------------------------------------------------------------------
EQUIPMENT_KEYWORDS = [
    "caser", "palletizer", "conveyor", "tray packer", "shrink tunnel",
    "labeler", "wrapper", "depal", "spiral", "x-ray", "printer",
    "ryson", "whallon", "bear", "diagraph", "domino", "highlight",
    "inspec", "kayat", "pai", "riverwood", "laner", "filler",
    "seamer", "closer", "feeder", "hopper", "accumulator",
]

PROCESS_KEYWORDS = [
    "day code", "changeover", "startup", "shutdown", "cip",
    "sanitation", "clean", "setup", "product change", "sku change",
]

SCHEDULED_KEYWORDS = [
    "not scheduled", "break", "lunch", "meeting", "comida", "training",
]


def classify_fault(reason):
    """Classify a downtime reason into a fault category."""
    r = reason.lower().strip()
    if any(kw in r for kw in ["unassigned", "unknown"]):
        return "Data Gap (uncoded)"
    if any(kw in r for kw in SCHEDULED_KEYWORDS):
        return "Scheduled / Non-Production"
    if "short stop" in r:
        return "Micro Stops"
    if any(kw in r for kw in PROCESS_KEYWORDS):
        return "Process / Changeover"
    if any(kw in r for kw in EQUIPMENT_KEYWORDS):
        return "Equipment / Mechanical"
    # Default: if it has a dash (like "Machine - Brand"), assume equipment
    if " - " in r or "-" in r:
        return "Equipment / Mechanical"
    return "Other / Unclassified"


# ---------------------------------------------------------------------------
# Product name normalization
# ---------------------------------------------------------------------------
# Operator-entered names are inconsistent. Map them to clean families.
PRODUCT_NORMALIZE = {
    # Cut Green Beans 8pk (1200 cpm)
    "dm cut gr bn": "Cut Green Beans 8pk",
    "dm cut grn beans": "Cut Green Beans 8pk",
    "dm cut grn bean": "Cut Green Beans 8pk",
    # Cut Green Beans 12pk (1200 cpm)
    "dm cut gr bn 12pk": "Cut Green Beans 12pk",
    "dm cut gr gn 12pk": "Cut Green Beans 12pk",
    "dm cut grn bean 12pk": "Cut Green Beans 12pk",
    # WK Corn 12pk (1200 cpm)
    "dm wk corn 12pk": "WK Corn 12pk",
    "dm wk corn": "WK Corn 12pk",
    # WK Gold Corn 8pk (1200 cpm)
    "dm wk gold corn": "WK Gold Corn 8pk",
    "dm wk gold corn 12pk": "WK Gold Corn 8pk",
    "dm wk gld corn": "WK Gold Corn 8pk",
    # Sweet Peas 8pk (1200 cpm)
    "dm swt peas": "Sweet Peas 8pk",
    # Sliced Peaches trayed (572 cpm)
    "dm slc yc peaches jce sams": "Sliced Peaches (trayed)",
    "dm slc peaches jce sams": "Sliced Peaches (trayed)",
    "dm slc yc peaches jce": "Sliced Peaches (trayed)",
    "dm sliced pch 100 jc": "Sliced Peaches (trayed)",
    # Pears trayed (720 cpm)
    "dm sliced pears": "Pears (trayed)",
    "dm pear halves": "Pears (trayed)",
    "dm sliced pears nsa": "Pears (trayed)",
    # Mexican Style Corn (1200 cpm)
    "dm mexican style sw corn": "Mexican Style Corn",
    # Whole Kernel Corn trayed (572 cpm)
    "dm whole kernel corn": "WK Corn (trayed)",
}


def normalize_product(name):
    """Map messy operator-entered product names to clean family names."""
    if not name or pd.isna(name):
        return "Unknown"
    key = name.strip().lower()
    return PRODUCT_NORMALIZE.get(key, name.strip())


# ---------------------------------------------------------------------------
# Product reference data
# ---------------------------------------------------------------------------
PRODUCT_RATED_SPEED = {
    "Cut Green Beans 8pk": 1200,
    "Cut Green Beans 12pk": 1200,
    "WK Corn 12pk": 1200,
    "WK Gold Corn 8pk": 1200,
    "Sweet Peas 8pk": 1200,
    "Sliced Peaches (trayed)": 572,
    "Pears (trayed)": 720,
    "Mexican Style Corn": 1200,
    "WK Corn (trayed)": 572,
}

PRODUCT_PACK_TYPE = {
    "Cut Green Beans 8pk": "Standard (8pk)",
    "Cut Green Beans 12pk": "Standard (12pk)",
    "WK Corn 12pk": "Standard (12pk)",
    "WK Gold Corn 8pk": "Standard (8pk)",
    "Sweet Peas 8pk": "Standard (8pk)",
    "Sliced Peaches (trayed)": "Trayed (6/4)",
    "Pears (trayed)": "Trayed (6/4)",
    "Mexican Style Corn": "Standard (8pk)",
    "WK Corn (trayed)": "Trayed (6/4)",
}

PRODUCT_PACK = {
    "Cut Green Beans 8pk": "8pk",
    "Cut Green Beans 12pk": "12pk",
    "WK Corn 12pk": "12pk",
    "WK Gold Corn 8pk": "8pk",
    "Sweet Peas 8pk": "8pk",
    "Mexican Style Corn": "8pk",
    "Sliced Peaches (trayed)": "Trayed",
    "Pears (trayed)": "Trayed",
    "WK Corn (trayed)": "Trayed",
}

# Per-shift case targets
PRODUCT_TARGET = {
    "Cut Green Beans 8pk": 30000,
    "Cut Green Beans 12pk": 25000,
    "WK Corn 12pk": 25000,
    "WK Gold Corn 8pk": 30000,
    "Sweet Peas 8pk": 30000,
    "Mexican Style Corn": 30000,
    "Sliced Peaches (trayed)": 5000,
    "Pears (trayed)": 10000,
    "WK Corn (trayed)": 5000,
}

IS_TRAYED = {k for k, v in PRODUCT_PACK.items() if v == "Trayed"}
