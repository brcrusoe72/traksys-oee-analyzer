"""
Shared constants and utilities for Traksys OEE Analyzer
========================================================
Single source of truth for product normalization, fault classification,
equipment keywords, and related helpers used across analyze.py
and shift_report.py.
"""

import os
import pandas as pd

# ---------------------------------------------------------------------------
# Shift duration — always 8 hours at this plant
# ---------------------------------------------------------------------------
SHIFT_HOURS = 8.0

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


# ---------------------------------------------------------------------------
# Plant production targets — cases per 8-hour shift by line and product
# Source: Del Monte Rochelle plant standards
# ---------------------------------------------------------------------------
PLANT_TARGETS = {
    "Line 1": {"6-Trayed": 4000, "6-Shrink": 3500, "6-Shrink/28oz": 3500},
    "Line 2": {"6 Pack": 7800, "8 Pack": 30000, "12 Pack": 25000},
    "Line 3": {"12 Pack": 11600, "24 Pack": 6000},
    "Line 4": {"6/4 Pack": 4800, "6 Pack": 4500, "12 Pack": 7800, "24 Pack": 3000},
    "Line 5": {"12 Pack": 7800, "24 Pack": 3000},
}

# Cases per pallet and pieces per case (reference)
PALLET_AND_PIECE = {
    "Line 1": {"6-Trayed": (56, 6), "6-Shrink": (56, 6), "6-Shrink/28oz": (200, 6)},
    "Line 2": {"6 Pack": (102, 6), "8 Pack": (240, 8), "12 Pack": (176, 12)},
    "Line 3": {"12 Pack": (204, 12), "24 Pack": (102, 24)},
    "Line 4": {"6/4 Pack": (77, 24), "6 Pack": (102, 6), "12 Pack": (204, 12), "24 Pack": (102, 24)},
    "Line 5": {"12 Pack": (204, 12), "24 Pack": (102, 24)},
}

# Map product_code from Traksys OEE Period Detail to pack size label
_PRODUCT_CODE_TO_PACK = {
    "6PK": "6 Pack", "6pk": "6 Pack", "6-TRAYED": "6-Trayed", "6-SHRINK": "6-Shrink",
    "8PK": "8 Pack", "8pk": "8 Pack",
    "12PK": "12 Pack", "12pk": "12 Pack",
    "24PK": "24 Pack", "24pk": "24 Pack",
    "6/4PK": "6/4 Pack", "6/4pk": "6/4 Pack",
    "Labeled_STD_300_12": "12 Pack", "Labeled_STD_300_24": "24 Pack",
}

# Auto-detect line from product codes that only run on one line
_PRODUCT_CODE_TO_LINE = {
    "Labeled_STD_300_12": "Line 3",
    "Labeled_STD_300_24": "Line 3",
}

# Also map normalized product family names to pack sizes
_FAMILY_TO_PACK = {
    "Cut Green Beans 8pk": "8 Pack", "WK Gold Corn 8pk": "8 Pack",
    "Sweet Peas 8pk": "8 Pack", "Mexican Style Corn": "8 Pack",
    "Cut Green Beans 12pk": "12 Pack", "WK Corn 12pk": "12 Pack",
    "Sliced Peaches (trayed)": "6/4 Pack", "Pears (trayed)": "6/4 Pack",
    "WK Corn (trayed)": "6/4 Pack",
}


def get_target_cph(product_code, line="Line 2"):
    """Get target cases per hour for a product on a given line.

    Accepts raw Traksys product_code (e.g. '8PK') or normalized family name.
    Returns target CPH (shift target / 8) or None if unknown.
    """
    if not product_code or pd.isna(product_code):
        return None
    code = str(product_code).strip()

    # Try direct product_code → pack mapping
    pack = _PRODUCT_CODE_TO_PACK.get(code) or _PRODUCT_CODE_TO_PACK.get(code.upper())
    # Try normalized family name → pack mapping
    if not pack:
        pack = _FAMILY_TO_PACK.get(code)
    # Try extracting digits + "pk" pattern
    if not pack:
        import re
        m = re.search(r'(\d+)\s*(?:pk|pack)', code, re.IGNORECASE)
        if m:
            n = m.group(1)
            pack = _PRODUCT_CODE_TO_PACK.get(f"{n}PK")

    # Auto-detect line when caller uses the default and the product code
    # is known to belong to a specific line (e.g. Line 3 products).
    if line == "Line 2" and code in _PRODUCT_CODE_TO_LINE:
        line = _PRODUCT_CODE_TO_LINE[code]

    if pack and line in PLANT_TARGETS and pack in PLANT_TARGETS[line]:
        return PLANT_TARGETS[line][pack] / 8.0  # 8-hour shift → CPH
    return None


# ---------------------------------------------------------------------------
# Equipment scanning — used by shift_report.py for operator note analysis
# ---------------------------------------------------------------------------
# Consolidated from both report and target-tracker scripts.
# Riverwood runs ALL products. Kayat (tray packer, shrink tunnel, wrapper)
# only runs trayed.
EQUIPMENT_SCAN = {
    "Riverwood": [
        "riverwood", "caser", "fiber jam", "fiber mispick",
        "fiber getting caught", "misformed cases", "ripping cases",
        "misshapped cases", "open flaps", "plastic drive bar",
    ],
    "Kayat (Tray/Shrink/Wrap)": [
        "tray packer", "kayat", "shrink tunnel", "shrink wrapper",
        "double-wrapped",
    ],
    "Labeler": [
        "bear labeler", "labeler a", "labeler b", "label machine",
        "flappers", "shiners", "shinner", "ripped labels",
        "loose labels", "labels weren't sticking", "curling bar",
        "label fingers",
    ],
    "Palletizer": [
        "palletizer", "misformed layers", "misshapped layers",
        "misshappen", "pallet conveyor",
    ],
    "Conveyors": [
        "conveyor", "conveyers", "overhead conveyor",
        "accumulation table", "overhead conveypr",
    ],
    "Depal": ["depal", "suction cup"],
    "Spiral": ["ryson", "spiral"],
    "Printer": ["diagraph", "print and apply", "laser jet", "laser printer", "no print"],
    "Stacker": ["double stacker", "case stacker"],
    "X-Ray": ["x-ray", "x ray"],
}


def extract_equipment_mentions(notes):
    """Scan operator notes for equipment names. Returns list of equipment mentioned."""
    if not notes or pd.isna(notes):
        return []
    text = notes.lower()
    found = []
    for equip_name, keywords in EQUIPMENT_SCAN.items():
        if any(kw in text for kw in keywords):
            found.append(equip_name)
    return found


def summarize_issues(notes):
    """Short issue summary from operator notes. No fluff."""
    if not notes or pd.isna(notes):
        return ""
    parts = [s.strip() for s in str(notes).split(";;") if s.strip()]
    key = []
    for part in parts:
        lower = part.lower().strip()
        if "x-ray" in lower and "failed" not in lower:
            continue
        if "both passed" in lower or "both yes" in lower:
            continue
        if lower.startswith(("set-up:", "start up:", "starting")) and len(part) < 45:
            continue
        clean = part.strip().rstrip(";").strip()
        if clean:
            key.append(clean)
    result = "; ".join(key[:2])  # Max 2 issues — keep it short
    return result[:180] + "..." if len(result) > 180 else result


def classify_support(equipment_list, notes):
    """Short support classification from equipment mentions and notes."""
    if not equipment_list:
        return ""
    needs = []
    equip_set = set(equipment_list)
    if "Riverwood" in equip_set:
        needs.append("Caser")
    if "Kayat (Tray/Shrink/Wrap)" in equip_set:
        needs.append("Kayat")
    if "Labeler" in equip_set:
        needs.append("Labeler")
    if "Palletizer" in equip_set:
        needs.append("Palletizer")
    if "Conveyors" in equip_set:
        needs.append("Conveyor")
    if "Depal" in equip_set:
        needs.append("Depal")
    if "Spiral" in equip_set:
        needs.append("Spiral")

    if notes:
        lower = str(notes).lower()
        if any(w in lower for w in ["labor", "checker", "short staff", "no checker"]):
            needs.append("Staffing")

    if len(equip_set) >= 3:
        return "MULTIPLE"
    return ", ".join(needs) if needs else ""


# ---------------------------------------------------------------------------
# Standards reference table (line/product targets and packaging constants)
# ---------------------------------------------------------------------------
def load_standards_reference():
    """Load standards reference used for UI/reference defaults.

    Returns a DataFrame with columns:
      line, product, target_cases_8h, cases_per_pallet, pcs_per_case
    """
    here = os.path.dirname(__file__)
    fp = os.path.join(here, "standards_reference.csv")
    if os.path.exists(fp):
        return pd.read_csv(fp)

    rows = []
    for line, products in PLANT_TARGETS.items():
        for product, target in products.items():
            pallet, pcs = PALLET_AND_PIECE.get(line, {}).get(product, (None, None))
            rows.append({
                "line": line,
                "product": product,
                "target_cases_8h": target,
                "cases_per_pallet": pallet,
                "pcs_per_case": pcs,
            })
    return pd.DataFrame(rows)
