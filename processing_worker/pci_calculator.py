"""
processing_worker/pci_calculator.py
=====================================
ASTM D6433-based Pavement Condition Index (PCI) calculator.

PCI = 100 - CDV

CDV (Corrected Deduct Value) is found by the iterative method:
  1. Compute a Deduct Value (DV) for each distress type + severity combination
     using density = (distress_area / pavement_area) × 100
  2. Sort DVs descending; set q = count of DVs > 2.0
  3. Look up CDV from correction curve for (TDV, q)
  4. Replace smallest DV > 2.0 with 2.0; q = q - 1; repeat until q == 1
  5. PCI = 100 - max(CDV across all iterations)
"""

# ─── Label → ASTM distress type mapping ─────────────────────────────────────
LABEL_TO_DISTRESS = {
    "pothole":            "pothole",
    "alligator_crack":    "alligator_crack",
    "longitudinal_crack": "longitudinal_crack",
    "transverse_crack":   "transverse_crack",
    "rutting":            "rutting",
    "road_crack":         "longitudinal_crack",
}

# ─── Deduct Value (DV) tables ────────────────────────────────────────────────
# (density_percent, deduct_value) breakpoints — piecewise linear interpolation.
# Values derived from ASTM D6433 published DV curves for asphalt pavement.

_DV_TABLE = {
    # Alligator (fatigue) cracking — area-based density
    ("alligator_crack", "low"):    [(0.1, 2),  (1, 12), (5, 27), (10, 38), (20, 44), (50, 53), (100, 58)],
    ("alligator_crack", "medium"): [(0.1, 9),  (1, 21), (5, 38), (10, 48), (20, 56), (50, 65), (100, 69)],
    ("alligator_crack", "high"):   [(0.1, 17), (1, 34), (5, 52), (10, 62), (20, 68), (50, 77), (100, 82)],

    # Longitudinal cracking — area-based density
    ("longitudinal_crack", "low"):    [(0.1, 2),  (1, 7),  (5, 14), (10, 19), (20, 24), (50, 33), (100, 40)],
    ("longitudinal_crack", "medium"): [(0.1, 5),  (1, 14), (5, 25), (10, 32), (20, 40), (50, 51), (100, 59)],
    ("longitudinal_crack", "high"):   [(0.1, 10), (1, 23), (5, 37), (10, 46), (20, 54), (50, 64), (100, 72)],

    # Transverse cracking — same curves as longitudinal
    ("transverse_crack", "low"):    [(0.1, 2),  (1, 7),  (5, 14), (10, 19), (20, 24), (50, 33), (100, 40)],
    ("transverse_crack", "medium"): [(0.1, 5),  (1, 14), (5, 25), (10, 32), (20, 40), (50, 51), (100, 59)],
    ("transverse_crack", "high"):   [(0.1, 10), (1, 23), (5, 37), (10, 46), (20, 54), (50, 64), (100, 72)],

    # Potholes — area-based density (% of pavement area)
    ("pothole", "low"):    [(0.01, 5),  (0.1, 16), (0.5, 30), (1, 38), (5, 60), (10, 72), (20, 82)],
    ("pothole", "medium"): [(0.01, 10), (0.1, 25), (0.5, 42), (1, 52), (5, 72), (10, 80), (20, 88)],
    ("pothole", "high"):   [(0.01, 15), (0.1, 35), (0.5, 55), (1, 65), (5, 80), (10, 87), (20, 93)],

    # Rutting — area-based density
    ("rutting", "low"):    [(0.1, 2),  (1, 8),  (5, 16), (10, 22), (20, 28), (50, 38), (100, 46)],
    ("rutting", "medium"): [(0.1, 6),  (1, 18), (5, 32), (10, 40), (20, 48), (50, 58), (100, 64)],
    ("rutting", "high"):   [(0.1, 12), (1, 28), (5, 48), (10, 58), (20, 65), (50, 73), (100, 79)],
}

# ─── CDV correction curves ───────────────────────────────────────────────────
# CDV = f(TDV, q) — ASTM D6433 graphical correction curves for asphalt pavement.
# q = number of individual deduct values > 2.0 in the current iteration.

_CDV_TABLE = {
    1: [(0, 0), (5, 5),  (10, 9),  (20, 17), (30, 24), (40, 31), (50, 38),
        (60, 45), (70, 51), (80, 57), (90, 63), (100, 69), (120, 79), (140, 88), (160, 96)],
    2: [(0, 0), (5, 4),  (10, 7),  (20, 13), (30, 18), (40, 24), (50, 29),
        (60, 35), (70, 40), (80, 45), (90, 51), (100, 56), (120, 65), (140, 73), (160, 82)],
    3: [(0, 0), (5, 3),  (10, 6),  (20, 11), (30, 15), (40, 20), (50, 24),
        (60, 29), (70, 33), (80, 37), (90, 42), (100, 46), (120, 55), (140, 63), (160, 71)],
    4: [(0, 0), (5, 3),  (10, 5),  (20, 9),  (30, 13), (40, 17), (50, 21),
        (60, 24), (70, 28), (80, 32), (90, 36), (100, 40), (120, 48), (140, 55), (160, 63)],
    5: [(0, 0), (5, 2),  (10, 4),  (20, 8),  (30, 11), (40, 14), (50, 18),
        (60, 21), (70, 24), (80, 28), (90, 31), (100, 35), (120, 42), (140, 49), (160, 55)],
    6: [(0, 0), (5, 2),  (10, 4),  (20, 7),  (30, 9),  (40, 12), (50, 15),
        (60, 18), (70, 20), (80, 23), (90, 26), (100, 29), (120, 35), (140, 41), (160, 47)],
    7: [(0, 0), (5, 2),  (10, 3),  (20, 6),  (30, 8),  (40, 10), (50, 13),
        (60, 15), (70, 17), (80, 20), (90, 22), (100, 25), (120, 30), (140, 36), (160, 41)],
    8: [(0, 0), (5, 1),  (10, 3),  (20, 5),  (30, 7),  (40, 9),  (50, 11),
        (60, 13), (70, 15), (80, 17), (90, 19), (100, 22), (120, 26), (140, 31), (160, 36)],
}


def _interp(table, x):
    """Piecewise linear interpolation from sorted (x, y) breakpoints."""
    if x <= table[0][0]:
        return float(table[0][1])
    if x >= table[-1][0]:
        return float(table[-1][1])
    for i in range(1, len(table)):
        x0, y0 = table[i - 1]
        x1, y1 = table[i]
        if x0 <= x <= x1:
            t = (x - x0) / (x1 - x0)
            return y0 + t * (y1 - y0)
    return float(table[-1][1])


def _get_dv(label, severity, density_pct):
    """Return the Deduct Value for a given distress type, severity, and density."""
    distress = LABEL_TO_DISTRESS.get(label.lower(), "longitudinal_crack")
    sev = (severity or "low").lower()
    if sev not in ("low", "medium", "high"):
        sev = "low"
    key = (distress, sev)
    table = _DV_TABLE.get(key, _DV_TABLE[("longitudinal_crack", "low")])
    return _interp(table, density_pct)


def _get_cdv(tdv, q):
    """Look up CDV for a given Total Deduct Value and q."""
    q_clamped = min(max(int(q), 1), 8)
    return _interp(_CDV_TABLE[q_clamped], tdv)


def compute_cdv(deduct_values):
    """
    ASTM D6433 iterative CDV computation.

    deduct_values: list of float DV values (one per distress-type + severity combo)
    Returns: float CDV
    """
    if not deduct_values:
        return 0.0

    dvs = sorted([max(float(dv), 0.0) for dv in deduct_values], reverse=True)

    max_cdv = 0.0
    working = list(dvs)

    while True:
        q = sum(1 for v in working if v > 2.0)
        if q == 0:
            q = 1
        tdv = sum(working)
        cdv = _get_cdv(tdv, q)
        if cdv > max_cdv:
            max_cdv = cdv
        if q <= 1:
            break
        # Replace smallest DV > 2.0 with 2.0 for next iteration
        for i in range(len(working) - 1, -1, -1):
            if working[i] > 2.0:
                working[i] = 2.0
                break

    return max_cdv


def _aggregate_distress_areas(violations):
    """
    Group violation polygon areas by (distress_label, severity).
    Returns dict: {(label, severity): total_area_mm2}
    """
    areas = {}
    for v in violations:
        label = (v.get("label") or "longitudinal_crack").lower()
        sev   = (v.get("severity") or "low").lower()
        area  = float(v.get("polygon_area_mm2") or 0)
        key   = (label, sev)
        areas[key] = areas.get(key, 0.0) + area
    return areas


def _frame_sample_area(image_width, image_height, gsd_mm_per_px):
    """
    Return the pavement sample area for one frame.

    When GSD is available, area is in mm².
    When GSD is absent (NULL), area is in px² — density remains dimensionally
    consistent because polygon_area_mm2 is also stored in px² units by the
    model in that case.
    """
    iw  = image_width  or 0
    ih  = image_height or 0
    gsd = gsd_mm_per_px or 0
    if iw <= 0 or ih <= 0:
        return 0.0
    return iw * ih * (gsd ** 2) if gsd > 0 else float(iw * ih)


def compute_frame_pci(violations, image_width, image_height, gsd_mm_per_px):
    """
    Compute PCI for a single camera frame.

    violations      : list of violation dicts (label, severity, polygon_area_mm2)
    image_width/height: frame dimensions in pixels
    gsd_mm_per_px   : ground sampling distance (mm per pixel); None/0 → pixel-area fallback

    Returns float PCI in [0, 100].
    """
    if not violations:
        return 100.0

    pavement_area = _frame_sample_area(image_width, image_height, gsd_mm_per_px)
    if pavement_area <= 0:
        return 100.0

    distress_areas = _aggregate_distress_areas(violations)
    if not distress_areas:
        return 100.0

    dvs = [
        _get_dv(label, sev, (area / pavement_area) * 100)
        for (label, sev), area in distress_areas.items()
    ]

    cdv = compute_cdv(dvs)
    return round(max(0.0, min(100.0, 100.0 - cdv)), 1)


def compute_segment_pci(segment_violations):
    """
    Compute PCI for a road segment by aggregating violations across all frames.

    segment_violations: list of violation dicts, each carrying:
        label, severity, polygon_area_mm2, image_width, image_height, gsd_mm_per_px, frame_id

    Total pavement area = sum of distinct frame areas (one per unique frame_id).
    Returns float PCI in [0, 100].
    """
    if not segment_violations:
        return 100.0

    # Accumulate one sample area per unique frame
    frame_areas = {}
    for v in segment_violations:
        fid = v.get("frame_id")
        if not fid:
            continue
        iw  = v.get("image_width")   or 0
        ih  = v.get("image_height")  or 0
        gsd = v.get("gsd_mm_per_px") or 0
        area = _frame_sample_area(iw, ih, gsd)
        if area > 0:
            frame_areas[fid] = area

    total_area = sum(frame_areas.values())
    if total_area <= 0:
        return 100.0

    distress_areas = _aggregate_distress_areas(segment_violations)
    if not distress_areas:
        return 100.0

    dvs = [
        _get_dv(label, sev, (area / total_area) * 100)
        for (label, sev), area in distress_areas.items()
    ]

    cdv = compute_cdv(dvs)
    return round(max(0.0, min(100.0, 100.0 - cdv)), 1)


def pci_rating(score):
    """Return ASTM D6433 condition rating string for a PCI score."""
    if score is None:
        return None
    if score >= 85:
        return "Excellent"
    if score >= 70:
        return "Very Good"
    if score >= 55:
        return "Good"
    if score >= 40:
        return "Fair"
    if score >= 25:
        return "Poor"
    if score >= 10:
        return "Very Poor"
    return "Failed"
