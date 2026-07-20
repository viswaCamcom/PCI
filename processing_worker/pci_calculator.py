"""
processing_worker/pci_calculator.py
=====================================
Pavement Health Score — same formula as calculate_health_score.py:

  Step 3 : px_area / segment_length_km  per distress type
  Step 4 : P25 / P50 / P75 thresholds across all segments (passed in by caller)
  Step 5 : score bucket  0 / 25 / 50 / 75 / 100
  Step 6 : penalty = 0.40×pothole + 0.40×alligator + 0.20×longitudinal
  Step 7 : health_score = 100 − penalty   (clamped 0–100, rounded)
  Step 8 : Good ≥70 / Moderate ≥40 / Poor <40
"""

# ─── Weights (Step 6) ─────────────────────────────────────────────────────────
WEIGHTS = {
    "pothole":            0.40,
    "alligator_crack":    0.40,
    "longitudinal_crack": 0.20,
}

# ─── Label → distress type (all crack variants → longitudinal) ────────────────
LABEL_TO_TYPE = {
    "pothole":            "pothole",
    "alligator_crack":    "alligator_crack",
    "longitudinal_crack": "longitudinal_crack",
    "transverse_crack":   "longitudinal_crack",
    "road_crack":         "longitudinal_crack",
    "rutting":            "longitudinal_crack",
}


# ─── Percentile helper ────────────────────────────────────────────────────────
def _percentile(sorted_vals: list, p: float) -> float:
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    idx = (p / 100.0) * (n - 1)
    lo  = int(idx)
    hi  = min(lo + 1, n - 1)
    return sorted_vals[lo] + (idx - lo) * (sorted_vals[hi] - sorted_vals[lo])


def compute_thresholds(pkm_rows: list) -> dict:
    """
    Build P25/P50/P75 thresholds from a list of per-segment px/km values.

    pkm_rows : [{"pot_pkm": float, "alli_pkm": float, "lon_pkm": float}, ...]
    Returns  : {"pothole": (p25,p50,p75), "alligator_crack": ..., "longitudinal_crack": ...}
    """
    pot  = sorted(r["pot_pkm"]  for r in pkm_rows if r["pot_pkm"]  > 0)
    alli = sorted(r["alli_pkm"] for r in pkm_rows if r["alli_pkm"] > 0)
    lon  = sorted(r["lon_pkm"]  for r in pkm_rows if r["lon_pkm"]  > 0)
    return {
        "pothole":            (_percentile(pot,  25), _percentile(pot,  50), _percentile(pot,  75)),
        "alligator_crack":    (_percentile(alli, 25), _percentile(alli, 50), _percentile(alli, 75)),
        "longitudinal_crack": (_percentile(lon,  25), _percentile(lon,  50), _percentile(lon,  75)),
    }


# ─── Step 5: score metric ─────────────────────────────────────────────────────
def _score_metric(val: float, p25: float, p50: float, p75: float) -> int:
    if val == 0:      return 0
    if val <= p25:    return 25
    if val <= p50:    return 50
    if val <= p75:    return 75
    return 100


# ─── Segment health score (Steps 3–8) ─────────────────────────────────────────
def compute_segment_health_score(
    distress_px: dict,   # {"pothole": px, "alligator_crack": px, "longitudinal_crack": px}
    length_km:   float,
    thresholds:  dict,   # from compute_thresholds()
) -> tuple:
    """
    Returns (health_score: int, condition: str, color: str)
    """
    if length_km <= 0:
        return 100, "Good", "Green"

    # Step 3 — normalise by length
    pot_pkm  = float(distress_px.get("pothole",            0)) / length_km
    alli_pkm = float(distress_px.get("alligator_crack",    0)) / length_km
    lon_pkm  = float(distress_px.get("longitudinal_crack", 0)) / length_km

    # Step 5 — bucket scores
    pot_score  = _score_metric(pot_pkm,  *thresholds.get("pothole",            (0, 0, 0)))
    alli_score = _score_metric(alli_pkm, *thresholds.get("alligator_crack",    (0, 0, 0)))
    lon_score  = _score_metric(lon_pkm,  *thresholds.get("longitudinal_crack", (0, 0, 0)))

    # Step 6 — weighted penalty
    penalty = (WEIGHTS["pothole"]            * pot_score
             + WEIGHTS["alligator_crack"]    * alli_score
             + WEIGHTS["longitudinal_crack"] * lon_score)

    # Step 7 — health score
    score = max(0, min(100, round(100 - penalty)))

    # Step 8 — condition band
    if score >= 70:   return score, "Good",     "Green"
    if score >= 40:   return score, "Moderate", "Orange"
    return score, "Poor", "Red"


# ─── Frame-level score (simplified — no cross-segment percentiles needed) ─────
def compute_frame_pci(violations, image_width, image_height, gsd_mm_per_px):
    """
    Per-frame health score: distress pixel coverage fraction → weighted penalty.
    Stored in frames.pci_score; not shown on the map panel.
    """
    if not violations:
        return 100.0
    iw = image_width  or 0
    ih = image_height or 0
    if iw <= 0 or ih <= 0:
        return 100.0
    frame_px = float(iw * ih)

    distress_px: dict = {}
    for v in violations:
        t = LABEL_TO_TYPE.get((v.get("label") or "").lower(), "longitudinal_crack")
        bbox = v.get("bounding_box") or {}
        area = float(
            (v.get("area") or {}).get("bbox_area_px")
            or max(0, (bbox.get("xmax", 0) - bbox.get("xmin", 0))
                      * (bbox.get("ymax", 0) - bbox.get("ymin", 0)))
        )
        distress_px[t] = distress_px.get(t, 0.0) + area

    pot_cov  = min(100.0, distress_px.get("pothole",            0) / frame_px * 100)
    alli_cov = min(100.0, distress_px.get("alligator_crack",    0) / frame_px * 100)
    lon_cov  = min(100.0, distress_px.get("longitudinal_crack", 0) / frame_px * 100)

    penalty = (WEIGHTS["pothole"]            * pot_cov
             + WEIGHTS["alligator_crack"]    * alli_cov
             + WEIGHTS["longitudinal_crack"] * lon_cov)

    return round(max(0.0, min(100.0, 100.0 - penalty)), 1)


# ─── Condition label ──────────────────────────────────────────────────────────
def pci_rating(score):
    if score is None: return None
    if score >= 70:   return "Good"
    if score >= 40:   return "Moderate"
    return "Poor"


# ─── Backward-compat stubs (unused but kept so old import lines still work) ───
def compute_segment_pci(segment_violations):
    return 100.0

def compute_segment_pci_from_aggregates(distress_rows, frame_rows):
    return 100.0
