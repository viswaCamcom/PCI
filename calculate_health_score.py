#!/usr/bin/env python3
"""
Pavement Health Score calculator — two independent commands:

  python3 calculate_health_score.py scores      # fast: compute + write scores (no OSM)
  python3 calculate_health_score.py road-type   # slow: enrich road_type + width via Nominatim
  python3 calculate_health_score.py scores --dry-run  # preview without writing

Score formula (document §3-8):
  px/km per distress → percentile bucket (P25/P50/P75) → score 0/25/50/75/100
  penalty = 0.40×pothole + 0.40×alligator + 0.20×longitudinal
  health_score = 100 - penalty   (clamped 0-100)
  Good ≥70 / Moderate ≥40 / Poor <40
"""

import argparse
import json
import logging
import time
import mysql.connector
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB = dict(host="127.0.0.1", port=3306,
          user="pci_user", password="pci_pass", database="pci")

# Saudi MOT standard widths (metres, single carriageway direction)
ROAD_WIDTH_M = {
    "motorway":       11.25,
    "motorway_link":  11.25,
    "trunk":          10.50,
    "trunk_link":     10.50,
    "primary":         7.50,
    "primary_link":    7.50,
    "secondary":       7.00,
    "secondary_link":  7.00,
    "tertiary":        6.50,
    "tertiary_link":   6.50,
    "residential":     6.00,
    "living_street":   5.00,
    "service":         4.00,
    "track":           4.00,
    "unclassified":    7.00,
    "road":            7.00,
}
DEFAULT_WIDTH_M = 7.0

WEIGHTS = {"pothole": 0.40, "alligator_crack": 0.40, "longitudinal_crack": 0.20}

# Nominatim settings
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_HDR = {"User-Agent": "PCI-Road-Monitor/1.0 (internal)"}
GRID_PREC     = 2        # ~1.1 km grid cells for caching
BASE_DELAY    = 2.0      # seconds between requests (Nominatim policy: ≤1/s, use 2 to be safe)
MAX_RETRIES   = 3


# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_conn():
    return mysql.connector.connect(**DB)


def ensure_columns(cur):
    schema = [
        ("road_type",        "VARCHAR(50)  DEFAULT NULL"),
        ("segment_width_m",  "FLOAT        DEFAULT NULL"),
        ("health_score",     "INT          DEFAULT NULL"),
        ("health_condition", "VARCHAR(20)  DEFAULT NULL"),
        ("health_color",     "VARCHAR(10)  DEFAULT NULL"),
    ]
    for col, defn in schema:
        try:
            cur.execute(f"ALTER TABLE segments ADD COLUMN {col} {defn}")
            log.info("Added column: %s", col)
        except mysql.connector.Error as e:
            if e.errno != 1060:
                raise


# ── Math helpers ───────────────────────────────────────────────────────────────

def percentile(sorted_vals: list, p: float) -> float:
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    idx = (p / 100.0) * (n - 1)
    lo  = int(idx)
    hi  = min(lo + 1, n - 1)
    return sorted_vals[lo] + (idx - lo) * (sorted_vals[hi] - sorted_vals[lo])


def score_metric(val: float, p25: float, p50: float, p75: float) -> int:
    if val == 0:    return 0
    if val <= p25:  return 25
    if val <= p50:  return 50
    if val <= p75:  return 75
    return 100


def assign_condition(score: int):
    if score >= 70:  return "Good",     "Green"
    if score >= 40:  return "Moderate", "Orange"
    return "Poor", "Red"


def parse_gps(raw) -> list:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    return raw if isinstance(raw, list) else []


# ── Command: scores ────────────────────────────────────────────────────────────

SCORE_MEANING = {0: "None", 25: "Low", 50: "Medium", 75: "High", 100: "Very High"}


def cmd_scores(dry_run: bool):
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    ensure_columns(cur)
    conn.commit()

    # ── Step 1: Load segments (input table) ────────────────────────────────────
    log.info("Step 1  Loading segments…")
    cur.execute("""
        SELECT segment_id, length_meters, frame_count,
               COALESCE(segment_width_m, %s) AS segment_width_m
        FROM   segments
        WHERE  length_meters IS NOT NULL AND length_meters > 0
    """, (DEFAULT_WIDTH_M,))
    segments = cur.fetchall()
    log.info("  %d segments loaded.", len(segments))

    # ── Step 2: segment_area_m2 = length_km × 1000 × width_m (reference only) ─
    for seg in segments:
        km = float(seg["length_meters"]) / 1000.0
        seg["segment_length_km"]  = km
        seg["segment_width_m"]    = float(seg["segment_width_m"] or DEFAULT_WIDTH_M)
        seg["segment_area_m2"]    = km * 1000.0 * seg["segment_width_m"]

    # ── Step 3: Aggregate distress counts + pixel areas in one query ───────────
    # Use bbox_area_px (stored after reprocess). Fall back to computing from bbox
    # coordinates for any older rows where bbox_area_px is still NULL.
    log.info("Step 3  Loading distress aggregates (count + pixel area per type)…")
    seg_ids = [s["segment_id"] for s in segments]
    fmt     = ",".join(["%s"] * len(seg_ids))
    cur.execute(f"""
        SELECT
            segment_id,
            label,
            COUNT(*)                                                          AS distress_count,
            SUM(COALESCE(
                bbox_area_px,
                GREATEST(0, (bbox_xmax - bbox_xmin) * (bbox_ymax - bbox_ymin))
            ))                                                                AS pixel_area
        FROM   violations
        WHERE  segment_id IN ({fmt})
          AND  label IN ('pothole', 'alligator_crack', 'longitudinal_crack')
        GROUP  BY segment_id, label
    """, seg_ids)

    # agg[segment_id][label] = {"count": N, "pixel_area": X}
    agg: dict = {}
    for row in cur.fetchall():
        agg.setdefault(row["segment_id"], {})[row["label"]] = {
            "count":      int(row["distress_count"] or 0),
            "pixel_area": float(row["pixel_area"]   or 0),
        }

    # Attach per-segment counts and areas (Step 1 input table fields)
    for seg in segments:
        km  = seg["segment_length_km"]
        v   = agg.get(seg["segment_id"], {})
        pot   = v.get("pothole",            {"count": 0, "pixel_area": 0.0})
        alli  = v.get("alligator_crack",    {"count": 0, "pixel_area": 0.0})
        longi = v.get("longitudinal_crack", {"count": 0, "pixel_area": 0.0})

        seg["pothole_count"]        = pot["count"]
        seg["pothole_pixel_area"]   = pot["pixel_area"]
        seg["alligator_count"]      = alli["count"]
        seg["alligator_pixel_area"] = alli["pixel_area"]
        seg["longitudinal_count"]   = longi["count"]
        seg["longitudinal_pixel_area"] = longi["pixel_area"]

        # ── Step 3: Normalize by segment length (px/km) ───────────────────────
        seg["pot_pkm"]   = pot["pixel_area"]   / km if km > 0 else 0.0
        seg["alli_pkm"]  = alli["pixel_area"]  / km if km > 0 else 0.0
        seg["longi_pkm"] = longi["pixel_area"] / km if km > 0 else 0.0

    # ── Step 4: Percentile thresholds (non-zero values only) ──────────────────
    log.info("Step 4  Computing percentile thresholds…")

    def thresholds(key):
        vals = sorted(s[key] for s in segments if s[key] > 0)
        if not vals:
            return 0.0, 0.0, 0.0
        return percentile(vals, 25), percentile(vals, 50), percentile(vals, 75)

    pot_p25,  pot_p50,  pot_p75  = thresholds("pot_pkm")
    alli_p25, alli_p50, alli_p75 = thresholds("alli_pkm")
    lon_p25,  lon_p50,  lon_p75  = thresholds("longi_pkm")

    log.info("  Distress      P25          P50          P75")
    log.info("  Pothole       %10.0f   %10.0f   %10.0f  px/km", pot_p25,  pot_p50,  pot_p75)
    log.info("  Alligator     %10.0f   %10.0f   %10.0f  px/km", alli_p25, alli_p50, alli_p75)
    log.info("  Longitudinal  %10.0f   %10.0f   %10.0f  px/km", lon_p25,  lon_p50,  lon_p75)

    # ── Steps 5-8: Score → penalty → health score → condition ─────────────────
    log.info("Steps 5-8  Calculating scores…")
    updates  = []
    cond_cnt = {"Good": 0, "Moderate": 0, "Poor": 0}

    for seg in segments:
        # Step 5: distress score per type (0/25/50/75/100)
        pot_score   = score_metric(seg["pot_pkm"],   pot_p25,  pot_p50,  pot_p75)
        alli_score  = score_metric(seg["alli_pkm"],  alli_p25, alli_p50, alli_p75)
        longi_score = score_metric(seg["longi_pkm"], lon_p25,  lon_p50,  lon_p75)

        # Step 6: weighted penalty
        penalty = (WEIGHTS["pothole"]          * pot_score
                 + WEIGHTS["alligator_crack"]  * alli_score
                 + WEIGHTS["longitudinal_crack"] * longi_score)

        # Step 7: Pavement Health Score
        h_score = max(0, min(100, round(100 - penalty)))

        # Step 8: condition band
        h_cond, h_color = assign_condition(h_score)
        cond_cnt[h_cond] += 1

        seg["_pot_score"]   = pot_score
        seg["_alli_score"]  = alli_score
        seg["_longi_score"] = longi_score
        seg["_penalty"]     = penalty
        seg["_h_score"]     = h_score
        seg["_h_cond"]      = h_cond

        updates.append((h_score, h_cond, h_color, seg["segment_id"]))

    log.info("Results: Good=%d  Moderate=%d  Poor=%d",
             cond_cnt["Good"], cond_cnt["Moderate"], cond_cnt["Poor"])

    # ── Sample input table (first 5 segments with any distress) ───────────────
    sample = [s for s in segments if s["pothole_count"] + s["alligator_count"] + s["longitudinal_count"] > 0][:5]
    if sample:
        log.info("")
        log.info("Sample input table (first 5 segments with distress):")
        log.info("  %-36s  %6s  %5s  %8s  %6s  %10s  %6s  %10s  %6s  %10s  %6s  %8s  %8s  %8s  %5s  %8s",
                 "segment_id", "len_km", "wid_m", "area_m2",
                 "pot_n", "pot_px", "alli_n", "alli_px", "lon_n", "lon_px",
                 "pot_sc", "alli_sc", "lon_sc", "penalty", "score", "cond")
        for s in sample:
            log.info("  %-36s  %6.2f  %5.1f  %8.0f  %6d  %10.0f  %6d  %10.0f  %6d  %10.0f  %6s  %8s  %8s  %7.1f  %5d  %8s",
                     s["segment_id"],
                     s["segment_length_km"], s["segment_width_m"], s["segment_area_m2"],
                     s["pothole_count"],    s["pothole_pixel_area"],
                     s["alligator_count"],  s["alligator_pixel_area"],
                     s["longitudinal_count"], s["longitudinal_pixel_area"],
                     SCORE_MEANING[s["_pot_score"]],
                     SCORE_MEANING[s["_alli_score"]],
                     SCORE_MEANING[s["_longi_score"]],
                     s["_penalty"], s["_h_score"], s["_h_cond"])
        log.info("")

    if dry_run:
        log.info("Dry-run — skipping DB write.")
        cur.close(); conn.close()
        return

    log.info("Writing %d rows to DB…", len(updates))
    cur.executemany("""
        UPDATE segments
        SET health_score=     %s,
            health_condition= %s,
            health_color=     %s
        WHERE segment_id = %s
    """, updates)
    conn.commit()
    log.info("Done.")
    cur.close(); conn.close()


# ── Command: road-type (Nominatim enrichment) ──────────────────────────────────

def cmd_road_type():
    _cache: dict = {}

    def fetch_road_type(lat: float, lon: float) -> str:
        cell = (round(lat, GRID_PREC), round(lon, GRID_PREC))
        if cell in _cache:
            return _cache[cell]
        road_type = "unclassified"
        delay = BASE_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(
                    NOMINATIM_URL,
                    params={"lat": f"{lat:.6f}", "lon": f"{lon:.6f}",
                            "format": "json", "zoom": 16},
                    headers=NOMINATIM_HDR, timeout=15,
                )
                if r.status_code == 429:
                    wait = delay * (2 ** attempt)
                    log.warning("Rate limited — waiting %.0fs…", wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                data = r.json()
                if data.get("class") == "highway":
                    road_type = data.get("type", "unclassified")
                break
            except Exception as exc:
                log.warning("Nominatim error (%.4f, %.4f) attempt %d: %s",
                            lat, lon, attempt + 1, exc)
                time.sleep(delay * (2 ** attempt))
        time.sleep(BASE_DELAY)
        _cache[cell] = road_type
        return road_type

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)

    log.info("Loading segment GPS paths…")
    cur.execute("SELECT segment_id, gps_path FROM segments")
    segments = cur.fetchall()
    log.info("  %d segments loaded.", len(segments))

    updates = []
    for i, seg in enumerate(segments, 1):
        path = parse_gps(seg["gps_path"])
        if not path:
            rt, wm = "unclassified", DEFAULT_WIDTH_M
        else:
            mid = path[len(path) // 2]
            rt  = fetch_road_type(float(mid[0]), float(mid[1]))
            wm  = ROAD_WIDTH_M.get(rt, DEFAULT_WIDTH_M)
        updates.append((rt, wm, seg["segment_id"]))

        if i % 50 == 0 or i == len(segments):
            log.info("  %d / %d done  (%d unique cells)", i, len(segments), len(_cache))

    log.info("Writing road types to DB…")
    cur.executemany(
        "UPDATE segments SET road_type=%s, segment_width_m=%s WHERE segment_id=%s",
        updates
    )
    conn.commit()
    from collections import Counter
    dist = Counter(u[0] for u in updates)
    log.info("Road types: %s", ", ".join(f"{k}:{v}" for k, v in dist.most_common(8)))
    log.info("Done. %d segments updated.", len(updates))
    cur.close(); conn.close()


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")

    p_scores = sub.add_parser("scores", help="Compute and store health scores")
    p_scores.add_argument("--dry-run", action="store_true")

    sub.add_parser("road-type", help="Enrich road_type + segment_width_m via Nominatim (slow)")

    args = parser.parse_args()
    if args.cmd == "scores":
        cmd_scores(dry_run=getattr(args, "dry_run", False))
    elif args.cmd == "road-type":
        cmd_road_type()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
