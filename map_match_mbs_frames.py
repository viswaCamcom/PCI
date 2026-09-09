#!/usr/bin/env python3
"""
map_match_mbs_frames.py
========================
Matches frames to the frozen mbs_segments (see freeze_mbs_segments.py) and
computes per-(segment, capture_date) health scores into mbs_segment_scores.

This is what makes date-based comparison possible: instead of re-deriving
segments from whatever frames happen to exist (like offline_resegment.py
does for the live pipeline), frames get assigned to a PERMANENT segment via
nearest-vertex matching against the frozen geometry, bucketed by their own
capture date. Re-running this script for a new month's data never changes
past dates' scores.

Percentile thresholds (used by the same weighted-penalty health-score formula
as calculate_health_score.py) are frozen once via --init-thresholds and
reused forever after — recomputing them every run would let old scores drift
as new months of data arrive, which defeats the whole point of comparing
"segment X on Jan 15" against "segment X on Jul 15".

Never joins through violations.segment_id (that column is nulled/reassigned
by offline_resegment.py on every run) — only violations.frame_id is stable.

Usage:
    python3 map_match_mbs_frames.py --dry-run              # match + preview, write nothing
    python3 map_match_mbs_frames.py --init-thresholds       # one-time: match + freeze thresholds + score
    python3 map_match_mbs_frames.py                         # normal: match new frames + score touched segments
    python3 map_match_mbs_frames.py --recheck-unmatched      # also retry previously-rejected frames

Environment variables (same as the app containers):
    MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB
"""

import argparse
import json
import logging
import math
import os
from collections import defaultdict
from datetime import datetime

import mysql.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger(__name__)

MBS_MATCH_TOLERANCE_M = 30.0   # looser than the live pipeline's 20m cross-track check —
                                 # accounts for GPS drift across different vehicles/months
GRID_CELL_M           = 50.0
GRID_CELL_DEG         = GRID_CELL_M / 111_320.0
BBOX_BUFFER_KM        = 2.0

LABEL_TO_TYPE = {
    "pothole":            "pothole",
    "alligator_crack":    "alligator_crack",
    "longitudinal_crack": "longitudinal_crack",
    "transverse_crack":   "longitudinal_crack",
    "road_crack":         "longitudinal_crack",
    "rutting":            "longitudinal_crack",
}
WEIGHTS = {"pothole": 0.40, "alligator_crack": 0.40, "longitudinal_crack": 0.20}


def get_conn():
    return mysql.connector.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", 3306)),
        user=os.environ.get("MYSQL_USER", "pci_user"),
        password=os.environ.get("MYSQL_PASSWORD", "pci_pass"),
        database=os.environ.get("MYSQL_DB", "pci"),
        autocommit=False,
    )


# ─── Geo helpers (copied from offline_resegment.py — this repo's convention is
#     each script carries its own copy rather than a shared module) ───────────

def haversine(lat1, lon1, lat2, lon2):
    R = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin(math.radians(lat2 - lat1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def percentile(sorted_vals: list, p: float) -> float:
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    idx = (p / 100.0) * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    return sorted_vals[lo] + (idx - lo) * (sorted_vals[hi] - sorted_vals[lo])


def score_metric(val, p25, p50, p75) -> int:
    if val == 0:   return 0
    if val <= p25: return 25
    if val <= p50: return 50
    if val <= p75: return 75
    return 100


def assign_condition(score: int):
    if score >= 70: return "Good", "Green"
    if score >= 40: return "Moderate", "Orange"
    return "Poor", "Red"


def parse_capture_date(datetime_utc, created_at):
    for raw in (datetime_utc, created_at):
        if not raw:
            continue
        s = raw.replace("Z", "+00:00") if isinstance(raw, str) and raw.endswith("Z") else raw
        try:
            return datetime.fromisoformat(s).date()
        except Exception:
            continue
    return None


# ─── Frozen segment loading + spatial grid ─────────────────────────────────────

def load_frozen_segments(cur):
    cur.execute("SELECT mbs_segment_id, gps_path, start_lat, start_lon, end_lat, end_lon FROM mbs_segments")
    segs = {}
    bounds = []
    for row in cur.fetchall():
        path = row["gps_path"]
        if isinstance(path, (bytes, bytearray)):
            path = path.decode()
        if isinstance(path, str):
            try:
                path = json.loads(path)
            except Exception:
                path = []
        segs[row["mbs_segment_id"]] = path
        bounds.append((row["start_lat"], row["start_lon"]))
        bounds.append((row["end_lat"], row["end_lon"]))
    if not segs:
        raise SystemExit("mbs_segments is empty — run freeze_mbs_segments.py first.")
    lats = [b[0] for b in bounds if b[0] is not None]
    lons = [b[1] for b in bounds if b[1] is not None]
    buf = BBOX_BUFFER_KM / 111.32
    bbox = {
        "min_lat": min(lats) - buf, "max_lat": max(lats) + buf,
        "min_lon": min(lons) - buf, "max_lon": max(lons) + buf,
    }
    return segs, bbox


def cell_key(lat, lon):
    return (int(lat / GRID_CELL_DEG), int(lon / GRID_CELL_DEG))


def build_grid(segs):
    grid = defaultdict(set)
    for sid, path in segs.items():
        for pt in path:
            grid[cell_key(pt[0], pt[1])].add(sid)
    return grid


def match_frame(lat, lon, grid, segs):
    r0, c0 = cell_key(lat, lon)
    candidates = set()
    for dr in (-1, 0, 1):
        for dc in (-1, 0, 1):
            candidates |= grid.get((r0 + dr, c0 + dc), set())
    if not candidates:
        return None, None
    best_id, best_dist = None, float("inf")
    for sid in candidates:
        for pt in segs[sid]:
            d = haversine(lat, lon, pt[0], pt[1])
            if d < best_dist:
                best_dist, best_id = d, sid
    if best_dist <= MBS_MATCH_TOLERANCE_M:
        return best_id, best_dist
    return None, best_dist


# ─── Phase A: matching ──────────────────────────────────────────────────────────

def fetch_candidate_frames(cur, bbox, recheck_unmatched):
    extra = "(m.frame_id IS NULL OR m.match_status = 'unmatched')" if recheck_unmatched else "m.frame_id IS NULL"
    cur.execute(
        f"""
        SELECT f.frame_id, f.latitude, f.longitude, f.datetime_utc, f.created_at
        FROM frames f
        LEFT JOIN mbs_frame_segment_map m ON m.frame_id = f.frame_id
        WHERE f.latitude BETWEEN %s AND %s AND f.longitude BETWEEN %s AND %s
          AND {extra}
        """,
        (bbox["min_lat"], bbox["max_lat"], bbox["min_lon"], bbox["max_lon"]),
    )
    return cur.fetchall()


def run_matching(cur, grid, segs, bbox, recheck_unmatched, dry_run):
    frames = fetch_candidate_frames(cur, bbox, recheck_unmatched)
    logger.info("Phase A: %d candidate frames to match in corridor bbox.", len(frames))

    rows = []
    touched_segment_ids = set()
    skipped_no_date = 0
    matched_n = unmatched_n = 0

    for f in frames:
        cap_date = parse_capture_date(f.get("datetime_utc"), f.get("created_at"))
        if cap_date is None:
            skipped_no_date += 1
            continue
        seg_id, dist = match_frame(f["latitude"], f["longitude"], grid, segs)
        status = "matched" if seg_id else "unmatched"
        if seg_id:
            matched_n += 1
            touched_segment_ids.add(seg_id)
        else:
            unmatched_n += 1
        rows.append((f["frame_id"], seg_id, cap_date, dist, status))

    logger.info(
        "Matched=%d  Unmatched=%d  SkippedNoDate=%d  TouchedSegments=%d",
        matched_n, unmatched_n, skipped_no_date, len(touched_segment_ids),
    )

    if dry_run:
        logger.info("Dry-run — skipping writes.")
        return touched_segment_ids

    if rows:
        cur.executemany(
            """
            INSERT INTO mbs_frame_segment_map (frame_id, mbs_segment_id, capture_date, distance_m, match_status)
            VALUES (%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                mbs_segment_id = VALUES(mbs_segment_id),
                capture_date   = VALUES(capture_date),
                distance_m     = VALUES(distance_m),
                match_status   = VALUES(match_status)
            """,
            rows,
        )
    return touched_segment_ids


# ─── Phase B/C/D: thresholds + scoring ─────────────────────────────────────────

def aggregate_pairs(cur, segment_ids):
    """Per (mbs_segment_id, capture_date, distress_type): count + pixel_area,
    for every date any of the given segments has mapped frames."""
    if not segment_ids:
        return {}
    fmt = ",".join(["%s"] * len(segment_ids))
    cur.execute(
        f"""
        SELECT m.mbs_segment_id, m.capture_date,
               CASE
                 WHEN v.label = 'pothole' THEN 'pothole'
                 WHEN v.label = 'alligator_crack' THEN 'alligator_crack'
                 ELSE 'longitudinal_crack'
               END AS distress_type,
               COUNT(*) AS cnt,
               SUM(COALESCE(v.bbox_area_px,
                   GREATEST(0, (v.bbox_xmax - v.bbox_xmin) * (v.bbox_ymax - v.bbox_ymin)))) AS pixel_area
        FROM violations v
        JOIN mbs_frame_segment_map m ON m.frame_id = v.frame_id
        WHERE m.mbs_segment_id IN ({fmt})
          AND v.label IN ('pothole','alligator_crack','longitudinal_crack','transverse_crack','road_crack','rutting')
        GROUP BY m.mbs_segment_id, m.capture_date, distress_type
        """,
        segment_ids,
    )
    agg = defaultdict(lambda: defaultdict(lambda: {"count": 0, "pixel_area": 0.0}))
    for row in cur.fetchall():
        key = (row["mbs_segment_id"], row["capture_date"])
        agg[key][row["distress_type"]] = {
            "count": int(row["cnt"] or 0),
            "pixel_area": float(row["pixel_area"] or 0),
        }

    # frame counts per pair (independent of whether they have violations)
    cur.execute(
        f"""
        SELECT mbs_segment_id, capture_date, COUNT(*) AS frame_count
        FROM mbs_frame_segment_map
        WHERE mbs_segment_id IN ({fmt})
        GROUP BY mbs_segment_id, capture_date
        """,
        segment_ids,
    )
    frame_counts = {(r["mbs_segment_id"], r["capture_date"]): r["frame_count"] for r in cur.fetchall()}

    return agg, frame_counts


def load_segment_lengths(cur, segment_ids):
    if not segment_ids:
        return {}
    fmt = ",".join(["%s"] * len(segment_ids))
    cur.execute(f"SELECT mbs_segment_id, length_meters FROM mbs_segments WHERE mbs_segment_id IN ({fmt})", segment_ids)
    return {r["mbs_segment_id"]: float(r["length_meters"] or 0) for r in cur.fetchall()}


def compute_pkm(agg, frame_counts, lengths):
    """Returns {(segment,date): {"pothole_pkm":.., "alligator_pkm":.., "longi_pkm":.., ...raw fields}}

    Iterates frame_counts (every mapped pair, from mbs_frame_segment_map) rather
    than agg (only pairs with at least one violation) — a segment/date with zero
    detected defects is a real, scoreable "clean" result (should land at 100/Good
    via score_metric's val==0 case), not a pair to silently drop.
    """
    out = {}
    for key in frame_counts:
        types = agg.get(key, {})
        seg_id, cap_date = key
        km = lengths.get(seg_id, 0) / 1000.0
        pot = types.get("pothole", {"count": 0, "pixel_area": 0.0})
        alli = types.get("alligator_crack", {"count": 0, "pixel_area": 0.0})
        longi = types.get("longitudinal_crack", {"count": 0, "pixel_area": 0.0})
        out[key] = {
            "frame_count": frame_counts.get(key, 0),
            "violation_count": pot["count"] + alli["count"] + longi["count"],
            "pothole_count": pot["count"], "alligator_count": alli["count"], "longitudinal_count": longi["count"],
            "pothole_pixel_area": pot["pixel_area"], "alligator_pixel_area": alli["pixel_area"],
            "longitudinal_pixel_area": longi["pixel_area"],
            "pothole_pkm": pot["pixel_area"] / km if km > 0 else 0.0,
            "alligator_pkm": alli["pixel_area"] / km if km > 0 else 0.0,
            "longi_pkm": longi["pixel_area"] / km if km > 0 else 0.0,
        }
    return out


def load_thresholds(cur):
    cur.execute("SELECT metric, p25, p50, p75 FROM mbs_score_thresholds")
    rows = {r["metric"]: (r["p25"], r["p50"], r["p75"]) for r in cur.fetchall()}
    if len(rows) < 3:
        raise SystemExit(
            "mbs_score_thresholds is not fully populated — run with --init-thresholds first "
            "(recommended: after backfilling all available months' data, not off a single day)."
        )
    # DB stores short metric names; score_pairs looks up the *_crack keys that
    # compute_and_store_thresholds's return value uses (see below) — keep the
    # two in sync rather than renaming DB rows.
    return {
        "pothole":            rows["pothole"],
        "alligator_crack":    rows["alligator"],
        "longitudinal_crack": rows["longitudinal"],
    }


def compute_and_store_thresholds(cur, pkm_by_pair, note):
    def vals(key):
        return sorted(v[key] for v in pkm_by_pair.values() if v[key] > 0)

    pot_vals, alli_vals, longi_vals = vals("pothole_pkm"), vals("alligator_pkm"), vals("longi_pkm")
    rows = [
        ("pothole", percentile(pot_vals, 25), percentile(pot_vals, 50), percentile(pot_vals, 75), len(pkm_by_pair), note),
        ("alligator", percentile(alli_vals, 25), percentile(alli_vals, 50), percentile(alli_vals, 75), len(pkm_by_pair), note),
        ("longitudinal", percentile(longi_vals, 25), percentile(longi_vals, 50), percentile(longi_vals, 75), len(pkm_by_pair), note),
    ]
    cur.executemany(
        """
        INSERT INTO mbs_score_thresholds (metric, p25, p50, p75, baseline_frame_count, baseline_note)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE p25=VALUES(p25), p50=VALUES(p50), p75=VALUES(p75),
                                baseline_frame_count=VALUES(baseline_frame_count),
                                baseline_note=VALUES(baseline_note), computed_at=CURRENT_TIMESTAMP
        """,
        rows,
    )
    logger.info("Thresholds written: pothole=%s alligator=%s longitudinal=%s",
                rows[0][1:4], rows[1][1:4], rows[2][1:4])
    return {"pothole": rows[0][1:4], "alligator_crack": rows[1][1:4], "longitudinal_crack": rows[2][1:4]}


def score_pairs(cur, pkm_by_pair, thresholds):
    updates = []
    for (seg_id, cap_date), v in pkm_by_pair.items():
        pot_score = score_metric(v["pothole_pkm"], *thresholds["pothole"])
        alli_score = score_metric(v["alligator_pkm"], *thresholds["alligator_crack"])
        longi_score = score_metric(v["longi_pkm"], *thresholds["longitudinal_crack"])
        penalty = (WEIGHTS["pothole"] * pot_score
                   + WEIGHTS["alligator_crack"] * alli_score
                   + WEIGHTS["longitudinal_crack"] * longi_score)
        h_score = max(0, min(100, round(100 - penalty)))
        h_cond, h_color = assign_condition(h_score)
        updates.append((
            seg_id, cap_date, v["frame_count"], v["violation_count"],
            v["pothole_count"], v["alligator_count"], v["longitudinal_count"],
            v["pothole_pixel_area"], v["alligator_pixel_area"], v["longitudinal_pixel_area"],
            v["pothole_pkm"], v["alligator_pkm"], v["longi_pkm"],
            h_score, h_cond, h_color,
        ))

    cur.executemany(
        """
        INSERT INTO mbs_segment_scores
            (mbs_segment_id, capture_date, frame_count, violation_count,
             pothole_count, alligator_count, longitudinal_count,
             pothole_pixel_area, alligator_pixel_area, longitudinal_pixel_area,
             pothole_px_km, alligator_px_km, longitudinal_px_km,
             health_score, health_condition, health_color)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            frame_count=VALUES(frame_count), violation_count=VALUES(violation_count),
            pothole_count=VALUES(pothole_count), alligator_count=VALUES(alligator_count),
            longitudinal_count=VALUES(longitudinal_count),
            pothole_pixel_area=VALUES(pothole_pixel_area), alligator_pixel_area=VALUES(alligator_pixel_area),
            longitudinal_pixel_area=VALUES(longitudinal_pixel_area),
            pothole_px_km=VALUES(pothole_px_km), alligator_px_km=VALUES(alligator_px_km),
            longitudinal_px_km=VALUES(longitudinal_px_km),
            health_score=VALUES(health_score), health_condition=VALUES(health_condition),
            health_color=VALUES(health_color)
        """,
        updates,
    )
    logger.info("Scored %d (segment, date) pairs.", len(updates))


# ─── Main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Map-match frames to frozen MBS segments and score them")
    ap.add_argument("--dry-run", action="store_true", help="Match + preview only, write nothing")
    ap.add_argument("--init-thresholds", action="store_true",
                     help="One-time: (re)compute percentile thresholds from ALL currently-mapped data")
    ap.add_argument("--recheck-unmatched", action="store_true",
                     help="Also retry frames previously marked unmatched")
    ap.add_argument("--threshold-note", default="", help="Note stored alongside --init-thresholds baseline")
    args = ap.parse_args()

    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    segs, bbox = load_frozen_segments(cur)
    logger.info("Loaded %d frozen segments. Bbox lat[%.5f,%.5f] lon[%.5f,%.5f]",
                len(segs), bbox["min_lat"], bbox["max_lat"], bbox["min_lon"], bbox["max_lon"])
    grid = build_grid(segs)

    touched = run_matching(cur, grid, segs, bbox, args.recheck_unmatched, args.dry_run)
    if args.dry_run:
        cur.close(); conn.close()
        return
    conn.commit()

    if args.init_thresholds:
        cur.execute("SELECT DISTINCT mbs_segment_id FROM mbs_frame_segment_map WHERE mbs_segment_id IS NOT NULL")
        scope_ids = [r["mbs_segment_id"] for r in cur.fetchall()]
    else:
        scope_ids = list(touched)

    if not scope_ids:
        logger.info("No segments touched this run — nothing to score.")
        conn.commit()
        cur.close(); conn.close()
        return

    agg, frame_counts = aggregate_pairs(cur, scope_ids)
    lengths = load_segment_lengths(cur, scope_ids)
    pkm_by_pair = compute_pkm(agg, frame_counts, lengths)

    if args.init_thresholds:
        note = args.threshold_note or f"init from {len(pkm_by_pair)} (segment,date) pairs"
        thresholds = compute_and_store_thresholds(cur, pkm_by_pair, note)
    else:
        thresholds = load_thresholds(cur)

    score_pairs(cur, pkm_by_pair, thresholds)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
