#!/usr/bin/env python3
"""
offline_resegment.py
====================
Delete all corrupted segments and rebuild them from scratch using the frames'
GPS coordinates and EXIF heading — in chronological capture order.

Why: merge_segments.py created zigzag segments by sorting unrelated frames from
different roads along a single bearing. This rebuilds correctly by replaying the
capture sequence: a turn or large gap creates a new segment, otherwise the nearest
matching segment is extended — exactly the same rules as processing_engine, but
without concurrent workers and without the now-removed computed-bearing hack.

Stop processing_engine before running:
    docker compose stop processing_engine
    python3 offline_resegment.py [--dry-run]
    docker compose start processing_engine

Environment variables (same as containers):
    MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB
"""

import argparse
import json
import logging
import math
import os
import uuid
from collections import defaultdict
from datetime import datetime

import mysql.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger(__name__)

# ─── Segmentation constants ───────────────────────────────────────────────────
MUNI_SHORT = {
    "002001": "Makkah",   "003001": "Madinah",  "004001": "Riyadh",
    "005001": "Jeddah",   "006001": "Eastern",  "007001": "Asir",
    "008001": "Qassim",   "009001": "Jazan",    "010001": "Jawf",
    "011001": "Tabuk",    "012001": "Hail",     "013001": "Northern",
    "014001": "Baha",     "015001": "Najran",   "016001": "Taif",
    "017001": "Ahsa",     "018001": "Hafar",
}

MAX_GAP_METERS    = 150    # GPS jump larger than this → new segment
TURN_THRESHOLD    = 30.0   # degrees — city roads cross at 30–45°; tighter catches intersections
SMOOTH_WINDOW     = 3      # same as tasks.py _segment_heading(): first (SMOOTH_WINDOW+1) points
TIME_GAP_SECONDS  = 10     # dashcam pause > 10 s → force new segment regardless of GPS
MAX_CROSS_TRACK_M = 20.0   # max perpendicular distance from road line before rejecting a frame;
                            # catches lateral jumps to parallel roads that share the same heading
                            # and therefore pass the bearing-only check

# Co-located segment merge (post-segmentation pass)
MERGE_DIST_M  = 20.0   # a point is "on the same road" if within 20 m of the other path
                       # (20 m catches divided dual-carriageway highways where both directions
                       # share the same segment; 15 m was too tight for divided roads)
MERGE_FRAC    = 0.70   # fraction of shorter segment's path that must overlap to trigger merge
MERGE_CELL_D  = MERGE_DIST_M / 111_320.0  # fine spatial grid for merge candidate lookup
HIGH_OVERLAP_FRAC = 0.90  # above this, skip the bearing guard entirely — near-total spatial
                           # containment is stronger evidence of same-road than a coarse
                           # whole-path bearing comparison, which breaks down when a short
                           # segment sits on a curve/bend (its start->end angle reflects the
                           # curve, not the road's macro direction)


# ─── DB ───────────────────────────────────────────────────────────────────────
def get_conn():
    return mysql.connector.connect(
        host      = os.environ.get("MYSQL_HOST",     "127.0.0.1"),
        port      = int(os.environ.get("MYSQL_PORT",  3306)),
        user      = os.environ.get("MYSQL_USER",     "pci_user"),
        password  = os.environ.get("MYSQL_PASSWORD", "pci_pass"),
        database  = os.environ.get("MYSQL_DB",       "pci"),
        autocommit = False,
    )


# ─── Geo helpers (mirrored from tasks.py) ─────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    R = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin(math.radians(lat2 - lat1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def compute_bearing(lat1, lon1, lat2, lon2):
    dlon = math.radians(lon2 - lon1)
    r1, r2 = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon) * math.cos(r2)
    y = math.cos(r1) * math.sin(r2) - math.sin(r1) * math.cos(r2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def heading_delta(h1, h2):
    d = abs(h1 - h2)
    return min(d, 360 - d)


def cross_track_dist(lat1, lon1, lat2, lon2, lat3, lon3):
    """Perpendicular distance (m) from (lat3,lon3) to the infinite line through the
    other two points.  Uses local flat-Earth approximation — accurate to < 0.1 % for
    the short distances (< 300 m) used here."""
    R    = 6_371_000
    mlat = R * math.pi / 180
    mlon = R * math.cos(math.radians((lat1 + lat2) / 2)) * math.pi / 180
    # Translate to local Cartesian with P1 at origin
    x2 = (lon2 - lon1) * mlon;  y2 = (lat2 - lat1) * mlat
    x3 = (lon3 - lon1) * mlon;  y3 = (lat3 - lat1) * mlat
    seg = math.hypot(x2, y2)
    # |cross product| / |direction vector| = perpendicular distance
    return abs(x2 * y3 - y2 * x3) / seg if seg > 1.0 else math.hypot(x3, y3)


def segment_heading(path):
    """Bearing using the first (SMOOTH_WINDOW+1) points — matches tasks.py exactly."""
    if len(path) < 2:
        return None
    n = min(SMOOTH_WINDOW + 1, len(path))
    return compute_bearing(path[0][0], path[0][1], path[n - 1][0], path[n - 1][1])


def path_length(path):
    total = 0.0
    for k in range(1, len(path)):
        total += haversine(path[k - 1][0], path[k - 1][1], path[k][0], path[k][1])
    return round(total, 1)


def _merge_colocated(segs: dict, frame_seg: dict) -> int:
    """Post-segmentation pass: merge segments that cover the same road stretch.

    Multiple recording passes of the same road produce separate segments (because
    the time-gap guard or cross-track check forces a new one each pass).  This
    function detects pairs where ≥ MERGE_FRAC of the shorter path lies within
    MERGE_DIST_M of the longer path, and absorbs the shorter into the longer.
    The GPS path of the primary (longer) segment is kept unchanged; only the
    frame count and violation assignments are updated.

    Returns the number of segments absorbed.
    """
    # Fine-grained spatial grid (MERGE_CELL_D ≈ 15 m) — every path point of
    # every segment is registered so we find only genuinely close candidates.
    pt_grid: dict = defaultdict(set)
    for sid, s in segs.items():
        for pt in s["path"]:
            r = int(pt[0] / MERGE_CELL_D)
            c = int(pt[1] / MERGE_CELL_D)
            pt_grid[(r, c)].add(sid)

    # Reverse index: segment_id → list of frame_ids (avoids O(N_frames) scan per merge)
    seg_frames: dict = defaultdict(list)
    for fid, fsid in frame_seg.items():
        seg_frames[fsid].append(fid)

    def nearby_segs(sid: str) -> set:
        cands: set = set()
        for pt in segs[sid]["path"]:
            r = int(pt[0] / MERGE_CELL_D)
            c = int(pt[1] / MERGE_CELL_D)
            for dr in (-1, 0, 1):
                for dc in (-1, 0, 1):
                    cands |= pt_grid.get((r + dr, c + dc), set())
        cands.discard(sid)
        return cands

    def overlap_frac(path_q: list, path_ref: list) -> float:
        """Fraction of path_q points within MERGE_DIST_M of any path_ref point.

        ALL path_ref points are indexed in a spatial hash — no subsampling on
        the reference side.  This is critical for long segments (e.g. a 14 km
        highway) where 25-point subsampling puts ref points 550 m apart, making
        a 20 m proximity test always fail.

        Only path_q is subsampled (to ≤ 50 points) to keep query cost bounded.
        """
        # Build spatial hash of ALL reference points
        cell_deg = (MERGE_DIST_M * 2.5) / 111_320.0   # ~50 m cells
        ref_grid: dict = defaultdict(list)
        for pr in path_ref:
            rr = int(pr[0] / cell_deg)
            rc = int(pr[1] / cell_deg)
            ref_grid[(rr, rc)].append(pr)

        # Subsample query path (only the query side, not the reference)
        step  = max(1, len(path_q) // 50)
        q_sub = path_q[::step]

        hits = 0
        for pq in q_sub:
            rr = int(pq[0] / cell_deg)
            rc = int(pq[1] / cell_deg)
            found = False
            for dr in (-1, 0, 1):
                for dc in (-1, 0, 1):
                    for pr in ref_grid.get((rr + dr, rc + dc), []):
                        if haversine(pq[0], pq[1], pr[0], pr[1]) <= MERGE_DIST_M:
                            found = True
                            break
                    if found:
                        break
                if found:
                    break
            hits += found

        return hits / max(1, len(q_sub))

    # Process from longest to shortest so long segments act as primaries
    order = sorted(segs.keys(), key=lambda s: -segs[s]["fc"])
    absorbed: set = set()
    n_merged = 0

    for primary in order:
        if primary in absorbed:
            continue
        path_p = segs[primary]["path"]

        for other in nearby_segs(primary):
            if other in absorbed:
                continue
            path_o = segs[other]["path"]

            # Overlap check: the shorter path must largely lie inside the longer
            short_p = path_o if len(path_o) <= len(path_p) else path_p
            long_p  = path_p if len(path_o) <= len(path_p) else path_o
            frac = overlap_frac(short_p, long_p)
            if frac < MERGE_FRAC:
                continue

            # Bearing guard: same direction (< TURN_THRESHOLD) or opposite direction
            # (> 180 - TURN_THRESHOLD) — bidirectional roads should merge too.
            # Skipped when overlap is near-total (>= HIGH_OVERLAP_FRAC) — see constant
            # comment above for why a coarse whole-path bearing check is unreliable there.
            if frac < HIGH_OVERLAP_FRAC and len(path_p) >= 2 and len(path_o) >= 2:
                b_p = compute_bearing(path_p[0][0], path_p[0][1],
                                      path_p[-1][0], path_p[-1][1])
                b_o = compute_bearing(path_o[0][0], path_o[0][1],
                                      path_o[-1][0], path_o[-1][1])
                delta = heading_delta(b_p, b_o)
                # Reject if neither same-direction nor opposite-direction
                if TURN_THRESHOLD < delta < (180 - TURN_THRESHOLD):
                    continue

            # Absorb other into primary — reassign its frames and bump frame count
            absorbed.add(other)
            segs[primary]["fc"] += segs[other]["fc"]
            for fid in seg_frames.pop(other, []):
                frame_seg[fid] = primary
                seg_frames[primary].append(fid)
            n_merged += 1

    for sid in absorbed:
        del segs[sid]

    return n_merged


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Rebuild segments from frame GPS data")
    ap.add_argument("--dry-run", action="store_true",
                    help="Run segmentation and print stats without writing to DB")
    args = ap.parse_args()

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)

    # ── 1. Load all processed/downloaded frames in capture order ──────────────
    logger.info("Loading frames from DB…")
    cur.execute("""
        SELECT frame_id, latitude, longitude,
               municipality, submunicipality,
               COALESCE(datetime_utc, created_at) AS ts,
               image_metadata
        FROM frames
        WHERE status IN ('processed', 'downloaded')
          AND latitude  IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY ts ASC
    """)
    frames = cur.fetchall()
    logger.info("  %d frames loaded", len(frames))

    # These dashcam images have no embedded GPS EXIF direction — heading will
    # be computed from the segment path itself (safe because frames are in order).
    for f in frames:
        f.pop("image_metadata", None)   # not needed; drop to save memory

    logger.info("  No EXIF heading in images — using path-computed bearing for turns")

    # ── 2. Segmentation with spatial grid ─────────────────────────────────────
    CELL_DEG = MAX_GAP_METERS / 111_320.0

    def cell_key(lat, lon):
        return (int(lat / CELL_DEG), int(lon / CELL_DEG))

    # grid: cell → [segment_ids whose current endpoint is in this cell]
    grid: dict[tuple, list] = defaultdict(list)

    # segs: segment_id → {path, fc, muni, submuni, ts}
    segs: dict[str, dict] = {}

    # frame_seg: frame_id → segment_id
    frame_seg: dict[str, str] = {}

    def grid_add(sid, lat, lon):
        grid[cell_key(lat, lon)].append(sid)

    def grid_remove(sid, lat, lon):
        key = cell_key(lat, lon)
        try:
            grid[key].remove(sid)
        except ValueError:
            pass

    def nearby_sids(lat, lon):
        r, c = cell_key(lat, lon)
        ids = []
        for dr in (-1, 0, 1):
            for dc in (-1, 0, 1):
                ids.extend(grid.get((r + dr, c + dc), []))
        return ids

    STATIONARY_METERS = 3    # skip if car hasn't moved (duplicate/stationary GPS fix)
    MIN_BEAR_DIST     = 8    # minimum metres for a reliable bearing computation
    RECENT_POINTS     = 8    # last N path points used for current direction estimate

    logger.info("Running segmentation…")
    prev_ts = None
    for i, f in enumerate(frames):
        lat  = f["latitude"]
        lon  = f["longitude"]
        muni = f.get("municipality")
        curr_ts = f.get("ts")

        # Recording-gap guard: if the dashcam was off for > TIME_GAP_SECONDS the
        # vehicle may have moved to a different road.  Force a new segment so we
        # never extend an old segment across a recording break.
        time_gap_break = False
        if prev_ts is not None and curr_ts is not None:
            try:
                gap_s = (curr_ts - prev_ts).total_seconds()
                if gap_s > TIME_GAP_SECONDS:
                    time_gap_break = True
            except Exception:
                pass
        prev_ts = curr_ts

        best_id, best_dist = None, float("inf")

        if not time_gap_break:
            for sid in nearby_sids(lat, lon):
                s    = segs[sid]
                path = s["path"]

                # Same municipality guard
                if s["muni"] and muni and s["muni"] != muni:
                    continue

                ep   = path[-1]
                dist = haversine(ep[0], ep[1], lat, lon)
                if dist > MAX_GAP_METERS:
                    continue
                if dist < STATIONARY_METERS:
                    # Stationary/duplicate GPS fix — extend without bearing check
                    if dist < best_dist:
                        best_dist, best_id = dist, sid
                    continue

                # Two complementary direction checks (both must pass to extend segment):
                #
                # 1. Bearing check — detects turns: the path changed heading so the
                #    new frame is on a different, intersecting road.
                #
                # 2. Cross-track check — detects lateral jumps: the new frame is on a
                #    parallel road with the same heading and therefore slips past the
                #    bearing check.  Measures the perpendicular distance from the new
                #    frame to the line defined by the segment's recent direction.
                if len(path) >= 2:
                    recent   = path[-min(RECENT_POINTS, len(path)):]
                    recent_d = haversine(recent[0][0], recent[0][1],
                                         recent[-1][0], recent[-1][1])
                    if recent_d >= MIN_BEAR_DIST:
                        # 1. Bearing check (bearing computation is unreliable for very
                        #    short endpoint→frame distances, so gate on MIN_BEAR_DIST)
                        if dist >= MIN_BEAR_DIST:
                            recent_h    = compute_bearing(recent[0][0], recent[0][1],
                                                          recent[-1][0], recent[-1][1])
                            new_bearing = compute_bearing(ep[0], ep[1], lat, lon)
                            if heading_delta(recent_h, new_bearing) > TURN_THRESHOLD:
                                continue  # turned → new segment
                        # 2. Cross-track check (applied regardless of dist)
                        ctd = cross_track_dist(recent[0][0], recent[0][1],
                                               recent[-1][0], recent[-1][1],
                                               lat, lon)
                        if ctd > MAX_CROSS_TRACK_M:
                            continue  # lateral jump → new segment
                    else:
                        # Path too short to establish direction reliably; apply a
                        # tighter gap so a directionless stub cannot reach far roads.
                        if dist > 40:
                            continue

                if dist < best_dist:
                    best_dist, best_id = dist, sid

        if best_id is not None:
            s    = segs[best_id]
            path = s["path"]
            # Move endpoint in grid
            grid_remove(best_id, path[-1][0], path[-1][1])
            path.append([lat, lon])
            s["fc"] += 1
            grid_add(best_id, lat, lon)
            frame_seg[f["frame_id"]] = best_id
        else:
            nid = str(uuid.uuid4())
            segs[nid] = {
                "path":    [[lat, lon]],
                "fc":      1,
                "muni":    muni,
                "submuni": f.get("submunicipality"),
                "ts":      f.get("ts") or datetime.utcnow().isoformat(),
            }
            grid_add(nid, lat, lon)
            frame_seg[f["frame_id"]] = nid

        if (i + 1) % 10_000 == 0:
            logger.info("  %d / %d frames  →  %d segments so far",
                        i + 1, len(frames), len(segs))

    logger.info("Segmentation complete: %d segments from %d frames",
                len(segs), len(frames))

    # ── 3. Stats ──────────────────────────────────────────────────────────────
    counts = sorted([s["fc"] for s in segs.values()], reverse=True)
    logger.info("  Top-10 sizes : %s", counts[:10])
    logger.info("  Single-frame : %d",  sum(1 for c in counts if c == 1))
    logger.info("  2–4 frames   : %d",  sum(1 for c in counts if 2 <= c <= 4))
    logger.info("  5+ frames    : %d",  sum(1 for c in counts if c >= 5))

    # ── 3b. Merge co-located segments (multiple passes of the same road) ──────
    logger.info("Merging co-located segments (same road, multiple recording passes)…")
    n_merged = _merge_colocated(segs, frame_seg)
    if n_merged:
        counts = sorted([s["fc"] for s in segs.values()], reverse=True)
        logger.info("  Absorbed %d duplicate-road segments → %d unique segments",
                    n_merged, len(segs))
        logger.info("  Top-10 after merge : %s", counts[:10])
    else:
        logger.info("  No co-located pairs found.")

    if args.dry_run:
        cur.close(); conn.close()
        return

    # ── 3c. Generate human-readable segment names ─────────────────────────────
    # Format: {CityShortName}-{submunicipality}-seg-{N}
    # N is a 1-based counter per (municipality, submunicipality) pair, ordered
    # by frame_count descending so the busiest segment gets -seg-1.
    name_counters: dict = {}   # (muni, submuni) → next integer
    for sid in sorted(segs, key=lambda s: -segs[s]["fc"]):
        s    = segs[sid]
        muni  = (s.get("muni")    or "").strip()
        sub   = (s.get("submuni") or "").strip()
        city  = MUNI_SHORT.get(muni, muni or "Unknown")
        key   = (muni, sub)
        n     = name_counters.get(key, 0) + 1
        name_counters[key] = n
        sub_part = sub if sub else muni
        s["name"] = f"{city}_{sub_part}_seg_{n}"

    # ── 4. Clear old segments + violation segment refs ────────────────────────
    logger.info("Clearing old segments…")
    cur.execute("DELETE FROM segments")
    cur.execute("UPDATE violations SET segment_id = NULL")
    conn.commit()
    logger.info("  Old data cleared.")

    # ── 5. Bulk-insert new segments ───────────────────────────────────────────
    INSERT_SQL = """
        INSERT INTO segments
            (segment_id, start_lat, start_lon, end_lat, end_lon,
             gps_path, frame_count, violation_count,
             municipality, submunicipality,
             status, length_meters, created_at, segment_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s, %s, 'active', %s, %s, %s)
    """
    logger.info("Inserting %d segments…", len(segs))
    batch = []
    inserted = 0
    for sid, s in segs.items():
        p = s["path"]
        batch.append((
            sid,
            p[0][0],  p[0][1],
            p[-1][0], p[-1][1],
            json.dumps(p),
            s["fc"],
            s["muni"], s["submuni"],
            path_length(p),
            s["ts"],
            s.get("name"),
        ))
        if len(batch) >= 500:
            cur.executemany(INSERT_SQL, batch)
            conn.commit()
            inserted += len(batch)
            batch.clear()
            logger.info("  %d segments inserted…", inserted)
    if batch:
        cur.executemany(INSERT_SQL, batch)
        conn.commit()
        inserted += len(batch)
    logger.info("  %d segments inserted.", inserted)

    # ── 6. Re-assign violations to their segments ─────────────────────────────
    logger.info("Re-assigning violations…")
    UPDATE_SQL = "UPDATE violations SET segment_id = %s WHERE frame_id = %s"
    rows  = [(seg_id, fid) for fid, seg_id in frame_seg.items()]
    for i in range(0, len(rows), 1000):
        cur.executemany(UPDATE_SQL, rows[i:i + 1000])
        conn.commit()
    logger.info("  %d violation assignments updated.", len(rows))

    # ── 7. Sync violation_count ───────────────────────────────────────────────
    logger.info("Syncing violation_count…")
    cur.execute("""
        UPDATE segments s
        INNER JOIN (
            SELECT segment_id, COUNT(*) AS cnt
            FROM violations
            WHERE segment_id IS NOT NULL
            GROUP BY segment_id
        ) v ON s.segment_id = v.segment_id
        SET s.violation_count = v.cnt
    """)
    conn.commit()

    # ── 8. Final stats ────────────────────────────────────────────────────────
    cur.execute("""
        SELECT COUNT(*)                     AS total_segments,
               ROUND(AVG(frame_count), 1)   AS avg_frames,
               MAX(frame_count)             AS max_frames,
               SUM(frame_count = 1)         AS single_frame,
               SUM(frame_count >= 5)        AS segs_5plus
        FROM segments
        WHERE status = 'active'
    """)
    logger.info("Final DB state: %s", cur.fetchone())

    cur.close(); conn.close()
    logger.info("Done — restart processing_engine when ready.")


if __name__ == "__main__":
    main()
