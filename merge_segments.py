#!/usr/bin/env python3
"""
merge_segments.py
=================
One-time cleanup: merge nearby collinear road segments that were fragmented
by the (now-fixed) out-of-order concurrent frame processing bug.

Two segments are merged when ALL of the following hold:
  1. Endpoint proximity  — the closest pair of endpoints is within MERGE_GAP_METERS
  2. Axis alignment      — headings are on the same road axis (within MERGE_HEADING_DEGREES,
                            treating N and S as the same axis)
  3. Collinearity        — each segment's endpoints lie within PERP_THRESHOLD_METERS of the
                            other segment's bearing line (prevents merging parallel roads)

Single-point micro-segments (1 GPS fix) skip the heading and collinearity checks and
merge into any nearby multi-point segment that passes proximity alone.

Usage:
    python3 merge_segments.py                  # run with defaults
    python3 merge_segments.py --dry-run        # print what would merge, no DB writes
    python3 merge_segments.py --gap 100        # widen endpoint gap to 100 m
    python3 merge_segments.py --heading 35     # allow 35° heading deviation
    python3 merge_segments.py --perp 20        # allow 20 m off-axis deviation

Environment variables (same as the app containers):
    MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB
"""

import argparse
import json
import math
import logging
import os
from collections import defaultdict

import mysql.connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Defaults ─────────────────────────────────────────────────────────────────
DEFAULT_GAP     = 80    # metres — max gap between closest endpoints
DEFAULT_HEADING = 30    # degrees — max axis deviation
DEFAULT_PERP    = 15    # metres — max perpendicular deviation from bearing line


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


# ─── Geodesic helpers ─────────────────────────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def compute_bearing(lat1, lon1, lat2, lon2):
    dlon = math.radians(lon2 - lon1)
    rl1, rl2 = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon) * math.cos(rl2)
    y = math.cos(rl1) * math.sin(rl2) - math.sin(rl1) * math.cos(rl2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def heading_delta(h1, h2):
    d = abs(h1 - h2)
    return min(d, 360 - d)


def axis_delta(h1, h2):
    """Difference on the undirected axis (treats 0° and 180° as identical)."""
    hd = heading_delta(h1, h2)
    return min(hd, 180 - hd)


def seg_heading(path):
    """Bearing from first to last GPS point; None for single-point paths."""
    if len(path) < 2:
        return None
    return compute_bearing(path[0][0], path[0][1], path[-1][0], path[-1][1])


def perp_dist_to_line(pt_lat, pt_lon, la1, lo1, la2, lo2):
    """
    Perpendicular distance (metres) from (pt_lat, pt_lon) to the line
    defined by (la1, lo1) → (la2, lo2), using a flat-Earth approximation.
    Returns 0 if the two reference points are the same.
    """
    R = 6_371_000
    mid_rlat = math.radians((la1 + la2) / 2)
    cos_lat  = math.cos(mid_rlat)

    def to_xy(lat, lon):
        return (math.radians(lon - lo1) * R * cos_lat,
                math.radians(lat - la1) * R)

    dx_l, dy_l = to_xy(la2, lo2)
    base = math.hypot(dx_l, dy_l)
    if base < 1.0:
        return 0.0
    dx_p, dy_p = to_xy(pt_lat, pt_lon)
    return abs(dx_l * dy_p - dy_l * dx_p) / base


# ─── Union-Find ───────────────────────────────────────────────────────────────
class UF:
    def __init__(self):
        self._p: dict = {}

    def find(self, x):
        if x not in self._p:
            self._p[x] = x
        while self._p[x] != x:
            self._p[x] = self._p[self._p[x]]
            x = self._p[x]
        return x

    def union(self, a, b):
        pa, pb = self.find(a), self.find(b)
        if pa != pb:
            self._p[pa] = pb


# ─── Parse gps_path column ────────────────────────────────────────────────────
def parse_path(raw, fallback_lat, fallback_lon):
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = []
    if not isinstance(raw, list):
        raw = []
    pts = [pt for pt in raw if isinstance(pt, (list, tuple)) and len(pt) >= 2]
    return pts if pts else [[fallback_lat, fallback_lon]]


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Merge collinear nearby segment fragments")
    ap.add_argument("--dry-run",  action="store_true",
                    help="Compute merges but do not write to DB")
    ap.add_argument("--gap",      type=float, default=DEFAULT_GAP,
                    metavar="M",   help=f"Max endpoint gap in metres (default {DEFAULT_GAP})")
    ap.add_argument("--heading",  type=float, default=DEFAULT_HEADING,
                    metavar="DEG", help=f"Max axis deviation in degrees (default {DEFAULT_HEADING})")
    ap.add_argument("--perp",     type=float, default=DEFAULT_PERP,
                    metavar="M",   help=f"Max perpendicular deviation in metres (default {DEFAULT_PERP})")
    args = ap.parse_args()

    gap_m   = args.gap
    hdg_deg = args.heading
    perp_m  = args.perp

    logger.info("Parameters: gap=%.0f m  heading=%.0f°  perp=%.0f m  dry-run=%s",
                gap_m, hdg_deg, perp_m, args.dry_run)

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)

    # ── 1. Load all active segments ───────────────────────────────────────────
    logger.info("Loading active segments from DB…")
    cur.execute("""
        SELECT segment_id, start_lat, start_lon,
               COALESCE(end_lat, start_lat) AS end_lat,
               COALESCE(end_lon, start_lon) AS end_lon,
               gps_path, frame_count, municipality, submunicipality
        FROM segments
        WHERE status = 'active'
    """)
    rows = cur.fetchall()
    logger.info("  %d active segments loaded", len(rows))

    # Enrich with parsed paths and computed headings
    segs = []
    for r in rows:
        r["gps_path"] = parse_path(r["gps_path"], r["start_lat"], r["start_lon"])
        r["heading"]  = seg_heading(r["gps_path"])
        segs.append(r)

    # ── 2. Spatial grid for O(n) neighbor lookup ──────────────────────────────
    cell_deg = gap_m / 111_320.0
    grid: dict[tuple, list[int]] = defaultdict(list)

    def cells_for(lat, lon):
        cr, cc = int(lat / cell_deg), int(lon / cell_deg)
        return [(cr + dr, cc + dc) for dr in (-1, 0, 1) for dc in (-1, 0, 1)]

    def grid_add(idx, lat, lon):
        for key in cells_for(lat, lon):
            grid[key].append(idx)

    for i, s in enumerate(segs):
        path = s["gps_path"]
        grid_add(i, path[0][0], path[0][1])
        if len(path) > 1:
            grid_add(i, path[-1][0], path[-1][1])

    # ── 3. Find merge candidates ──────────────────────────────────────────────
    uf = UF()
    pair_count = 0

    for i, s in enumerate(segs):
        spath   = s["gps_path"]
        s_multi = len(spath) >= 2
        sh      = s.get("heading")

        # Representative endpoint(s) of s for proximity check
        s_pts = [spath[0], spath[-1]] if s_multi else [spath[0]]

        # Candidate set from grid
        cands: set[int] = set()
        for pt in s_pts:
            for key in cells_for(pt[0], pt[1]):
                cands.update(grid.get(key, []))
        cands.discard(i)

        for j in cands:
            if j <= i:
                continue

            t      = segs[j]
            tpath  = t["gps_path"]
            t_multi = len(tpath) >= 2
            th     = t.get("heading")

            # Municipality guard
            if s.get("municipality") and t.get("municipality"):
                if s["municipality"] != t["municipality"]:
                    continue

            t_pts = [tpath[0], tpath[-1]] if t_multi else [tpath[0]]

            # ── Check 1: endpoint proximity ──
            min_ep = min(
                haversine(sp[0], sp[1], tp[0], tp[1])
                for sp in s_pts for tp in t_pts
            )
            if min_ep > gap_m:
                continue

            # ── Check 2: axis alignment (skip if either is single-point) ──
            if sh is not None and th is not None:
                if axis_delta(sh, th) > hdg_deg:
                    continue

            # ── Check 3: collinearity — s's line vs t's points ──
            if s_multi:
                min_perp_t = min(
                    perp_dist_to_line(tp[0], tp[1],
                                      spath[0][0], spath[0][1],
                                      spath[-1][0], spath[-1][1])
                    for tp in t_pts
                )
                if min_perp_t > perp_m:
                    continue

            # ── Check 3: collinearity — t's line vs s's points ──
            if t_multi:
                min_perp_s = min(
                    perp_dist_to_line(sp[0], sp[1],
                                      tpath[0][0], tpath[0][1],
                                      tpath[-1][0], tpath[-1][1])
                    for sp in s_pts
                )
                if min_perp_s > perp_m:
                    continue

            uf.union(s["segment_id"], t["segment_id"])
            pair_count += 1

    logger.info("Merge pairs identified: %d", pair_count)

    # ── 4. Build merge groups ─────────────────────────────────────────────────
    groups: dict[str, list] = defaultdict(list)
    for s in segs:
        groups[uf.find(s["segment_id"])].append(s)

    multi_groups = {r: g for r, g in groups.items() if len(g) > 1}
    n_segs_affected = sum(len(g) for g in multi_groups.values())
    logger.info("Groups to merge: %d  covering %d segments  (%d singletons stay)",
                len(multi_groups), n_segs_affected, len(segs) - n_segs_affected)

    if args.dry_run:
        logger.info("── Dry run: showing first 15 groups ──")
        for root, grp in list(multi_groups.items())[:15]:
            frames = sum(s.get("frame_count", 0) or 0 for s in grp)
            logger.info("  root=%-36s  size=%d  total_frames=%d", root, len(grp), frames)
        return

    # ── 5. Execute merges ─────────────────────────────────────────────────────
    R_earth      = 6_371_000
    merged_ok    = 0
    deleted_segs = 0
    errors       = 0

    for root, grp in multi_groups.items():
        # Master = segment with the most frames (becomes the surviving row)
        master = max(grp, key=lambda s: s.get("frame_count", 0) or 0)
        others = [s for s in grp if s["segment_id"] != master["segment_id"]]

        # Pool all GPS points from the group
        all_pts = []
        for s in grp:
            all_pts.extend(s["gps_path"])

        if not all_pts:
            continue

        # Sort points along the master's heading so the path is coherent
        mh_rad  = math.radians(master.get("heading") or 0.0)
        ref_lat = all_pts[0][0]
        ref_lon = all_pts[0][1]
        cos_ref = math.cos(math.radians(ref_lat))

        def proj(pt):
            dx = math.radians(pt[1] - ref_lon) * R_earth * cos_ref
            dy = math.radians(pt[0] - ref_lat) * R_earth
            return dx * math.sin(mh_rad) + dy * math.cos(mh_rad)

        all_pts.sort(key=proj)

        # Deduplicate points within 2 m of each other
        merged_path = [all_pts[0]]
        for pt in all_pts[1:]:
            if haversine(pt[0], pt[1], merged_path[-1][0], merged_path[-1][1]) >= 2.0:
                merged_path.append(pt)

        new_start    = merged_path[0]
        new_end      = merged_path[-1]
        total_frames = sum(s.get("frame_count", 0) or 0 for s in grp)
        total_len    = sum(
            haversine(merged_path[k-1][0], merged_path[k-1][1],
                      merged_path[k][0],   merged_path[k][1])
            for k in range(1, len(merged_path))
        )

        try:
            cur.execute("""
                UPDATE segments
                SET start_lat=%s, start_lon=%s, end_lat=%s, end_lon=%s,
                    gps_path=%s, frame_count=%s, length_meters=%s
                WHERE segment_id=%s
            """, (
                new_start[0], new_start[1],
                new_end[0],   new_end[1],
                json.dumps(merged_path),
                total_frames,
                round(total_len, 1),
                master["segment_id"],
            ))

            for o in others:
                cur.execute(
                    "UPDATE violations SET segment_id=%s WHERE segment_id=%s",
                    (master["segment_id"], o["segment_id"]),
                )
                cur.execute(
                    "DELETE FROM segments WHERE segment_id=%s",
                    (o["segment_id"],),
                )
                deleted_segs += 1

            conn.commit()
            merged_ok += 1

        except Exception as exc:
            conn.rollback()
            logger.error("Merge failed for group root=%s: %s", root, exc)
            errors += 1

    logger.info("Merged %d groups  |  deleted %d redundant segments  |  errors %d",
                merged_ok, deleted_segs, errors)

    # ── 6. Sync violation_count for all segments ──────────────────────────────
    logger.info("Syncing violation_count…")
    cur.execute("""
        UPDATE segments s
        INNER JOIN (
            SELECT segment_id, COUNT(*) AS cnt
            FROM violations
            GROUP BY segment_id
        ) v ON s.segment_id = v.segment_id
        SET s.violation_count = v.cnt
    """)
    conn.commit()

    # ── 7. Final stats ────────────────────────────────────────────────────────
    cur.execute("""
        SELECT COUNT(*)                     AS total_segments,
               ROUND(AVG(frame_count), 1)   AS avg_frames,
               MAX(frame_count)             AS max_frames,
               SUM(frame_count >= 5)        AS segs_5plus_frames,
               SUM(frame_count >= 20)       AS segs_20plus_frames
        FROM segments
        WHERE status = 'active'
    """)
    stats = cur.fetchone()
    logger.info("After merge: %s", stats)

    cur.close()
    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
