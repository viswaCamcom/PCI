#!/usr/bin/env python3
"""
freeze_mbs_segments.py
=======================
One-time snapshot: freeze the current Makkah<->Jeddah corridor segments into
mbs_segments as permanent, comparable units.

Why: the live `segments` table is dynamically rebuilt by offline_resegment.py
on every run (new UUIDs, geometry can shift) — there is no way to say "the
same segment" across two different capture dates. This script copies today's
corridor segments (already well-formed and interchange-bounded, since
offline_resegment.py's turn-detection naturally breaks at real interchanges)
into a separate, permanent table that offline_resegment.py never touches.

The corridor bounding box is derived empirically from the live `segments`
table (min/max endpoints of Makkah + Jeddah municipality segments, +buffer),
not hardcoded — run --dry-run first and sanity-check the candidate count.

Usage:
    python3 freeze_mbs_segments.py --dry-run     # show candidates, write nothing
    python3 freeze_mbs_segments.py               # freeze into mbs_segments
    python3 freeze_mbs_segments.py --buffer-km 3 # widen the bbox buffer (default 2km)

Environment variables (same as the app containers):
    MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB
"""

import argparse
import json
import logging
import os

import mysql.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger(__name__)

CORRIDOR_MUNICIPALITIES = ["002001", "005001"]  # Makkah, Jeddah
DEG_PER_KM = 1 / 111.32  # rough conversion, fine for a small local buffer


def get_conn():
    return mysql.connector.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", 3306)),
        user=os.environ.get("MYSQL_USER", "pci_user"),
        password=os.environ.get("MYSQL_PASSWORD", "pci_pass"),
        database=os.environ.get("MYSQL_DB", "pci"),
        autocommit=False,
    )


def compute_bbox(cur, buffer_km: float):
    cur.execute(
        """
        SELECT MIN(LEAST(start_lat, end_lat))   AS min_lat,
               MAX(GREATEST(start_lat, end_lat)) AS max_lat,
               MIN(LEAST(start_lon, end_lon))    AS min_lon,
               MAX(GREATEST(start_lon, end_lon)) AS max_lon,
               COUNT(*)                          AS n
        FROM segments
        WHERE municipality IN (%s, %s)
        """,
        CORRIDOR_MUNICIPALITIES,
    )
    row = cur.fetchone()
    if not row or row["n"] == 0:
        raise SystemExit(
            f"No live segments found for municipalities {CORRIDOR_MUNICIPALITIES} — "
            "has the corridor data been uploaded/processed yet?"
        )
    buf = buffer_km * DEG_PER_KM
    bbox = {
        "min_lat": row["min_lat"] - buf,
        "max_lat": row["max_lat"] + buf,
        "min_lon": row["min_lon"] - buf,
        "max_lon": row["max_lon"] + buf,
    }
    logger.info(
        "Corridor bbox from %d municipality-matched segments (+%.1fkm buffer): "
        "lat [%.5f, %.5f]  lon [%.5f, %.5f]",
        row["n"], buffer_km, bbox["min_lat"], bbox["max_lat"], bbox["min_lon"], bbox["max_lon"],
    )
    return bbox


def find_candidates(cur, bbox):
    """
    Any segment with EITHER endpoint inside the bbox is a candidate — this
    deliberately doesn't filter by municipality again, since a segment could
    straddle a boundary or be tagged with a neighbouring submunicipality.
    """
    cur.execute(
        """
        SELECT segment_id, start_lat, start_lon, end_lat, end_lon, gps_path,
               length_meters, municipality, submunicipality, segment_name,
               road_type, segment_width_m, frame_count
        FROM segments
        WHERE (start_lat BETWEEN %s AND %s AND start_lon BETWEEN %s AND %s)
           OR (end_lat   BETWEEN %s AND %s AND end_lon   BETWEEN %s AND %s)
        """,
        (
            bbox["min_lat"], bbox["max_lat"], bbox["min_lon"], bbox["max_lon"],
            bbox["min_lat"], bbox["max_lat"], bbox["min_lon"], bbox["max_lon"],
        ),
    )
    return cur.fetchall()


def main():
    ap = argparse.ArgumentParser(description="Freeze corridor segments into mbs_segments")
    ap.add_argument("--dry-run", action="store_true", help="Show candidates, write nothing")
    ap.add_argument("--buffer-km", type=float, default=2.0, help="Bbox buffer in km (default 2)")
    args = ap.parse_args()

    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    bbox = compute_bbox(cur, args.buffer_km)
    candidates = find_candidates(cur, bbox)
    logger.info("Found %d candidate segments to freeze.", len(candidates))

    total_km = sum(float(c["length_meters"] or 0) for c in candidates) / 1000.0
    logger.info("Total candidate length: %.1f km", total_km)

    if args.dry_run:
        logger.info("─── DRY RUN — first 10 candidates ───────────────────────────")
        for c in candidates[:10]:
            logger.info(
                "  %s  name=%-25s  muni=%s/%s  frames=%-5s  len=%.0fm",
                c["segment_id"][:8], c["segment_name"] or "(unnamed)",
                c["municipality"], c["submunicipality"], c["frame_count"], c["length_meters"] or 0,
            )
        logger.info("Total: %d segments, %.1f km — no rows written.", len(candidates), total_km)
        cur.close()
        conn.close()
        return

    if not candidates:
        logger.warning("No candidates found — nothing to freeze.")
        cur.close()
        conn.close()
        return

    rows = []
    for c in candidates:
        path = c["gps_path"]
        if isinstance(path, (bytes, bytearray)):
            path = path.decode()
        # gps_path comes back from mysql-connector as a Python object already
        # decoded from JSON when using dictionary cursors on a JSON column —
        # normalise defensively in case it's a raw string.
        if isinstance(path, str):
            try:
                path = json.loads(path)
            except Exception:
                path = []
        rows.append((
            c["segment_id"],            # mbs_segment_id (reuse live UUID at freeze time)
            c["segment_id"],            # source_segment_id (audit only)
            c["start_lat"], c["start_lon"],
            c["end_lat"], c["end_lon"],
            json.dumps(path),
            c["length_meters"] or 0,
            c["municipality"], c["submunicipality"],
            c["segment_name"], c["road_type"], c["segment_width_m"],
        ))

    cur.executemany(
        """
        INSERT INTO mbs_segments
            (mbs_segment_id, source_segment_id, start_lat, start_lon, end_lat, end_lon,
             gps_path, length_meters, municipality, submunicipality,
             segment_name, road_type, segment_width_m)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            gps_path = VALUES(gps_path),
            length_meters = VALUES(length_meters),
            segment_name = VALUES(segment_name),
            road_type = VALUES(road_type),
            segment_width_m = VALUES(segment_width_m)
        """,
        rows,
    )
    conn.commit()
    logger.info("Froze %d segments into mbs_segments (%.1f km total).", len(rows), total_km)
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
