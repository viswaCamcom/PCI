#!/usr/bin/env python3
"""
enqueue_reprocess.py
====================
Prepares the DB and re-enqueues all 209K frames for model reprocessing
through the existing RabbitMQ pipeline.

What this script does (in order):
  1. Adds px columns to violations (idempotent)
  2. Builds frame→segment_id map from current violations (before clearing)
  3. Builds GPS grid from segment paths for fast lookup of undetected frames
  4. Clears all violations (DELETE FROM violations)
  5. Resets segment PCI/violation metrics (keeps GPS paths intact)
  6. Resets frame statuses to 'pending' so the idempotency guard lets them through
  7. Publishes one message per frame to frame_queue (image already on disk —
     model_call workers skip OCI download and go straight to model call)

After this script finishes, the 32 model_call workers + 3 processing_engine
workers handle everything automatically.  Monitor progress at:
  http://localhost:15672  (RabbitMQ management — admin / mypass)

Usage:
  python3 enqueue_reprocess.py            # all frames
  python3 enqueue_reprocess.py --limit 100   # test on 100 frames first
"""

import argparse
import json
import logging
import sys
import time

import mysql.connector
import pika

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
DB = dict(host="127.0.0.1", port=3306,
          user="pci_user", password="pci_pass", database="pci")

RABBITMQ_URL = "amqp://admin:mypass@127.0.0.1:5672/"
FRAME_QUEUE  = "frame_queue"

# Docker-internal image path prefix (what the model_call containers see)
DOCKER_IMG_DIR = "/app/downloaded_images"


# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_conn():
    return mysql.connector.connect(**DB)


def ensure_px_columns(conn):
    cur = conn.cursor()
    for col, defn in [
        ("length_px",       "FLOAT DEFAULT NULL"),
        ("breadth_px",      "FLOAT DEFAULT NULL"),
        ("bbox_area_px",    "FLOAT DEFAULT NULL"),
        ("polygon_area_px", "FLOAT DEFAULT NULL"),
    ]:
        try:
            cur.execute(f"ALTER TABLE violations ADD COLUMN {col} {defn}")
            log.info("Added column violations.%s", col)
        except mysql.connector.Error as e:
            if e.errno != 1060:
                raise
    conn.commit()
    cur.close()


# ── Segment lookup helpers ──────────────────────────────────────────────────────

def build_frame_segment_map(conn) -> dict:
    """
    frame_id → segment_id from existing violations.
    Must be called BEFORE clearing violations.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT frame_id, MAX(segment_id) AS segment_id
        FROM   violations
        WHERE  segment_id IS NOT NULL
        GROUP  BY frame_id
    """)
    m = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    log.info("Frame→segment map built: %d entries", len(m))
    return m


def build_gps_grid(conn) -> dict:
    """
    Build (rounded_lat4, rounded_lon4) → segment_id from every GPS path point
    in all segments.  Used to assign segment_id to frames that previously had
    no violations (and are therefore absent from the frame→segment map).

    4 decimal places ≈ 11 m precision — tight enough to avoid cross-segment
    collisions on parallel roads, loose enough to absorb GPS jitter.
    """
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT segment_id, gps_path FROM segments WHERE gps_path IS NOT NULL")
    grid = {}
    for row in cur.fetchall():
        raw = row["gps_path"]
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        try:
            pts = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            continue
        if not isinstance(pts, list):
            continue
        for pt in pts:
            try:
                key = (round(float(pt[0]), 4), round(float(pt[1]), 4))
                grid[key] = row["segment_id"]
            except Exception:
                continue
    cur.close()
    log.info("GPS grid built: %d cells from segment paths", len(grid))
    return grid


def resolve_segment(frame_id, lat, lon, frame_seg_map, gps_grid) -> str | None:
    """Return segment_id for a frame, using two fallback levels."""
    # 1. Prior violation record (exact match)
    seg = frame_seg_map.get(frame_id)
    if seg:
        return seg

    if lat is None or lon is None:
        return None

    lat, lon = float(lat), float(lon)

    # 2. GPS grid at 4 decimal places (~11 m)
    seg = gps_grid.get((round(lat, 4), round(lon, 4)))
    if seg:
        return seg

    # 3. GPS grid at 3 decimal places (~111 m) — broader cell
    seg = gps_grid.get((round(lat, 3), round(lon, 3)))
    return seg


# ── Destructive DB prep ────────────────────────────────────────────────────────

def clear_violations(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM violations")
    n = cur.fetchone()[0]
    log.info("Deleting %d violation rows…", n)
    cur.execute("DELETE FROM violations")
    conn.commit()
    log.info("violations table cleared.")
    cur.close()


def reset_segment_metrics(conn):
    cur = conn.cursor()
    cur.execute("""
        UPDATE segments
        SET violation_count = 0,
            pci_score       = NULL,
            pci_rating      = NULL,
            health_score    = NULL,
            health_condition= NULL,
            health_color    = NULL
    """)
    conn.commit()
    log.info("Segment PCI/health metrics reset (%d rows).", cur.rowcount)
    cur.close()


def reset_frame_statuses(conn, limit_ids: list | None):
    cur = conn.cursor()
    if limit_ids:
        fmt = ",".join(["%s"] * len(limit_ids))
        cur.execute(
            f"UPDATE frames SET status='pending' WHERE frame_id IN ({fmt})",
            limit_ids,
        )
    else:
        cur.execute("UPDATE frames SET status='pending'")
    conn.commit()
    log.info("Frame statuses reset to 'pending': %d rows.", cur.rowcount)
    cur.close()


# ── RabbitMQ publish ───────────────────────────────────────────────────────────

def publish_frames(frames: list):
    params  = pika.URLParameters(RABBITMQ_URL)
    conn_mq = pika.BlockingConnection(params)
    ch      = conn_mq.channel()
    ch.queue_declare(queue=FRAME_QUEUE, durable=True)

    published = 0
    t0 = time.time()
    for f in frames:
        frame_id = f["frame_id"]
        msg = {
            "frame_id":        frame_id,
            # Docker-internal path — model_call workers find it on the shared volume
            "image_path":      f"{DOCKER_IMG_DIR}/{frame_id}.jpg",
            "image_metadata":  f.get("image_metadata_parsed", {}),
            "latitude":        f.get("latitude"),
            "longitude":       f.get("longitude"),
            "municipality":    f.get("municipality",    ""),
            "submunicipality": f.get("submunicipality", ""),
            "datetime_utc":    f.get("datetime_utc"),
            # Reprocess flags — processing_engine uses these to skip
            # segment detection and wipe old violations atomically
            "reprocess":       True,
            "segment_id":      f.get("segment_id"),
        }
        ch.basic_publish(
            exchange    = "",
            routing_key = FRAME_QUEUE,
            body        = json.dumps(msg).encode(),
            properties  = pika.BasicProperties(delivery_mode=2),
        )
        published += 1
        if published % 5000 == 0:
            elapsed = time.time() - t0
            rate = published / elapsed if elapsed else 0
            log.info("Enqueued %d / %d  (%.0f msg/s)", published, len(frames), rate)

    conn_mq.close()
    elapsed = time.time() - t0
    log.info("Published %d messages in %.1fs  (%.0f msg/s)",
             published, elapsed, published / elapsed if elapsed else 0)


# ── Main ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Re-enqueue all frames for model reprocessing via RabbitMQ."
    )
    parser.add_argument("--limit", type=int, default=0,
                        help="Only re-enqueue first N frames (0 = all)")
    args = parser.parse_args()

    log.info("=== PCI Reprocess Enqueue ===")
    if args.limit:
        log.info("TEST MODE: processing first %d frames only", args.limit)

    conn = get_conn()

    # ── Step 1: schema migration (idempotent) ──────────────────────────────────
    log.info("Step 1/7  Ensuring px columns exist…")
    ensure_px_columns(conn)

    # ── Step 2: build frame→segment map BEFORE clearing violations ────────────
    log.info("Step 2/7  Building frame→segment map from current violations…")
    frame_seg_map = build_frame_segment_map(conn)

    # ── Step 3: build GPS grid for undetected frames ──────────────────────────
    log.info("Step 3/7  Building GPS grid from segment paths…")
    gps_grid = build_gps_grid(conn)

    # ── Step 4: load frames ───────────────────────────────────────────────────
    log.info("Step 4/7  Loading frames from DB…")
    cur = conn.cursor(dictionary=True)
    limit_clause = f"LIMIT {args.limit}" if args.limit else ""
    cur.execute(f"""
        SELECT frame_id, local_image_path, image_metadata,
               latitude, longitude, municipality, submunicipality, datetime_utc
        FROM   frames
        WHERE  local_image_path IS NOT NULL
        ORDER  BY frame_id
        {limit_clause}
    """)
    frames = cur.fetchall()
    cur.close()
    log.info("  %d frames loaded.", len(frames))

    if not frames:
        log.error("No frames found — exiting.")
        sys.exit(1)

    # Resolve segment_id and parse metadata for each frame
    no_segment = 0
    for f in frames:
        # Parse stored EXIF JSON
        raw = f.get("image_metadata")
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        try:
            f["image_metadata_parsed"] = json.loads(raw) if isinstance(raw, str) else {}
        except Exception:
            f["image_metadata_parsed"] = {}

        seg = resolve_segment(
            f["frame_id"], f.get("latitude"), f.get("longitude"),
            frame_seg_map, gps_grid,
        )
        f["segment_id"] = seg
        if seg is None:
            no_segment += 1

    log.info("  Segment resolved: %d  |  No segment found: %d",
             len(frames) - no_segment, no_segment)

    # ── Step 5: clear violations ──────────────────────────────────────────────
    log.info("Step 5/7  Clearing all violations…")
    clear_violations(conn)

    # ── Step 6: reset segment + frame metrics ─────────────────────────────────
    log.info("Step 6/7  Resetting segment metrics and frame statuses…")
    reset_segment_metrics(conn)
    limit_ids = [f["frame_id"] for f in frames] if args.limit else None
    reset_frame_statuses(conn, limit_ids)

    conn.close()

    # ── Step 7: publish to RabbitMQ ───────────────────────────────────────────
    log.info("Step 7/7  Publishing %d messages to '%s'…", len(frames), FRAME_QUEUE)
    publish_frames(frames)

    log.info("")
    log.info("=== Enqueue complete ===")
    log.info("Monitor progress:  http://localhost:15672  (admin / mypass)")
    log.info("Queue '%s' now has %d messages.", FRAME_QUEUE, len(frames))
    log.info("The 32 model_call + 3 processing_engine workers handle the rest.")
    log.info("")
    log.info("When all frames are processed, run:")
    log.info("  python3 calculate_health_score.py scores")


if __name__ == "__main__":
    main()
