"""
processing_worker/tasks.py
===========================
Consumes from : result_queue

For each message:
  1. Parse model_response → extract detections
  2. Detect/extend road segment (single atomic transaction with FOR UPDATE)
  3. Draw bounding boxes + polygons on image via PIL → save annotated image
  4. Save violations to DB
  5. Update frame status → 'processed'
"""

import os
import json
import math
import uuid
import logging
import time
from datetime import datetime

import pika
import mysql.connector
import mysql.connector.pooling
from mysql.connector import Error as MySQLError
from PIL import Image, ImageDraw

from pci_calculator import compute_frame_pci, compute_segment_pci, pci_rating

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST",    "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER",    "admin")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS",    "mypass")
RESULT_QUEUE  = "result_queue"

DOWNLOAD_DIR  = os.environ.get("DOWNLOAD_DIR",  "/app/downloaded_images")
ANNOTATED_DIR = os.environ.get("ANNOTATED_DIR", "/app/downloaded_images/annotated")

TURN_THRESHOLD     = 45.0   # degrees — real intersection turn
SMOOTH_WINDOW      = 3      # GPS points used to derive segment heading
MIN_SEGMENT_FRAMES = 2      # frames before computed-bearing turn detection arms
MAX_GAP_METERS     = 150    # GPS jump larger than this → different road
MIN_SPEED_KMH      = 5.0    # skip GPS advancement for stationary frames

os.makedirs(ANNOTATED_DIR, exist_ok=True)

# ─── DB connection pool ───────────────────────────────────────────────────────
_db_config = {
    "host":               os.environ.get("MYSQL_HOST",     "mysql"),
    "port":               int(os.environ.get("MYSQL_PORT",  3306)),
    "user":               os.environ.get("MYSQL_USER",     "pci_user"),
    "password":           os.environ.get("MYSQL_PASSWORD", "pci_pass"),
    "database":           os.environ.get("MYSQL_DB",       "pci"),
    "charset":            "utf8mb4",
    "autocommit":         False,
    "connection_timeout": 10,
}

_pool: "mysql.connector.pooling.MySQLConnectionPool | None" = None


def _get_pool():
    global _pool
    if _pool is None:
        _pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="proc_pool",
            pool_size=10,
            **_db_config,
        )
        logger.info("MySQL connection pool created (size=10)")
    return _pool


def get_conn():
    return _get_pool().get_connection()


# ─── Label colour map ─────────────────────────────────────────────────────────
LABEL_COLORS = {
    "pothole":            (255,  50,  50),
    "longitudinal_crack": (255, 165,   0),
    "transverse_crack":   (255, 255,   0),
    "alligator_crack":    (255,   0, 255),
    "rutting":            (  0, 200, 255),
    "default":            (  0, 255,   0),
}

def get_color(label):
    return LABEL_COLORS.get(label.lower(), LABEL_COLORS["default"])


# ─── Heading / distance math ──────────────────────────────────────────────────

def compute_bearing(lat1, lon1, lat2, lon2):
    dlon  = math.radians(lon2 - lon1)
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon) * math.cos(rlat2)
    y = (math.cos(rlat1) * math.sin(rlat2)
         - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon))
    return (math.degrees(math.atan2(x, y)) + 360) % 360

def heading_delta(h1, h2):
    d = abs(h1 - h2)
    return min(d, 360 - d)

def haversine_meters(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ─── GPS path helpers ──────────────────────────────────────────────────────────

def _parse_gps_path(row):
    gp = row.get("gps_path")
    if isinstance(gp, str):
        try:
            row["gps_path"] = json.loads(gp)
        except Exception:
            row["gps_path"] = []
    if not isinstance(row.get("gps_path"), list):
        row["gps_path"] = []
    return row

def _segment_heading(path):
    """Bearing from the first to the (SMOOTH_WINDOW+1)th GPS point."""
    if len(path) < 2:
        return None
    n = min(SMOOTH_WINDOW + 1, len(path))
    return compute_bearing(path[0][0], path[0][1], path[n - 1][0], path[n - 1][1])

def _exif_val(meta, *keys):
    for k in keys:
        v = meta.get(k)
        if v is not None:
            return v
    return None


# ─── Segment detection — atomic transaction with row-level locking ─────────────
#
# Using SELECT … FOR UPDATE inside a READ COMMITTED transaction ensures that
# two concurrent processing_engine replicas cannot both see "no matching segment"
# and both insert a duplicate segment for the same GPS point.
#
# Deadlock retry handles the rare case where two replicas lock overlapping
# bounding boxes in opposite order.

def _nearest_path_dist(lat, lon, row):
    """Minimum haversine distance from (lat,lon) to any GPS path point in row."""
    best = haversine_meters(lat, lon, row["end_lat"], row["end_lon"])
    for pt in row.get("gps_path") or []:
        d = haversine_meters(lat, lon, pt[0], pt[1])
        if d < best:
            best = d
    return best


def _process_segment_tx(lat, lon, municipality, submunicipality,
                         heading, is_stationary):
    """Single DB transaction: find-or-create a segment. Returns segment_id."""
    lat_delta = MAX_GAP_METERS / 111_320.0
    lon_delta = MAX_GAP_METERS / (111_320.0 * math.cos(math.radians(lat)) + 1e-9)
    # Wider box (3×) used to catch out-of-order frames that land near the
    # start or middle of an existing segment, not just its end point.
    lat_wide  = lat_delta * 3
    lon_wide  = lon_delta * 3
    now = datetime.utcnow().isoformat()

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        conn.start_transaction(isolation_level="READ COMMITTED")

        # Primary search: end point within normal radius (indexed, fast path).
        # Fallback: start point within 3× radius — catches frames that arrive
        # out of order after concurrent workers already advanced the segment end.
        cur.execute(
            """SELECT segment_id, start_lat, start_lon, end_lat, end_lon,
                      gps_path, frame_count, municipality, submunicipality,
                      status, created_at
               FROM segments
               WHERE status = 'active'
                 AND (
                   (end_lat   BETWEEN %s AND %s AND end_lon   BETWEEN %s AND %s)
                   OR
                   (start_lat BETWEEN %s AND %s AND start_lon BETWEEN %s AND %s)
                 )
               FOR UPDATE""",
            (lat - lat_delta, lat + lat_delta, lon - lon_delta, lon + lon_delta,
             lat - lat_wide,  lat + lat_wide,  lon - lon_wide,  lon + lon_wide),
        )
        rows = cur.fetchall()

        best_seg  = None
        best_dist = float("inf")

        for row in rows:
            _parse_gps_path(row)
            # Match against the nearest point on the path, not just the end —
            # this correctly identifies segments for out-of-order frames.
            dist = _nearest_path_dist(lat, lon, row)
            if dist > MAX_GAP_METERS:
                continue
            if not is_stationary and heading is not None:
                seg_h = _segment_heading(row["gps_path"])
                if seg_h is not None and heading_delta(seg_h, heading) > TURN_THRESHOLD:
                    continue
            if dist < best_dist:
                best_dist = dist
                best_seg  = row

        def _insert_new_segment(new_lat, new_lon):
            seg_id = str(uuid.uuid4())
            cur.execute(
                """INSERT INTO segments
                   (segment_id, start_lat, start_lon, end_lat, end_lon,
                    gps_path, frame_count, municipality, submunicipality,
                    status, created_at, sealed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'active',%s,NULL)""",
                (seg_id, new_lat, new_lon, new_lat, new_lon,
                 json.dumps([[new_lat, new_lon]]), 1,
                 municipality, submunicipality, now),
            )
            return seg_id

        if best_seg is None:
            seg_id = _insert_new_segment(lat, lon)
            conn.commit()
            logger.info("New segment %s  lat=%.5f lon=%.5f  heading=%s",
                        seg_id, lat, lon,
                        f"{heading:.1f}°" if heading is not None else "n/a")
            return seg_id

        seg_id = best_seg["segment_id"]
        path   = best_seg["gps_path"]

        # Computed-bearing turn detection (when EXIF heading absent)
        if not is_stationary and heading is None and len(path) >= 1:
            prev = path[-1]
            if prev[0] != lat or prev[1] != lon:
                computed  = compute_bearing(prev[0], prev[1], lat, lon)
                seg_h     = _segment_heading(path)
                if (seg_h is not None
                        and best_seg.get("frame_count", 0) >= MIN_SEGMENT_FRAMES
                        and heading_delta(seg_h, computed) > TURN_THRESHOLD):
                    seg_id = _insert_new_segment(lat, lon)
                    conn.commit()
                    logger.info("Turn detected → new segment %s", seg_id)
                    return seg_id

        # Extend matched segment
        if not is_stationary:
            path.append([lat, lon])
        cur.execute(
            """UPDATE segments
               SET end_lat=%s, end_lon=%s, gps_path=%s, frame_count=%s
               WHERE segment_id=%s""",
            (lat, lon, json.dumps(path),
             best_seg.get("frame_count", 0) + 1, seg_id),
        )
        conn.commit()
        return seg_id

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def process_segment(lat, lon, municipality, submunicipality, image_metadata=None):
    """
    GPS-proximity segment detection — order-independent, safe for concurrent replicas.

    Uses a single READ COMMITTED transaction with SELECT … FOR UPDATE so that
    multiple processing_engine replicas cannot create duplicate segments for
    the same GPS location simultaneously.

    Retries automatically on MySQL deadlocks (errno 1213).
    """
    meta = image_metadata or {}

    # Prefer EXIF GPS over form-field values
    exif_lat = _exif_val(meta, "EXIF:GPSLatitude",  "Composite:GPSLatitude")
    exif_lon = _exif_val(meta, "EXIF:GPSLongitude", "Composite:GPSLongitude")
    if exif_lat is not None and exif_lon is not None:
        lat, lon = float(exif_lat), float(exif_lon)

    speed_kmh    = _exif_val(meta, "EXIF:GPSSpeed")
    is_stationary = speed_kmh is not None and float(speed_kmh) < MIN_SPEED_KMH
    if is_stationary:
        logger.info("Stationary frame (%.1f km/h) — GPS not advanced", float(speed_kmh))

    exif_heading = _exif_val(meta, "EXIF:GPSImgDirection")
    heading = float(exif_heading) if exif_heading is not None else None

    for attempt in range(3):
        try:
            return _process_segment_tx(lat, lon, municipality, submunicipality,
                                        heading, is_stationary)
        except MySQLError as e:
            if e.errno == 1213 and attempt < 2:   # ER_LOCK_DEADLOCK
                logger.warning("Deadlock in segment detection — retry %d", attempt + 1)
                time.sleep(0.05 * (attempt + 1))
                continue
            raise


# ─── Annotation drawing ───────────────────────────────────────────────────────

def draw_annotations(image_path, results, frame_id):
    img  = Image.open(image_path).convert("RGB")
    draw = ImageDraw.Draw(img, "RGBA")

    for det in results:
        label   = det.get("label", "unknown")
        conf    = det.get("confidence", 0)
        bbox    = det.get("bounding_box", {})
        polygon = det.get("polygon_points", [])
        color   = get_color(label)
        color_t = color + (70,)

        if polygon and len(polygon) >= 3:
            pts = [tuple(p) for p in polygon]
            draw.polygon(pts, fill=color_t, outline=color + (255,))

        if bbox:
            x0, y0 = bbox.get("xmin", 0), bbox.get("ymin", 0)
            x1, y1 = bbox.get("xmax", 0), bbox.get("ymax", 0)
            draw.rectangle([x0, y0, x1, y1], outline=color, width=3)
            text   = f"{label} {conf:.0%}"
            tx, ty = x0, max(y0 - 20, 0)
            tw     = len(text) * 7 + 8
            draw.rectangle([tx, ty, tx + tw, ty + 18], fill=color)
            draw.text((tx + 4, ty + 2), text, fill=(0, 0, 0))

    out_path = os.path.join(ANNOTATED_DIR, f"{frame_id}.jpg")
    img.save(out_path, "JPEG", quality=90)
    logger.info("Annotated image → %s", out_path)
    return out_path


# ─── DB writes ────────────────────────────────────────────────────────────────

def save_violations(frame_id, segment_id, results, lat, lon,
                    annotated_path, image_width, image_height, gsd):
    if not results:
        return
    conn = get_conn()
    cur  = conn.cursor()
    now  = datetime.utcnow().isoformat()
    try:
        for r in results:
            bbox = r.get("bounding_box", {})
            dims = r.get("dimensions",   {})
            area = r.get("area",         {})
            cur.execute(
                """INSERT INTO violations
                   (frame_id, segment_id, label, confidence, severity,
                    bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
                    polygon_points, length_mm, breadth_mm,
                    bbox_area_mm2, polygon_area_mm2,
                    gsd_mm_per_px, image_width, image_height,
                    latitude, longitude, annotated_image_path, created_at)
                   VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (frame_id, segment_id,
                 r.get("label"), r.get("confidence"), r.get("severity", "low"),
                 bbox.get("xmin"), bbox.get("ymin"),
                 bbox.get("xmax"), bbox.get("ymax"),
                 json.dumps(r.get("polygon_points", [])),
                 dims.get("length_mm"), dims.get("breadth_mm"),
                 area.get("bbox_area_mm2"), area.get("polygon_area_mm2"),
                 gsd, image_width, image_height,
                 lat, lon, annotated_path, now),
            )
        conn.commit()
        logger.info("Saved %d violations for frame_id=%s", len(results), frame_id)
    finally:
        cur.close(); conn.close()


def update_frame_status(frame_id, status):
    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute(
            "UPDATE frames SET status=%s, updated_at=%s WHERE frame_id=%s",
            (status, datetime.utcnow().isoformat(), frame_id),
        )
        conn.commit()
    finally:
        cur.close(); conn.close()


def update_frame_pci(frame_id, score, rating):
    """Persist PCI score and rating for a single frame."""
    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute(
            "UPDATE frames SET pci_score=%s, pci_rating=%s, updated_at=%s WHERE frame_id=%s",
            (score, rating, datetime.utcnow().isoformat(), frame_id),
        )
        conn.commit()
        logger.info("PCI frame_id=%s  score=%.1f  rating=%s", frame_id, score, rating)
    finally:
        cur.close(); conn.close()


def recompute_segment_pci(segment_id):
    """
    Query all violations in a segment, compute PCI, and update the segment record.
    Called every time a new frame is processed for the segment so the score stays current.
    """
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """SELECT label, severity, polygon_area_mm2,
                      image_width, image_height, gsd_mm_per_px, frame_id
               FROM violations
               WHERE segment_id = %s""",
            (segment_id,),
        )
        viols = cur.fetchall()

        score  = compute_segment_pci(viols)
        rating = pci_rating(score)

        cur.execute(
            "UPDATE segments SET pci_score=%s, pci_rating=%s WHERE segment_id=%s",
            (score, rating, segment_id),
        )
        conn.commit()
        logger.info("PCI segment=%s  score=%.1f  rating=%s  violations=%d",
                    segment_id, score, rating, len(viols))
        return score, rating
    finally:
        cur.close(); conn.close()


# ─── Message handler ──────────────────────────────────────────────────────────

def handle_result(channel, method, properties, body):
    try:
        msg             = json.loads(body)
        frame_id        = msg["frame_id"]
        image_path      = msg.get("image_path", "")
        lat             = msg.get("latitude")
        lon             = msg.get("longitude")
        municipality    = msg.get("municipality",    "")
        submunicipality = msg.get("submunicipality", "")
        model_response  = msg.get("model_response",  {})
        image_metadata  = msg.get("image_metadata",  {})

        results      = model_response.get("results",       [])
        image_width  = model_response.get("image_width",   0)
        image_height = model_response.get("image_height",  0)
        gsd          = model_response.get("gsd_mm_per_px", 0)

        speed   = image_metadata.get("EXIF:GPSSpeed", "n/a")
        heading = image_metadata.get("EXIF:GPSImgDirection", "n/a")
        logger.info("frame_id=%s  lat=%s  lon=%s  speed=%s km/h  heading=%s°  detections=%d",
                    frame_id, lat, lon, speed, heading, len(results))

        # ── 1. Segment detection (atomic) ─────────────────────────────────────
        segment_id = process_segment(lat, lon, municipality, submunicipality, image_metadata)

        # ── 2. Annotate image ─────────────────────────────────────────────────
        annotated_path = None
        if results and os.path.exists(image_path):
            try:
                annotated_path = draw_annotations(image_path, results, frame_id)
            except Exception as e:
                logger.error("Annotation failed frame_id=%s: %s", frame_id, e)

        # ── 3. Save violations ────────────────────────────────────────────────
        save_violations(
            frame_id, segment_id, results,
            lat, lon, annotated_path,
            image_width, image_height, gsd,
        )

        # ── 4. Compute and store PCI ──────────────────────────────────────────
        frame_score  = compute_frame_pci(results, image_width, image_height, gsd)
        frame_rating = pci_rating(frame_score)
        update_frame_pci(frame_id, frame_score, frame_rating)
        recompute_segment_pci(segment_id)

        # ── 5. Mark processed ─────────────────────────────────────────────────
        update_frame_status(frame_id, "processed")
        logger.info("frame_id=%s done  segment=%s  violations=%d  pci=%.1f (%s)",
                    frame_id, segment_id, len(results), frame_score, frame_rating)

        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.exception("Unhandled error processing frame: %s", e)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


# ─── Main loop ────────────────────────────────────────────────────────────────

def main():
    logger.info("processing_worker starting ...")
    while True:
        try:
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host        = RABBITMQ_HOST,
                    port        = RABBITMQ_PORT,
                    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
                    heartbeat   = 600,
                    blocked_connection_timeout = 300,
                )
            )
            ch = conn.channel()
            ch.queue_declare(queue=RESULT_QUEUE, durable=True)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=RESULT_QUEUE, on_message_callback=handle_result)
            logger.info("Waiting for results ...")
            ch.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error("RabbitMQ lost: %s — retry in 5s", e)
            time.sleep(5)
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()
