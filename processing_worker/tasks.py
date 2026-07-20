"""
processing_worker/tasks.py
===========================
Consumes from : result_queue

For each message:
  1. Parse model_response → extract detections
  2. Detect/extend road segment (advisory lock + FOR UPDATE — multi-worker safe)
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

from pci_calculator import (compute_frame_pci, compute_segment_health_score,
                             compute_thresholds, pci_rating, LABEL_TO_TYPE)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Percentile threshold cache (refreshed every hour from DB) ────────────────
_THRESHOLD_CACHE: dict = {}          # {"pothole": (p25,p50,p75), ...}
_THRESHOLD_TS:    float = 0.0
_THRESHOLD_TTL:   int   = 3600       # seconds

# Fallback thresholds from last known batch run (used until first DB refresh)
_THRESHOLD_DEFAULTS = {
    "pothole":            (3588,   9518,   27764),
    "alligator_crack":    (34681,  95725,  266439),
    "longitudinal_crack": (60581,  169786, 397511),
}


def _get_thresholds() -> dict:
    global _THRESHOLD_CACHE, _THRESHOLD_TS
    if _THRESHOLD_CACHE and (time.time() - _THRESHOLD_TS) < _THRESHOLD_TTL:
        return _THRESHOLD_CACHE

    try:
        conn = get_conn()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT v.segment_id, s.length_meters,
                    SUM(CASE WHEN v.label = 'pothole'
                        THEN COALESCE(v.bbox_area_px, GREATEST(0,
                             (v.bbox_xmax - v.bbox_xmin) * (v.bbox_ymax - v.bbox_ymin)))
                        ELSE 0 END) AS pot_px,
                    SUM(CASE WHEN v.label = 'alligator_crack'
                        THEN COALESCE(v.bbox_area_px, GREATEST(0,
                             (v.bbox_xmax - v.bbox_xmin) * (v.bbox_ymax - v.bbox_ymin)))
                        ELSE 0 END) AS alli_px,
                    SUM(CASE WHEN v.label IN
                             ('longitudinal_crack','road_crack','transverse_crack','rutting')
                        THEN COALESCE(v.bbox_area_px, GREATEST(0,
                             (v.bbox_xmax - v.bbox_xmin) * (v.bbox_ymax - v.bbox_ymin)))
                        ELSE 0 END) AS lon_px
                FROM violations v
                JOIN segments s ON s.segment_id = v.segment_id
                WHERE s.length_meters > 0
                GROUP BY v.segment_id, s.length_meters
            """)
            rows = cur.fetchall()
        finally:
            cur.close(); conn.close()

        pkm_rows = []
        for r in rows:
            km = float(r.get("length_meters") or 0) / 1000.0
            if km <= 0:
                continue
            pkm_rows.append({
                "pot_pkm":  float(r.get("pot_px")  or 0) / km,
                "alli_pkm": float(r.get("alli_px") or 0) / km,
                "lon_pkm":  float(r.get("lon_px")  or 0) / km,
            })

        if pkm_rows:
            _THRESHOLD_CACHE = compute_thresholds(pkm_rows)
            _THRESHOLD_TS    = time.time()
            logger.info("Threshold cache refreshed (%d segments) pot=%s alli=%s lon=%s",
                        len(pkm_rows),
                        _THRESHOLD_CACHE["pothole"],
                        _THRESHOLD_CACHE["alligator_crack"],
                        _THRESHOLD_CACHE["longitudinal_crack"])
    except Exception as exc:
        logger.warning("Threshold refresh failed: %s — using defaults", exc)
        if not _THRESHOLD_CACHE:
            _THRESHOLD_CACHE = _THRESHOLD_DEFAULTS

    return _THRESHOLD_CACHE or _THRESHOLD_DEFAULTS


# ─── Config ───────────────────────────────────────────────────────────────────
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST",    "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER",    "admin")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS",    "mypass")
RESULT_QUEUE  = "result_queue"

DOWNLOAD_DIR  = os.environ.get("DOWNLOAD_DIR",  "/app/downloaded_images")
ANNOTATED_DIR = os.environ.get("ANNOTATED_DIR", "/app/downloaded_images/annotated")

TURN_THRESHOLD     = 30.0   # degrees — city roads cross at 30–45°; tighter catches more
SMOOTH_WINDOW      = 3      # GPS points used to derive segment heading
MIN_SEGMENT_FRAMES = 2      # frames before computed-bearing turn detection arms
MAX_GAP_METERS     = 150    # GPS jump larger than this → different road
MIN_SPEED_KMH      = 5.0    # skip GPS advancement for stationary frames

# Path-computed bearing constants (mirrors offline_resegment.py)
_BEAR_MIN_DIST    = 30    # metres — skip bearing check when frame is closer than this
_BEAR_RECENT_N    = 10    # last N path points used to derive current road direction
_STATIONARY_M     = 3     # frames closer than this are treated as stationary GPS fixes
MAX_CROSS_TRACK_M = 30.0  # max perpendicular deviation from road line → new segment
                           # (slightly relaxed vs offline because online frames can arrive
                           # out-of-order from 32 concurrent workers)

# Advisory lock grid — must be wider than MAX_GAP_METERS so any two GPS points
# that could belong to the same segment share at least one lock key.
_LOCK_CELL = 0.002          # ≈ 222 m at the equator

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

def cross_track_dist(lat1, lon1, lat2, lon2, lat3, lon3):
    """Perpendicular distance (m) from (lat3,lon3) to the infinite line through the
    other two points.  Catches lateral jumps to parallel roads that share the same
    heading and therefore pass the bearing-only check."""
    R    = 6_371_000
    mlat = R * math.pi / 180
    mlon = R * math.cos(math.radians((lat1 + lat2) / 2)) * math.pi / 180
    x2 = (lon2 - lon1) * mlon;  y2 = (lat2 - lat1) * mlat
    x3 = (lon3 - lon1) * mlon;  y3 = (lat3 - lat1) * mlat
    seg = math.hypot(x2, y2)
    return abs(x2 * y3 - y2 * x3) / seg if seg > 1.0 else math.hypot(x3, y3)

def haversine_meters(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def compute_segment_length(path):
    """Sum haversine distances between consecutive GPS path points (meters)."""
    total = 0.0
    for i in range(1, len(path)):
        total += haversine_meters(path[i - 1][0], path[i - 1][1], path[i][0], path[i][1])
    return round(total, 1)


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


def _geo_lock_keys(lat, lon):
    """
    Return ≤4 sorted MySQL advisory-lock keys covering the segment search area.

    The bounding box is 2×MAX_GAP_METERS wide (~0.0027°).  Since _LOCK_CELL
    (0.002°) > half that width, the box touches at most 2 cells per axis → at
    most 4 keys total.  Sorting prevents inter-worker deadlock (all workers
    acquire the same keys in the same order).
    """
    lat_delta = MAX_GAP_METERS / 111_320.0
    lon_delta = MAX_GAP_METERS / (111_320.0 * math.cos(math.radians(lat)) + 1e-9)

    def _snap(v):
        return math.floor(v / _LOCK_CELL) * _LOCK_CELL

    return sorted({
        f"seg:{_snap(la):.4f}:{_snap(lo):.4f}"
        for la in (lat - lat_delta, lat + lat_delta)
        for lo in (lon - lon_delta, lon + lon_delta)
    })


# ─── Segment detection — advisory lock + row-level locking ────────────────────
#
# Multi-worker safety requires two layers of locking:
#
#   1. MySQL advisory lock (GET_LOCK) on geographic grid keys — prevents the
#      "phantom insert" race: SELECT … FOR UPDATE only locks existing rows, so
#      two workers that both find "no matching segment" would both INSERT,
#      creating duplicate segments for the same road.  The advisory lock
#      serialises workers within the same ~222 m cell before they even read.
#
#   2. SELECT … FOR UPDATE inside READ COMMITTED — still needed as a secondary
#      guard for the row-update path (frame_count, gps_path) when two workers
#      race to extend the same existing segment.
#
#   Deadlock / timeout retry is handled in the public process_segment() wrapper.

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
    lat_wide  = lat_delta * 3
    lon_wide  = lon_delta * 3
    now = datetime.utcnow().isoformat()

    lock_keys = _geo_lock_keys(lat, lon)
    conn      = get_conn()
    cur       = conn.cursor(dictionary=True)
    acquired  = []
    try:
        # Layer 1: advisory locks — prevent phantom inserts from concurrent workers.
        # Sorted keys are always acquired in the same order → no inter-worker deadlock.
        for key in lock_keys:
            cur.execute("SELECT GET_LOCK(%s, 30) AS ok", (key,))
            if not (cur.fetchone() or {}).get("ok"):
                raise RuntimeError(f"advisory lock timeout: {key}")
            acquired.append(key)

        # Layer 2: transactional row lock — safe extension of existing segments.
        conn.start_transaction(isolation_level="READ COMMITTED")

        # Primary search: end point within normal radius (indexed, fast path).
        # Fallback: start point within 3× radius — catches out-of-order frames.
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
            # correctly handles out-of-order frames.
            dist = _nearest_path_dist(lat, lon, row)
            if dist > MAX_GAP_METERS:
                continue

            # Path-computed bearing turn detection.
            # Gate: only run when the new frame is ≥_BEAR_MIN_DIST from the
            # segment endpoint.  This tolerates out-of-order deliveries from
            # concurrent model_call workers — frames that land within 30 m of
            # the current endpoint are accepted without a bearing check.
            # Frames from a different road are typically 50–150 m away, so the
            # gate does not weaken cross-road rejection.
            path = row["gps_path"]
            if not is_stationary and len(path) >= 2:
                ep       = path[-1]
                ep_dist  = haversine_meters(ep[0], ep[1], lat, lon)
                recent   = path[-min(_BEAR_RECENT_N, len(path)):]
                recent_d = haversine_meters(recent[0][0], recent[0][1],
                                            recent[-1][0], recent[-1][1])
                if recent_d >= _BEAR_MIN_DIST:
                    # 1. Bearing check (turn detection) — only reliable when the new
                    #    frame is far enough from the endpoint for a stable bearing.
                    #    The _BEAR_MIN_DIST gate tolerates out-of-order deliveries.
                    if ep_dist >= _BEAR_MIN_DIST:
                        recent_h    = compute_bearing(recent[0][0], recent[0][1],
                                                      recent[-1][0], recent[-1][1])
                        new_bearing = compute_bearing(ep[0], ep[1], lat, lon)
                        if heading_delta(recent_h, new_bearing) > TURN_THRESHOLD:
                            continue   # turned → try other candidates
                    # 2. Cross-track check (parallel-road rejection) — applied regardless
                    #    of ep_dist; catches lateral jumps that share the same heading.
                    ctd = cross_track_dist(recent[0][0], recent[0][1],
                                           recent[-1][0], recent[-1][1],
                                           lat, lon)
                    if ctd > MAX_CROSS_TRACK_M:
                        continue   # lateral jump to parallel road → new segment

            if dist < best_dist:
                best_dist = dist
                best_seg  = row

        def _insert_new_segment(new_lat, new_lon):
            seg_id = str(uuid.uuid4())
            cur.execute(
                """INSERT INTO segments
                   (segment_id, start_lat, start_lon, end_lat, end_lon,
                    gps_path, frame_count, municipality, submunicipality,
                    status, length_meters, created_at, sealed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'active',%s,%s,NULL)""",
                (seg_id, new_lat, new_lon, new_lat, new_lon,
                 json.dumps([[new_lat, new_lon]]), 1,
                 municipality, submunicipality, 0.0, now),
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

        # Extend matched segment
        if not is_stationary:
            path.append([lat, lon])
        seg_length = compute_segment_length(path)
        cur.execute(
            """UPDATE segments
               SET end_lat=%s, end_lon=%s, gps_path=%s, frame_count=%s, length_meters=%s
               WHERE segment_id=%s""",
            (lat, lon, json.dumps(path),
             best_seg.get("frame_count", 0) + 1, seg_length, seg_id),
        )
        conn.commit()
        return seg_id

    except Exception:
        conn.rollback()
        raise
    finally:
        for key in acquired:
            try:
                cur.execute("SELECT RELEASE_LOCK(%s)", (key,))
                cur.fetchone()
            except Exception:
                pass
        cur.close()
        conn.close()


def process_segment(lat, lon, municipality, submunicipality, image_metadata=None):
    """
    GPS-proximity segment detection — order-independent, safe for concurrent replicas.

    Two-layer locking inside _process_segment_tx:
      1. MySQL GET_LOCK on geographic grid keys — prevents phantom inserts.
      2. SELECT … FOR UPDATE — serialises extension of existing segment rows.

    Retries automatically on deadlocks (errno 1213) and advisory lock timeouts.
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
        except RuntimeError as e:
            if "advisory lock timeout" in str(e) and attempt < 2:
                logger.warning("Advisory lock contention — retry %d: %s", attempt + 1, e)
                time.sleep(0.1 * (attempt + 1))
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
                    polygon_points,
                    length_mm,   breadth_mm,
                    bbox_area_mm2,  polygon_area_mm2,
                    length_px,   breadth_px,
                    bbox_area_px,   polygon_area_px,
                    gsd_mm_per_px, image_width, image_height,
                    latitude, longitude, annotated_image_path, created_at)
                   VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (frame_id, segment_id,
                 r.get("label"), r.get("confidence"), r.get("severity", "low"),
                 bbox.get("xmin"), bbox.get("ymin"),
                 bbox.get("xmax"), bbox.get("ymax"),
                 json.dumps(r.get("polygon_points", [])),
                 dims.get("length_mm"),  dims.get("breadth_mm"),
                 area.get("bbox_area_mm2"),  area.get("polygon_area_mm2"),
                 dims.get("length_px"),  dims.get("breadth_px"),
                 area.get("bbox_area_px"),   area.get("polygon_area_px"),
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
    Recompute health score for a segment using the same formula as
    calculate_health_score.py:
      px_area / length_km → percentile bucket → weighted penalty → 100 − penalty
    """
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        # Segment length
        cur.execute("SELECT length_meters FROM segments WHERE segment_id = %s",
                    (segment_id,))
        seg_row = cur.fetchone()
        length_km = float((seg_row or {}).get("length_meters") or 0) / 1000.0

        # Total bbox pixel area per distress type (Step 3 input)
        cur.execute(
            """SELECT label,
                      SUM(COALESCE(bbox_area_px,
                          GREATEST(0, (bbox_xmax - bbox_xmin) * (bbox_ymax - bbox_ymin))
                      )) AS total_px
               FROM violations
               WHERE segment_id = %s
               GROUP BY label""",
            (segment_id,),
        )
        distress_px: dict = {}
        for row in cur.fetchall():
            raw_label = (row.get("label") or "").lower()
            dtype     = LABEL_TO_TYPE.get(raw_label, "longitudinal_crack")
            distress_px[dtype] = distress_px.get(dtype, 0.0) + float(row.get("total_px") or 0)

        # Violation count
        cur.execute("SELECT COUNT(*) AS cnt FROM violations WHERE segment_id = %s",
                    (segment_id,))
        vcount = (cur.fetchone() or {}).get("cnt", 0)

        # Compute health score using cached global percentile thresholds
        thresholds         = _get_thresholds()
        score, cond, color = compute_segment_health_score(distress_px, length_km, thresholds)
        rating             = pci_rating(score)

        cur.execute(
            """UPDATE segments
               SET pci_score=%s, pci_rating=%s,
                   health_score=%s, health_condition=%s, health_color=%s,
                   violation_count=%s
               WHERE segment_id=%s""",
            (score, rating, score, cond, color, vcount, segment_id),
        )
        conn.commit()
        logger.info("Health score segment=%s  score=%d  condition=%s  vcount=%d",
                    segment_id, score, cond, vcount)
        return score, rating
    finally:
        cur.close(); conn.close()


# ─── Message handler ──────────────────────────────────────────────────────────

def delete_frame_violations(frame_id: str):
    """Remove all existing violation rows for a frame (used before reprocessing)."""
    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute("DELETE FROM violations WHERE frame_id = %s", (frame_id,))
        conn.commit()
    finally:
        cur.close(); conn.close()


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
        reprocess       = msg.get("reprocess",       False)
        seg_id_override = msg.get("segment_id")

        results      = model_response.get("results",       [])
        image_width  = model_response.get("image_width",   0)
        image_height = model_response.get("image_height",  0)
        gsd          = model_response.get("gsd_mm_per_px", 0)

        speed   = image_metadata.get("EXIF:GPSSpeed", "n/a")
        heading = image_metadata.get("EXIF:GPSImgDirection", "n/a")
        logger.info("frame_id=%s  lat=%s  lon=%s  speed=%s km/h  heading=%s°  detections=%d  reprocess=%s",
                    frame_id, lat, lon, speed, heading, len(results), reprocess)

        # ── 1. Segment detection ──────────────────────────────────────────────
        # Reprocess mode: use the pre-assigned segment_id from the queue message.
        # This skips advisory locks and GPS-path extension so processing_engine
        # workers stay fast and segment data (frame_count, gps_path) is unchanged.
        if reprocess and seg_id_override:
            segment_id = seg_id_override
            # Wipe old violations for this frame before inserting fresh results
            delete_frame_violations(frame_id)
        else:
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
