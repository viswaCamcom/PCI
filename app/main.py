"""
app/main.py  —  Flask Producer (gunicorn)
==========================================
Flow:
  1. Validate request fields
  2. Insert frame record into DB (pooled connection)
  3. Publish {frame_id, image_key, image_bucket, ...} to frame_queue
  4. Return 200 immediately

OCI download, EXIF extraction, and DB status update now happen inside the
model_call workers (16 replicas), keeping gunicorn threads free for read
requests at all times.
"""

import os
import json
import logging
import threading
import time
from datetime import datetime

import pika
from flask import Flask, request, jsonify, send_from_directory, send_file

from db_connection import init_db, get_conn

# ─── Simple per-worker in-memory response cache ───────────────────────────────
# Store raw JSON bytes (not Response objects — Flask mutates Response on send).
# The map refreshes every 30 s; TTL of 25 s ensures at most one DB hit per
# worker per refresh cycle.
_CACHE: dict = {}
_CACHE_LOCK = threading.Lock()
_CACHE_TTL  = 60  # seconds (map JS refreshes every 30 s; 60 s keeps DB hit rate low)

def _get_cached(key: str) -> "bytes | None":
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if entry and time.monotonic() - entry["ts"] < _CACHE_TTL:
            return entry["data"]
    return None

def _set_cached(key: str, value):
    """Serialize value to JSON bytes once and cache the bytes."""
    data = json.dumps(value, default=str).encode()
    with _CACHE_LOCK:
        _CACHE[key] = {"ts": time.monotonic(), "data": data}

def _json_response(cached_bytes: bytes):
    from flask import Response
    return Response(cached_bytes, mimetype="application/json")

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
DOWNLOAD_DIR    = os.environ.get("DOWNLOAD_DIR", "/app/downloaded_images")
ANNOTATED_DIR   = os.environ.get("ANNOTATED_DIR", "/app/downloaded_images/annotated")
RABBITMQ_HOST   = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT   = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER   = os.environ.get("RABBITMQ_USER", "admin")
RABBITMQ_PASS   = os.environ.get("RABBITMQ_PASS", "mypass")
FRAME_QUEUE     = "frame_queue"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ─── RabbitMQ — thread-local persistent channel ───────────────────────────────
# With gunicorn gthread, each OS thread handles multiple requests sequentially.
# One pika channel per thread avoids reconnecting on every /upload call.

_mq_local = threading.local()


def _get_mq_channel():
    """Return a thread-local pika channel, reconnecting transparently if stale."""
    conn = getattr(_mq_local, "conn", None)
    ch   = getattr(_mq_local, "channel", None)
    if conn is None or conn.is_closed or ch is None or ch.is_closed:
        if conn and not conn.is_closed:
            try:
                conn.close()
            except Exception:
                pass
        _mq_local.conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                host        = RABBITMQ_HOST,
                port        = RABBITMQ_PORT,
                credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
                heartbeat   = 600,
                blocked_connection_timeout = 300,
            )
        )
        ch = _mq_local.conn.channel()
        ch.queue_declare(queue=FRAME_QUEUE, durable=True)
        _mq_local.channel = ch
        logger.info("RabbitMQ channel (re)connected on thread %s",
                    threading.current_thread().name)
    return _mq_local.channel


def publish_to_rabbitmq(payload: dict):
    try:
        ch = _get_mq_channel()
        ch.basic_publish(
            exchange    = "",
            routing_key = FRAME_QUEUE,
            body        = json.dumps(payload).encode(),
            properties  = pika.BasicProperties(delivery_mode=2),
        )
        logger.info("Published frame_id=%s to %s", payload["frame_id"], FRAME_QUEUE)
    except Exception:
        _mq_local.conn    = None
        _mq_local.channel = None
        raise


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return jsonify({"status": "ok", "ts": datetime.utcnow().isoformat()})


@app.post("/upload")
def upload():
    """
    Accepts multipart/form-data:
        citylens_id, latitude, longitude,
        image_key, image_url, image_bucket,
        municipality_id, submunicipality_id, datetime_utc
    """

    # ── 1. Extract + validate fields ─────────────────────────────────────────
    data = request.form

    frame_id        = data.get("citylens_id")
    latitude        = data.get("latitude")
    longitude       = data.get("longitude")
    image_key       = data.get("image_key")
    image_url       = data.get("image_url", "")
    image_bucket    = data.get("image_bucket")
    municipality    = data.get("municipality_id", "")
    submunicipality = data.get("submunicipality_id", "")
    datetime_utc    = data.get("datetime_utc")

    missing = [f for f, v in {
        "citylens_id":  frame_id,
        "latitude":     latitude,
        "longitude":    longitude,
        "image_key":    image_key,
        "image_bucket": image_bucket,
        "datetime_utc": datetime_utc,
    }.items() if v is None]
    if missing:
        return jsonify({"status": "error", "message": f"Missing fields: {missing}"}), 400

    # ── 2. Insert frame record (check duplicate first) ────────────────────────
    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT status FROM frames WHERE frame_id = %s", (frame_id,))
        existing = cur.fetchone()
        if existing:
            return jsonify({
                "status":   "already_exists",
                "frame_id": frame_id,
                "message":  f"frame already exists with status '{existing[0]}'",
            }), 409

        cur.execute(
            """
            INSERT INTO frames (
                frame_id, latitude, longitude,
                image_key, image_url, image_bucket,
                municipality, submunicipality,
                datetime_utc, status, created_at
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, 'pending', %s
            )
            ON DUPLICATE KEY UPDATE
                status     = 'pending',
                updated_at = %s
            """,
            (
                frame_id,
                float(latitude), float(longitude),
                image_key, image_url, image_bucket,
                municipality, submunicipality,
                datetime_utc,
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        logger.info("DB record created frame_id=%s", frame_id)
    except Exception as e:
        conn.rollback()
        logger.error("DB insert failed: %s", e)
        return jsonify({"status": "error", "message": f"DB error: {e}"}), 500
    finally:
        cur.close(); conn.close()

    # ── 3. Publish to frame_queue — model_call workers handle download + model ─
    mq_payload = {
        "frame_id":        frame_id,
        "image_key":       image_key,
        "image_bucket":    image_bucket,
        "latitude":        float(latitude),
        "longitude":       float(longitude),
        "municipality":    municipality,
        "submunicipality": submunicipality,
        "datetime_utc":    datetime_utc,
    }
    try:
        publish_to_rabbitmq(mq_payload)
    except Exception as e:
        logger.error("MQ publish failed frame_id=%s: %s", frame_id, e)
        return jsonify({"status": "error", "message": f"MQ error: {e}"}), 500

    return jsonify({"status": "success", "frame_id": frame_id}), 200


# ─── Segment query helper ─────────────────────────────────────────────────────

def _fetch_segments_sorted(cur, limit=15000):
    """Two-phase query: sort only the tiny (segment_id, frame_count) rows first,
    then fetch full rows by primary key.  Avoids MySQL sort-buffer overflow when
    gps_path blobs are large (up to 700 KB each after segment merging).

    Segments with fewer than 5 frames are excluded — they produce at most 4 GPS
    points so they render as nearly-straight line stubs that visually appear to
    cut across non-road areas and add map clutter.

    GPS path resolution: capped at 500 points per segment.  Most segments have
    ≤ 20 frames and show every point.  For large segments (1000+ frames) step=2
    keeps ~10 m between displayed points, which follows curved roads faithfully.
    """
    cur.execute(
        "SELECT segment_id FROM segments WHERE frame_count >= 10 ORDER BY frame_count DESC LIMIT %s",
        (limit,)
    )
    ids = [r["segment_id"] for r in cur.fetchall()]
    if not ids:
        return []
    fmt  = ",".join(["%s"] * len(ids))
    cur.execute(f"""
        SELECT segment_id, start_lat, start_lon,
               end_lat, end_lon, gps_path,
               frame_count, violation_count,
               municipality, submunicipality,
               status, created_at, pci_score, pci_rating,
               length_meters, segment_name
        FROM segments
        WHERE segment_id IN ({fmt})
    """, ids)
    rows = cur.fetchall()
    for r in rows:
        path = r.get("gps_path")
        if isinstance(path, (bytes, bytearray)):
            path = path.decode()
        if isinstance(path, str):
            try:
                path = json.loads(path)
            except Exception:
                path = []
        if not isinstance(path, list):
            path = []
        if len(path) > 500:
            step = max(1, len(path) // 500)
            path = path[::step]
        r["gps_path"] = path
        # Decimal → float so json.dumps doesn't choke
        lm = r.get("length_meters")
        if lm is not None:
            r["length_meters"] = float(lm)
    id_order = {sid: i for i, sid in enumerate(ids)}
    rows.sort(key=lambda r: id_order.get(r["segment_id"], 999999))
    return rows


# ─── Cache warmup (called by gunicorn post_fork hook in each worker) ─────────

def warmup_cache():
    """Run all three map queries and populate the response cache.
    FORCE INDEX(PRIMARY) guarantees a clustered-index scan that stops at LIMIT
    without a full-table sort — critical when MySQL is under heavy insert load.
    """
    try:
        with app.app_context():
            conn = get_conn()
            cur  = conn.cursor(dictionary=True)
            try:
                cur.execute("""
                    SELECT frame_id, latitude, longitude,
                           municipality, submunicipality,
                           datetime_utc, status, pci_score, pci_rating
                    FROM frames FORCE INDEX(PRIMARY)
                    LIMIT 5000
                """)
                _set_cached("frames", cur.fetchall())

                cur.execute("""
                    SELECT id, frame_id, segment_id,
                           label, confidence, severity,
                           bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
                           length_mm, breadth_mm,
                           latitude, longitude, polygon_area_mm2
                    FROM violations FORCE INDEX(PRIMARY)
                    LIMIT 10000
                """)
                _set_cached("violations", cur.fetchall())

                _set_cached("segments", _fetch_segments_sorted(cur))

                cur.execute("""
                    SELECT
                      (SELECT COUNT(*) FROM frames)     AS total_frames,
                      (SELECT COUNT(*) FROM violations) AS total_violations,
                      (SELECT COUNT(*) FROM segments)   AS total_segments,
                      (SELECT COUNT(*) FROM violations WHERE LOWER(severity)='high')   AS total_high,
                      (SELECT COUNT(*) FROM violations WHERE LOWER(severity)='medium') AS total_medium,
                      (SELECT COUNT(*) FROM violations WHERE LOWER(severity)='low')    AS total_low
                """)
                _set_cached("stats", cur.fetchone() or {})

                logger.info("Cache refreshed (frames + violations + segments + stats)")
            finally:
                cur.close(); conn.close()
    except Exception as e:
        logger.warning("Cache refresh failed: %s", e)


def start_cache_refresher():
    """Spawn a daemon thread that refreshes the cache every 50 s.
    The cache TTL is 60 s; refreshing at 50 s ensures it never expires cold,
    eliminating the thundering-herd DB hit when TTL lapses under browser load.
    """
    def _loop():
        time.sleep(5)   # short initial delay so failed warmups self-heal quickly
        while True:
            warmup_cache()
            time.sleep(50)
    t = threading.Thread(target=_loop, daemon=True, name="cache-refresher")
    t.start()
    logger.info("Cache refresher daemon started (50 s interval)")


# ─── Entry point (dev only — production uses gunicorn) ────────────────────────
if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=False)


# ─── Map / data API endpoints ─────────────────────────────────────────────────

@app.get("/api/frames")
def api_frames():
    """
    Frames with GPS + status for map popups.

    No ORDER BY — the map renders markers regardless of order, and avoiding the
    full-table sort on 35K+ rows saves 3-5 s of MySQL time.
    LIMIT 10000 keeps the payload under 1 MB.  Cached for 25 s.
    """
    cached = _get_cached("frames")
    if cached is not None:
        return _json_response(cached)

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT frame_id, latitude, longitude,
                   municipality, submunicipality,
                   datetime_utc, status,
                   pci_score, pci_rating
            FROM frames FORCE INDEX(PRIMARY)
            LIMIT 5000
        """)
        rows = cur.fetchall()
        _set_cached("frames", rows)
        return _json_response(_get_cached("frames"))
    finally:
        cur.close(); conn.close()


@app.get("/api/stats")
def api_stats():
    """Global totals for the header stats bar — three fast COUNT(*) queries."""
    cached = _get_cached("stats")
    if cached is not None:
        return _json_response(cached)
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT
              (SELECT COUNT(*) FROM frames)     AS total_frames,
              (SELECT COUNT(*) FROM violations) AS total_violations,
              (SELECT COUNT(*) FROM segments)   AS total_segments,
              (SELECT COUNT(*) FROM violations WHERE LOWER(severity)='high')   AS total_high,
              (SELECT COUNT(*) FROM violations WHERE LOWER(severity)='medium') AS total_medium,
              (SELECT COUNT(*) FROM violations WHERE LOWER(severity)='low')    AS total_low
        """)
        row = cur.fetchone() or {}
        _set_cached("stats", row)
        return _json_response(_get_cached("stats"))
    finally:
        cur.close(); conn.close()


@app.get("/api/frames/summary")
def api_frames_summary():
    """Pipeline status counts: pending / downloaded / processed / total."""
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT
                COUNT(*)                        AS total,
                SUM(status = 'pending')         AS pending,
                SUM(status = 'downloaded')      AS downloaded,
                SUM(status = 'processed')       AS processed
            FROM frames
        """)
        return jsonify(cur.fetchone() or {})
    finally:
        cur.close(); conn.close()


@app.get("/api/violations")
def api_violations():
    """
    Violations for map markers — minimal fields only.

    When ?segment_id=<id> is supplied the cache is bypassed and only
    violations for that segment are returned (used by the detail panel).
    The global response is still capped at 10 000 rows for map markers.
    """
    segment_id = request.args.get("segment_id")
    if segment_id:
        conn = get_conn()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT id, frame_id, segment_id,
                       label, confidence, severity,
                       bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
                       length_mm, breadth_mm,
                       latitude, longitude,
                       polygon_area_mm2
                FROM violations
                WHERE segment_id = %s
                ORDER BY confidence DESC
            """, (segment_id,))
            return jsonify(cur.fetchall())
        finally:
            cur.close(); conn.close()

    cached = _get_cached("violations")
    if cached is not None:
        return _json_response(cached)

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, frame_id, segment_id,
                   label, confidence, severity,
                   bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
                   length_mm, breadth_mm,
                   latitude, longitude,
                   polygon_area_mm2
            FROM violations FORCE INDEX(PRIMARY)
            LIMIT 10000
        """)
        rows = cur.fetchall()
        _set_cached("violations", rows)
        return _json_response(_get_cached("violations"))
    finally:
        cur.close(); conn.close()


@app.get("/api/segments")
def api_segments():
    """
    Road segments with GPS paths for polyline rendering.

    violation_count comes from the stored column — no runtime JOIN.
    GPS paths are downsampled to ≤ 50 points (sufficient for any zoom level).
    Cached for 25 s; map refreshes every 30 s.
    """
    cached = _get_cached("segments")
    if cached is not None:
        return _json_response(cached)

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        rows = _fetch_segments_sorted(cur)
        _set_cached("segments", rows)
        return _json_response(_get_cached("segments"))
    finally:
        cur.close(); conn.close()


@app.get("/api/image/<frame_id>")
def api_image(frame_id):
    """Serve annotated image (falls back to original) as JPEG."""
    try:
        annotated = os.path.join(ANNOTATED_DIR, f"{frame_id}.jpg")
        original  = os.path.join(DOWNLOAD_DIR,  f"{frame_id}.jpg")
        if os.path.exists(annotated):
            return send_file(annotated, mimetype="image/jpeg")
        if os.path.exists(original):
            return send_file(original, mimetype="image/jpeg")
        return jsonify({"error": "Image not found"}), 404
    except Exception as e:
        logger.exception("api_image error: %s", e)
        return jsonify({"error": str(e)}), 500


# ─── Serve map UI ─────────────────────────────────────────────────────────────

@app.get("/map")
def serve_map():
    return send_from_directory(".", "map.html")
