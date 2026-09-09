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
import re
import threading
import time
from datetime import datetime

import boto3
import pika
from botocore.config import Config as BotoConfig
from flask import Flask, request, jsonify, redirect, send_from_directory, send_file

from db_connection import init_db, get_conn

# ─── Simple per-worker in-memory response cache ───────────────────────────────
# Store raw JSON bytes (not Response objects — Flask mutates Response on send).
# The map refreshes every 30 s; TTL of 25 s ensures at most one DB hit per
# worker per refresh cycle.
_CACHE: dict = {}
_CACHE_LOCK = threading.Lock()
_CACHE_TTL  = 60  # seconds (map JS refreshes every 30 s; 60 s keeps DB hit rate low)

def _get_cached(key: str, ttl: int = _CACHE_TTL) -> "bytes | None":
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if entry and time.monotonic() - entry["ts"] < ttl:
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

# Annotated images live in object storage; local disk is only a fallback. Reads
# the canonical names, then the mixed-case spellings first added to .env.
ANN_S3_BUCKET   = os.environ.get("ANNOTATED_S3_BUCKET")     or os.environ.get("Bucket_name")
ANN_S3_ENDPOINT = os.environ.get("ANNOTATED_S3_ENDPOINT")   or os.environ.get("S3_endpoint")
ANN_S3_KEY      = os.environ.get("ANNOTATED_S3_ACCESS_KEY") or os.environ.get("Access_Key_ID")
ANN_S3_SECRET   = os.environ.get("ANNOTATED_S3_SECRET_KEY") or os.environ.get("Secret_Access_Key")
ANN_S3_PREFIX   = os.environ.get("ANNOTATED_S3_PREFIX", "annotated").strip("/")
ANN_URL_TTL     = int(os.environ.get("ANNOTATED_URL_TTL", 3600))
ANN_S3_ENABLED  = all([ANN_S3_BUCKET, ANN_S3_ENDPOINT, ANN_S3_KEY, ANN_S3_SECRET])
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


# ─── GPS path smoothing ────────────────────────────────────────────────────────

def _smooth_path(path: list, window: int = 5) -> list:
    """Moving-average smooth to remove GPS noise from recorded paths.
    Each point is replaced by the mean of its neighbours within `window`.
    Endpoints are kept exact so segment start/end don't drift.
    """
    n = len(path)
    if n < window:
        return path
    half = window // 2
    out  = []
    for i in range(n):
        s   = max(0, i - half)
        e   = min(n, i + half + 1)
        pts = path[s:e]
        out.append([
            sum(p[0] for p in pts) / len(pts),
            sum(p[1] for p in pts) / len(pts),
        ])
    return out


def _douglas_peucker(pts: list, eps: float) -> list:
    """Ramer-Douglas-Peucker path simplification.
    eps in degrees (~0.00005 ≈ 5 m at Saudi latitudes).
    """
    if len(pts) < 3:
        return pts
    x1, y1 = pts[0]
    x2, y2 = pts[-1]
    dx, dy  = x2 - x1, y2 - y1
    seg_len = (dx*dx + dy*dy) ** 0.5

    dmax, idx = 0.0, 0
    for i in range(1, len(pts) - 1):
        px, py = pts[i]
        if seg_len > 0:
            d = abs(dy*px - dx*py + x2*y1 - y2*x1) / seg_len
        else:
            d = ((px - x1)**2 + (py - y1)**2) ** 0.5
        if d > dmax:
            dmax, idx = d, i

    if dmax > eps:
        left  = _douglas_peucker(pts[:idx + 1], eps)
        right = _douglas_peucker(pts[idx:],     eps)
        return left[:-1] + right
    return [pts[0], pts[-1]]


# ─── Segment query helper ─────────────────────────────────────────────────────

_MIN_FRAMES   = 5        # hide very short stubs
_MAX_POINTS   = 300      # max GPS points per segment sent to browser
_SMOOTH_WIN   = 5        # moving-average window for GPS noise removal
_DP_EPSILON   = 0.00006  # Douglas-Peucker tolerance (~6-7 m)


def _process_gps_path(raw_path):
    """Parse a stored gps_path (JSON string/bytes/list) and apply the
    smooth -> simplify -> cap pipeline used for map rendering:
      1. Parse JSON
      2. Moving-average smooth  (window=5) -> removes GPS noise
      3. Douglas-Peucker        (eps~6 m)  -> removes collinear points
      4. Cap at 300 points
    Shared by /api/segments and /api/mbs/segments.
    """
    path = raw_path
    if isinstance(path, (bytes, bytearray)):
        path = path.decode()
    if isinstance(path, str):
        try:
            path = json.loads(path)
        except Exception:
            path = []
    if not isinstance(path, list):
        path = []
    # Smoothing and Douglas-Peucker exist purely to make an oversized path
    # (thousands of dense GPS points) cheap to render — they have no benefit
    # on a path that's already small, and can actively hurt it: DP measures
    # deviation from the straight line between a path's two endpoints, which
    # is a meaningless reference on a short, tightly-curved segment (e.g. an
    # interchange loop) — it happily collapses a real loop down to 2-3 points
    # (start/apex/end), which renders as straight chords cutting across the
    # inside of the curve instead of following it. Below _MAX_POINTS there's
    # no rendering-cost reason to simplify at all, so skip both steps and use
    # the raw path as-is; only paths that actually need capping go through it.
    if len(path) > _MAX_POINTS:
        path = _smooth_path(path, _SMOOTH_WIN)
        path = _douglas_peucker(path, _DP_EPSILON)
        if len(path) > _MAX_POINTS:
            step = max(1, len(path) // _MAX_POINTS)
            path = path[::step]
    return path


def _fetch_segments_sorted(cur, limit=15000):
    """Two-phase query: sort only the tiny (segment_id, frame_count) rows first,
    then fetch full rows by primary key.  Avoids MySQL sort-buffer overflow when
    gps_path blobs are large.

    Path pipeline per segment:
      1. Parse JSON
      2. Moving-average smooth  (window=5) → removes GPS noise
      3. Douglas-Peucker        (eps≈6 m)  → removes collinear points
      4. Cap at 300 points
    """
    cur.execute(
        "SELECT segment_id FROM segments WHERE frame_count >= %s ORDER BY frame_count DESC LIMIT %s",
        (_MIN_FRAMES, limit)
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
               length_meters, segment_name,
               road_type, segment_width_m,
               health_score, health_condition, health_color
        FROM segments
        WHERE segment_id IN ({fmt})
    """, ids)
    rows = cur.fetchall()
    for r in rows:
        r["gps_path"] = _process_gps_path(r.get("gps_path"))
        # Decimal → float so json.dumps doesn't choke
        for fld in ("length_meters", "segment_width_m", "pci_score", "health_score"):
            v = r.get(fld)
            if v is not None:
                r[fld] = float(v)
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


@app.get("/api/mbs/segments")
def api_mbs_segments():
    """
    Frozen Makkah<->Jeddah corridor segments (see freeze_mbs_segments.py).
    Unlike /api/segments, this data never changes between reprocessing runs —
    cached with a much longer TTL since there's no benefit to refreshing it
    on the same 60s cadence as live, frequently-changing data.
    """
    cached = _get_cached("mbs_segments", ttl=3600)
    if cached is not None:
        return _json_response(cached)

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT mbs_segment_id, segment_name, municipality, submunicipality,
                   start_lat, start_lon, end_lat, end_lon, gps_path,
                   length_meters, road_type, segment_width_m
            FROM mbs_segments
        """)
        rows = cur.fetchall()
        for r in rows:
            r["gps_path"] = _process_gps_path(r.get("gps_path"))
            if r.get("length_meters") is not None:
                r["length_meters"] = float(r["length_meters"])
            if r.get("segment_width_m") is not None:
                r["segment_width_m"] = float(r["segment_width_m"])
        _set_cached("mbs_segments", rows)
        return _json_response(_get_cached("mbs_segments", ttl=3600))
    finally:
        cur.close(); conn.close()


@app.get("/api/mbs/dates")
def api_mbs_dates():
    """Distinct capture dates with computed scores, ascending — powers the date filter."""
    cached = _get_cached("mbs_dates", ttl=3600)
    if cached is not None:
        return _json_response(cached)

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT DISTINCT capture_date FROM mbs_segment_scores ORDER BY capture_date ASC")
        payload = {"dates": [str(r["capture_date"]) for r in cur.fetchall()]}
        _set_cached("mbs_dates", payload)
        return _json_response(_get_cached("mbs_dates", ttl=3600))
    finally:
        cur.close(); conn.close()


@app.get("/api/mbs/segment-dates")
def api_mbs_segment_dates():
    """
    Distinct capture dates for ONE segment — powers the compare modal's date
    pickers, since a given segment may only have data for a subset of the
    corridor's overall dates. Required: ?segment_id=
    """
    segment_id = request.args.get("segment_id", "")
    if not segment_id:
        return jsonify({"status": "error", "message": "segment_id required"}), 400

    cache_key = f"mbs_segment_dates:{segment_id}"
    cached = _get_cached(cache_key, ttl=3600)
    if cached is not None:
        return _json_response(cached)

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT DISTINCT capture_date FROM mbs_segment_scores WHERE mbs_segment_id = %s ORDER BY capture_date ASC",
            (segment_id,),
        )
        payload = {"dates": [str(r["capture_date"]) for r in cur.fetchall()]}
        _set_cached(cache_key, payload)
        return _json_response(_get_cached(cache_key, ttl=3600))
    finally:
        cur.close(); conn.close()


@app.get("/api/mbs/segment-violations")
def api_mbs_segment_violations():
    """
    Violations for one frozen segment on one capture date — feeds the segment
    detail panel's defect breakdown + violation list, same shape as
    /api/violations?segment_id= but joined through mbs_frame_segment_map
    (violations.segment_id itself isn't stable for MBS — it gets reassigned
    by offline_resegment.py — only violations.frame_id is). Not cached: same
    precedent as the existing per-segment violations lookup.
    Required: ?segment_id=&date=YYYY-MM-DD
    """
    segment_id = request.args.get("segment_id", "")
    date       = request.args.get("date", "")
    if not segment_id or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        return jsonify({"status": "error", "message": "segment_id and date (YYYY-MM-DD) required"}), 400

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT v.id, v.frame_id, m.mbs_segment_id AS segment_id,
                   v.label, v.confidence, v.severity,
                   v.bbox_xmin, v.bbox_ymin, v.bbox_xmax, v.bbox_ymax,
                   v.length_mm, v.breadth_mm,
                   v.latitude, v.longitude,
                   v.polygon_area_mm2
            FROM violations v
            JOIN mbs_frame_segment_map m ON m.frame_id = v.frame_id
            WHERE m.mbs_segment_id = %s AND m.capture_date = %s
            ORDER BY v.confidence DESC
        """, (segment_id, date))
        return jsonify(cur.fetchall())
    finally:
        cur.close(); conn.close()


@app.get("/api/mbs/scores")
def api_mbs_scores():
    """Per-segment health scores for one capture date. Required: ?date=YYYY-MM-DD."""
    date = request.args.get("date", "")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        return jsonify({"status": "error", "message": "date must be YYYY-MM-DD"}), 400

    cache_key = f"mbs_scores:{date}"
    cached = _get_cached(cache_key, ttl=3600)
    if cached is not None:
        return _json_response(cached)

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT s.mbs_segment_id, seg.segment_name, s.capture_date,
                   s.frame_count, s.violation_count,
                   s.pothole_count, s.alligator_count, s.longitudinal_count,
                   s.health_score, s.health_condition, s.health_color
            FROM mbs_segment_scores s
            JOIN mbs_segments seg ON seg.mbs_segment_id = s.mbs_segment_id
            WHERE s.capture_date = %s
        """, (date,))
        rows = cur.fetchall()
        _set_cached(cache_key, rows)
        return _json_response(_get_cached(cache_key, ttl=3600))
    finally:
        cur.close(); conn.close()


@app.get("/api/mbs/compare")
def api_mbs_compare():
    """
    Two-date comparison for one frozen segment. Not cached — low-frequency,
    user-triggered action, same precedent as /api/violations?segment_id=.
    Required: ?segment_id=&date_a=YYYY-MM-DD&date_b=YYYY-MM-DD
    """
    segment_id = request.args.get("segment_id", "")
    date_a     = request.args.get("date_a", "")
    date_b     = request.args.get("date_b", "")
    if not segment_id or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_a) \
                       or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_b):
        return jsonify({"status": "error", "message": "segment_id, date_a, date_b (YYYY-MM-DD) required"}), 400

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM mbs_segments WHERE mbs_segment_id = %s", (segment_id,))
        seg = cur.fetchone()
        if not seg:
            return jsonify({"status": "error", "message": "segment not found"}), 404
        seg["gps_path"] = _process_gps_path(seg.get("gps_path"))

        def _score_for(d):
            cur.execute(
                "SELECT * FROM mbs_segment_scores WHERE mbs_segment_id=%s AND capture_date=%s",
                (segment_id, d),
            )
            return cur.fetchone()

        return jsonify({
            "segment": seg,
            "date_a": {"date": date_a, "score": _score_for(date_a)},
            "date_b": {"date": date_b, "score": _score_for(date_b)},
        })
    finally:
        cur.close(); conn.close()


@app.get("/api/mbs/geometry")
def api_mbs_geometry():
    """OSM reference centerline for the corridor overlay — static, display-only."""
    path = os.path.join(os.path.dirname(__file__), "static", "mbs_road_geometry.geojson")
    if not os.path.exists(path):
        return jsonify({"status": "error", "message": "geometry not fetched yet — run fetch_mbs_road_geometry.py"}), 404
    return send_file(path, mimetype="application/geo+json")


# ─── Annotated images in object storage ───────────────────────────────────────
# The annotated JPEGs live in a bucket rather than on the app's disk, so a fresh
# deployment serves them without a 13 GB file copy. /api/image redirects the
# browser to a presigned URL — the bytes go straight from object storage to the
# client instead of being proxied through Flask.

_s3_client = None
_s3_lock   = threading.Lock()

# frame_id -> (exists: bool, checked_at: float). A HEAD per image request would
# double latency, so results are cached; negatives expire sooner so a frame
# annotated after its first miss starts resolving without a restart.
_ann_exists       = {}
_ann_lock         = threading.Lock()
_ANN_TTL_HIT      = 3600
_ANN_TTL_MISS     = 60
_ANN_CACHE_MAX    = 200_000


def get_s3():
    global _s3_client
    if _s3_client is None:
        with _s3_lock:
            if _s3_client is None:
                _s3_client = boto3.client(
                    "s3",
                    endpoint_url          = ANN_S3_ENDPOINT,
                    aws_access_key_id     = ANN_S3_KEY,
                    aws_secret_access_key = ANN_S3_SECRET,
                    # OCI rejects the aws-chunked encoding boto3 >= 1.36 adds by
                    # default; opt back out of the trailing-checksum behaviour.
                    config = BotoConfig(
                        signature_version            = "s3v4",
                        request_checksum_calculation = "when_required",
                        response_checksum_validation = "when_required",
                        max_pool_connections         = 32,
                    ),
                )
    return _s3_client


def annotated_in_s3(frame_id):
    key = f"{ANN_S3_PREFIX}/{frame_id}.jpg"
    now = time.time()

    with _ann_lock:
        hit = _ann_exists.get(frame_id)
        if hit and now - hit[1] < (_ANN_TTL_HIT if hit[0] else _ANN_TTL_MISS):
            return key if hit[0] else None

    try:
        get_s3().head_object(Bucket=ANN_S3_BUCKET, Key=key)
        found = True
    except Exception:
        found = False

    with _ann_lock:
        # Cheap bound: the map works a fixed corridor, so this rarely trips.
        if len(_ann_exists) > _ANN_CACHE_MAX:
            _ann_exists.clear()
        _ann_exists[frame_id] = (found, now)

    return key if found else None


@app.get("/api/image/<frame_id>")
def api_image(frame_id):
    """Redirect to the annotated image in object storage; fall back to disk."""
    try:
        if ANN_S3_ENABLED:
            key = annotated_in_s3(frame_id)
            if key:
                url = get_s3().generate_presigned_url(
                    "get_object",
                    Params    = {"Bucket": ANN_S3_BUCKET, "Key": key},
                    ExpiresIn = ANN_URL_TTL,
                )
                return redirect(url, code=302)

        # Local disk: still authoritative on the machine that produced the
        # images, and the only source for frames with no annotation yet.
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
