"""
app/main.py  —  Flask Producer (gunicorn)
==========================================
Flow:
  1. Validate request fields
  2. Insert frame record into DB (pooled connection)
  3. Download image from OCI via boto3 → save as JPEG
  4. Extract EXIF from the local file (no second S3 download)
  5. Update DB with local path + metadata
  6. Publish to RabbitMQ via thread-local persistent channel
"""

import os
import json
import logging
import time
import threading
from io import BytesIO
from datetime import datetime

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from PIL import Image
import exiftool
import pika
from flask import Flask, request, jsonify, send_from_directory, send_file

from db_connection import init_db, get_conn

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

OCI_S3_ENDPOINT = os.environ.get("OCI_S3_ENDPOINT")
S3_ACCESS_KEY   = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY   = os.environ.get("S3_SECRET_KEY")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
logger.info("Download directory: %s", DOWNLOAD_DIR)


# ─── S3 client (module-level singleton, boto3 clients are thread-safe) ────────

_s3_client     = None
_s3_client_lock = threading.Lock()

def get_s3_client():
    global _s3_client
    if _s3_client is None:
        with _s3_client_lock:
            if _s3_client is None:
                _s3_client = boto3.client(
                    "s3",
                    endpoint_url          = OCI_S3_ENDPOINT,
                    aws_access_key_id     = S3_ACCESS_KEY,
                    aws_secret_access_key = S3_SECRET_KEY,
                    config                = Config(signature_version="s3v4"),
                )
                logger.info("S3 client created (endpoint=%s)", OCI_S3_ENDPOINT)
    return _s3_client


# ─── Image download ───────────────────────────────────────────────────────────

def download_image(s3_client, citylens_id: str, image_bucket: str, image_key: str) -> str:
    """Download from OCI, convert RGBA→RGB, save as JPEG. Returns local path."""
    start = time.time()
    response      = s3_client.get_object(Bucket=image_bucket, Key=image_key)
    image_content = response["Body"].read()
    image         = Image.open(BytesIO(image_content))

    if image.mode == "RGBA":
        image = image.convert("RGB")

    image_path = os.path.join(DOWNLOAD_DIR, f"{citylens_id}.jpg")
    image.save(image_path, "JPEG")

    if not os.path.exists(image_path):
        raise RuntimeError(f"Image save failed — file not found at {image_path}")

    logger.info("Image saved → %s (%.1f KB) in %.2fs",
                image_path, os.path.getsize(image_path) / 1024, time.time() - start)
    return image_path


# ─── EXIF extraction from local file (no second S3 download) ─────────────────

def extract_exif_from_local(image_path: str) -> dict:
    """Run ExifTool on the already-downloaded local JPEG. Returns metadata dict."""
    try:
        with exiftool.ExifToolHelper() as et:
            metadata_list = et.get_metadata(image_path)
            if metadata_list:
                return metadata_list[0]
    except Exception as e:
        logger.warning("EXIF extraction failed for %s: %s", image_path, e)
    return {}


# ─── RabbitMQ — thread-local persistent channel ───────────────────────────────
# With gunicorn gthread, each OS thread handles multiple requests sequentially.
# Reusing one pika channel per thread avoids creating a new TCP connection on
# every /upload call (which is expensive at 80-worker concurrency × 50k frames).

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
        # Reset so next call reconnects fresh
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
        # Release connection back to pool BEFORE the slow S3 download so we
        # don't hold pool slots while waiting for OCI network I/O.
        cur.close(); conn.close()

    # ── 3. Download image from OCI ────────────────────────────────────────────
    s3 = get_s3_client()
    try:
        image_path = download_image(s3, frame_id, image_bucket, image_key)
    except (BotoCoreError, ClientError, RuntimeError) as e:
        logger.error("OCI download failed: %s", e)
        return jsonify({"status": "error", "message": f"OCI download failed: {e}"}), 502

    # ── 4. Extract EXIF from local file (no second S3 download) ──────────────
    image_metadata = {}
    try:
        image_metadata = extract_exif_from_local(image_path)
        logger.info("EXIF extracted for frame_id=%s (%d keys)",
                    frame_id, len(image_metadata))
    except Exception as e:
        logger.error("EXIF extraction failed for frame_id=%s: %s", frame_id, e)

    # ── 5. Update DB with local path + metadata (fresh connection) ────────────
    conn2 = get_conn()
    cur2  = conn2.cursor()
    try:
        cur2.execute(
            """
            UPDATE frames
            SET local_image_path = %s,
                image_metadata   = %s,
                status           = 'downloaded',
                updated_at       = %s
            WHERE frame_id = %s
            """,
            (
                image_path,
                json.dumps(image_metadata),
                datetime.utcnow().isoformat(),
                frame_id,
            ),
        )
        conn2.commit()
        logger.info("DB updated frame_id=%s → downloaded", frame_id)
    except Exception as e:
        conn2.rollback()
        logger.error("DB update failed: %s", e)
        return jsonify({"status": "error", "message": f"DB update error: {e}"}), 500
    finally:
        cur2.close(); conn2.close()

    # ── 6. Publish to RabbitMQ ────────────────────────────────────────────────
    mq_payload = {
        "frame_id":        frame_id,
        "image_path":      image_path,
        "image_metadata":  image_metadata,
        "latitude":        float(latitude),
        "longitude":       float(longitude),
        "municipality":    municipality,
        "submunicipality": submunicipality,
        "datetime_utc":    datetime_utc,
    }
    try:
        publish_to_rabbitmq(mq_payload)
    except Exception as e:
        logger.error("RabbitMQ publish failed: %s", e)
        return jsonify({
            "status":   "partial",
            "frame_id": frame_id,
            "message":  f"Saved to DB but queue publish failed: {e}",
        }), 207

    return jsonify({"status": "success", "frame_id": frame_id}), 200


# ─── Entry point (dev only — production uses gunicorn) ────────────────────────
if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=False)


# ─── Map / data API endpoints ─────────────────────────────────────────────────

@app.get("/api/frames")
def api_frames():
    """All uploaded frames with GPS, status and metadata for map popups."""
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT f.frame_id, f.latitude, f.longitude,
                   f.municipality, f.submunicipality,
                   f.datetime_utc, f.status, f.created_at,
                   COUNT(v.id) AS violation_count
            FROM frames f
            LEFT JOIN violations v ON f.frame_id = v.frame_id
            WHERE f.latitude IS NOT NULL AND f.longitude IS NOT NULL
            GROUP BY f.frame_id
            ORDER BY f.created_at DESC
        """)
        return jsonify(cur.fetchall())
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
    """All violations with GPS for Leaflet/PyDeck map."""
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT v.id, v.frame_id, v.segment_id,
                   v.label, v.confidence, v.severity,
                   v.bbox_xmin, v.bbox_ymin, v.bbox_xmax, v.bbox_ymax,
                   v.polygon_points, v.length_mm, v.breadth_mm,
                   v.latitude, v.longitude,
                   v.polygon_area_mm2,
                   v.annotated_image_path, v.created_at,
                   v.image_width, v.image_height
            FROM violations v
            WHERE v.latitude IS NOT NULL AND v.longitude IS NOT NULL
            ORDER BY v.created_at DESC
        """)
        rows = cur.fetchall()
        for r in rows:
            if isinstance(r.get("polygon_points"), str):
                r["polygon_points"] = json.loads(r["polygon_points"])
        return jsonify(rows)
    finally:
        cur.close(); conn.close()


@app.get("/api/segments")
def api_segments():
    """All road segments with GPS path for polyline rendering."""
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT s.segment_id, s.start_lat, s.start_lon,
                   s.end_lat, s.end_lon, s.gps_path,
                   s.frame_count, s.municipality, s.submunicipality,
                   s.status, s.created_at,
                   COUNT(v.id) AS violation_count
            FROM segments s
            LEFT JOIN violations v ON s.segment_id = v.segment_id
            GROUP BY s.segment_id
            ORDER BY s.created_at DESC
        """)
        rows = cur.fetchall()
        for r in rows:
            if isinstance(r.get("gps_path"), str):
                r["gps_path"] = json.loads(r["gps_path"])
        return jsonify(rows)
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
