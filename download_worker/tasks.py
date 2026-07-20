"""
download_worker/tasks.py
========================
Consumes from : frame_queue
Publishes to  : model_queue

For each message:
  1. Idempotency guard — drop if already processed
  2. Download image from OCI (skip if already on disk)
  3. Extract EXIF metadata
  4. Update frames table: status=downloaded, local_image_path, image_metadata
  5. Publish to model_queue (image_path now populated for model worker)
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
import exiftool
import mysql.connector
import pika
import requests
from PIL import Image

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "mypass")

FRAME_QUEUE = "frame_queue"
MODEL_QUEUE = "model_queue"

DOWNLOAD_DIR    = os.environ.get("DOWNLOAD_DIR", "/app/downloaded_images")
OCI_S3_ENDPOINT = os.environ.get("OCI_S3_ENDPOINT")
S3_ACCESS_KEY   = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY   = os.environ.get("S3_SECRET_KEY")

MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))
MYSQL_DB   = os.environ.get("MYSQL_DB", "pci")
MYSQL_USER = os.environ.get("MYSQL_USER", "pci_user")
MYSQL_PASS = os.environ.get("MYSQL_PASSWORD", "pci_pass")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ─── S3 client ────────────────────────────────────────────────────────────────

_s3_client      = None
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
    return _s3_client


# ─── Image download ───────────────────────────────────────────────────────────

def download_image(frame_id: str, image_bucket: str, image_key: str) -> str:
    path = os.path.join(DOWNLOAD_DIR, f"{frame_id}.jpg")
    if os.path.exists(path) and os.path.getsize(path) > 0:
        logger.info("frame_id=%s already on disk — skipping OCI download", frame_id)
        return path
    start    = time.time()
    s3       = get_s3_client()
    response = s3.get_object(Bucket=image_bucket, Key=image_key)
    image    = Image.open(BytesIO(response["Body"].read()))
    if image.mode == "RGBA":
        image = image.convert("RGB")
    image.save(path, "JPEG")
    logger.info("Downloaded frame_id=%s (%.1f KB) in %.2fs",
                frame_id, os.path.getsize(path) / 1024, time.time() - start)
    return path


# ─── EXIF extraction ──────────────────────────────────────────────────────────

_exiftool_instance = None

def _get_exiftool():
    global _exiftool_instance
    if _exiftool_instance is None:
        _exiftool_instance = exiftool.ExifToolHelper()
        _exiftool_instance.run()
        logger.info("ExifTool process started")
    return _exiftool_instance


def extract_exif(image_path: str) -> dict:
    try:
        result = _get_exiftool().get_metadata(image_path)
        if result:
            return result[0]
    except Exception as e:
        logger.warning("EXIF extraction failed for %s: %s", image_path, e)
        global _exiftool_instance
        _exiftool_instance = None
    return {}


# ─── MySQL ────────────────────────────────────────────────────────────────────

def get_db_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DB,
        user=MYSQL_USER, password=MYSQL_PASS, connect_timeout=10,
    )


def _frame_status(frame_id: str):
    conn = get_db_conn()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT status FROM frames WHERE frame_id=%s", (frame_id,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close(); conn.close()


def update_frame_downloaded(frame_id: str, image_path: str, image_metadata: dict):
    conn = get_db_conn()
    cur  = conn.cursor()
    try:
        cur.execute(
            """UPDATE frames
               SET local_image_path=%s, image_metadata=%s,
                   status='downloaded', updated_at=%s
               WHERE frame_id=%s""",
            (image_path, json.dumps(image_metadata),
             datetime.utcnow().isoformat(), frame_id),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("DB update failed frame_id=%s: %s", frame_id, e)
    finally:
        cur.close(); conn.close()


# ─── RabbitMQ ─────────────────────────────────────────────────────────────────

def get_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host        = RABBITMQ_HOST,
            port        = RABBITMQ_PORT,
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
            heartbeat   = 600,
            blocked_connection_timeout = 300,
        )
    )


def publish_to_model_queue(channel, payload: dict):
    channel.queue_declare(queue=MODEL_QUEUE, durable=True)
    channel.basic_publish(
        exchange    = "",
        routing_key = MODEL_QUEUE,
        body        = json.dumps(payload).encode(),
        properties  = pika.BasicProperties(delivery_mode=2),
    )
    logger.info("frame_id=%s → model_queue", payload.get("frame_id"))


# ─── Message handler ──────────────────────────────────────────────────────────

def handle_frame(channel, method, properties, body):
    frame_id = None
    try:
        message  = json.loads(body)
        frame_id = message.get("frame_id")
        logger.info("Received frame_id=%s", frame_id)

        # Drop frames already processed — idempotency guard
        status = _frame_status(frame_id)
        if status == "processed":
            logger.info("frame_id=%s already processed — dropping", frame_id)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Resolve image path — download from OCI if needed
        image_path     = message.get("image_path")
        image_metadata = message.get("image_metadata", {})

        if not image_path or not os.path.exists(image_path):
            image_key    = message.get("image_key")
            image_bucket = message.get("image_bucket")
            if not image_key or not image_bucket:
                logger.error("No image_path and no image_key/bucket for frame_id=%s — dropping", frame_id)
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            try:
                image_path = download_image(frame_id, image_bucket, image_key)
            except Exception as e:
                logger.error("OCI download failed frame_id=%s: %s — requeue", frame_id, e)
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            image_metadata = extract_exif(image_path)
            update_frame_downloaded(frame_id, image_path, image_metadata)

        # Forward to model_queue — model worker calls the API
        publish_to_model_queue(channel, {
            "frame_id":        frame_id,
            "image_path":      image_path,
            "image_metadata":  image_metadata,
            "latitude":        message.get("latitude"),
            "longitude":       message.get("longitude"),
            "municipality":    message.get("municipality"),
            "submunicipality": message.get("submunicipality"),
            "datetime_utc":    message.get("datetime_utc"),
            "reprocess":       message.get("reprocess", False),
            "segment_id":      message.get("segment_id"),
        })

        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.exception("Unexpected error frame_id=%s: %s", frame_id, e)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    logger.info("download_worker starting — frame_queue → model_queue")
    while True:
        try:
            conn    = get_connection()
            channel = conn.channel()
            channel.queue_declare(queue=FRAME_QUEUE, durable=True)
            channel.queue_declare(queue=MODEL_QUEUE,  durable=True)
            channel.basic_qos(prefetch_count=2)
            channel.basic_consume(queue=FRAME_QUEUE, on_message_callback=handle_frame)
            logger.info("Waiting for frames...")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error("RabbitMQ connection lost: %s — reconnecting in 5s", e)
            time.sleep(5)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
