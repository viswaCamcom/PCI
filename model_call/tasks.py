"""
model_call/tasks.py  (model worker)
=====================================
Consumes from : model_queue   (image already on disk — no OCI download here)
Publishes to  : result_queue

For each message:
  1. image_path is already populated by download_worker
  2. POST image + metadata to PCI model API
  3. Publish model response to result_queue
"""

import os
import json
import logging
import time
import tempfile

import pika
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "mypass")

MODEL_QUEUE  = "model_queue"
RESULT_QUEUE = "result_queue"
FAILED_QUEUE = "model_queue_failed"

MODEL_URL     = os.environ.get("MODEL_URL", "http://10.0.1.187:8010/detect/predict-pci")
MODEL_TIMEOUT = int(os.environ.get("MODEL_TIMEOUT", 120))

# A frame whose model call keeps 500-ing (bad image, unsupported input, etc.)
# used to requeue forever, cycling between workers and burning most of the
# fleet's capacity on messages that will never succeed. Cap retries and park
# anything past the limit in FAILED_QUEUE so it stops blocking real work.
MODEL_MAX_RETRIES = int(os.environ.get("MODEL_MAX_RETRIES", 3))


# ─── Model call ───────────────────────────────────────────────────────────────

def call_pci_model(image_path: str, image_metadata: dict) -> dict:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        json.dump(image_metadata, tmp)
        meta_path = tmp.name
    try:
        start = time.time()
        with open(image_path, "rb") as img_f, open(meta_path, "rb") as meta_f:
            resp = requests.post(
                MODEL_URL,
                files=[
                    ("image",         ("image.jpg",     img_f,  "image/jpeg")),
                    ("metadata_file", ("metadata.json", meta_f, "application/json")),
                ],
                timeout=MODEL_TIMEOUT,
            )
            resp.raise_for_status()
        logger.info("Model response in %.2fs for image=%s", time.time() - start, image_path)
        return resp.json()
    finally:
        if os.path.exists(meta_path):
            os.remove(meta_path)


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


def publish_result(channel, payload: dict):
    channel.queue_declare(queue=RESULT_QUEUE, durable=True)
    channel.basic_publish(
        exchange    = "",
        routing_key = RESULT_QUEUE,
        body        = json.dumps(payload).encode(),
        properties  = pika.BasicProperties(delivery_mode=2),
    )
    logger.info("frame_id=%s → result_queue", payload.get("frame_id"))


def republish_to_model_queue(channel, message: dict):
    channel.queue_declare(queue=MODEL_QUEUE, durable=True)
    channel.basic_publish(
        exchange    = "",
        routing_key = MODEL_QUEUE,
        body        = json.dumps(message).encode(),
        properties  = pika.BasicProperties(delivery_mode=2),
    )


def publish_to_failed_queue(channel, message: dict, error: str):
    channel.queue_declare(queue=FAILED_QUEUE, durable=True)
    message["_error"] = error
    channel.basic_publish(
        exchange    = "",
        routing_key = FAILED_QUEUE,
        body        = json.dumps(message).encode(),
        properties  = pika.BasicProperties(delivery_mode=2),
    )
    logger.error("frame_id=%s → %s after %d attempts — giving up",
                 message.get("frame_id"), FAILED_QUEUE, message.get("_retry_count", 0))


# ─── Message handler ──────────────────────────────────────────────────────────

def handle_frame(channel, method, properties, body):
    frame_id = None
    message  = {}
    try:
        message    = json.loads(body)
        frame_id   = message.get("frame_id")
        image_path = message.get("image_path")
        retry_count = message.get("_retry_count", 0)

        logger.info("Received frame_id=%s (attempt %d/%d)", frame_id, retry_count + 1, MODEL_MAX_RETRIES)

        if not image_path or not os.path.exists(image_path):
            logger.error("image_path missing or not on disk for frame_id=%s — dropping", frame_id)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        model_response = call_pci_model(image_path, message.get("image_metadata", {}))
        logger.info("Model result frame_id=%s: %s", frame_id, model_response)

        publish_result(channel, {
            "frame_id":        frame_id,
            "image_path":      image_path,
            "image_metadata":  message.get("image_metadata", {}),
            "latitude":        message.get("latitude"),
            "longitude":       message.get("longitude"),
            "municipality":    message.get("municipality"),
            "submunicipality": message.get("submunicipality"),
            "datetime_utc":    message.get("datetime_utc"),
            "model_response":  model_response,
            "reprocess":       message.get("reprocess", False),
            "segment_id":      message.get("segment_id"),
        })

        channel.basic_ack(delivery_tag=method.delivery_tag)

    except requests.exceptions.RequestException as e:
        retry_count = message.get("_retry_count", 0)
        if retry_count + 1 >= MODEL_MAX_RETRIES:
            publish_to_failed_queue(channel, {**message, "_retry_count": retry_count + 1}, str(e))
        else:
            logger.warning("Model API failed frame_id=%s: %s — retry %d/%d",
                            frame_id, e, retry_count + 1, MODEL_MAX_RETRIES)
            republish_to_model_queue(channel, {**message, "_retry_count": retry_count + 1})
        # Original delivery is always resolved here — retries happen via a
        # fresh republish above, never via requeue=True, so the count sticks.
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    except Exception as e:
        logger.exception("Unexpected error frame_id=%s: %s", frame_id, e)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    logger.info("model_call worker starting — model_queue → result_queue")
    logger.info("Model URL: %s", MODEL_URL)
    while True:
        try:
            conn    = get_connection()
            channel = conn.channel()
            channel.queue_declare(queue=MODEL_QUEUE,  durable=True)
            channel.queue_declare(queue=RESULT_QUEUE, durable=True)
            channel.queue_declare(queue=FAILED_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=2)
            channel.basic_consume(queue=MODEL_QUEUE, on_message_callback=handle_frame)
            logger.info("Waiting for frames...")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error("RabbitMQ connection lost: %s — reconnecting in 5s", e)
            time.sleep(5)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
