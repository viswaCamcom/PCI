"""
model_call/tasks.py
====================
Consumes from : frame_queue
Publishes to  : result_queue

For each message:
  1. Read frame_id, image_path, image_metadata from message
  2. Write image_metadata to a temp JSON file
  3. POST both files (image + metadata JSON) to PCI model API
  4. Publish model response to result_queue
"""

import os
import json
import logging
import time
import tempfile

import pika
import requests

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "mypass")

FRAME_QUEUE   = "frame_queue"
RESULT_QUEUE  = "result_queue"

MODEL_URL     = os.environ.get("MODEL_URL", "http://10.0.1.187:8010/detect/predict-pci")
MODEL_TIMEOUT = int(os.environ.get("MODEL_TIMEOUT", 120))


# ─── RabbitMQ helpers ─────────────────────────────────────────────────────────

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
    logger.info("Published result for frame_id=%s to %s",
                payload.get("frame_id"), RESULT_QUEUE)


# ─── Model call ───────────────────────────────────────────────────────────────

def call_pci_model(image_path: str, image_metadata: dict) -> dict:
    """
    POST image file + metadata JSON file to PCI model API.
    Returns the model response JSON.
    """
    # Write image_metadata dict to a temp JSON file
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as tmp_meta:
        json.dump(image_metadata, tmp_meta)
        meta_path = tmp_meta.name

    try:
        logger.info("Calling PCI model at %s", MODEL_URL)
        start = time.time()

        with open(image_path, "rb") as img_file, \
             open(meta_path, "rb") as meta_file:

            files = [
                ("image",         ("image.jpg",    img_file,  "image/jpeg")),
                ("metadata_file", ("metadata.json", meta_file, "application/json")),
            ]
            response = requests.post(MODEL_URL, files=files, timeout=MODEL_TIMEOUT)
            response.raise_for_status()

        elapsed = time.time() - start
        logger.info("Model response received in %.2fs", elapsed)
        return response.json()

    finally:
        # Always clean up temp metadata file
        if os.path.exists(meta_path):
            os.remove(meta_path)


# ─── Message handler ──────────────────────────────────────────────────────────

def handle_frame(channel, method, properties, body):
    """
    Called for each message consumed from frame_queue.
    """
    try:
        message  = json.loads(body)
        frame_id = message.get("frame_id")
        image_path    = message.get("image_path")
        image_metadata = message.get("image_metadata", {})

        logger.info("Received frame_id=%s image_path=%s", frame_id, image_path)

        # Validate image file exists before calling model
        if not image_path or not os.path.exists(image_path):
            logger.error("Image not found at path=%s for frame_id=%s",
                         image_path, frame_id)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        # ── Call PCI model ────────────────────────────────────────────────────
        model_response = call_pci_model(image_path, image_metadata)
        logger.info("Model result for frame_id=%s: %s", frame_id, model_response)

        # ── Build result payload ──────────────────────────────────────────────
        result_payload = {
            "frame_id":       frame_id,
            "image_path":     image_path,
            "image_metadata": image_metadata,
            "latitude":       message.get("latitude"),
            "longitude":      message.get("longitude"),
            "municipality":   message.get("municipality"),
            "submunicipality": message.get("submunicipality"),
            "datetime_utc":   message.get("datetime_utc"),
            "model_response": model_response,
        }
        # ── Publish to result_queue ───────────────────────────────────────────
        publish_result(channel, result_payload)

        # Ack only after successful publish
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except requests.exceptions.RequestException as e:
        logger.error("Model API call failed for frame_id=%s: %s", frame_id, e)
        # Requeue — model might be temporarily down
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    except Exception as e:
        logger.exception("Unexpected error handling frame_id=%s: %s", frame_id, e)
        # Don't requeue unknown errors — goes to dead letter if configured
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


# ─── Entry point ─────────────────────────────────────────────────────────────

def main():
    logger.info("model_call worker starting ...")
    logger.info("Consuming from: %s", FRAME_QUEUE)
    logger.info("Publishing to:  %s", RESULT_QUEUE)
    logger.info("Model URL:      %s", MODEL_URL)

    while True:
        try:
            conn    = get_connection()
            channel = conn.channel()

            channel.queue_declare(queue=FRAME_QUEUE,  durable=True)
            channel.queue_declare(queue=RESULT_QUEUE, durable=True)

            channel.basic_qos(prefetch_count=2)

            channel.basic_consume(
                queue             = FRAME_QUEUE,
                on_message_callback = handle_frame,
            )

            logger.info("Waiting for frames ...")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            logger.error("RabbitMQ connection lost: %s — reconnecting in 5s", e)
            time.sleep(5)

        except KeyboardInterrupt:
            logger.info("Shutting down model_call worker")
            break


if __name__ == "__main__":
    main()



# import requests
# import time
# import json

# image_path = "/nvme_mount/rohith_nvme/PCI/fastapi/runs/segment/predict2/original_img/308399134.jpg"
# metadata_path = "/nvme_mount/rohith_nvme/PCI/fastapi/runs/segment/predict2/image_metadata/308399134.json"

# start = time.time()
# url = "http://10.0.1.187:8010/detect/predict-pci"
# files = [
#     ("image", ("image.jpg", open(image_path, "rb"), "image/jpeg")),
#     ("metadata_file", ("metadata.json", open(metadata_path, "rb"), "application/json")),
# ]
# response = requests.post(url, files=files)
# print("time taken : ", time.time() - start)
# print(json.dumps(response.json(), indent=2))



