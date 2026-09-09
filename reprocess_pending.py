#!/usr/bin/env python3
"""
reprocess_pending.py
====================
Re-publish every pending frame to frame_queue so model_call workers pick them up.

Safe to run while the pipeline is live — model_call now has an idempotency
guard that drops duplicate messages for frames already downloaded or processed.

Usage:
    python3 reprocess_pending.py                   # re-publish all pending
    python3 reprocess_pending.py --rate 200        # 200 publishes/sec (default 500)
    python3 reprocess_pending.py --dry-run         # print count only
    python3 reprocess_pending.py --limit 1000      # test with first 1000

Environment variables (same as the containers):
    MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB
    RABBITMQ_HOST / RABBITMQ_PORT / RABBITMQ_USER / RABBITMQ_PASS
"""

import argparse
import json
import logging
import os
import time

import mysql.connector
import pika

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

FRAME_QUEUE = "frame_queue"


def get_db_conn():
    return mysql.connector.connect(
        host     = os.environ.get("MYSQL_HOST",     "127.0.0.1"),
        port     = int(os.environ.get("MYSQL_PORT",  3306)),
        user     = os.environ.get("MYSQL_USER",     "pci_user"),
        password = os.environ.get("MYSQL_PASSWORD", "pci_pass"),
        database = os.environ.get("MYSQL_DB",       "pci"),
    )


def get_mq_channel():
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host        = os.environ.get("RABBITMQ_HOST", "127.0.0.1"),
            port        = int(os.environ.get("RABBITMQ_PORT", 5672)),
            credentials = pika.PlainCredentials(
                os.environ.get("RABBITMQ_USER", "admin"),
                os.environ.get("RABBITMQ_PASS", "mypass"),
            ),
            heartbeat                  = 600,
            blocked_connection_timeout = 300,
        )
    )
    ch = conn.channel()
    ch.queue_declare(queue=FRAME_QUEUE, durable=True)
    return conn, ch


def main():
    ap = argparse.ArgumentParser(description="Re-publish pending frames to frame_queue")
    ap.add_argument("--rate",    type=float, default=500,  metavar="N",
                    help="Publish rate in messages/sec (default 500)")
    ap.add_argument("--limit",   type=int,   default=None, metavar="N",
                    help="Only re-publish the first N pending frames (for testing)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print count of pending frames then exit without publishing")
    args = ap.parse_args()

    # ── 1. Load pending frames from DB ────────────────────────────────────────
    logger.info("Connecting to DB…")
    db = get_db_conn()
    cur = db.cursor(dictionary=True)

    cur.execute("""
        SELECT frame_id, image_key, image_bucket,
               latitude, longitude, municipality, submunicipality, datetime_utc
        FROM frames
        WHERE status = 'pending'
        ORDER BY created_at ASC
    """)
    rows = cur.fetchall()
    cur.close()
    db.close()

    total = len(rows)
    logger.info("Found %d pending frames", total)

    if args.dry_run:
        return

    if args.limit:
        rows = rows[: args.limit]
        logger.info("Limit applied — publishing %d frames", len(rows))

    # ── 2. Connect to RabbitMQ ────────────────────────────────────────────────
    logger.info("Connecting to RabbitMQ…")
    mq_conn, ch = get_mq_channel()

    # ── 3. Publish ────────────────────────────────────────────────────────────
    interval   = 1.0 / args.rate
    published  = 0
    start      = time.time()

    for row in rows:
        payload = {
            "frame_id":        row["frame_id"],
            "image_key":       row["image_key"],
            "image_bucket":    row["image_bucket"],
            "latitude":        row["latitude"],
            "longitude":       row["longitude"],
            "municipality":    row.get("municipality"),
            "submunicipality": row.get("submunicipality"),
            "datetime_utc":    row.get("datetime_utc"),
        }

        ch.basic_publish(
            exchange    = "",
            routing_key = FRAME_QUEUE,
            body        = json.dumps(payload).encode(),
            properties  = pika.BasicProperties(delivery_mode=2),
        )
        published += 1

        if published % 1000 == 0:
            elapsed  = time.time() - start
            rate_act = published / elapsed if elapsed > 0 else 0
            logger.info("  Published %d / %d  (%.0f msg/s)", published, len(rows), rate_act)

        time.sleep(interval)

    mq_conn.close()

    elapsed  = time.time() - start
    rate_act = published / elapsed if elapsed > 0 else 0
    logger.info("Done — published %d messages in %.1fs (%.0f msg/s)", published, elapsed, rate_act)
    logger.info("model_call workers will pick these up and process them.")
    logger.info("Duplicate messages (frames already in queue) will be dropped by the")
    logger.info("idempotency guard in model_call — no duplicate violations will be created.")


if __name__ == "__main__":
    main()
