"""
reprocess_july17.py
====================
Republishes the July_17th.csv frame batch directly to frame_queue.

Why not just re-run pci_api_upload.py: these frame_id rows already exist in
`frames` (inserted by the first run), so /upload returns 409 and never
republishes. These frames never reached status='processed' though, because
save_violations()/recompute_segment_pci() threw a schema error before step 5
in processing_worker/tasks.py — so download_worker's idempotency guard won't
block a redrive, and it'll skip re-downloading since the image is already on
disk. This republishes straight to frame_queue so the whole pipeline reruns
end-to-end against the now-working model API.

Usage:
    python3 reprocess_july17.py --csv July_17th.csv --limit 1   # test one frame first
    python3 reprocess_july17.py --csv July_17th.csv             # full 32,908
"""

import argparse
import csv
import json

import mysql.connector
import pika

SRC_DB = dict(
    host     = "10.20.1.201",
    port     = 5506,
    user     = "momrah-admin",
    password = "MoMrah!6!6@Gifto0709$",
    database = "baladylens_prod",
    charset  = "utf8mb4",
    connection_timeout = 15,
)

RABBITMQ_HOST = "10.0.1.97"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "admin"
RABBITMQ_PASS = "mypass"
FRAME_QUEUE   = "frame_queue"

BATCH_SIZE = 1000


def read_ids(csv_path):
    ids = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row:
                continue
            val = row[0].strip()
            if val.lower() in ("citylens_id", "id", ""):
                continue
            ids.append(val)
    return ids


def fetch_rows(ids):
    conn = mysql.connector.connect(**SRC_DB)
    cur  = conn.cursor(dictionary=True)
    rows = []
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i:i + BATCH_SIZE]
        placeholders = ",".join(["%s"] * len(batch))
        cur.execute(
            f"""
            SELECT citylens_id, latitude, longitude, image_key, image_bucket,
                   municipality_id, submunicipality_id,
                   submitted_date AS datetime_utc
            FROM upload_data
            WHERE citylens_id IN ({placeholders})
            """,
            batch,
        )
        rows.extend(cur.fetchall())
    cur.close()
    conn.close()
    return rows


def publish(rows):
    params = pika.ConnectionParameters(
        host        = RABBITMQ_HOST,
        port        = RABBITMQ_PORT,
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
    )
    conn = pika.BlockingConnection(params)
    ch   = conn.channel()
    ch.queue_declare(queue=FRAME_QUEUE, durable=True)

    for r in rows:
        msg = {
            "frame_id":        str(r["citylens_id"]),
            "image_key":       r["image_key"],
            "image_bucket":    r["image_bucket"],
            "latitude":        float(r["latitude"]),
            "longitude":       float(r["longitude"]),
            "municipality":    r.get("municipality_id", "") or "",
            "submunicipality": r.get("submunicipality_id", "") or "",
            "datetime_utc":    str(r["datetime_utc"]),
        }
        ch.basic_publish(
            exchange    = "",
            routing_key = FRAME_QUEUE,
            body        = json.dumps(msg).encode(),
            properties  = pika.BasicProperties(delivery_mode=2),
        )
        print(f"  published frame_id={msg['frame_id']}")

    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    ids = read_ids(args.csv)
    if args.limit:
        ids = ids[:args.limit]

    print(f"Fetching {len(ids)} rows from baladylens_prod...")
    rows = fetch_rows(ids)
    print(f"Publishing {len(rows)} messages to frame_queue on {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
    publish(rows)
    print("Done.")


if __name__ == "__main__":
    main()
