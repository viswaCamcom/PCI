#!/usr/bin/env python3
"""
Re-download original frame images from OCI object storage.

Every row in `frames` carries image_bucket + image_key, so the originals never
need to be copied between machines — they are pulled straight from the bucket
on whichever host needs them.

This deliberately does NOT go through frame_queue. Enqueueing would re-run the
whole pipeline (download → model → processing) and overwrite the violation rows
restored from the dump. We only want the image bytes.

Run inside the download_worker image (it already has boto3 + mysql-connector):

    docker compose run --rm --no-deps \
      -v "$PWD/downloaded_images:/app/downloaded_images" \
      download_worker python /app/deploy/fetch_images.py
"""
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import mysql.connector
from botocore.config import Config

DOWNLOAD_DIR    = os.environ.get("DOWNLOAD_DIR", "/app/downloaded_images")
OCI_S3_ENDPOINT = os.environ.get("OCI_S3_ENDPOINT")
S3_ACCESS_KEY   = os.environ.get("S3_ACCESS_KEY")   or os.environ.get("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY   = os.environ.get("S3_SECRET_KEY")   or os.environ.get("AWS_SECRET_ACCESS_KEY")

MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))
MYSQL_DB   = os.environ.get("MYSQL_DB", "pci")
MYSQL_USER = os.environ.get("MYSQL_USER", "pci_user")
MYSQL_PASS = os.environ.get("MYSQL_PASSWORD", "pci_pass")

WORKERS = int(os.environ.get("FETCH_WORKERS", 32))
# Smoke-test knob: LIMIT=20 fetches only the first 20 frames, so credentials and
# bucket access can be proven before committing to a multi-hour run.
LIMIT   = int(os.environ.get("LIMIT", 0))
# Optional scoping, e.g. MUNICIPALITIES=002001,005001 for Makkah + Jeddah.
# Empty means every frame in the table.
MUNICIPALITIES = [m.strip() for m in os.environ.get("MUNICIPALITIES", "").split(",") if m.strip()]

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# boto3 clients are not thread-safe; give each worker thread its own.
_local = threading.local()


def s3():
    if not hasattr(_local, "client"):
        _local.client = boto3.client(
            "s3",
            endpoint_url          = OCI_S3_ENDPOINT,
            aws_access_key_id     = S3_ACCESS_KEY,
            aws_secret_access_key = S3_SECRET_KEY,
            # Default pool is 10; without this, 32 threads serialise on sockets.
            config = Config(signature_version="s3v4", max_pool_connections=WORKERS),
        )
    return _local.client


_counter_lock = threading.Lock()
_done = {"ok": 0, "skip": 0, "fail": 0}


def progress(kind, total):
    with _counter_lock:
        _done[kind] += 1
        n = _done["ok"] + _done["skip"] + _done["fail"]
        if n % 500 == 0 or n == total:
            print(f"  {n}/{total}  downloaded={_done['ok']} "
                  f"skipped={_done['skip']} failed={_done['fail']}", flush=True)


def fetch(row, total):
    frame_id, bucket, key = row
    path = os.path.join(DOWNLOAD_DIR, f"{frame_id}.jpg")

    # Resumable: an existing non-empty file is left alone, so re-running after
    # an interruption costs one stat() per frame instead of a re-download.
    if os.path.exists(path) and os.path.getsize(path) > 0:
        progress("skip", total)
        return None

    try:
        body = s3().get_object(Bucket=bucket, Key=key)["Body"].read()
        # Write to a temp name then rename, so an interrupted run never leaves a
        # truncated file that the skip-check above would treat as complete.
        tmp = f"{path}.part"
        with open(tmp, "wb") as fh:
            fh.write(body)
        os.replace(tmp, path)
        progress("ok", total)
        return None
    except Exception as exc:
        progress("fail", total)
        return f"{frame_id}\t{bucket}\t{key}\t{type(exc).__name__}: {exc}"


def main():
    conn = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DB,
        user=MYSQL_USER, password=MYSQL_PASS,
    )
    cur = conn.cursor()
    sql    = "SELECT frame_id, image_bucket, image_key FROM frames"
    params = []
    if MUNICIPALITIES:
        sql += " WHERE municipality IN (%s)" % ",".join(["%s"] * len(MUNICIPALITIES))
        params = MUNICIPALITIES
    if LIMIT:
        sql += f" LIMIT {LIMIT}"
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    total = len(rows)
    scope = ",".join(MUNICIPALITIES) if MUNICIPALITIES else "all municipalities"
    print(f"==> {total} frames ({scope}), target {DOWNLOAD_DIR}")
    print(f"    {WORKERS} parallel workers, endpoint {OCI_S3_ENDPOINT}")

    failures = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(fetch, r, total) for r in rows]
        for fut in as_completed(futures):
            err = fut.result()
            if err:
                failures.append(err)

    print(f"\n==> downloaded={_done['ok']} skipped={_done['skip']} failed={_done['fail']}")

    if failures:
        out = os.path.join(DOWNLOAD_DIR, "fetch_failures.tsv")
        with open(out, "w") as fh:
            fh.write("\n".join(failures) + "\n")
        print(f"    failures written to {out}")
        print(f"    re-run this script to retry them (completed files are skipped)")
        sys.exit(1)


if __name__ == "__main__":
    main()
