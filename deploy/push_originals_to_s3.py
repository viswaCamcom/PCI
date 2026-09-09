#!/usr/bin/env python3
"""
Upload original frame images to the bucket under a separate prefix.

This is disaster recovery, not a serving path. Originals referenced by the
`frames` table are already in the citylens-momrah-frames* buckets and are
re-fetched by deploy/fetch_images.py — but the orphans (files on disk with no
row in `frames`) have no second copy anywhere, and unlike annotated images they
are not derived from anything, so no amount of compute reconstructs them.

    MODE=orphans   files on disk with no row in `frames`  (default)
    MODE=all       everything in downloaded_images/
    MODE=file      read frame ids from $ID_FILE, one per line

Run inside the download_worker image:

    docker compose run --rm --no-deps -v "$PWD/deploy:/app/deploy" \
      -e PUSH_WORKERS=24 download_worker python /app/deploy/push_originals_to_s3.py

Knobs: LIMIT (smoke test), PUSH_WORKERS (default 16), FORCE=1 (re-upload all).
"""
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import mysql.connector
from botocore.config import Config

DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/app/downloaded_images")

BUCKET   = os.environ.get("ANNOTATED_S3_BUCKET")     or os.environ.get("Bucket_name")
ENDPOINT = os.environ.get("ANNOTATED_S3_ENDPOINT")   or os.environ.get("S3_endpoint")
KEY_ID   = os.environ.get("ANNOTATED_S3_ACCESS_KEY") or os.environ.get("Access_Key_ID")
SECRET   = os.environ.get("ANNOTATED_S3_SECRET_KEY") or os.environ.get("Secret_Access_Key")
# Deliberately NOT the annotated prefix — these are unannotated source frames.
PREFIX   = os.environ.get("ORIGINALS_S3_PREFIX", "originals").strip("/")

MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))
MYSQL_DB   = os.environ.get("MYSQL_DB", "pci")
MYSQL_USER = os.environ.get("MYSQL_USER", "pci_user")
MYSQL_PASS = os.environ.get("MYSQL_PASSWORD", "pci_pass")

MODE    = os.environ.get("MODE", "orphans")
ID_FILE = os.environ.get("ID_FILE", "")
WORKERS = int(os.environ.get("PUSH_WORKERS", 16))
LIMIT   = int(os.environ.get("LIMIT", 0))
FORCE   = os.environ.get("FORCE", "") == "1"

if not all([BUCKET, ENDPOINT, KEY_ID, SECRET]):
    sys.exit("ERROR: bucket settings missing from .env")

# OCI rejects the aws-chunked encoding boto3 >= 1.36 sends by default.
CFG = Config(
    signature_version            = "s3v4",
    request_checksum_calculation = "when_required",
    response_checksum_validation = "when_required",
    max_pool_connections         = WORKERS,
)

_local = threading.local()


def s3():
    if not hasattr(_local, "c"):
        _local.c = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY_ID,
                                aws_secret_access_key=SECRET, config=CFG)
    return _local.c


def db_frame_ids():
    conn = mysql.connector.connect(host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DB,
                                   user=MYSQL_USER, password=MYSQL_PASS)
    cur = conn.cursor()
    cur.execute("SELECT frame_id FROM frames")
    ids = {r[0] for r in cur}
    cur.close()
    conn.close()
    return ids


def existing_keys():
    out    = {}
    client = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY_ID,
                          aws_secret_access_key=SECRET, config=CFG)
    token = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": f"{PREFIX}/"}
        if token:
            kw["ContinuationToken"] = token
        resp = client.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            out[o["Key"]] = o["Size"]
        if not resp.get("IsTruncated"):
            break
        token = resp["NextContinuationToken"]
        print(f"    listed {len(out)}…", flush=True)
    return out


_lock  = threading.Lock()
_stats = {"ok": 0, "skip": 0, "fail": 0, "bytes": 0}
_t0    = time.time()


def bump(kind, total, nbytes=0):
    with _lock:
        _stats[kind] += 1
        _stats["bytes"] += nbytes
        n = _stats["ok"] + _stats["skip"] + _stats["fail"]
        if n % 1000 == 0 or n == total:
            el  = max(time.time() - _t0, 0.001)
            mb  = _stats["bytes"] / 1024 / 1024
            eta = (total - n) / (n / el) / 60 if n else 0
            print(f"  {n}/{total}  up={_stats['ok']} skip={_stats['skip']} "
                  f"fail={_stats['fail']}  {mb:.0f} MB  {mb/el:.1f} MB/s  "
                  f"eta {eta:.0f}m", flush=True)


def push(fid, have, total):
    path = os.path.join(DOWNLOAD_DIR, f"{fid}.jpg")
    key  = f"{PREFIX}/{fid}.jpg"
    try:
        size = os.path.getsize(path)
    except OSError:
        bump("fail", total)
        return f"{fid}\tmissing on disk"

    if not FORCE and have.get(key) == size:
        bump("skip", total)
        return None

    try:
        with open(path, "rb") as fh:
            s3().put_object(Bucket=BUCKET, Key=key, Body=fh.read(),
                            ContentType="image/jpeg")
        bump("ok", total, size)
        return None
    except Exception as exc:
        bump("fail", total)
        return f"{fid}\t{type(exc).__name__}: {exc}"


def main():
    on_disk = {f[:-4] for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".jpg")}
    print(f"==> {len(on_disk):,} original jpgs in {DOWNLOAD_DIR}")

    if MODE == "file":
        if not ID_FILE:
            sys.exit("ERROR: MODE=file needs ID_FILE")
        wanted = {l.strip() for l in open(ID_FILE) if l.strip()} & on_disk
        print(f"    MODE=file — {len(wanted):,} ids from {ID_FILE} present on disk")
    elif MODE == "all":
        wanted = on_disk
        print(f"    MODE=all — every file on disk")
    else:
        indb   = db_frame_ids()
        wanted = on_disk - indb
        print(f"    MODE=orphans — {len(indb):,} in DB, {len(wanted):,} orphans")

    files = sorted(wanted)
    if LIMIT:
        files = files[:LIMIT]

    print(f"    target s3://{BUCKET}/{PREFIX}/  via {ENDPOINT}")
    print("==> Listing what the bucket already has")
    have = {} if FORCE else existing_keys()
    print(f"    {len(have):,} objects already present")

    total    = len(files)
    failures = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for err in pool.map(lambda f: push(f, have, total), files):
            if err:
                failures.append(err)

    el = time.time() - _t0
    print(f"\n==> uploaded={_stats['ok']} skipped={_stats['skip']} "
          f"failed={_stats['fail']}  {_stats['bytes']/1024**3:.1f} GB in {el/60:.1f} min")

    if failures:
        out = os.path.join(DOWNLOAD_DIR, "push_originals_failures.tsv")
        with open(out, "w") as fh:
            fh.write("\n".join(failures) + "\n")
        print(f"    failures written to {out} — re-run to retry")
        sys.exit(1)


if __name__ == "__main__":
    main()
