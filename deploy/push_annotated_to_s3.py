#!/usr/bin/env python3
"""
Upload downloaded_images/annotated/*.jpg to the annotated-image bucket.

Resumable: the bucket is listed once up front and anything already there at the
same byte size is skipped, so re-running after an interruption costs one LIST
sweep instead of re-uploading.

Run inside the download_worker image (it has boto3):

    docker compose run --rm --no-deps \
      -v "$PWD/deploy:/app/deploy" \
      download_worker python /app/deploy/push_annotated_to_s3.py

Knobs: LIMIT (smoke test), PUSH_WORKERS (default 16), FORCE=1 (re-upload all).
"""
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

ANNOTATED_DIR = os.environ.get("ANNOTATED_DIR", "/app/downloaded_images/annotated")

# Canonical names, falling back to the mixed-case spellings originally added to
# .env so an un-normalised file still works.
BUCKET   = os.environ.get("ANNOTATED_S3_BUCKET")     or os.environ.get("Bucket_name")
ENDPOINT = os.environ.get("ANNOTATED_S3_ENDPOINT")   or os.environ.get("S3_endpoint")
KEY_ID   = os.environ.get("ANNOTATED_S3_ACCESS_KEY") or os.environ.get("Access_Key_ID")
SECRET   = os.environ.get("ANNOTATED_S3_SECRET_KEY") or os.environ.get("Secret_Access_Key")
PREFIX   = os.environ.get("ANNOTATED_S3_PREFIX", "annotated").strip("/")

WORKERS = int(os.environ.get("PUSH_WORKERS", 16))
LIMIT   = int(os.environ.get("LIMIT", 0))
FORCE   = os.environ.get("FORCE", "") == "1"

if not all([BUCKET, ENDPOINT, KEY_ID, SECRET]):
    sys.exit("ERROR: ANNOTATED_S3_BUCKET / _ENDPOINT / _ACCESS_KEY / _SECRET_KEY not set in .env")

# OCI's S3 API rejects the aws-chunked content-encoding that boto3 >= 1.36 adds
# by default ("NotImplemented: AWS chunked encoding not supported"), so opt back
# out of the trailing-checksum behaviour.
CFG = Config(
    signature_version            = "s3v4",
    request_checksum_calculation = "when_required",
    response_checksum_validation = "when_required",
    max_pool_connections         = WORKERS,
)

_local = threading.local()


def s3():
    # boto3 clients are not thread-safe — one per worker thread.
    if not hasattr(_local, "c"):
        _local.c = boto3.client(
            "s3", endpoint_url=ENDPOINT,
            aws_access_key_id=KEY_ID, aws_secret_access_key=SECRET, config=CFG,
        )
    return _local.c


def existing_keys():
    """Map of key -> size for everything already under the prefix."""
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
        if n % 500 == 0 or n == total:
            el = max(time.time() - _t0, 0.001)
            mb = _stats["bytes"] / 1024 / 1024
            eta = (total - n) / (n / el) / 60 if n else 0
            print(f"  {n}/{total}  up={_stats['ok']} skip={_stats['skip']} "
                  f"fail={_stats['fail']}  {mb:.0f} MB  {mb/el:.1f} MB/s  "
                  f"eta {eta:.0f}m", flush=True)


def push(fname, have, total):
    path = os.path.join(ANNOTATED_DIR, fname)
    key  = f"{PREFIX}/{fname}"
    try:
        size = os.path.getsize(path)
    except OSError:
        bump("fail", total)
        return f"{fname}\tstat failed"

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
        return f"{fname}\t{type(exc).__name__}: {exc}"


def main():
    if not os.path.isdir(ANNOTATED_DIR):
        sys.exit(f"ERROR: {ANNOTATED_DIR} does not exist")

    files = sorted(f for f in os.listdir(ANNOTATED_DIR) if f.endswith(".jpg"))
    if LIMIT:
        files = files[:LIMIT]

    print(f"==> {len(files)} local files in {ANNOTATED_DIR}")
    print(f"    target s3://{BUCKET}/{PREFIX}/  via {ENDPOINT}")
    print("==> Listing what the bucket already has")
    have = {} if FORCE else existing_keys()
    print(f"    {len(have)} objects already present")

    total    = len(files)
    failures = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for err in pool.map(lambda f: push(f, have, total), files):
            if err:
                failures.append(err)

    el = time.time() - _t0
    print(f"\n==> uploaded={_stats['ok']} skipped={_stats['skip']} "
          f"failed={_stats['fail']}  in {el/60:.1f} min")

    if failures:
        out = "/app/downloaded_images/push_failures.tsv"
        with open(out, "w") as fh:
            fh.write("\n".join(failures) + "\n")
        print(f"    failures written to {out} — re-run to retry (uploads are skipped)")
        sys.exit(1)


if __name__ == "__main__":
    main()
