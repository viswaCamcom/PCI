#!/usr/bin/env python3
"""
How far along is the annotated-image upload?

Counts objects under the bucket prefix and compares with the local directory,
so it works regardless of who started the upload or whether that terminal is
still open.

    docker compose run --rm --no-deps -v "$PWD/deploy:/app/deploy" \
      download_worker python /app/deploy/bucket_status.py
"""
import os
import sys

import boto3
from botocore.config import Config

ANNOTATED_DIR = os.environ.get("ANNOTATED_DIR", "/app/downloaded_images/annotated")

BUCKET   = os.environ.get("ANNOTATED_S3_BUCKET")     or os.environ.get("Bucket_name")
ENDPOINT = os.environ.get("ANNOTATED_S3_ENDPOINT")   or os.environ.get("S3_endpoint")
KEY_ID   = os.environ.get("ANNOTATED_S3_ACCESS_KEY") or os.environ.get("Access_Key_ID")
SECRET   = os.environ.get("ANNOTATED_S3_SECRET_KEY") or os.environ.get("Secret_Access_Key")
PREFIX   = os.environ.get("ANNOTATED_S3_PREFIX", "annotated").strip("/")

if not all([BUCKET, ENDPOINT, KEY_ID, SECRET]):
    sys.exit("ERROR: annotated-bucket settings missing from .env")

s3 = boto3.client(
    "s3", endpoint_url=ENDPOINT,
    aws_access_key_id=KEY_ID, aws_secret_access_key=SECRET,
    config=Config(signature_version="s3v4",
                  request_checksum_calculation="when_required",
                  response_checksum_validation="when_required"),
)

def sweep(prefix):
    count = size = 0
    token = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": f"{prefix}/"}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            count += 1
            size  += o["Size"]
        if not resp.get("IsTruncated"):
            break
        token = resp["NextContinuationToken"]
    return count, size


ORIG_PREFIX = os.environ.get("ORIGINALS_S3_PREFIX", "originals").strip("/")
ID_FILE     = os.environ.get("ID_FILE", "/app/deploy/dump/unrecoverable_ids.txt")

print(f"bucket  s3://{BUCKET}/\n")

ann_n, ann_b = sweep(PREFIX)
local_ann = 0
if os.path.isdir(ANNOTATED_DIR):
    local_ann = sum(1 for f in os.listdir(ANNOTATED_DIR) if f.endswith(".jpg"))
pct = f"{ann_n / local_ann * 100:.1f}%" if local_ann else "—"
print(f"{PREFIX + '/':12s} {ann_n:>9,} objects  {ann_b/1024**3:>6.2f} GB   "
      f"local {local_ann:,}  ({pct})")

orig_n, orig_b = sweep(ORIG_PREFIX)
target = 0
if os.path.exists(ID_FILE):
    target = sum(1 for l in open(ID_FILE) if l.strip())
pct = f"{orig_n / target * 100:.1f}%" if target else "—"
print(f"{ORIG_PREFIX + '/':12s} {orig_n:>9,} objects  {orig_b/1024**3:>6.2f} GB   "
      f"target {target:,}  ({pct})")
if target and orig_n < target:
    print(f"{'':12s} {target - orig_n:>9,} remaining")

map_n, map_b = sweep("mappings")
print(f"{'mappings/':12s} {map_n:>9,} objects  {map_b/1024**3:>6.2f} GB")
