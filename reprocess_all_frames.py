#!/usr/bin/env python3
"""
reprocess_all_frames.py
=======================
Re-run every frame through the PCI model API, refresh violations with the
new mm + px fields, recompute ASTM PCI scores for frames and segments.

Images already on disk are NOT re-downloaded.
Detections may differ from the original run — that is expected and desired.

Usage:
  python3 reprocess_all_frames.py                   # all 209K frames, 16 threads
  python3 reprocess_all_frames.py --workers 32      # more parallelism
  python3 reprocess_all_frames.py --limit 500       # test run on first 500
  python3 reprocess_all_frames.py --resume          # skip already checkpointed frames
  python3 reprocess_all_frames.py --dry-run         # schema migration only, no model calls

Checkpoint file: reprocess_checkpoint.txt  (one frame_id per line)
After the run: python3 calculate_health_score.py scores
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import mysql.connector
import requests
from PIL import Image, ImageDraw

# Import ASTM PCI calculator from the processing_worker directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "processing_worker"))
from pci_calculator import (
    compute_frame_pci,
    compute_segment_pci_from_aggregates,
    pci_rating,
)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
DB = dict(host="127.0.0.1", port=3306,
          user="pci_user", password="pci_pass", database="pci")

MODEL_URL     = "http://10.0.1.187:8010/detect/predict-pci"
MODEL_TIMEOUT = 120

# Host paths (mounted volume)
_HERE         = os.path.dirname(os.path.abspath(__file__))
IMAGE_DIR     = os.path.join(_HERE, "downloaded_images")
ANNOTATED_DIR = os.path.join(IMAGE_DIR, "annotated")

# Docker-internal prefix stored in DB, used only when the bucket is unavailable
# (so the Flask app can still serve annotated images off its mounted volume)
DOCKER_ANNOTATED_PREFIX = "/app/downloaded_images/annotated"

CHECKPOINT_FILE = os.path.join(_HERE, "reprocess_checkpoint.txt")


# ── Annotated-image bucket ─────────────────────────────────────────────────────
# This script runs on the host, outside compose, so nothing has loaded .env for
# us — read the few keys we need directly.

def _load_env_file(path):
    out = {}
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip("'\"")
    except OSError:
        pass
    return out


_ENV = _load_env_file(os.path.join(_HERE, ".env"))


def _cfg(*names):
    for n in names:
        v = os.environ.get(n) or _ENV.get(n)
        if v:
            return v
    return None


ANN_S3_BUCKET   = _cfg("ANNOTATED_S3_BUCKET",     "Bucket_name")
ANN_S3_ENDPOINT = _cfg("ANNOTATED_S3_ENDPOINT",   "S3_endpoint")
ANN_S3_KEY      = _cfg("ANNOTATED_S3_ACCESS_KEY", "Access_Key_ID")
ANN_S3_SECRET   = _cfg("ANNOTATED_S3_SECRET_KEY", "Secret_Access_Key")
ANN_S3_PREFIX   = (_cfg("ANNOTATED_S3_PREFIX") or "annotated").strip("/")
ANN_KEEP_LOCAL  = os.environ.get("ANNOTATED_KEEP_LOCAL", "0") == "1"

# boto3 is a container dependency; on a bare host it may not be installed. Treat
# that as "bucket disabled" rather than refusing to run the whole reprocess.
try:
    import boto3
    from botocore.config import Config as BotoConfig
    _BOTO_OK = True
except ImportError:
    _BOTO_OK = False

ANN_S3_ENABLED = _BOTO_OK and all(
    [ANN_S3_BUCKET, ANN_S3_ENDPOINT, ANN_S3_KEY, ANN_S3_SECRET]
)

_ann_s3      = None
_ann_s3_lock = threading.Lock()


def get_ann_s3():
    global _ann_s3
    if _ann_s3 is None:
        with _ann_s3_lock:
            if _ann_s3 is None:
                _ann_s3 = boto3.client(
                    "s3",
                    endpoint_url          = ANN_S3_ENDPOINT,
                    aws_access_key_id     = ANN_S3_KEY,
                    aws_secret_access_key = ANN_S3_SECRET,
                    # OCI rejects the aws-chunked encoding boto3 >= 1.36 sends by
                    # default; opt back out of the trailing-checksum behaviour.
                    config = BotoConfig(
                        signature_version            = "s3v4",
                        request_checksum_calculation = "when_required",
                        response_checksum_validation = "when_required",
                        max_pool_connections         = 32,
                    ),
                )
    return _ann_s3


def upload_annotated(path, frame_id):
    """Upload one annotated JPEG. Returns the key, or None if it failed."""
    key = f"{ANN_S3_PREFIX}/{frame_id}.jpg"
    try:
        with open(path, "rb") as fh:
            get_ann_s3().put_object(Bucket=ANN_S3_BUCKET, Key=key,
                                    Body=fh.read(), ContentType="image/jpeg")
        return key
    except Exception as exc:
        log.error("upload_annotated %s failed: %s", frame_id, exc)
        return None

LABEL_COLORS = {
    "pothole":            (255,  50,  50),
    "longitudinal_crack": (255, 165,   0),
    "transverse_crack":   (255, 255,   0),
    "alligator_crack":    (255,   0, 255),
    "rutting":            (  0, 200, 255),
    "default":            (  0, 255,   0),
}

os.makedirs(ANNOTATED_DIR, exist_ok=True)

# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_conn():
    return mysql.connector.connect(**DB)


def ensure_px_columns():
    """Add the four px columns to violations — idempotent."""
    cols = [
        ("length_px",       "FLOAT DEFAULT NULL"),
        ("breadth_px",      "FLOAT DEFAULT NULL"),
        ("bbox_area_px",    "FLOAT DEFAULT NULL"),
        ("polygon_area_px", "FLOAT DEFAULT NULL"),
    ]
    conn = get_conn()
    cur  = conn.cursor()
    added = []
    for col, defn in cols:
        try:
            cur.execute(f"ALTER TABLE violations ADD COLUMN {col} {defn}")
            added.append(col)
        except mysql.connector.Error as e:
            if e.errno != 1060:   # 1060 = duplicate column — already exists
                raise
    conn.commit()
    cur.close(); conn.close()
    if added:
        log.info("Added columns to violations: %s", ", ".join(added))
    else:
        log.info("px columns already present — no schema change needed.")


# ── Pre-load lookup structures ─────────────────────────────────────────────────

def load_frame_segment_map() -> dict:
    """
    Build frame_id → segment_id from existing violations.
    Covers 37K frames that already have detections.
    Frames without violations need the nearest-segment fallback.
    """
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT frame_id, MAX(segment_id) AS segment_id
        FROM   violations
        WHERE  segment_id IS NOT NULL
        GROUP  BY frame_id
    """)
    m = {row[0]: row[1] for row in cur.fetchall()}
    cur.close(); conn.close()
    log.info("Frame→segment map: %d entries", len(m))
    return m


def load_segment_endpoints() -> list:
    """
    Load (end_lat, end_lon, segment_id) for all segments.
    Used to find the nearest segment for frames without a prior violation record.
    9,425 entries — Python linear scan takes ~10 µs per lookup.
    """
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT segment_id, end_lat, end_lon FROM segments WHERE end_lat IS NOT NULL")
    pts = [(float(r[1]), float(r[2]), r[0]) for r in cur.fetchall()]
    cur.close(); conn.close()
    log.info("Segment endpoints loaded: %d", len(pts))
    return pts


def nearest_segment(lat: float, lon: float, seg_pts: list) -> str | None:
    """Return segment_id of the geographically closest segment endpoint."""
    best_d, best_id = float("inf"), None
    for slat, slon, sid in seg_pts:
        d = (slat - lat) ** 2 + (slon - lon) ** 2
        if d < best_d:
            best_d, best_id = d, sid
    return best_id


# ── Checkpoint ─────────────────────────────────────────────────────────────────

def load_checkpoint() -> set:
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE) as f:
        ids = {line.strip() for line in f if line.strip()}
    log.info("Checkpoint: %d frames already done — will skip.", len(ids))
    return ids


_checkpoint_lock = threading.Lock()


def save_checkpoint(frame_id: str):
    with _checkpoint_lock:
        with open(CHECKPOINT_FILE, "a") as f:
            f.write(frame_id + "\n")


# ── Model call ─────────────────────────────────────────────────────────────────

def call_model(image_path: str, image_metadata: dict) -> dict:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        json.dump(image_metadata, tmp)
        meta_path = tmp.name
    try:
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
            return resp.json()
    finally:
        if os.path.exists(meta_path):
            os.remove(meta_path)


# ── Annotation ─────────────────────────────────────────────────────────────────

def draw_annotations(image_path: str, results: list, frame_id: str) -> str:
    img  = Image.open(image_path).convert("RGB")
    draw = ImageDraw.Draw(img, "RGBA")
    for det in results:
        label   = det.get("label", "unknown")
        conf    = det.get("confidence", 0)
        bbox    = det.get("bounding_box", {})
        polygon = det.get("polygon_points", [])
        color   = LABEL_COLORS.get(label.lower(), LABEL_COLORS["default"])
        if polygon and len(polygon) >= 3:
            pts = [tuple(p) for p in polygon]
            draw.polygon(pts, fill=color + (70,), outline=color + (255,))
        if bbox:
            x0, y0 = bbox.get("xmin", 0), bbox.get("ymin", 0)
            x1, y1 = bbox.get("xmax", 0), bbox.get("ymax", 0)
            draw.rectangle([x0, y0, x1, y1], outline=color, width=3)
            text = f"{label} {conf:.0%}"
            tx, ty = x0, max(y0 - 20, 0)
            draw.rectangle([tx, ty, tx + len(text) * 7 + 8, ty + 18], fill=color)
            draw.text((tx + 4, ty + 2), text, fill=(0, 0, 0))
    out_path = os.path.join(ANNOTATED_DIR, f"{frame_id}.jpg")
    img.save(out_path, "JPEG", quality=90)

    if ANN_S3_ENABLED:
        key = upload_annotated(out_path, frame_id)
        if key:
            if not ANN_KEEP_LOCAL:
                try:
                    os.remove(out_path)
                except OSError:
                    pass
            return key
        # Upload failed — keep the local copy so the app's disk fallback still
        # has something, and store the docker-internal path instead.
        log.warning("S3 upload failed for %s — keeping local copy", frame_id)

    return f"{DOCKER_ANNOTATED_PREFIX}/{frame_id}.jpg"


# ── Per-frame worker ───────────────────────────────────────────────────────────

_counter_lock = threading.Lock()
_done   = 0
_errors = 0
_total  = 0


def _tick(error=False):
    global _done, _errors
    with _counter_lock:
        if error:
            _errors += 1
        _done += 1
        n = _done
    if n % 500 == 0:
        log.info("Progress: %d / %d  errors=%d", n, _total, _errors)


def process_frame(frame: dict, frame_seg_map: dict, seg_pts: list,
                  resume_set: set, do_checkpoint: bool):
    frame_id = frame["frame_id"]

    if frame_id in resume_set:
        _tick()
        return

    host_path = os.path.join(IMAGE_DIR, f"{frame_id}.jpg")
    if not os.path.exists(host_path):
        log.debug("Image missing: %s — skip", frame_id)
        _tick(error=True)
        return

    lat = frame.get("latitude")
    lon = frame.get("longitude")

    try:
        # Parse stored EXIF metadata
        meta_raw = frame.get("image_metadata")
        if isinstance(meta_raw, (bytes, bytearray)):
            meta_raw = meta_raw.decode()
        image_metadata = json.loads(meta_raw) if isinstance(meta_raw, str) else {}

        # ── Call model ───────────────────────────────────────────────────────
        model_resp   = call_model(host_path, image_metadata)
        results      = model_resp.get("results",       [])
        image_width  = model_resp.get("image_width",   0)
        image_height = model_resp.get("image_height",  0)
        gsd          = model_resp.get("gsd_mm_per_px") or None

        now  = datetime.utcnow().isoformat()
        conn = get_conn()
        cur  = conn.cursor()

        # ── Delete old violations for this frame ─────────────────────────────
        cur.execute("DELETE FROM violations WHERE frame_id = %s", (frame_id,))

        annotated_path = None

        if results:
            # Resolve segment_id: use prior map first, fall back to nearest endpoint
            seg_id = frame_seg_map.get(frame_id)
            if seg_id is None and lat and lon:
                seg_id = nearest_segment(float(lat), float(lon), seg_pts)

            # Annotate image
            try:
                # Returns the bucket key when the upload succeeded, otherwise
                # the docker-internal path — either way it is what the DB stores.
                annotated_path = draw_annotations(host_path, results, frame_id)
            except Exception as ann_err:
                log.warning("Annotation failed frame_id=%s: %s", frame_id, ann_err)

            # Insert new violations (mm + px)
            for r in results:
                bbox = r.get("bounding_box", {})
                dims = r.get("dimensions",   {})
                area = r.get("area",         {})
                cur.execute(
                    """INSERT INTO violations
                       (frame_id, segment_id, label, confidence, severity,
                        bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
                        polygon_points,
                        length_mm,   breadth_mm,
                        bbox_area_mm2,  polygon_area_mm2,
                        length_px,   breadth_px,
                        bbox_area_px,   polygon_area_px,
                        gsd_mm_per_px, image_width, image_height,
                        latitude, longitude, annotated_image_path, created_at)
                       VALUES
                       (%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,
                        %s,%s, %s,%s,
                        %s,%s, %s,%s,
                        %s,%s,%s, %s,%s,%s,%s)""",
                    (frame_id, seg_id,
                     r.get("label"), r.get("confidence"), r.get("severity", "low"),
                     bbox.get("xmin"), bbox.get("ymin"),
                     bbox.get("xmax"), bbox.get("ymax"),
                     json.dumps(r.get("polygon_points", [])),
                     dims.get("length_mm"),  dims.get("breadth_mm"),
                     area.get("bbox_area_mm2"),  area.get("polygon_area_mm2"),
                     dims.get("length_px"),  dims.get("breadth_px"),
                     area.get("bbox_area_px"),   area.get("polygon_area_px"),
                     gsd, image_width, image_height,
                     lat, lon, annotated_path, now),
                )

        # ── Compute ASTM PCI for this frame ──────────────────────────────────
        viol_dicts = [
            {"label":           r.get("label"),
             "severity":        r.get("severity", "low"),
             "polygon_area_mm2": (r.get("area") or {}).get("polygon_area_mm2") or
                                  (r.get("area") or {}).get("polygon_area_px") or 0}
            for r in results
        ]
        frame_score  = compute_frame_pci(viol_dicts, image_width, image_height, gsd)
        frame_rating = pci_rating(frame_score)

        cur.execute(
            """UPDATE frames
               SET pci_score=%s, pci_rating=%s, updated_at=%s
               WHERE frame_id=%s""",
            (frame_score, frame_rating, now, frame_id),
        )
        conn.commit()
        cur.close(); conn.close()

        if do_checkpoint:
            save_checkpoint(frame_id)

    except Exception as exc:
        log.warning("frame_id=%s failed: %s", frame_id, exc)
        _tick(error=True)
        return

    _tick()


# ── Segment PCI recompute ──────────────────────────────────────────────────────

def recompute_all_segment_pci():
    log.info("Recomputing ASTM PCI for all segments…")
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)

    cur.execute("SELECT segment_id FROM segments")
    seg_ids = [r["segment_id"] for r in cur.fetchall()]

    updated = 0
    for sid in seg_ids:
        cur.execute(
            """SELECT label, severity,
                      SUM(polygon_area_mm2) AS total_area_mm2
               FROM   violations
               WHERE  segment_id = %s
               GROUP  BY label, severity""",
            (sid,),
        )
        distress_rows = cur.fetchall()

        cur.execute(
            """SELECT MAX(image_width)   AS image_width,
                      MAX(image_height)  AS image_height,
                      MAX(gsd_mm_per_px) AS gsd_mm_per_px
               FROM   violations
               WHERE  segment_id = %s
               GROUP  BY frame_id""",
            (sid,),
        )
        frame_rows = cur.fetchall()

        cur.execute(
            "SELECT COUNT(*) AS cnt FROM violations WHERE segment_id = %s", (sid,)
        )
        vcount = (cur.fetchone() or {}).get("cnt", 0)

        score  = compute_segment_pci_from_aggregates(distress_rows, frame_rows)
        rating = pci_rating(score)

        cur.execute(
            """UPDATE segments
               SET pci_score=%s, pci_rating=%s, violation_count=%s
               WHERE segment_id=%s""",
            (score, rating, vcount, sid),
        )
        updated += 1
        if updated % 1000 == 0:
            conn.commit()
            log.info("  Segment PCI: %d / %d done", updated, len(seg_ids))

    conn.commit()
    log.info("Segment PCI recompute done: %d segments updated.", updated)
    cur.close(); conn.close()


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    global _total

    parser = argparse.ArgumentParser(
        description="Re-run all frames through the PCI model with updated px fields."
    )
    parser.add_argument("--workers",    type=int,  default=16,
                        help="Parallel model-call threads (default 16)")
    parser.add_argument("--limit",      type=int,  default=0,
                        help="Process only first N frames — 0 means all")
    parser.add_argument("--resume",     action="store_true",
                        help="Skip frames already listed in reprocess_checkpoint.txt")
    parser.add_argument("--dry-run",    action="store_true",
                        help="Run schema migration only — no model calls")
    args = parser.parse_args()

    log.info("=== PCI Full Reprocess ===")
    log.info("Workers: %d  Model: %s", args.workers, MODEL_URL)
    log.info("Resume: %s  Dry-run: %s  Limit: %s",
             args.resume, args.dry_run, args.limit or "all")

    # 1. Schema migration (always runs, idempotent)
    ensure_px_columns()

    if args.dry_run:
        log.info("Dry-run — exiting after schema migration.")
        return

    # 2. Pre-load lookup structures
    frame_seg_map = load_frame_segment_map()
    seg_pts       = load_segment_endpoints()

    # 3. Checkpoint (for --resume)
    resume_set    = load_checkpoint() if args.resume else set()
    do_checkpoint = True   # always write checkpoint so you can --resume after a crash

    # 4. Fetch frames
    log.info("Loading frames from DB…")
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    limit_clause = f"LIMIT {args.limit}" if args.limit else ""
    cur.execute(f"""
        SELECT frame_id, local_image_path, image_metadata, latitude, longitude
        FROM   frames
        WHERE  local_image_path IS NOT NULL
        ORDER  BY frame_id
        {limit_clause}
    """)
    frames = cur.fetchall()
    cur.close(); conn.close()

    _total = len(frames)
    log.info("Frames to process: %d  (already checkpointed: %d → effective: %d)",
             _total, len(resume_set), _total - len(resume_set & {f["frame_id"] for f in frames}))

    # 5. Process in parallel
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = [
            pool.submit(process_frame, f, frame_seg_map, seg_pts, resume_set, do_checkpoint)
            for f in frames
        ]
        for _ in as_completed(futs):
            pass

    elapsed = time.time() - t0
    rate    = (_total - len(resume_set)) / elapsed if elapsed > 0 else 0
    log.info("Model pass done in %.0fs  (%.1f frames/sec)  errors=%d / %d",
             elapsed, rate, _errors, _total)

    # 6. Recompute segment PCI
    recompute_all_segment_pci()

    log.info("")
    log.info("=== Reprocess complete ===")
    log.info("Next step: python3 calculate_health_score.py scores")
    log.info("           (recomputes health scores from new violation data)")


if __name__ == "__main__":
    main()
