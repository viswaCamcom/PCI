"""
pci_api_upload.py
=================
Reads citylens_ids from a CSV, fetches the required fields from
baladylens_prod.upload_data for those specific IDs, and uploads to PCI API.

CSV format (one column, with or without header):
    citylens_id
    abc123
    def456
    ...

Usage:
    python3 pci_api_upload.py --csv riyadh_ids.csv
    python3 pci_api_upload.py --csv riyadh_ids.csv --limit 100
    python3 pci_api_upload.py --csv riyadh_ids.csv --workers 5
    python3 pci_api_upload.py --csv riyadh_ids.csv --resume
    python3 pci_api_upload.py --csv riyadh_ids.csv --dry-run
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import mysql.connector
import requests

# ── Source DB (baladylens_prod) ───────────────────────────────────────────────
SRC_DB = dict(
    host     = "10.20.1.201",
    port     = 5506,
    user     = "momrah-admin",
    password = "MoMrah!6!6@Gifto0709$",
    database = "baladylens_prod",
    charset  = "utf8mb4",
    connection_timeout = 15,
)

BATCH_SIZE   = 1000  # IDs per IN-query
DEFAULT_CSV  = os.path.join(os.path.dirname(__file__), "riyadh_violation_10_days.csv")
DONE_FILE    = os.path.join(os.path.dirname(__file__), "pci_uploaded_done.txt")

# ── PCI API ───────────────────────────────────────────────────────────────────
API_BASE    = "http://localhost:5000"
TIMEOUT_SEC = 60
RETRY_COUNT = 2
RETRY_DELAY = 3
DEFAULT_WORKERS = 80

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_FILE = "pci_upload_riyadh.log"
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(message)s",
    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ],
)
logger = logging.getLogger(__name__)


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_src_conn():
    return mysql.connector.connect(**SRC_DB)


def read_ids_from_csv(csv_path: str) -> list[str]:
    """Read citylens_ids from a CSV. Accepts with or without a header row."""
    ids = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            val = row[0].strip()
            if val.lower() in ("citylens_id", "id", ""):
                continue          # skip header
            ids.append(val)
    logger.info("CSV loaded: %d citylens_ids from %s", len(ids), csv_path)
    return ids


def fetch_frames_by_ids(ids: list[str]) -> list[dict]:
    """
    Fetch upload_data rows for the given citylens_ids in batches.
    Returns row dicts ready for the PCI /upload endpoint.
    """
    if not ids:
        return []

    logger.info("Connecting to baladylens_prod @ %s:%d …", SRC_DB["host"], SRC_DB["port"])
    conn = get_src_conn()
    cur  = conn.cursor(dictionary=True)

    rows = []
    total_batches = (len(ids) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i : i + BATCH_SIZE]
        placeholders = ",".join(["%s"] * len(batch))
        cur.execute(
            f"""
            SELECT
                citylens_id,
                latitude,
                longitude,
                image_key,
                image_bucket,
                municipality_id,
                submunicipality_id,
                submitted_date AS datetime_utc
            FROM upload_data
            WHERE citylens_id IN ({placeholders})
            """,
            batch,
        )
        batch_rows = cur.fetchall()
        rows.extend(batch_rows)
        logger.info("  Batch %d/%d — fetched %d rows (running total: %d)",
                    i // BATCH_SIZE + 1, total_batches, len(batch_rows), len(rows))

    cur.close()
    conn.close()

    not_found = len(ids) - len(rows)
    if not_found:
        logger.warning("%d citylens_ids from CSV not found in upload_data", not_found)
    logger.info("Total fetched from baladylens_prod: %d rows", len(rows))
    return rows


# ── Done-file helpers (fast local skip — no API call needed) ──────────────────

def load_done_ids() -> set:
    """Load citylens_ids already successfully uploaded from local done-file."""
    if not os.path.exists(DONE_FILE):
        return set()
    with open(DONE_FILE, "r", encoding="utf-8") as f:
        ids = {line.strip() for line in f if line.strip()}
    logger.info("Resume: %d frames already uploaded (from %s)", len(ids), DONE_FILE)
    return ids


_done_lock = __import__("threading").Lock()

def mark_done(cid: str):
    """Append a successfully uploaded citylens_id to the done-file (thread-safe)."""
    with _done_lock:
        with open(DONE_FILE, "a", encoding="utf-8") as f:
            f.write(cid + "\n")


def upload_frame(row: dict) -> dict:
    """POST one frame to PCI /upload with retry. Returns result dict."""
    cid = str(row["citylens_id"])

    payload = {
        "citylens_id":        cid,
        "latitude":           str(row["latitude"]),
        "longitude":          str(row["longitude"]),
        "image_key":          row["image_key"],
        "image_bucket":       row["image_bucket"],
        "municipality_id":    row.get("municipality_id",    ""),
        "submunicipality_id": row.get("submunicipality_id", "") or "",
        "datetime_utc":       str(row["datetime_utc"]),
    }

    for attempt in range(1, RETRY_COUNT + 2):
        try:
            resp = requests.post(f"{API_BASE}/upload", data=payload, timeout=TIMEOUT_SEC)

            if resp.status_code in (200, 207):
                mark_done(cid)
                return {"citylens_id": cid, "status": "ok",
                        "http_code": resp.status_code,
                        "message": resp.json().get("status", "")}

            if resp.status_code == 409:
                mark_done(cid)
                return {"citylens_id": cid, "status": "skipped",
                        "http_code": 409, "message": "already exists"}

            if resp.status_code >= 500 and attempt <= RETRY_COUNT:
                logger.warning("  [%s] HTTP %d — retry %d/%d",
                               cid, resp.status_code, attempt, RETRY_COUNT)
                time.sleep(RETRY_DELAY * attempt)
                continue

            try:
                msg = resp.json().get("message", resp.text[:120])
            except Exception:
                msg = resp.text[:120]
            return {"citylens_id": cid, "status": "failed",
                    "http_code": resp.status_code, "message": msg}

        except requests.exceptions.Timeout:
            if attempt <= RETRY_COUNT:
                logger.warning("  [%s] Timeout — retry %d/%d", cid, attempt, RETRY_COUNT)
                time.sleep(RETRY_DELAY * attempt)
            else:
                return {"citylens_id": cid, "status": "failed",
                        "http_code": 0, "message": "Timeout"}

        except requests.exceptions.ConnectionError as e:
            return {"citylens_id": cid, "status": "failed",
                    "http_code": 0, "message": f"Connection error: {e}"}

    return {"citylens_id": cid, "status": "failed",
            "http_code": 0, "message": "Exceeded retries"}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Upload frames to PCI API from a citylens_ids CSV")
    parser.add_argument("--csv",     default=DEFAULT_CSV,
                        help=f"Path to CSV with citylens_ids (default: {DEFAULT_CSV})")
    parser.add_argument("--limit",   type=int, default=None,
                        help="Max frames to upload")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Concurrent workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--resume",  action="store_true",
                        help=f"Skip frames already uploaded (reads {DONE_FILE})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch from DB and print rows, no upload")
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        logger.error("CSV not found: %s", args.csv)
        return

    # ── Read IDs from CSV ─────────────────────────────────────────────────────
    ids = read_ids_from_csv(args.csv)
    if not ids:
        logger.warning("No citylens_ids found in CSV — nothing to do.")
        return

    # ── Fetch matching rows from upload_data ──────────────────────────────────
    rows = fetch_frames_by_ids(ids)
    if not rows:
        logger.warning("No rows found in upload_data — nothing to upload.")
        return

    # ── Dry-run: just show what would be uploaded ─────────────────────────────
    if args.dry_run:
        logger.info("─── DRY RUN — first 5 rows ──────────────────────────────")
        for r in rows[:5]:
            logger.info("  %s", r)
        logger.info("Total: %d rows", len(rows))
        return

    # ── Resume: skip already-uploaded (fast local file, no API call) ─────────
    done = load_done_ids()
    if done:
        before = len(rows)
        rows = [r for r in rows if str(r["citylens_id"]) not in done]
        logger.info("Skipped %d already-uploaded frames — %d remaining",
                    before - len(rows), len(rows))

    # ── Apply limit ───────────────────────────────────────────────────────────
    if args.limit:
        rows = rows[:args.limit]
        logger.info("Limit applied: uploading %d frames", len(rows))

    total   = len(rows)
    ok      = 0
    skipped = 0
    failed  = 0
    start   = time.time()

    logger.info("Starting upload — %d frames, %d workers", total, args.workers)
    logger.info("─" * 60)

    # ── Concurrent upload ─────────────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(upload_frame, row): row for row in rows}

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            cid    = result["citylens_id"]

            if result["status"] == "ok":
                ok += 1
                logger.info("  [%d/%d] ✓  %s  (HTTP %d)",
                            i, total, cid, result["http_code"])
            elif result["status"] == "skipped":
                skipped += 1
                logger.info("  [%d/%d] ⟳  %s  already exists (409)",
                            i, total, cid)
            else:
                failed += 1
                logger.error("  [%d/%d] ✗  %s  (HTTP %d)  %s",
                             i, total, cid, result["http_code"], result["message"])

            if i % 10 == 0 or i == total:
                elapsed = time.time() - start
                rate    = i / elapsed if elapsed > 0 else 0
                eta     = int((total - i) / rate) if rate > 0 else 0
                logger.info("  Progress: %d/%d  |  ✓ %d  ⟳ %d  ✗ %d  |  %.1f f/s  |  ETA %ds",
                            i, total, ok, skipped, failed, rate, eta)

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = time.time() - start
    logger.info("─" * 60)
    logger.info("Done in %.1fs", elapsed)
    logger.info("  Uploaded : %d / %d", ok,      total)
    logger.info("  Skipped  : %d / %d  (already exists)", skipped, total)
    logger.info("  Failed   : %d / %d", failed,  total)
    logger.info("  Log file : %s", LOG_FILE)


if __name__ == "__main__":
    main()
