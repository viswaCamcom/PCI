"""
upload_016001.py
================
Upload frames from a full-data CSV (all fields present — no DB lookup needed).

CSV columns:
    citylens_id, latitude, longitude, image_key, image_bucket,
    datetime_utc, municipality_id, submunicipality_id

Usage:
    python3 upload_004001.py
    python3 upload_004001.py --csv 004001_municipality_frames.csv
    python3 upload_004001.py --rate 30          # slow down to 30 req/s
    python3 upload_004001.py --limit 1000       # test with first 1000 rows
    python3 upload_004001.py --resume           # skip already-uploaded frames
    python3 upload_004001.py --dry-run          # print first 5 rows, no upload
    python3 upload_004001.py --api http://10.0.1.10:5000
"""

import argparse
import csv
import logging
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

# ── Defaults ──────────────────────────────────────────────────────────────────
_HERE           = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV     = os.path.join(_HERE, "/Users/apple/Desktop/pci_backend/Riyadh_july_2026.csv")
DEFAULT_API     = "http://localhost:5000"
DEFAULT_RATE    = 60        # target requests / second
DEFAULT_WORKERS = 120       # thread pool — needs to be ≥ rate × avg_latency(s)
TIMEOUT_SEC     = 60
RETRY_COUNT     = 2
RETRY_DELAY     = 3
DONE_FILE       = os.path.join(_HERE, "004001_uploaded_done.txt")
LOG_FILE        = os.path.join(_HERE, "004001_upload.log")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s  %(levelname)-8s  %(message)s",
    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ── Token-bucket rate limiter ─────────────────────────────────────────────────

class _TokenBucket:
    """Thread-safe token bucket.  acquire() blocks until one token is available."""

    def __init__(self, rate: float):
        self._rate    = rate
        self._tokens  = float(rate)   # start full so the first burst isn't delayed
        self._updated = time.monotonic()
        self._lock    = threading.Lock()

    def acquire(self):
        with self._lock:
            now           = time.monotonic()
            self._tokens  = min(self._rate,
                                self._tokens + (now - self._updated) * self._rate)
            self._updated = now
            if self._tokens < 1.0:
                wait          = (1.0 - self._tokens) / self._rate
                time.sleep(wait)
                self._tokens  = 0.0
                self._updated = time.monotonic()
            else:
                self._tokens -= 1.0


# ── CSV reading ───────────────────────────────────────────────────────────────

def read_csv(csv_path: str) -> list[dict]:
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    logger.info("CSV loaded: %d frames from %s", len(rows), csv_path)
    return rows


# ── Done-file helpers (persist across runs for --resume) ──────────────────────

def load_done_ids() -> set:
    if not os.path.exists(DONE_FILE):
        return set()
    with open(DONE_FILE, encoding="utf-8") as f:
        ids = {l.strip() for l in f if l.strip()}
    logger.info("Resume: %d frames already uploaded (%s)", len(ids), DONE_FILE)
    return ids


_done_lock = threading.Lock()

def _mark_done(cid: str):
    with _done_lock:
        with open(DONE_FILE, "a", encoding="utf-8") as f:
            f.write(cid + "\n")


# ── Single-frame upload (runs in thread pool) ─────────────────────────────────

def _upload_frame(row: dict, api_base: str) -> dict:
    cid = str(row["citylens_id"])
    payload = {
        "citylens_id":        cid,
        "latitude":           row["latitude"],
        "longitude":          row["longitude"],
        "image_key":          row["image_key"],
        "image_bucket":       row["image_bucket"],
        "municipality_id":    row.get("municipality_id",    ""),
        "submunicipality_id": row.get("submunicipality_id", ""),
        "datetime_utc":       row["datetime_utc"],
    }

    for attempt in range(1, RETRY_COUNT + 2):
        try:
            resp = requests.post(
                f"{api_base}/upload", data=payload, timeout=TIMEOUT_SEC
            )

            if resp.status_code in (200, 207):
                _mark_done(cid)
                return {"cid": cid, "status": "ok", "code": resp.status_code}

            if resp.status_code == 409:
                _mark_done(cid)
                return {"cid": cid, "status": "skip", "code": 409}

            if resp.status_code >= 500 and attempt <= RETRY_COUNT:
                time.sleep(RETRY_DELAY * attempt)
                continue

            try:
                msg = resp.json().get("message", resp.text[:120])
            except Exception:
                msg = resp.text[:120]
            return {"cid": cid, "status": "fail", "code": resp.status_code, "msg": msg}

        except requests.exceptions.Timeout:
            if attempt <= RETRY_COUNT:
                time.sleep(RETRY_DELAY * attempt)
            else:
                return {"cid": cid, "status": "fail", "code": 0, "msg": "Timeout"}

        except requests.exceptions.ConnectionError as e:
            if attempt <= RETRY_COUNT:
                time.sleep(RETRY_DELAY * attempt)
                continue
            return {"cid": cid, "status": "fail", "code": 0, "msg": f"ConnError: {e}"}

    return {"cid": cid, "status": "fail", "code": 0, "msg": "Exceeded retries"}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Upload 004001 municipality frames to PCI API at a fixed rate"
    )
    parser.add_argument("--csv",     default=DEFAULT_CSV,       help="Path to full-data CSV")
    parser.add_argument("--api",     default=DEFAULT_API,       help="PCI API base URL")
    parser.add_argument("--rate",    type=float, default=DEFAULT_RATE,
                        help=f"Target requests/sec (default: {DEFAULT_RATE})")
    parser.add_argument("--workers", type=int,   default=DEFAULT_WORKERS,
                        help=f"Thread pool size (default: {DEFAULT_WORKERS})")
    parser.add_argument("--limit",   type=int,   default=None,  help="Max frames to upload")
    parser.add_argument("--resume",  action="store_true",       help="Skip already-uploaded frames")
    parser.add_argument("--dry-run", action="store_true",       help="Print first 5 rows, no upload")
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        logger.error("CSV not found: %s", args.csv)
        return

    rows = read_csv(args.csv)
    if not rows:
        logger.warning("No rows found in CSV.")
        return

    if args.dry_run:
        logger.info("─── DRY RUN (first 5 rows) ───────────────────────────────")
        for r in rows[:5]:
            logger.info("  %s", r)
        logger.info("Total: %d rows", len(rows))
        return

    if args.resume:
        done   = load_done_ids()
        before = len(rows)
        rows   = [r for r in rows if str(r["citylens_id"]) not in done]
        logger.info("Skipped %d already-uploaded — %d remaining",
                    before - len(rows), len(rows))

    if args.limit:
        rows = rows[: args.limit]
        logger.info("Limit applied: %d frames", len(rows))

    total   = len(rows)
    ok      = 0
    skipped = 0
    failed  = 0
    start   = time.time()
    limiter = _TokenBucket(args.rate)

    # Estimated time at the target rate
    eta_min = total / args.rate / 60
    logger.info("═" * 60)
    logger.info("Municipality : 016001")
    logger.info("Frames       : %d", total)
    logger.info("Target rate  : %.0f req/s", args.rate)
    logger.info("Workers      : %d", args.workers)
    logger.info("API          : %s", args.api)
    logger.info("ETA          : ~%.0f min at %.0f req/s", eta_min, args.rate)
    logger.info("Done file    : %s", DONE_FILE)
    logger.info("Log file     : %s", LOG_FILE)
    logger.info("═" * 60)

    # result_q receives one item per completed frame (from callback thread)
    result_q: queue.SimpleQueue = queue.SimpleQueue()

    def _on_done(fut):
        """Callback fired by ThreadPoolExecutor when a frame completes."""
        try:
            result_q.put(fut.result())
        except Exception as e:
            result_q.put({"cid": "?", "status": "fail", "code": 0, "msg": str(e)})

    def _submit_all(pool):
        """Background thread: dispatch frames at the token-bucket rate."""
        for row in rows:
            limiter.acquire()                               # blocks to hit target rate
            pool.submit(_upload_frame, row, args.api).add_done_callback(_on_done)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        submit_thread = threading.Thread(target=_submit_all, args=(pool,), daemon=True)
        submit_thread.start()

        # Collect results as they complete (in any order)
        for i in range(1, total + 1):
            res = result_q.get()

            if res["status"] == "ok":
                ok += 1
            elif res["status"] == "skip":
                skipped += 1
            else:
                failed += 1
                logger.error("  FAIL  %s  HTTP %s  %s",
                             res["cid"], res.get("code", "?"), res.get("msg", ""))

            if i % 100 == 0 or i == total:
                elapsed  = time.time() - start
                rate_act = i / elapsed if elapsed > 0 else 0
                remaining = (total - i) / rate_act if rate_act > 0 else 0
                logger.info(
                    "  [%d/%d]  ok=%-6d skip=%-5d fail=%-5d  "
                    "%.1f req/s  ETA %.0fs",
                    i, total, ok, skipped, failed, rate_act, remaining,
                )

        submit_thread.join()

    elapsed = time.time() - start
    avg_rate = total / elapsed if elapsed > 0 else 0

    logger.info("═" * 60)
    logger.info("Finished in %.1fs  (%.1f avg req/s)", elapsed, avg_rate)
    logger.info("  Uploaded  : %d / %d", ok,      total)
    logger.info("  Skipped   : %d / %d  (already existed)", skipped, total)
    logger.info("  Failed    : %d / %d", failed,  total)
    logger.info("  Log       : %s", LOG_FILE)


if __name__ == "__main__":
    main()
