"""
gunicorn.conf.py
-----------------
Gunicorn configuration for production.
The on_starting hook calls init_db() so tables are ready
before the first worker accepts a request.
"""

import logging
from db_connection import init_db

# ─── Server config ────────────────────────────────────────────────────────────
bind         = "0.0.0.0:5000"
worker_class = "gthread"   # thread-per-request — ideal for I/O-bound (OCI download)
workers      = 4           # parallel processes
threads      = 24          # threads per worker → 4×24 = 96 concurrent slots
timeout      = 120         # seconds — OCI download can be slow
keepalive    = 5
loglevel     = "info"
accesslog    = "-"         # stdout
errorlog     = "-"         # stdout

# ─── Startup hook ─────────────────────────────────────────────────────────────
def on_starting(server):
    """Runs once before workers are forked — safe place to init DB."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    )
    init_db()
    logging.getLogger(__name__).info("DB ready — gunicorn starting workers")


def post_fork(server, worker):
    """Runs in each worker right after fork.
    1. Stagger startup so 4 workers don't hammer DB simultaneously.
    2. Warm the cache so the first browser request is always instant.
    3. Start a daemon thread that refreshes the cache every 50 s so it
       never expires cold (TTL is 60 s — refresh at 50 s avoids thundering herd).
    """
    import time as _time
    log = logging.getLogger(__name__)
    _time.sleep(worker.age * 0.5)
    for attempt in range(1, 3):
        try:
            from main import warmup_cache, start_cache_refresher
            warmup_cache()
            start_cache_refresher()
            return
        except Exception as e:
            log.warning("Cache warmup attempt %d failed: %s", attempt, e)
            if attempt < 2:
                _time.sleep(5)