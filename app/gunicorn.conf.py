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