"""
app/db_connection.py
=====================
MySQL connection pool using MySQLConnectionPool (pool_size=32).
Supports 4 gunicorn workers × 8 concurrent threads each with headroom.

Pool is created lazily on first request in each worker process — safe
for gunicorn's fork model (no shared state across workers).

Tables are created via db/init.sql mounted into the MySQL container.

Exports:
  get_conn()  — returns a pooled connection; caller must close() to return it
  init_db()   — startup connectivity check
"""

import os
import time
import logging

import mysql.connector
import mysql.connector.pooling
from mysql.connector import Error

logger = logging.getLogger(__name__)

db_config = {
    "host":               os.environ.get("MYSQL_HOST",     "mysql"),
    "port":               int(os.environ.get("MYSQL_PORT",  3306)),
    "user":               os.environ.get("MYSQL_USER",     "pci_user"),
    "password":           os.environ.get("MYSQL_PASSWORD", "pci_pass"),
    "database":           os.environ.get("MYSQL_DB",       "pci"),
    "charset":            "utf8mb4",
    "use_unicode":        True,
    "autocommit":         False,
    "connection_timeout": 10,
}

# Created lazily per worker process (gunicorn forks after module load)
_pool: "mysql.connector.pooling.MySQLConnectionPool | None" = None


def _get_pool() -> "mysql.connector.pooling.MySQLConnectionPool":
    global _pool
    if _pool is None:
        _pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="pci_pool",
            pool_size=32,
            **db_config,
        )
        logger.info("MySQL connection pool created (size=32)")
    return _pool


def get_conn(max_retries: int = 5, retry_delay: float = 0.5):
    """
    Return a connection from the pool.
    Retries on startup races (MySQL container not yet ready).
    Caller must call conn.close() to return it to the pool.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return _get_pool().get_connection()
        except Error as e:
            if attempt == max_retries:
                raise RuntimeError(
                    f"MySQL pool unavailable after {max_retries} attempts: {e}"
                )
            wait = retry_delay * attempt
            logger.warning(
                "MySQL not ready (attempt %d/%d), retrying in %.0fs: %s",
                attempt, max_retries, wait, e,
            )
            time.sleep(wait)


def init_db():
    """Called once at startup — verifies pool connectivity."""
    conn = get_conn()
    conn.close()
    logger.info("DB connection pool verified — tables managed by init.sql")
