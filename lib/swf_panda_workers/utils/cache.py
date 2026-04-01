#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""
SQLite-backed TTL cache that survives process restarts.

Each PersistentTTLCache instance maps to one table in a shared SQLite database
file.  Entries are expired by wall-clock time (not by LRU/maxsize).  Expired
entries are purged lazily on every write.

Interface is a subset of cachetools.TTLCache:
    cache[key] = value
    value = cache[key]          # raises KeyError if missing / expired
    value = cache.get(key)      # returns None (or default) if missing / expired
"""

import json
import logging
import os
import sqlite3
import threading
import time

_logger = logging.getLogger(__name__)


class PersistentTTLCache:
    """
    Thread-safe, SQLite-backed key/value cache with per-entry TTL.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database file.  The parent directory is created
        automatically if it does not exist.
    table : str
        Table name within the database (one cache = one table).
    ttl : int | float
        Time-to-live in seconds.  Entries older than this are invisible and
        are purged on the next write.
    """

    def __init__(self, db_path: str, table: str, ttl: float):
        self._db_path = os.path.expanduser(db_path)
        self._table = table
        self._ttl = ttl
        self._lock = threading.Lock()

        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        self._init_db()
        _logger.info(
            "PersistentTTLCache '%s' opened at %s (ttl=%ss)",
            table, self._db_path, ttl,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def __setitem__(self, key: str, value):
        expires_at = time.time() + self._ttl
        serialized = json.dumps(value, default=str)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    f"INSERT OR REPLACE INTO {self._table} (key, value, expires_at) "
                    f"VALUES (?, ?, ?)",
                    (str(key), serialized, expires_at),
                )
                conn.execute(
                    f"DELETE FROM {self._table} WHERE expires_at <= ?",
                    (time.time(),),
                )
                conn.commit()
            finally:
                conn.close()

    def __getitem__(self, key: str):
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    f"SELECT value FROM {self._table} "
                    f"WHERE key = ? AND expires_at > ?",
                    (str(key), time.time()),
                ).fetchone()
            finally:
                conn.close()
        if row is None:
            raise KeyError(key)
        return json.loads(row[0])

    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: str):
        try:
            self[key]
            return True
        except KeyError:
            return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {self._table} ("
                    f"  key        TEXT PRIMARY KEY,"
                    f"  value      TEXT NOT NULL,"
                    f"  expires_at REAL NOT NULL"
                    f")"
                )
                conn.commit()
            finally:
                conn.close()
