from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from .private_fs import ensure_private_dir, ensure_private_file


class DedupeStore:
    def __init__(self, path: Path | str = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            db_path = Path(self.path)
            ensure_private_dir(db_path.parent)
            ensure_private_file(db_path)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._closed = False
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        columns = {
            str(row[1]) for row in self._db.execute("PRAGMA table_info(dedupe_events)").fetchall()
        }
        if columns and "route_key" not in columns:
            self._db.execute("ALTER TABLE dedupe_events RENAME TO dedupe_events_legacy_v1")
            columns = set()
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS dedupe_events ("
            "route_key TEXT NOT NULL, "
            "source_uuid TEXT NOT NULL, "
            "timestamp INTEGER NOT NULL, "
            "status TEXT NOT NULL, "
            "PRIMARY KEY (route_key, source_uuid, timestamp))"
        )
        if not columns and self._legacy_table_exists():
            self._db.execute(
                "INSERT OR IGNORE INTO dedupe_events "
                "(route_key, source_uuid, timestamp, status) "
                "SELECT '', source_uuid, timestamp, 'handled' FROM dedupe_events_legacy_v1"
            )
        self._db.commit()

    def _legacy_table_exists(self) -> bool:
        cursor = self._db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'dedupe_events_legacy_v1'"
        )
        return cursor.fetchone() is not None

    def claim(self, route_key: str, source_uuid: str, timestamp: int) -> bool:
        with self._lock:
            try:
                self._db.execute(
                    "INSERT INTO dedupe_events (route_key, source_uuid, timestamp, status) "
                    "VALUES (?, ?, ?, 'processing')",
                    (route_key, source_uuid, int(timestamp)),
                )
                self._db.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def is_handled(self, route_key: str, source_uuid: str, timestamp: int) -> bool:
        with self._lock:
            cursor = self._db.execute(
                "SELECT 1 FROM dedupe_events "
                "WHERE route_key = ? AND source_uuid = ? AND timestamp = ? AND status = 'handled'",
                (route_key, source_uuid, int(timestamp)),
            )
            return cursor.fetchone() is not None

    def status(self, route_key: str, source_uuid: str, timestamp: int) -> str | None:
        with self._lock:
            cursor = self._db.execute(
                "SELECT status FROM dedupe_events "
                "WHERE route_key = ? AND source_uuid = ? AND timestamp = ?",
                (route_key, source_uuid, int(timestamp)),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return str(row[0])

    def mark_handled(self, route_key: str, source_uuid: str, timestamp: int) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO dedupe_events (route_key, source_uuid, timestamp, status) "
                "VALUES (?, ?, ?, 'handled') "
                "ON CONFLICT(route_key, source_uuid, timestamp) DO UPDATE SET status = 'handled'",
                (route_key, source_uuid, int(timestamp)),
            )
            self._db.commit()

    def release(self, route_key: str, source_uuid: str, timestamp: int) -> None:
        with self._lock:
            self._db.execute(
                "DELETE FROM dedupe_events "
                "WHERE route_key = ? AND source_uuid = ? AND timestamp = ? AND status = 'processing'",
                (route_key, source_uuid, int(timestamp)),
            )
            self._db.commit()

    def seen_or_record(self, source_uuid: str, timestamp: int, route_key: str = "") -> bool:
        if not self.claim(route_key, source_uuid, timestamp):
            return True
        self.mark_handled(route_key, source_uuid, timestamp)
        return False

    def close(self) -> None:
        if not self._closed:
            self._db.close()
            self._closed = True

    def __enter__(self) -> "DedupeStore":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
