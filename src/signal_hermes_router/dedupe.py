from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path

from .private_fs import PRIVATE_FILE_MODE, ensure_private_dir, ensure_private_file

LOGGER = logging.getLogger(__name__)

# Retention prune / space-reclamation chunk sizes. Chunks bound every
# self._lock hold so a sweep can never freeze event-loop dedupe calls behind
# one long statement. Code constants by design, not configuration.
PRUNE_CHUNK_ROWS = 1000
VACUUM_CHUNK_PAGES = 200
MIGRATION_BACKUP_SUFFIX = ".migration-backup"

_AUTO_VACUUM_INCREMENTAL = 2


class DedupeStore:
    def __init__(
        self,
        path: Path | str = ":memory:",
        *,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.path = str(path)
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        if self.path != ":memory:":
            db_path = Path(self.path)
            ensure_private_dir(db_path.parent)
            if db_path.is_file():
                # Never reopen an existing DB outside sqlite: closing any
                # descriptor for the inode drops this process's POSIX record
                # locks, including a live store's exclusive lock. chmod
                # enforces the private mode without opening the file.
                db_path.chmod(PRIVATE_FILE_MODE)
            elif not db_path.exists():
                ensure_private_file(db_path)
            # A non-regular existing path (for example a directory) is left
            # untouched; sqlite3.connect below fails loudly on it.
        self._lock = threading.Lock()
        # _state_lock guards the close/sweep handoff flags below. It is held
        # only for flag reads/writes and the connection close itself, never
        # across a statement, so DedupeStore.close() stays non-blocking with
        # respect to an in-flight retention chunk.
        self._state_lock = threading.Lock()
        self._sweep_active = False
        self._close_requested = False
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._closed = False
        try:
            # Hold the sqlite file lock for the connection's lifetime. The
            # reclaim below assumes this process owns the state DB
            # exclusively, so an overlapping router over the same file must
            # fail loudly at startup instead of erasing this process's
            # in-flight claims.
            self._db.execute("PRAGMA locking_mode=EXCLUSIVE")
            self._ensure_schema()
            self._reclaim_orphaned_claims()
        except BaseException:
            self.close()
            raise

    def _ensure_schema(self) -> None:
        columns = {
            str(row[1]) for row in self._db.execute("PRAGMA table_info(dedupe_events)").fetchall()
        }
        auto_vacuum = int(self._db.execute("PRAGMA auto_vacuum").fetchone()[0])
        if not columns and auto_vacuum != _AUTO_VACUUM_INCREMENTAL:
            # Fresh store: enable incremental auto-vacuum before the first
            # table is written, which needs no VACUUM to take effect.
            self._db.execute("PRAGMA auto_vacuum=INCREMENTAL")
        needs_migration = bool(columns) and (
            "route_key" not in columns
            or "updated_at_ms" not in columns
            or auto_vacuum != _AUTO_VACUUM_INCREMENTAL
        )
        if needs_migration:
            self._backup_before_migration()
        if columns and "route_key" not in columns:
            self._db.execute("ALTER TABLE dedupe_events RENAME TO dedupe_events_legacy_v1")
            columns = set()
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS dedupe_events ("
            "route_key TEXT NOT NULL, "
            "source_uuid TEXT NOT NULL, "
            "timestamp INTEGER NOT NULL, "
            "status TEXT NOT NULL, "
            "updated_at_ms INTEGER NOT NULL DEFAULT 0, "
            "PRIMARY KEY (route_key, source_uuid, timestamp))"
        )
        if columns and "updated_at_ms" not in columns:
            # Backfill pre-migration rows with a fresh retention clock so
            # they are not pruned on the first sweep. This deliberately
            # covers idempotency-key identities whose event `timestamp` is
            # the sentinel 0.
            self._db.execute(
                "ALTER TABLE dedupe_events ADD COLUMN updated_at_ms INTEGER NOT NULL DEFAULT 0"
            )
            self._db.execute(
                "UPDATE dedupe_events SET updated_at_ms = ? WHERE updated_at_ms = 0",
                (self._clock_ms(),),
            )
        # Partial prune index: every retention chunk's row lookup is bounded
        # by the index, never a table scan over sparse old rows.
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS dedupe_events_handled_updated_at "
            "ON dedupe_events (updated_at_ms) WHERE status = 'handled'"
        )
        if not columns and self._legacy_table_exists():
            self._db.execute(
                "INSERT OR IGNORE INTO dedupe_events "
                "(route_key, source_uuid, timestamp, status, updated_at_ms) "
                "SELECT '', source_uuid, timestamp, 'handled', ? FROM dedupe_events_legacy_v1",
                (self._clock_ms(),),
            )
        self._db.commit()
        auto_vacuum = int(self._db.execute("PRAGMA auto_vacuum").fetchone()[0])
        if auto_vacuum != _AUTO_VACUUM_INCREMENTAL:
            # One-time conversion so periodic space reclamation can use
            # bounded incremental_vacuum chunks instead of a long full
            # VACUUM at sweep time. Runs at startup, before any transport
            # work exists to contend with, and keeps the exclusive lock.
            self._db.execute("PRAGMA auto_vacuum=INCREMENTAL")
            self._db.execute("VACUUM")

    def _backup_before_migration(self) -> None:
        # Database-safety rule: never migrate persistent state without an
        # automatic backup. Written through the already-open exclusive
        # connection so no second-writer window opens; retained for the
        # operator rather than auto-deleted.
        if self.path == ":memory:":
            return
        backup_path = Path(self.path + MIGRATION_BACKUP_SUFFIX)
        ensure_private_file(backup_path)
        target = sqlite3.connect(backup_path)
        try:
            self._db.backup(target)
        finally:
            target.close()
        backup_path.chmod(PRIVATE_FILE_MODE)
        LOGGER.info("wrote dedupe state DB backup before schema migration")

    def _reclaim_orphaned_claims(self) -> None:
        # No turn is in flight when the store is constructed (construction
        # fails on the exclusive lock above if another live store owns the
        # DB), so any persisted 'processing' claim was orphaned by a dead
        # process and would otherwise dedupe its retries forever. This write
        # also escalates the exclusive lock, so it is held from startup even
        # when nothing is reclaimed.
        cursor = self._db.execute("DELETE FROM dedupe_events WHERE status = 'processing'")
        self._db.commit()
        if cursor.rowcount > 0:
            LOGGER.info("reclaimed %d orphaned processing dedupe claims", cursor.rowcount)

    def _legacy_table_exists(self) -> bool:
        cursor = self._db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'dedupe_events_legacy_v1'"
        )
        return cursor.fetchone() is not None

    def claim(self, route_key: str, source_uuid: str, timestamp: int) -> bool:
        with self._lock:
            try:
                self._db.execute(
                    "INSERT INTO dedupe_events "
                    "(route_key, source_uuid, timestamp, status, updated_at_ms) "
                    "VALUES (?, ?, ?, 'processing', ?)",
                    (route_key, source_uuid, int(timestamp), self._clock_ms()),
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
                "INSERT INTO dedupe_events "
                "(route_key, source_uuid, timestamp, status, updated_at_ms) "
                "VALUES (?, ?, ?, 'handled', ?) "
                "ON CONFLICT(route_key, source_uuid, timestamp) "
                "DO UPDATE SET status = 'handled', updated_at_ms = excluded.updated_at_ms",
                (route_key, source_uuid, int(timestamp), self._clock_ms()),
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

    def prune_handled_before(self, cutoff_ms: int) -> int:
        """Delete handled rows with a retention clock older than ``cutoff_ms``.

        Runs as bounded chunks so no single self._lock hold scales with table
        size; 'processing' claims are never pruned. Safe to call from a sweep
        worker thread; stops early when a close was requested.
        """
        if not self._begin_sweep_operation():
            return 0
        total = 0
        try:
            while True:
                with self._state_lock:
                    if self._close_requested:
                        return total
                with self._lock:
                    cursor = self._db.execute(
                        "DELETE FROM dedupe_events WHERE rowid IN ("
                        "SELECT rowid FROM dedupe_events "
                        "WHERE status = 'handled' AND updated_at_ms < ? LIMIT ?)",
                        (int(cutoff_ms), PRUNE_CHUNK_ROWS),
                    )
                    self._db.commit()
                if cursor.rowcount <= 0:
                    return total
                total += cursor.rowcount
        finally:
            self._end_sweep_operation()

    def incremental_vacuum(self) -> None:
        """Drain the freelist in bounded chunks (requires INCREMENTAL mode)."""
        if not self._begin_sweep_operation():
            return
        try:
            previous_freelist: int | None = None
            while True:
                with self._state_lock:
                    if self._close_requested:
                        return
                with self._lock:
                    self._db.execute(f"PRAGMA incremental_vacuum({VACUUM_CHUNK_PAGES})")
                    self._db.commit()
                    freelist = int(self._db.execute("PRAGMA freelist_count").fetchone()[0])
                if freelist <= 0:
                    return
                if previous_freelist is not None and freelist >= previous_freelist:
                    # No progress (for example a store not in incremental
                    # auto-vacuum mode); never spin.
                    return
                previous_freelist = freelist
        finally:
            self._end_sweep_operation()

    def _begin_sweep_operation(self) -> bool:
        with self._state_lock:
            if self._closed or self._close_requested:
                return False
            self._sweep_active = True
            return True

    def _end_sweep_operation(self) -> None:
        with self._state_lock:
            self._sweep_active = False
            if self._close_requested and not self._closed:
                # Deferred finalizer: close() handed the connection close to
                # this sweep worker instead of blocking the event loop.
                self._db.close()
                self._closed = True

    def close(self) -> bool:
        """Close the store without blocking on an in-flight sweep chunk.

        Returns True when the connection is closed on return. Returns False
        when a retention operation is mid-statement: the close is then
        deferred to that worker's completion, and callers must treat the
        cleanup as incomplete rather than finished.
        """
        with self._state_lock:
            if self._closed:
                return True
            if self._sweep_active:
                self._close_requested = True
                return False
            self._db.close()
            self._closed = True
            return True

    def __enter__(self) -> "DedupeStore":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
