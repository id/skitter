"""Database interface with SQLite and PostgreSQL backends.

All repository methods are synchronous. The coordinator must use AsyncDB
(which wraps calls via asyncio.to_thread) to avoid blocking the event loop.
"""

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

log = logging.getLogger("skitter.db")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- DB record dataclasses ---


@dataclass
class App:
    id: str
    name: str
    description: str = ""
    card_json: str = ""
    created_at: str = ""


@dataclass
class AppVersion:
    id: str
    app_id: str
    version: int
    source_cards: str = ""
    instructions: str = ""
    graph_json: str = ""
    created_at: str = ""


@dataclass
class DBSession:
    id: str
    app_version_id: str
    request_task_id: str = ""
    context_id: str = ""
    request_json: str = ""
    variables: str = ""
    caller_reply_topic: str = ""
    caller_correlation: str = ""
    state: str = "running"
    result: str = ""
    created_at: str = ""
    completed_at: str = ""


@dataclass
class DBTask:
    id: str
    session_id: str
    node_id: str
    agent: str
    description: str = ""
    needs: str = ""
    terminal: str = ""
    target_json: str = ""
    dispatch_task_id: str = ""
    reply_topic: str = ""
    dispatched_at: str = ""
    state: str = "pending"
    result: str = ""
    error: str = ""
    started_at: str = ""
    completed_at: str = ""


# --- DB Protocol ---


class DB(Protocol):
    """Abstract database interface."""

    def create_app(self, app: App) -> None: ...
    def get_app(self, app_id: str) -> App | None: ...
    def list_apps(self) -> list[App]: ...
    def update_app_card(self, app_id: str, card_json: str) -> None: ...
    def delete_app(self, app_id: str) -> None: ...

    def create_app_version(self, version: AppVersion) -> None: ...
    def get_app_version(self, version_id: str) -> AppVersion | None: ...
    def get_current_version(self, app_id: str) -> AppVersion | None: ...
    def list_app_versions(self, app_id: str) -> list[AppVersion]: ...

    def create_session(self, session: DBSession) -> None: ...
    def get_session(self, session_id: str) -> DBSession | None: ...
    def get_session_by_request_task_id(
        self, request_task_id: str
    ) -> DBSession | None: ...
    def list_sessions(self, app_id: str | None = None) -> list[DBSession]: ...
    def list_context_sessions(
        self, app_id: str, context_id: str, limit: int = 10
    ) -> list[DBSession]: ...
    def update_session_state(
        self, session_id: str, state: str, result: str = ""
    ) -> None: ...

    def create_task(self, task: DBTask) -> None: ...
    def get_task(self, row_id: str) -> DBTask | None: ...
    def list_tasks(self, session_id: str) -> list[DBTask]: ...

    def update_task(self, row_id: str, **fields) -> None: ...

    def close(self) -> None: ...


# --- Async facade ---


class AsyncDB:
    """Async wrapper around a synchronous DB, using asyncio.to_thread."""

    def __init__(self, db: DB) -> None:
        self._db = db

    async def create_app(self, app: App) -> None:
        await asyncio.to_thread(self._db.create_app, app)

    async def get_app(self, app_id: str) -> App | None:
        return await asyncio.to_thread(self._db.get_app, app_id)

    async def list_apps(self) -> list[App]:
        return await asyncio.to_thread(self._db.list_apps)

    async def update_app_card(self, app_id: str, card_json: str) -> None:
        await asyncio.to_thread(self._db.update_app_card, app_id, card_json)

    async def delete_app(self, app_id: str) -> None:
        await asyncio.to_thread(self._db.delete_app, app_id)

    async def create_app_version(self, version: AppVersion) -> None:
        await asyncio.to_thread(self._db.create_app_version, version)

    async def get_app_version(self, version_id: str) -> AppVersion | None:
        return await asyncio.to_thread(self._db.get_app_version, version_id)

    async def get_current_version(self, app_id: str) -> AppVersion | None:
        return await asyncio.to_thread(self._db.get_current_version, app_id)

    async def list_app_versions(self, app_id: str) -> list[AppVersion]:
        return await asyncio.to_thread(self._db.list_app_versions, app_id)

    async def create_session(self, session: DBSession) -> None:
        await asyncio.to_thread(self._db.create_session, session)

    async def get_session(self, session_id: str) -> DBSession | None:
        return await asyncio.to_thread(self._db.get_session, session_id)

    async def get_session_by_request_task_id(
        self, request_task_id: str
    ) -> DBSession | None:
        return await asyncio.to_thread(
            self._db.get_session_by_request_task_id, request_task_id
        )

    async def list_sessions(self, app_id: str | None = None) -> list[DBSession]:
        return await asyncio.to_thread(self._db.list_sessions, app_id)

    async def list_context_sessions(
        self, app_id: str, context_id: str, limit: int = 10
    ) -> list[DBSession]:
        return await asyncio.to_thread(
            self._db.list_context_sessions, app_id, context_id, limit
        )

    async def update_session_state(
        self, session_id: str, state: str, result: str = ""
    ) -> None:
        await asyncio.to_thread(
            self._db.update_session_state, session_id, state, result
        )

    async def create_task(self, task: DBTask) -> None:
        await asyncio.to_thread(self._db.create_task, task)

    async def get_task(self, row_id: str) -> DBTask | None:
        return await asyncio.to_thread(self._db.get_task, row_id)

    async def list_tasks(self, session_id: str) -> list[DBTask]:
        return await asyncio.to_thread(self._db.list_tasks, session_id)

    async def update_task(self, row_id: str, **fields) -> None:
        await asyncio.to_thread(self._db.update_task, row_id, **fields)

    def close(self) -> None:
        self._db.close()


# --- Schema ---

_MIGRATIONS: list[list[str]] = [
    # v1: initial schema
    [
        """CREATE TABLE app (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            description TEXT DEFAULT '',
            card_json   TEXT DEFAULT '',
            created_at  TEXT DEFAULT ''
        )""",
        """CREATE TABLE app_version (
            id           TEXT PRIMARY KEY,
            app_id       TEXT NOT NULL REFERENCES app(id) ON DELETE CASCADE,
            version      INTEGER NOT NULL,
            source_cards TEXT DEFAULT '',
            instructions TEXT DEFAULT '',
            graph_json   TEXT DEFAULT '',
            created_at   TEXT DEFAULT ''
        )""",
        """CREATE TABLE session (
            id                 TEXT PRIMARY KEY,
            app_version_id     TEXT NOT NULL REFERENCES app_version(id) ON DELETE CASCADE,
            request_json       TEXT DEFAULT '',
            variables          TEXT DEFAULT '',
            caller_reply_topic TEXT DEFAULT '',
            caller_correlation TEXT DEFAULT '',
            state              TEXT DEFAULT 'running',
            created_at         TEXT DEFAULT '',
            completed_at       TEXT DEFAULT ''
        )""",
        """CREATE TABLE task (
            id            TEXT PRIMARY KEY,
            session_id    TEXT NOT NULL REFERENCES session(id) ON DELETE CASCADE,
            task_id       TEXT NOT NULL,
            agent         TEXT NOT NULL,
            description   TEXT DEFAULT '',
            needs         TEXT DEFAULT '',
            next          TEXT DEFAULT '',
            target_json   TEXT DEFAULT '',
            request_id    TEXT DEFAULT '',
            a2a_task_id   TEXT DEFAULT '',
            reply_topic   TEXT DEFAULT '',
            dispatched_at TEXT DEFAULT '',
            state         TEXT DEFAULT 'pending',
            result        TEXT DEFAULT '',
            error         TEXT DEFAULT '',
            started_at    TEXT DEFAULT '',
            completed_at  TEXT DEFAULT ''
        )""",
    ],
    # v2: contextId support
    ["ALTER TABLE session ADD COLUMN context_id TEXT DEFAULT ''"],
    # v3: identity model cleanup (Phase 1)
    [
        "ALTER TABLE session ADD COLUMN request_task_id TEXT DEFAULT ''",
        "UPDATE session SET request_task_id = id WHERE request_task_id = ''",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_session_request_task_id "
        "ON session(request_task_id)",
        "ALTER TABLE task RENAME COLUMN task_id TO node_id",
        "ALTER TABLE task RENAME COLUMN a2a_task_id TO dispatch_task_id",
    ],
    # v4: graph model cleanup (Phase 2) — replace next with terminal flag
    [
        "ALTER TABLE task ADD COLUMN terminal TEXT DEFAULT ''",
        "UPDATE task SET terminal = '1' WHERE next = 'output' OR next = '' OR next IS NULL",
    ],
    # v5: index on context_id for conversation continuity queries
    [
        "CREATE INDEX IF NOT EXISTS idx_session_context_id "
        "ON session(context_id) WHERE context_id != ''",
    ],
    # v6: store final result on session for conversation continuity
    ["ALTER TABLE session ADD COLUMN result TEXT DEFAULT ''"],
]

_TASK_UPDATABLE_FIELDS = frozenset(
    {
        "state",
        "result",
        "error",
        "dispatch_task_id",
        "reply_topic",
        "dispatched_at",
        "started_at",
        "completed_at",
        "target_json",
        "description",
    }
)


# --- Row converters ---


def _row_to_app(row) -> App:
    return App(
        id=row["id"],
        name=row["name"],
        description=row["description"] or "",
        card_json=row["card_json"] or "",
        created_at=row["created_at"] or "",
    )


def _row_to_app_version(row) -> AppVersion:
    return AppVersion(
        id=row["id"],
        app_id=row["app_id"],
        version=row["version"],
        source_cards=row["source_cards"] or "",
        instructions=row["instructions"] or "",
        graph_json=row["graph_json"] or "",
        created_at=row["created_at"] or "",
    )


def _row_to_session(row) -> DBSession:
    return DBSession(
        id=row["id"],
        app_version_id=row["app_version_id"],
        request_task_id=row["request_task_id"] or "",
        context_id=row["context_id"] or "",
        request_json=row["request_json"] or "",
        variables=row["variables"] or "",
        caller_reply_topic=row["caller_reply_topic"] or "",
        caller_correlation=row["caller_correlation"] or "",
        state=row["state"] or "running",
        result=row["result"] or "",
        created_at=row["created_at"] or "",
        completed_at=row["completed_at"] or "",
    )


def _row_to_task(row) -> DBTask:
    return DBTask(
        id=row["id"],
        session_id=row["session_id"],
        node_id=row["node_id"],
        agent=row["agent"],
        description=row["description"] or "",
        needs=row["needs"] or "",
        terminal=row["terminal"] or "",
        target_json=row["target_json"] or "",
        dispatch_task_id=row["dispatch_task_id"] or "",
        reply_topic=row["reply_topic"] or "",
        dispatched_at=row["dispatched_at"] or "",
        state=row["state"] or "pending",
        result=row["result"] or "",
        error=row["error"] or "",
        started_at=row["started_at"] or "",
        completed_at=row["completed_at"] or "",
    )


# --- Shared repository logic ---


class _BaseDB:
    """Shared CRUD logic; subclasses provide _exec, _fetchone, _fetchall, _ph."""

    _ph: str  # placeholder: "?" for SQLite, "%s" for PostgreSQL

    def _exec(self, sql: str, params=()) -> None:
        raise NotImplementedError

    def _fetchone(self, sql: str, params=()):
        raise NotImplementedError

    def _fetchall(self, sql: str, params=()):
        raise NotImplementedError

    # -- App --

    def create_app(self, app: App) -> None:
        p = self._ph
        self._exec(
            f"INSERT INTO app (id, name, description, card_json, created_at) "
            f"VALUES ({p}, {p}, {p}, {p}, {p})",
            (
                app.id,
                app.name,
                app.description,
                app.card_json,
                app.created_at or _now(),
            ),
        )

    def get_app(self, app_id: str) -> App | None:
        row = self._fetchone(f"SELECT * FROM app WHERE id = {self._ph}", (app_id,))
        return _row_to_app(row) if row else None

    def list_apps(self) -> list[App]:
        return [
            _row_to_app(r)
            for r in self._fetchall("SELECT * FROM app ORDER BY created_at")
        ]

    def update_app_card(self, app_id: str, card_json: str) -> None:
        p = self._ph
        self._exec(
            f"UPDATE app SET card_json = {p} WHERE id = {p}", (card_json, app_id)
        )

    def delete_app(self, app_id: str) -> None:
        self._exec(f"DELETE FROM app WHERE id = {self._ph}", (app_id,))

    # -- App version --

    def create_app_version(self, version: AppVersion) -> None:
        p = self._ph
        self._exec(
            f"INSERT INTO app_version (id, app_id, version, source_cards, "
            f"instructions, graph_json, created_at) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p})",
            (
                version.id,
                version.app_id,
                version.version,
                version.source_cards,
                version.instructions,
                version.graph_json,
                version.created_at or _now(),
            ),
        )

    def get_app_version(self, version_id: str) -> AppVersion | None:
        row = self._fetchone(
            f"SELECT * FROM app_version WHERE id = {self._ph}", (version_id,)
        )
        return _row_to_app_version(row) if row else None

    def get_current_version(self, app_id: str) -> AppVersion | None:
        row = self._fetchone(
            f"SELECT * FROM app_version WHERE app_id = {self._ph} ORDER BY version DESC LIMIT 1",
            (app_id,),
        )
        return _row_to_app_version(row) if row else None

    def list_app_versions(self, app_id: str) -> list[AppVersion]:
        return [
            _row_to_app_version(r)
            for r in self._fetchall(
                f"SELECT * FROM app_version WHERE app_id = {self._ph} ORDER BY version",
                (app_id,),
            )
        ]

    # -- Session --

    def create_session(self, session: DBSession) -> None:
        p = self._ph
        self._exec(
            f"INSERT INTO session (id, app_version_id, request_task_id, context_id, "
            f"request_json, variables, caller_reply_topic, caller_correlation, "
            f"state, created_at) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})",
            (
                session.id,
                session.app_version_id,
                session.request_task_id,
                session.context_id,
                session.request_json,
                session.variables,
                session.caller_reply_topic,
                session.caller_correlation,
                session.state,
                session.created_at or _now(),
            ),
        )

    def get_session(self, session_id: str) -> DBSession | None:
        row = self._fetchone(
            f"SELECT * FROM session WHERE id = {self._ph}", (session_id,)
        )
        return _row_to_session(row) if row else None

    def get_session_by_request_task_id(self, request_task_id: str) -> DBSession | None:
        row = self._fetchone(
            f"SELECT * FROM session WHERE request_task_id = {self._ph}",
            (request_task_id,),
        )
        return _row_to_session(row) if row else None

    def list_sessions(self, app_id: str | None = None) -> list[DBSession]:
        p = self._ph
        if app_id:
            rows = self._fetchall(
                f"SELECT s.* FROM session s "
                f"JOIN app_version av ON s.app_version_id = av.id "
                f"WHERE av.app_id = {p} ORDER BY s.created_at DESC",
                (app_id,),
            )
        else:
            rows = self._fetchall("SELECT * FROM session ORDER BY created_at DESC")
        return [_row_to_session(r) for r in rows]

    def list_context_sessions(
        self, app_id: str, context_id: str, limit: int = 10
    ) -> list[DBSession]:
        p = self._ph
        rows = self._fetchall(
            f"SELECT * FROM ("
            f"SELECT s.* FROM session s "
            f"JOIN app_version av ON s.app_version_id = av.id "
            f"WHERE av.app_id = {p} AND s.context_id = {p} AND s.state = 'completed' "
            f"ORDER BY s.created_at DESC LIMIT {p}"
            f") sub ORDER BY created_at ASC",
            (app_id, context_id, limit),
        )
        return [_row_to_session(r) for r in rows]

    def update_session_state(
        self, session_id: str, state: str, result: str = ""
    ) -> None:
        p = self._ph
        if state in ("completed", "failed", "canceled"):
            self._exec(
                f"UPDATE session SET state = {p}, result = {p}, completed_at = {p} WHERE id = {p}",
                (state, result, _now(), session_id),
            )
        else:
            self._exec(
                f"UPDATE session SET state = {p} WHERE id = {p}", (state, session_id)
            )

    # -- Task --

    def create_task(self, task: DBTask) -> None:
        p = self._ph
        self._exec(
            f"INSERT INTO task (id, session_id, node_id, agent, description, "
            f"needs, terminal, target_json, state) VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})",
            (
                task.id,
                task.session_id,
                task.node_id,
                task.agent,
                task.description,
                task.needs,
                task.terminal,
                task.target_json,
                task.state,
            ),
        )

    def get_task(self, row_id: str) -> DBTask | None:
        row = self._fetchone(f"SELECT * FROM task WHERE id = {self._ph}", (row_id,))
        return _row_to_task(row) if row else None

    def list_tasks(self, session_id: str) -> list[DBTask]:
        return [
            _row_to_task(r)
            for r in self._fetchall(
                f"SELECT * FROM task WHERE session_id = {self._ph}", (session_id,)
            )
        ]

    def update_task(self, row_id: str, **fields) -> None:
        bad = fields.keys() - _TASK_UPDATABLE_FIELDS
        if bad:
            raise ValueError(f"Non-updatable fields: {bad}")
        if not fields:
            return
        p = self._ph
        set_clause = ", ".join(f"{k} = {p}" for k in fields)
        self._exec(
            f"UPDATE task SET {set_clause} WHERE id = {p}",
            (*fields.values(), row_id),
        )


# --- SQLite implementation ---


def _migrate_sqlite(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
    row = conn.execute("SELECT version FROM schema_version").fetchone()
    if row is None:
        conn.execute("INSERT INTO schema_version VALUES (0)")
        conn.commit()
        current = 0
    else:
        current = row[0]

    for i in range(current, len(_MIGRATIONS)):
        conn.execute("BEGIN")
        try:
            for stmt in _MIGRATIONS[i]:
                conn.execute(stmt)
            conn.execute("UPDATE schema_version SET version = ?", (i + 1,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        log.info("Applied migration %d", i + 1)


class SqliteDB(_BaseDB):
    _ph = "?"

    def __init__(self, path: str) -> None:
        if path != ":memory:":
            from pathlib import Path

            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        _migrate_sqlite(self._conn)

    def _exec(self, sql: str, params=()) -> None:
        self._conn.execute(sql, params)
        self._conn.commit()

    def _fetchone(self, sql: str, params=()):
        return self._conn.execute(sql, params).fetchone()

    def _fetchall(self, sql: str, params=()):
        return self._conn.execute(sql, params).fetchall()

    def close(self) -> None:
        self._conn.close()


# --- PostgreSQL implementation ---


class PostgresDB(_BaseDB):
    _ph = "%s"

    def __init__(self, dsn: str) -> None:
        try:
            from psycopg.rows import dict_row
            from psycopg_pool import ConnectionPool
        except ImportError:
            raise RuntimeError(
                "psycopg not installed — run: uv add 'psycopg[binary]' psycopg_pool"
            ) from None

        self._pool = ConnectionPool(dsn, kwargs={"row_factory": dict_row})
        with self._pool.connection() as conn:
            self._migrate_pg(conn)

    def _migrate_pg(self, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
        )
        row = conn.execute("SELECT version FROM schema_version").fetchone()
        if row is None:
            conn.execute("INSERT INTO schema_version VALUES (0)")
            current = 0
        else:
            current = row["version"]

        for i in range(current, len(_MIGRATIONS)):
            for stmt in _MIGRATIONS[i]:
                conn.execute(stmt)
            conn.execute("UPDATE schema_version SET version = %s", (i + 1,))
        conn.commit()

    def _exec(self, sql: str, params=()) -> None:
        with self._pool.connection() as conn:
            conn.execute(sql, params)
            conn.commit()

    def _fetchone(self, sql: str, params=()):
        with self._pool.connection() as conn:
            return conn.execute(sql, params).fetchone()

    def _fetchall(self, sql: str, params=()):
        with self._pool.connection() as conn:
            return conn.execute(sql, params).fetchall()

    def close(self) -> None:
        self._pool.close()


# --- Factory ---


def open_db() -> SqliteDB | PostgresDB:
    """Open the database configured in ~/.skitter/config.yaml."""
    from skitter.config import load_config

    cfg = load_config().db
    if cfg.backend == "postgres":
        return PostgresDB(cfg.postgres_dsn)
    return SqliteDB(cfg.sqlite_path)
