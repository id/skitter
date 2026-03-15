"""Database interface with SQLite and PostgreSQL backends.

All methods are synchronous — the supervisor calls them from async code
via asyncio.loop.run_in_executor().
"""

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
    request_json: str = ""
    variables: str = ""
    caller_reply_topic: str = ""
    caller_correlation: str = ""
    state: str = "running"
    created_at: str = ""
    completed_at: str = ""


@dataclass
class DBTask:
    id: str
    session_id: str
    task_id: str
    agent: str
    description: str = ""
    needs: str = ""
    next: str = ""
    target_json: str = ""
    request_id: str = ""
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
    def list_sessions(self, app_id: str | None = None) -> list[DBSession]: ...
    def update_session_state(self, session_id: str, state: str) -> None: ...

    def create_task(self, task: DBTask) -> None: ...
    def get_task(self, row_id: str) -> DBTask | None: ...
    def list_tasks(self, session_id: str) -> list[DBTask]: ...
    def update_task(self, row_id: str, **fields) -> None: ...

    def close(self) -> None: ...


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
            reply_topic   TEXT DEFAULT '',
            dispatched_at TEXT DEFAULT '',
            state         TEXT DEFAULT 'pending',
            result        TEXT DEFAULT '',
            error         TEXT DEFAULT '',
            started_at    TEXT DEFAULT '',
            completed_at  TEXT DEFAULT ''
        )""",
    ],
]

_TASK_UPDATABLE_FIELDS = frozenset(
    {
        "state",
        "result",
        "error",
        "request_id",
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
        request_json=row["request_json"] or "",
        variables=row["variables"] or "",
        caller_reply_topic=row["caller_reply_topic"] or "",
        caller_correlation=row["caller_correlation"] or "",
        state=row["state"] or "running",
        created_at=row["created_at"] or "",
        completed_at=row["completed_at"] or "",
    )


def _row_to_task(row) -> DBTask:
    return DBTask(
        id=row["id"],
        session_id=row["session_id"],
        task_id=row["task_id"],
        agent=row["agent"],
        description=row["description"] or "",
        needs=row["needs"] or "",
        next=row["next"] or "",
        target_json=row["target_json"] or "",
        request_id=row["request_id"] or "",
        reply_topic=row["reply_topic"] or "",
        dispatched_at=row["dispatched_at"] or "",
        state=row["state"] or "pending",
        result=row["result"] or "",
        error=row["error"] or "",
        started_at=row["started_at"] or "",
        completed_at=row["completed_at"] or "",
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


class SqliteDB:
    def __init__(self, path: str) -> None:
        if path != ":memory:":
            from pathlib import Path

            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        _migrate_sqlite(self._conn)

    # -- App --

    def create_app(self, app: App) -> None:
        self._conn.execute(
            "INSERT INTO app (id, name, description, card_json, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                app.id,
                app.name,
                app.description,
                app.card_json,
                app.created_at or _now(),
            ),
        )
        self._conn.commit()

    def get_app(self, app_id: str) -> App | None:
        row = self._conn.execute("SELECT * FROM app WHERE id = ?", (app_id,)).fetchone()
        return _row_to_app(row) if row else None

    def list_apps(self) -> list[App]:
        rows = self._conn.execute("SELECT * FROM app ORDER BY created_at").fetchall()
        return [_row_to_app(r) for r in rows]

    def update_app_card(self, app_id: str, card_json: str) -> None:
        self._conn.execute(
            "UPDATE app SET card_json = ? WHERE id = ?", (card_json, app_id)
        )
        self._conn.commit()

    def delete_app(self, app_id: str) -> None:
        self._conn.execute("DELETE FROM app WHERE id = ?", (app_id,))
        self._conn.commit()

    # -- App version --

    def create_app_version(self, version: AppVersion) -> None:
        self._conn.execute(
            "INSERT INTO app_version (id, app_id, version, source_cards, "
            "instructions, graph_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
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
        self._conn.commit()

    def get_app_version(self, version_id: str) -> AppVersion | None:
        row = self._conn.execute(
            "SELECT * FROM app_version WHERE id = ?", (version_id,)
        ).fetchone()
        return _row_to_app_version(row) if row else None

    def get_current_version(self, app_id: str) -> AppVersion | None:
        row = self._conn.execute(
            "SELECT * FROM app_version WHERE app_id = ? ORDER BY version DESC LIMIT 1",
            (app_id,),
        ).fetchone()
        return _row_to_app_version(row) if row else None

    def list_app_versions(self, app_id: str) -> list[AppVersion]:
        rows = self._conn.execute(
            "SELECT * FROM app_version WHERE app_id = ? ORDER BY version",
            (app_id,),
        ).fetchall()
        return [_row_to_app_version(r) for r in rows]

    # -- Session --

    def create_session(self, session: DBSession) -> None:
        self._conn.execute(
            "INSERT INTO session (id, app_version_id, request_json, variables, "
            "caller_reply_topic, caller_correlation, state, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                session.id,
                session.app_version_id,
                session.request_json,
                session.variables,
                session.caller_reply_topic,
                session.caller_correlation,
                session.state,
                session.created_at or _now(),
            ),
        )
        self._conn.commit()

    def get_session(self, session_id: str) -> DBSession | None:
        row = self._conn.execute(
            "SELECT * FROM session WHERE id = ?", (session_id,)
        ).fetchone()
        return _row_to_session(row) if row else None

    def list_sessions(self, app_id: str | None = None) -> list[DBSession]:
        if app_id:
            rows = self._conn.execute(
                "SELECT s.* FROM session s "
                "JOIN app_version av ON s.app_version_id = av.id "
                "WHERE av.app_id = ? ORDER BY s.created_at DESC",
                (app_id,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM session ORDER BY created_at DESC"
            ).fetchall()
        return [_row_to_session(r) for r in rows]

    def update_session_state(self, session_id: str, state: str) -> None:
        if state in ("completed", "failed", "cancelled"):
            self._conn.execute(
                "UPDATE session SET state = ?, completed_at = ? WHERE id = ?",
                (state, _now(), session_id),
            )
        else:
            self._conn.execute(
                "UPDATE session SET state = ? WHERE id = ?", (state, session_id)
            )
        self._conn.commit()

    # -- Task --

    def create_task(self, task: DBTask) -> None:
        self._conn.execute(
            "INSERT INTO task (id, session_id, task_id, agent, description, "
            "needs, next, target_json, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task.id,
                task.session_id,
                task.task_id,
                task.agent,
                task.description,
                task.needs,
                task.next,
                task.target_json,
                task.state,
            ),
        )
        self._conn.commit()

    def get_task(self, row_id: str) -> DBTask | None:
        row = self._conn.execute(
            "SELECT * FROM task WHERE id = ?", (row_id,)
        ).fetchone()
        return _row_to_task(row) if row else None

    def list_tasks(self, session_id: str) -> list[DBTask]:
        rows = self._conn.execute(
            "SELECT * FROM task WHERE session_id = ?", (session_id,)
        ).fetchall()
        return [_row_to_task(r) for r in rows]

    def update_task(self, row_id: str, **fields) -> None:
        bad = fields.keys() - _TASK_UPDATABLE_FIELDS
        if bad:
            raise ValueError(f"Non-updatable fields: {bad}")
        if not fields:
            return
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        self._conn.execute(
            f"UPDATE task SET {set_clause} WHERE id = ?",
            (*fields.values(), row_id),
        )
        self._conn.commit()

    # -- Lifecycle --

    def close(self) -> None:
        self._conn.close()


# --- PostgreSQL implementation ---


class PostgresDB:
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

    # -- App --

    def create_app(self, app: App) -> None:
        self._exec(
            "INSERT INTO app (id, name, description, card_json, created_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            (
                app.id,
                app.name,
                app.description,
                app.card_json,
                app.created_at or _now(),
            ),
        )

    def get_app(self, app_id: str) -> App | None:
        row = self._fetchone("SELECT * FROM app WHERE id = %s", (app_id,))
        return _row_to_app(row) if row else None

    def list_apps(self) -> list[App]:
        return [
            _row_to_app(r)
            for r in self._fetchall("SELECT * FROM app ORDER BY created_at")
        ]

    def update_app_card(self, app_id: str, card_json: str) -> None:
        self._exec("UPDATE app SET card_json = %s WHERE id = %s", (card_json, app_id))

    def delete_app(self, app_id: str) -> None:
        self._exec("DELETE FROM app WHERE id = %s", (app_id,))

    # -- App version --

    def create_app_version(self, version: AppVersion) -> None:
        self._exec(
            "INSERT INTO app_version (id, app_id, version, source_cards, "
            "instructions, graph_json, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
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
        row = self._fetchone("SELECT * FROM app_version WHERE id = %s", (version_id,))
        return _row_to_app_version(row) if row else None

    def get_current_version(self, app_id: str) -> AppVersion | None:
        row = self._fetchone(
            "SELECT * FROM app_version WHERE app_id = %s ORDER BY version DESC LIMIT 1",
            (app_id,),
        )
        return _row_to_app_version(row) if row else None

    def list_app_versions(self, app_id: str) -> list[AppVersion]:
        return [
            _row_to_app_version(r)
            for r in self._fetchall(
                "SELECT * FROM app_version WHERE app_id = %s ORDER BY version",
                (app_id,),
            )
        ]

    # -- Session --

    def create_session(self, session: DBSession) -> None:
        self._exec(
            "INSERT INTO session (id, app_version_id, request_json, variables, "
            "caller_reply_topic, caller_correlation, state, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                session.id,
                session.app_version_id,
                session.request_json,
                session.variables,
                session.caller_reply_topic,
                session.caller_correlation,
                session.state,
                session.created_at or _now(),
            ),
        )

    def get_session(self, session_id: str) -> DBSession | None:
        row = self._fetchone("SELECT * FROM session WHERE id = %s", (session_id,))
        return _row_to_session(row) if row else None

    def list_sessions(self, app_id: str | None = None) -> list[DBSession]:
        if app_id:
            rows = self._fetchall(
                "SELECT s.* FROM session s "
                "JOIN app_version av ON s.app_version_id = av.id "
                "WHERE av.app_id = %s ORDER BY s.created_at DESC",
                (app_id,),
            )
        else:
            rows = self._fetchall("SELECT * FROM session ORDER BY created_at DESC")
        return [_row_to_session(r) for r in rows]

    def update_session_state(self, session_id: str, state: str) -> None:
        if state in ("completed", "failed", "cancelled"):
            self._exec(
                "UPDATE session SET state = %s, completed_at = %s WHERE id = %s",
                (state, _now(), session_id),
            )
        else:
            self._exec(
                "UPDATE session SET state = %s WHERE id = %s", (state, session_id)
            )

    # -- Task --

    def create_task(self, task: DBTask) -> None:
        self._exec(
            "INSERT INTO task (id, session_id, task_id, agent, description, "
            "needs, next, target_json, state) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                task.id,
                task.session_id,
                task.task_id,
                task.agent,
                task.description,
                task.needs,
                task.next,
                task.target_json,
                task.state,
            ),
        )

    def get_task(self, row_id: str) -> DBTask | None:
        row = self._fetchone("SELECT * FROM task WHERE id = %s", (row_id,))
        return _row_to_task(row) if row else None

    def list_tasks(self, session_id: str) -> list[DBTask]:
        return [
            _row_to_task(r)
            for r in self._fetchall(
                "SELECT * FROM task WHERE session_id = %s", (session_id,)
            )
        ]

    def update_task(self, row_id: str, **fields) -> None:
        bad = fields.keys() - _TASK_UPDATABLE_FIELDS
        if bad:
            raise ValueError(f"Non-updatable fields: {bad}")
        if not fields:
            return
        set_clause = ", ".join(f"{k} = %s" for k in fields)
        self._exec(
            f"UPDATE task SET {set_clause} WHERE id = %s",
            (*fields.values(), row_id),
        )

    # -- Lifecycle --

    def close(self) -> None:
        self._pool.close()


# --- Factory ---


def open_db() -> SqliteDB | PostgresDB:
    """Open the database configured in ~/.skitter/config.yaml."""
    from skitter.config import load_db_config

    cfg = load_db_config()
    if cfg.backend == "postgres":
        return PostgresDB(cfg.postgres_dsn)
    return SqliteDB(cfg.sqlite_path)
