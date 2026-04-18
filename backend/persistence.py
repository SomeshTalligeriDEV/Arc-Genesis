"""
Arc Genesis — Persistence Layer
SQLite-backed storage for queries, threats, and anomalies.
Zero-config: creates DB file automatically on first run.
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("ARC_DB_PATH", str(Path(__file__).parent / "arc_genesis.db"))

# ─── Schema ──────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT NOT NULL,
    decision TEXT NOT NULL,
    risk_score REAL NOT NULL,
    risk_level TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS queries (
    id TEXT PRIMARY KEY,
    sql_text TEXT NOT NULL,
    sql_preview TEXT,
    source TEXT NOT NULL DEFAULT 'manual',
    timestamp TEXT NOT NULL,
    execution_time_ms INTEGER,
    rows_scanned INTEGER,
    user_name TEXT,
    database_name TEXT,
    app_name TEXT,
    metadata_json TEXT,
    status TEXT DEFAULT 'PENDING',
    decision TEXT,
    risk_score INTEGER DEFAULT 0,
    risk_level TEXT,
    cost_score INTEGER DEFAULT 0,
    is_injection INTEGER DEFAULT 0,
    injection_type TEXT,
    severity TEXT,
    services_affected TEXT,
    issues_json TEXT,
    impact_json TEXT,
    suggested_fix TEXT,
    explanation TEXT,
    profiling_json TEXT,
    impact_analysis_json TEXT,
    lineage_json TEXT,
    result_json TEXT,
    duration_ms INTEGER,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS threats (
    id TEXT PRIMARY KEY,
    query_id TEXT,
    threat_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    description TEXT,
    sql_text TEXT,
    detected_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (query_id) REFERENCES queries(id)
);

CREATE TABLE IF NOT EXISTS anomalies (
    id TEXT PRIMARY KEY,
    anomaly_type TEXT NOT NULL,
    description TEXT,
    metadata_json TEXT,
    query_count INTEGER,
    window_seconds INTEGER,
    detected_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_queries_timestamp ON queries(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_queries_decision ON queries(decision);
CREATE INDEX IF NOT EXISTS idx_queries_risk ON queries(risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_queries_injection ON queries(is_injection);
CREATE INDEX IF NOT EXISTS idx_threats_severity ON threats(severity);
CREATE INDEX IF NOT EXISTS idx_anomalies_type ON anomalies(anomaly_type);
CREATE INDEX IF NOT EXISTS idx_reviews_created_at ON reviews(created_at DESC);
"""


class Database:
    """Synchronous SQLite database wrapper (run in thread executor for async)."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()
            logger.info("📦 Database initialized: %s", self.db_path)
        return self._conn

    async def initialize(self):
        """Initialize the database and create tables."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._get_conn)

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    # ─── Query Operations ─────────────────────────────────

    async def save_query(
        self,
        query_id: str,
        sql: str,
        source: str,
        timestamp: str,
        metadata: dict = None,
        execution_time_ms: int = None,
        rows_scanned: int = None,
        user_name: str = None,
        database_name: str = None,
        app_name: str = None,
    ):
        """Save an ingested query."""
        preview = sql[:200] + ("..." if len(sql) > 200 else "")

        def _insert():
            conn = self._get_conn()
            conn.execute(
                """INSERT OR REPLACE INTO queries
                (id, sql_text, sql_preview, source, timestamp,
                 execution_time_ms, rows_scanned, user_name,
                 database_name, app_name, metadata_json, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')""",
                (
                    query_id, sql, preview, source, timestamp,
                    execution_time_ms, rows_scanned, user_name,
                    database_name, app_name,
                    json.dumps(metadata) if metadata else None,
                ),
            )
            conn.commit()

        async with self._lock:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _insert)

    async def update_query_result(self, query_id: str, result: dict):
        """Update a query with its analysis result."""
        def _update():
            conn = self._get_conn()
            conn.execute(
                """UPDATE queries SET
                    status = 'DONE',
                    decision = ?,
                    risk_score = ?,
                    risk_level = ?,
                    cost_score = ?,
                    is_injection = ?,
                    injection_type = ?,
                    severity = ?,
                    services_affected = ?,
                    issues_json = ?,
                    impact_json = ?,
                    suggested_fix = ?,
                    explanation = ?,
                    profiling_json = ?,
                    impact_analysis_json = ?,
                    lineage_json = ?,
                    result_json = ?,
                    duration_ms = ?
                WHERE id = ?""",
                (
                    result.get("decision"),
                    result.get("risk_score", 0),
                    result.get("risk_level"),
                    result.get("cost_score", 0),
                    1 if result.get("is_injection") else 0,
                    result.get("injection_type"),
                    result.get("severity"),
                    json.dumps(result.get("services_affected", [])),
                    json.dumps(result.get("issues", [])),
                    json.dumps(result.get("impact", [])),
                    result.get("suggested_fix"),
                    result.get("explanation"),
                    json.dumps(result.get("profiling")) if result.get("profiling") else None,
                    json.dumps(result.get("impact_analysis")) if result.get("impact_analysis") else None,
                    json.dumps(result.get("lineage")) if result.get("lineage") else None,
                    json.dumps(result),
                    result.get("duration_ms", 0),
                    query_id,
                ),
            )
            conn.commit()

        async with self._lock:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _update)

    async def get_recent_queries(self, limit: int = 50, decision: str = None, injection_only: bool = False) -> list[dict]:
        """Get recent queries with optional filters."""
        def _select():
            conn = self._get_conn()
            where_parts = []
            params = []
            if decision:
                where_parts.append("decision = ?")
                params.append(decision)
            if injection_only:
                where_parts.append("is_injection = 1")

            where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
            params.append(min(limit, 200))

            rows = conn.execute(
                f"SELECT * FROM queries {where_clause} ORDER BY timestamp DESC LIMIT ?",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

        async with self._lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _select)

    async def get_query(self, query_id: str) -> Optional[dict]:
        """Get a single query by ID."""
        def _select():
            conn = self._get_conn()
            row = conn.execute("SELECT * FROM queries WHERE id = ?", (query_id,)).fetchone()
            return dict(row) if row else None

        async with self._lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _select)

    async def get_stats(self) -> dict:
        """Get aggregate statistics."""
        def _stats():
            conn = self._get_conn()
            total = conn.execute("SELECT COUNT(*) FROM queries").fetchone()[0]
            analyzed = conn.execute("SELECT COUNT(*) FROM queries WHERE status = 'DONE'").fetchone()[0]
            blocked = conn.execute("SELECT COUNT(*) FROM queries WHERE decision = 'REJECT' OR decision = 'BLOCKED'").fetchone()[0]
            warnings = conn.execute("SELECT COUNT(*) FROM queries WHERE decision = 'WARNING'").fetchone()[0]
            approved = conn.execute("SELECT COUNT(*) FROM queries WHERE decision = 'APPROVE'").fetchone()[0]
            injections = conn.execute("SELECT COUNT(*) FROM queries WHERE is_injection = 1").fetchone()[0]
            threats = conn.execute("SELECT COUNT(*) FROM threats").fetchone()[0]
            return {
                "total_ingested": total,
                "total_analyzed": analyzed,
                "threats_blocked": blocked,
                "warnings": warnings,
                "approved": approved,
                "injections_detected": injections,
                "total_threats": threats,
            }

        async with self._lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _stats)

    # ─── Review History (MVP) ─────────────────────────────

    async def save_review(self, query: str, decision: str, risk_score: float, risk_level: str):
        """Persist one completed review for lightweight history browsing."""
        query_preview = query[:200] + ("..." if len(query) > 200 else "")

        def _insert():
            conn = self._get_conn()
            conn.execute(
                """INSERT INTO reviews (query, decision, risk_score, risk_level)
                VALUES (?, ?, ?, ?)""",
                (query, decision, risk_score, risk_level),
            )
            conn.commit()

        async with self._lock:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _insert)

    async def get_recent_reviews(self, limit: int = 50, decision: str = None) -> list[dict]:
        """Return recent persisted reviews in a shape compatible with existing UI."""
        def _select():
            conn = self._get_conn()
            params = []
            where = ""
            if decision:
                where = "WHERE decision = ?"
                params.append(decision)

            params.append(min(limit, 200))
            rows = conn.execute(
                f"""SELECT id, query, decision, risk_score, risk_level, created_at
                FROM reviews {where}
                ORDER BY created_at DESC
                LIMIT ?""",
                params,
            ).fetchall()

            return [
                {
                    "id": row["id"],
                    "sql_text": row["query"],
                    "sql_preview": row["query"][:200] + ("..." if len(row["query"]) > 200 else ""),
                    "decision": row["decision"],
                    "risk_score": row["risk_score"],
                    "risk_level": row["risk_level"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ]

        async with self._lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _select)

    # ─── Threat Operations ────────────────────────────────

    async def save_threat(self, query_id: str, threat_type: str, severity: str, description: str, sql_text: str = ""):
        """Record a threat."""
        def _insert():
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO threats (id, query_id, threat_type, severity, description, sql_text) VALUES (?, ?, ?, ?, ?, ?)",
                (str(uuid.uuid4())[:8], query_id, threat_type, severity, description, sql_text),
            )
            conn.commit()

        async with self._lock:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _insert)

    async def get_recent_threats(self, limit: int = 20) -> list[dict]:
        """Get recent threats."""
        def _select():
            conn = self._get_conn()
            rows = conn.execute(
                "SELECT * FROM threats ORDER BY detected_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

        async with self._lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _select)

    # ─── Anomaly Detection ────────────────────────────────

    async def check_frequency_anomaly(self, window_seconds: int = 60, threshold: int = 20) -> Optional[dict]:
        """Detect sudden spike in query frequency."""
        def _check():
            conn = self._get_conn()
            cutoff = (datetime.utcnow() - timedelta(seconds=window_seconds)).isoformat()
            count = conn.execute(
                "SELECT COUNT(*) FROM queries WHERE timestamp > ?", (cutoff,)
            ).fetchone()[0]

            if count >= threshold:
                anomaly_id = str(uuid.uuid4())[:8]
                conn.execute(
                    "INSERT INTO anomalies (id, anomaly_type, description, query_count, window_seconds) VALUES (?, ?, ?, ?, ?)",
                    (anomaly_id, "frequency_spike",
                     f"{count} queries in {window_seconds}s (threshold: {threshold})",
                     count, window_seconds),
                )
                conn.commit()
                return {
                    "id": anomaly_id,
                    "type": "frequency_spike",
                    "count": count,
                    "window": window_seconds,
                    "threshold": threshold,
                }
            return None

        async with self._lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _check)

    async def check_unusual_tables(self, table_name: str, window_hours: int = 24, threshold: int = 3) -> Optional[dict]:
        """Detect unusual table access patterns."""
        def _check():
            conn = self._get_conn()
            cutoff = (datetime.utcnow() - timedelta(hours=window_hours)).isoformat()
            count = conn.execute(
                "SELECT COUNT(*) FROM queries WHERE sql_text LIKE ? AND timestamp > ?",
                (f"%{table_name}%", cutoff),
            ).fetchone()[0]

            if count <= threshold:
                # Table rarely accessed — this access IS unusual
                anomaly_id = str(uuid.uuid4())[:8]
                conn.execute(
                    "INSERT INTO anomalies (id, anomaly_type, description, metadata_json) VALUES (?, ?, ?, ?)",
                    (anomaly_id, "unusual_table_access",
                     f"Rarely-accessed table '{table_name}' queried (only {count} times in {window_hours}h)",
                     json.dumps({"table": table_name, "access_count": count})),
                )
                conn.commit()
                return {
                    "id": anomaly_id,
                    "type": "unusual_table_access",
                    "table": table_name,
                    "access_count": count,
                }
            return None

        async with self._lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _check)


# ─── Global instance ─────────────────────────────────────

db = Database()
