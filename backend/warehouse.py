"""
Arc Genesis — Warehouse Connectors
Real-time query capture from PostgreSQL, MySQL, Snowflake, BigQuery.
Uses incremental cursors to avoid full scans. Retry + backoff built in.
"""

import asyncio
import hashlib
import logging
import os
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from stream import StreamMessage

logger = logging.getLogger(__name__)


@dataclass
class WarehouseQuery:
    sql: str
    execution_time_ms: int = 0
    rows_scanned: int = 0
    user_name: str = ""
    database_name: str = ""
    query_id: str = ""
    timestamp: str = ""


class WarehouseAdapter(ABC):
    """Base class for warehouse connectors."""

    def __init__(self, name: str):
        self.name = name
        self._connected = False
        self._last_cursor: Optional[str] = None  # Incremental cursor
        self._seen_hashes: set[str] = set()  # Dedup (bounded to 10K)
        self._error_count = 0
        self._max_errors = 5

    @abstractmethod
    async def connect(self) -> bool:
        ...

    @abstractmethod
    async def disconnect(self):
        ...

    @abstractmethod
    async def fetch_new_queries(self) -> list[WarehouseQuery]:
        ...

    def _hash_query(self, sql: str) -> str:
        return hashlib.md5(sql.encode()).hexdigest()[:12]

    def _dedup(self, queries: list[WarehouseQuery]) -> list[WarehouseQuery]:
        """Deduplicate queries by hash, keeping the set bounded."""
        new = []
        for q in queries:
            h = self._hash_query(q.sql)
            if h not in self._seen_hashes:
                self._seen_hashes.add(h)
                new.append(q)
        # Bound the set
        if len(self._seen_hashes) > 10000:
            self._seen_hashes = set(list(self._seen_hashes)[-5000:])
        return new

    @property
    def status(self) -> dict:
        return {
            "name": self.name,
            "connected": self._connected,
            "last_cursor": self._last_cursor,
            "queries_seen": len(self._seen_hashes),
            "error_count": self._error_count,
        }


class PostgreSQLAdapter(WarehouseAdapter):
    """
    Poll pg_stat_statements for real query data.
    Requires: pg_stat_statements extension enabled.
    """

    def __init__(self):
        super().__init__("postgresql")
        self._conn = None

    async def connect(self) -> bool:
        host = os.getenv("PG_HOST", "")
        if not host:
            logger.debug("PostgreSQL: No PG_HOST configured")
            return False

        try:
            import asyncpg
            self._conn = await asyncpg.connect(
                host=host,
                port=int(os.getenv("PG_PORT", "5432")),
                database=os.getenv("PG_DATABASE", "postgres"),
                user=os.getenv("PG_USER", "postgres"),
                password=os.getenv("PG_PASSWORD", ""),
                timeout=10,
            )
            self._connected = True
            logger.info("🐘 PostgreSQL connected: %s:%s/%s", host, os.getenv("PG_PORT", "5432"), os.getenv("PG_DATABASE", "postgres"))

            # Check if pg_stat_statements is available
            try:
                await self._conn.fetchval("SELECT 1 FROM pg_stat_statements LIMIT 1")
                logger.info("   pg_stat_statements: available")
            except Exception:
                logger.warning("   pg_stat_statements: NOT available — will use pg_stat_activity")

            return True
        except ImportError:
            logger.warning("asyncpg not installed. Run: pip install asyncpg")
            return False
        except Exception as e:
            logger.error("PostgreSQL connection failed: %s", str(e))
            self._error_count += 1
            return False

    async def disconnect(self):
        if self._conn:
            await self._conn.close()
            self._connected = False

    async def fetch_new_queries(self) -> list[WarehouseQuery]:
        if not self._conn:
            return []

        try:
            # Try pg_stat_statements first
            try:
                rows = await self._conn.fetch("""
                    SELECT query, total_exec_time, rows, calls, queryid::text
                    FROM pg_stat_statements
                    WHERE query NOT LIKE '%pg_stat%'
                      AND query NOT LIKE '%pg_catalog%'
                      AND length(query) > 10
                    ORDER BY total_exec_time DESC
                    LIMIT 20
                """)
                queries = []
                for row in rows:
                    queries.append(WarehouseQuery(
                        sql=row["query"],
                        execution_time_ms=int(row["total_exec_time"]),
                        rows_scanned=int(row["rows"]),
                        query_id=str(row.get("queryid", "")),
                        database_name=os.getenv("PG_DATABASE", ""),
                        timestamp=datetime.utcnow().isoformat() + "Z",
                    ))
                self._error_count = 0
                return self._dedup(queries)

            except Exception:
                # Fallback to pg_stat_activity (live queries)
                rows = await self._conn.fetch("""
                    SELECT query, state, usename,
                           EXTRACT(EPOCH FROM (now() - query_start)) * 1000 AS exec_time_ms,
                           datname
                    FROM pg_stat_activity
                    WHERE state = 'active'
                      AND query NOT LIKE '%pg_stat%'
                      AND query != ''
                      AND pid != pg_backend_pid()
                    ORDER BY query_start DESC
                    LIMIT 20
                """)
                queries = []
                for row in rows:
                    queries.append(WarehouseQuery(
                        sql=row["query"],
                        execution_time_ms=int(row["exec_time_ms"] or 0),
                        user_name=row["usename"] or "",
                        database_name=row["datname"] or "",
                        timestamp=datetime.utcnow().isoformat() + "Z",
                    ))
                self._error_count = 0
                return self._dedup(queries)

        except Exception as e:
            self._error_count += 1
            logger.error("PostgreSQL fetch error: %s", str(e))
            if self._error_count >= self._max_errors:
                logger.error("Too many errors — disconnecting PostgreSQL")
                await self.disconnect()
            return []


class MySQLAdapter(WarehouseAdapter):
    """
    Poll performance_schema for real query data.
    """

    def __init__(self):
        super().__init__("mysql")
        self._pool = None

    async def connect(self) -> bool:
        host = os.getenv("MYSQL_HOST", "")
        if not host:
            logger.debug("MySQL: No MYSQL_HOST configured")
            return False

        try:
            import aiomysql
            self._pool = await aiomysql.create_pool(
                host=host,
                port=int(os.getenv("MYSQL_PORT", "3306")),
                db=os.getenv("MYSQL_DATABASE", ""),
                user=os.getenv("MYSQL_USER", "root"),
                password=os.getenv("MYSQL_PASSWORD", ""),
                maxsize=3,
                connect_timeout=10,
            )
            self._connected = True
            logger.info("🐬 MySQL connected: %s:%s/%s", host, os.getenv("MYSQL_PORT", "3306"), os.getenv("MYSQL_DATABASE", ""))
            return True
        except ImportError:
            logger.warning("aiomysql not installed. Run: pip install aiomysql")
            return False
        except Exception as e:
            logger.error("MySQL connection failed: %s", str(e))
            self._error_count += 1
            return False

    async def disconnect(self):
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._connected = False

    async def fetch_new_queries(self) -> list[WarehouseQuery]:
        if not self._pool:
            return []

        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        SELECT DIGEST_TEXT, SUM_TIMER_WAIT/1000000000 AS exec_time_ms,
                               SUM_ROWS_EXAMINED, SUM_ROWS_SENT, SCHEMA_NAME, FIRST_SEEN
                        FROM performance_schema.events_statements_summary_by_digest
                        WHERE DIGEST_TEXT IS NOT NULL
                          AND SCHEMA_NAME IS NOT NULL
                          AND LENGTH(DIGEST_TEXT) > 10
                        ORDER BY LAST_SEEN DESC
                        LIMIT 20
                    """)
                    rows = await cur.fetchall()
                    queries = []
                    for row in rows:
                        queries.append(WarehouseQuery(
                            sql=row[0],
                            execution_time_ms=int(row[1] or 0),
                            rows_scanned=int(row[2] or 0),
                            database_name=row[4] or "",
                            timestamp=datetime.utcnow().isoformat() + "Z",
                        ))
                    self._error_count = 0
                    return self._dedup(queries)
        except Exception as e:
            self._error_count += 1
            logger.error("MySQL fetch error: %s", str(e))
            return []


class SnowflakeAdapter(WarehouseAdapter):
    """Poll Snowflake QUERY_HISTORY for recent queries."""

    def __init__(self):
        super().__init__("snowflake")
        self._conn = None

    async def connect(self) -> bool:
        account = os.getenv("SNOWFLAKE_ACCOUNT", "")
        if not account:
            return False
        try:
            import snowflake.connector
            loop = asyncio.get_event_loop()
            self._conn = await loop.run_in_executor(None, lambda: snowflake.connector.connect(
                account=account,
                user=os.getenv("SNOWFLAKE_USER", ""),
                password=os.getenv("SNOWFLAKE_PASSWORD", ""),
                database=os.getenv("SNOWFLAKE_DATABASE", ""),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", ""),
            ))
            self._connected = True
            logger.info("❄️ Snowflake connected")
            return True
        except ImportError:
            logger.warning("snowflake-connector-python not installed")
            return False
        except Exception as e:
            logger.error("Snowflake connection failed: %s", str(e))
            return False

    async def disconnect(self):
        if self._conn:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._conn.close)
            self._connected = False

    async def fetch_new_queries(self) -> list[WarehouseQuery]:
        if not self._conn:
            return []
        try:
            loop = asyncio.get_event_loop()
            cursor = self._conn.cursor()

            cutoff = (datetime.utcnow() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")

            def _fetch():
                cursor.execute(f"""
                    SELECT QUERY_TEXT, TOTAL_ELAPSED_TIME, ROWS_PRODUCED,
                           USER_NAME, DATABASE_NAME, START_TIME
                    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                        END_TIME_RANGE_START => '{cutoff}'::TIMESTAMP_LTZ
                    ))
                    ORDER BY START_TIME DESC
                    LIMIT 20
                """)
                return cursor.fetchall()

            rows = await loop.run_in_executor(None, _fetch)
            queries = []
            for row in rows:
                queries.append(WarehouseQuery(
                    sql=row[0],
                    execution_time_ms=int(row[1] or 0),
                    rows_scanned=int(row[2] or 0),
                    user_name=row[3] or "",
                    database_name=row[4] or "",
                    timestamp=str(row[5]) if row[5] else datetime.utcnow().isoformat() + "Z",
                ))
            return self._dedup(queries)
        except Exception as e:
            self._error_count += 1
            logger.error("Snowflake fetch error: %s", str(e))
            return []


class BigQueryAdapter(WarehouseAdapter):
    """Poll BigQuery JOBS for recent queries."""

    def __init__(self):
        super().__init__("bigquery")
        self._client = None

    async def connect(self) -> bool:
        project_id = os.getenv("BIGQUERY_PROJECT_ID", "")
        if not project_id:
            return False
        try:
            from google.cloud import bigquery
            loop = asyncio.get_event_loop()
            creds_path = os.getenv("BIGQUERY_CREDENTIALS_PATH", "")
            if creds_path:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
            self._client = await loop.run_in_executor(
                None, lambda: bigquery.Client(project=project_id)
            )
            self._connected = True
            logger.info("🔷 BigQuery connected: project=%s", project_id)
            return True
        except ImportError:
            logger.warning("google-cloud-bigquery not installed")
            return False
        except Exception as e:
            logger.error("BigQuery connection failed: %s", str(e))
            return False

    async def disconnect(self):
        self._client = None
        self._connected = False

    async def fetch_new_queries(self) -> list[WarehouseQuery]:
        if not self._client:
            return []
        try:
            loop = asyncio.get_event_loop()
            project_id = os.getenv("BIGQUERY_PROJECT_ID", "")

            def _fetch():
                query = f"""
                    SELECT query, total_bytes_processed, total_slot_ms,
                           user_email, creation_time
                    FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
                    WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
                      AND job_type = 'QUERY'
                      AND state = 'DONE'
                    ORDER BY creation_time DESC
                    LIMIT 20
                """
                return list(self._client.query(query).result())

            rows = await loop.run_in_executor(None, _fetch)
            queries = []
            for row in rows:
                queries.append(WarehouseQuery(
                    sql=row.query,
                    execution_time_ms=int((row.total_slot_ms or 0)),
                    rows_scanned=int((row.total_bytes_processed or 0) / 100),  # Rough row estimate
                    user_name=row.user_email or "",
                    database_name=project_id,
                    timestamp=row.creation_time.isoformat() + "Z" if row.creation_time else datetime.utcnow().isoformat() + "Z",
                ))
            return self._dedup(queries)
        except Exception as e:
            self._error_count += 1
            logger.error("BigQuery fetch error: %s", str(e))
            return []


# ─── Warehouse Poller ────────────────────────────────────

class WarehousePoller:
    """
    Background poller that connects to configured warehouses
    and streams captured queries into the message stream.
    """

    def __init__(self, publish_fn):
        """
        Args:
            publish_fn: Async function to publish StreamMessage
        """
        self.publish_fn = publish_fn
        self.adapters: list[WarehouseAdapter] = []
        self._poll_task: Optional[asyncio.Task] = None
        self._interval = int(os.getenv("WAREHOUSE_POLL_INTERVAL", "10"))
        self._running = False

    async def start(self):
        """Connect to all configured warehouses and start polling."""
        # Try each adapter
        for AdapterClass in [PostgreSQLAdapter, MySQLAdapter, SnowflakeAdapter, BigQueryAdapter]:
            adapter = AdapterClass()
            try:
                if await adapter.connect():
                    self.adapters.append(adapter)
            except Exception as e:
                logger.debug("Adapter %s skipped: %s", AdapterClass.__name__, str(e))

        if self.adapters:
            logger.info("🏭 Warehouse poller: %d adapter(s) connected", len(self.adapters))
            self._running = True
            self._poll_task = asyncio.create_task(self._poll_loop())
        else:
            logger.info("🏭 Warehouse poller: no warehouses configured (demo mode available)")

    async def stop(self):
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        for adapter in self.adapters:
            await adapter.disconnect()
        logger.info("Warehouse poller stopped")

    async def _poll_loop(self):
        """Poll all adapters on interval with backoff on errors."""
        backoff = self._interval
        while self._running:
            try:
                for adapter in self.adapters:
                    if not adapter._connected:
                        continue
                    queries = await adapter.fetch_new_queries()
                    for q in queries:
                        msg = StreamMessage(
                            id=str(uuid.uuid4())[:8],
                            sql=q.sql,
                            source=f"warehouse:{adapter.name}",
                            timestamp=q.timestamp or datetime.utcnow().isoformat() + "Z",
                            execution_time_ms=q.execution_time_ms,
                            rows_scanned=q.rows_scanned,
                            user_name=q.user_name,
                            database_name=q.database_name,
                        )
                        await self.publish_fn(msg)

                backoff = self._interval  # Reset on success
                await asyncio.sleep(backoff)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Poller error: %s", str(e))
                backoff = min(backoff * 2, 60)  # Exponential backoff, max 60s
                await asyncio.sleep(backoff)

    @property
    def status(self) -> dict:
        return {
            "running": self._running,
            "poll_interval_s": self._interval,
            "adapters": [a.status for a in self.adapters],
            "connected_count": sum(1 for a in self.adapters if a._connected),
        }
