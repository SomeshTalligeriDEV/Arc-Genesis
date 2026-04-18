"""
Arc Genesis — Real-Time Ingestion Layer (v3)
Stream-backed + persistent event bus with SSE broadcasting.
"""

import asyncio
import json
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import AsyncGenerator

from stream import StreamMessage, create_stream

logger = logging.getLogger(__name__)

MAX_EVENTS = 500  # In-memory buffer for SSE
MAX_QUEUE = 100


class EventStatus(str, Enum):
    PENDING = "PENDING"
    ANALYZING = "ANALYZING"
    DONE = "DONE"
    ERROR = "ERROR"


@dataclass
class QueryEvent:
    id: str
    sql: str
    source: str  # sdk | webhook | manual | simulated | warehouse:pg | warehouse:mysql
    timestamp: str
    metadata: dict = field(default_factory=dict)
    status: EventStatus = EventStatus.PENDING
    result: dict | None = None
    # v3: Real metadata
    execution_time_ms: int = 0
    rows_scanned: int = 0
    user_name: str = ""
    database_name: str = ""
    app_name: str = ""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "sql": self.sql[:200] + ("..." if len(self.sql) > 200 else ""),
            "sql_full": self.sql,
            "source": self.source,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
            "status": self.status.value,
            "result": self.result,
            "execution_time_ms": self.execution_time_ms,
            "rows_scanned": self.rows_scanned,
            "user_name": self.user_name,
            "database_name": self.database_name,
            "app_name": self.app_name,
        }


class IngestionEngine:
    """
    Central event bus for Arc Genesis v3.
    Flow: ingest → stream → worker → analyze → persist → broadcast SSE
    """

    def __init__(self):
        self.queue: asyncio.Queue[QueryEvent] = asyncio.Queue(maxsize=MAX_QUEUE)
        self.events: deque[QueryEvent] = deque(maxlen=MAX_EVENTS)
        self.subscribers: list[asyncio.Queue] = []
        self.stats = {
            "total_ingested": 0,
            "total_analyzed": 0,
            "threats_blocked": 0,
            "warnings": 0,
            "approved": 0,
            "injections_detected": 0,
        }
        self._worker_task: asyncio.Task | None = None
        self._stream = create_stream()
        self._db = None  # Set in start_worker

    async def start_worker(self, analyze_fn, db=None):
        """Start background worker that processes the queue."""
        self._analyze_fn = analyze_fn
        self._db = db

        # Load persisted stats if DB available
        if self._db:
            try:
                persisted = await self._db.get_stats()
                self.stats.update(persisted)
            except Exception:
                pass

        # Start stream consumer
        self._stream.start_consumer(self._handle_stream_message)

        # Start queue worker (for backward compat with direct queue usage)
        self._worker_task = asyncio.create_task(self._process_loop())
        logger.info("🔄 Ingestion engine started (stream + queue)")

    async def stop_worker(self):
        await self._stream.stop_consumer()
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info("Ingestion engine stopped")

    async def ingest(
        self,
        sql: str,
        source: str = "manual",
        metadata: dict = None,
        execution_time_ms: int = 0,
        rows_scanned: int = 0,
        user_name: str = "",
        database_name: str = "",
        app_name: str = "",
    ) -> QueryEvent:
        """Add a query to the processing pipeline via stream."""
        event = QueryEvent(
            id=str(uuid.uuid4())[:8],
            sql=sql.strip(),
            source=source,
            timestamp=datetime.utcnow().isoformat() + "Z",
            metadata=metadata or {},
            execution_time_ms=execution_time_ms,
            rows_scanned=rows_scanned,
            user_name=user_name,
            database_name=database_name,
            app_name=app_name or (metadata or {}).get("app", ""),
        )

        # Add to in-memory buffer for SSE
        self.events.appendleft(event)
        self.stats["total_ingested"] += 1

        # Persist to DB
        if self._db:
            try:
                await self._db.save_query(
                    query_id=event.id,
                    sql=event.sql,
                    source=event.source,
                    timestamp=event.timestamp,
                    metadata=event.metadata,
                    execution_time_ms=execution_time_ms,
                    rows_scanned=rows_scanned,
                    user_name=user_name,
                    database_name=database_name,
                    app_name=event.app_name,
                )
            except Exception as e:
                logger.error("DB save failed: %s", str(e))

        # Publish to stream for processing
        msg = StreamMessage(
            id=event.id,
            sql=event.sql,
            source=event.source,
            timestamp=event.timestamp,
            metadata=event.metadata,
            execution_time_ms=execution_time_ms,
            rows_scanned=rows_scanned,
            user_name=user_name,
            database_name=database_name,
            app_name=event.app_name,
        )
        await self._stream.publish(msg)

        await self._broadcast({"type": "ingested", "event": event.to_dict()})
        return event

    async def _handle_stream_message(self, msg: StreamMessage):
        """Process a message from the stream (Redis or in-memory)."""
        # Find or create event
        event = None
        for e in self.events:
            if e.id == msg.id:
                event = e
                break

        if event is None:
            # Message from external source (warehouse, another worker)
            event = QueryEvent(
                id=msg.id,
                sql=msg.sql,
                source=msg.source,
                timestamp=msg.timestamp,
                metadata=msg.metadata,
                execution_time_ms=msg.execution_time_ms,
                rows_scanned=msg.rows_scanned,
                user_name=msg.user_name,
                database_name=msg.database_name,
                app_name=msg.app_name,
            )
            self.events.appendleft(event)
            self.stats["total_ingested"] += 1

            # Persist
            if self._db:
                try:
                    await self._db.save_query(
                        query_id=event.id,
                        sql=event.sql,
                        source=event.source,
                        timestamp=event.timestamp,
                        metadata=event.metadata,
                        execution_time_ms=msg.execution_time_ms,
                        rows_scanned=msg.rows_scanned,
                        user_name=msg.user_name,
                        database_name=msg.database_name,
                        app_name=msg.app_name,
                    )
                except Exception:
                    pass

            await self._broadcast({"type": "ingested", "event": event.to_dict()})

        # Analyze
        event.status = EventStatus.ANALYZING
        await self._broadcast({"type": "analyzing", "event": event.to_dict()})

        try:
            result = await self._analyze_fn(event.sql)
            event.status = EventStatus.DONE
            event.result = result
            self.stats["total_analyzed"] += 1

            # Update stats
            decision = result.get("decision", "")
            if result.get("status") == "BLOCKED" or decision == "REJECT":
                self.stats["threats_blocked"] += 1
            elif decision == "WARNING":
                self.stats["warnings"] += 1
            elif decision == "APPROVE":
                self.stats["approved"] += 1

            if result.get("is_injection"):
                self.stats["injections_detected"] += 1

            # Persist result
            if self._db:
                try:
                    await self._db.update_query_result(event.id, result)
                    # Save threat if blocked/injection
                    if result.get("is_injection") or result.get("status") == "BLOCKED":
                        await self._db.save_threat(
                            query_id=event.id,
                            threat_type=result.get("injection_type", "security_violation"),
                            severity=result.get("severity", "HIGH"),
                            description=result.get("explanation", ""),
                            sql_text=event.sql[:500],
                        )
                    # Check anomalies
                    await self._db.check_frequency_anomaly()
                except Exception as e:
                    logger.error("DB persist error: %s", str(e))

        except Exception as e:
            logger.error("Analysis failed for %s: %s", event.id, str(e))
            event.status = EventStatus.ERROR
            event.result = {"error": str(e)}

        await self._broadcast({"type": "result", "event": event.to_dict()})

    async def _process_loop(self):
        """Legacy queue worker (backward compat for direct queue usage)."""
        while True:
            try:
                event = await self.queue.get()
                # Pass through to stream handler
                msg = StreamMessage(
                    id=event.id,
                    sql=event.sql,
                    source=event.source,
                    timestamp=event.timestamp,
                    metadata=event.metadata,
                )
                await self._handle_stream_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Worker error: %s", str(e))
                await asyncio.sleep(1)

    def subscribe(self) -> asyncio.Queue:
        """Create a new SSE subscriber."""
        q: asyncio.Queue = asyncio.Queue(maxsize=50)
        self.subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self.subscribers:
            self.subscribers.remove(q)

    async def _broadcast(self, message: dict):
        """Send message to all SSE subscribers."""
        dead = []
        for sub in self.subscribers:
            try:
                sub.put_nowait(message)
            except asyncio.QueueFull:
                dead.append(sub)
        for d in dead:
            self.subscribers.remove(d)

    def get_recent_events(self, limit: int = 50) -> list[dict]:
        return [e.to_dict() for e in list(self.events)[:limit]]

    def get_stats(self) -> dict:
        return {
            **self.stats,
            "queue_size": self.queue.qsize(),
            "stream_pending": self._stream.pending_count,
            "sse_subscribers": len(self.subscribers),
        }

    async def sse_stream(self, subscriber: asyncio.Queue) -> AsyncGenerator[str, None]:
        try:
            while True:
                msg = await subscriber.get()
                yield f"data: {json.dumps(msg)}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            self.unsubscribe(subscriber)

    async def ingest_from_stream(self, msg: StreamMessage):
        """Public entry point for warehouse poller / external sources."""
        await self._handle_stream_message(msg)

    async def broadcast_result(self, sql: str, source: str, result: dict):
        """
        Broadcast a pre-analyzed result directly to the live feed.
        Used by /stream-review so manual reviews appear in the dashboard
        without triggering a second analysis pass.
        """
        event = QueryEvent(
            id=result.get("id", str(uuid.uuid4())[:8]),
            sql=sql.strip(),
            source=source,
            timestamp=datetime.utcnow().isoformat() + "Z",
            status=EventStatus.DONE,
            result=result,
        )
        self.events.appendleft(event)
        self.stats["total_ingested"] += 1
        self.stats["total_analyzed"] += 1

        decision = result.get("decision", "")
        if result.get("status") == "BLOCKED" or decision == "REJECT":
            self.stats["threats_blocked"] += 1
        elif decision == "WARNING":
            self.stats["warnings"] += 1
        elif decision == "APPROVE":
            self.stats["approved"] += 1
        if result.get("is_injection"):
            self.stats["injections_detected"] += 1

        logger.info("📤 Event published to live feed: %s → %s", event.id, decision or result.get("status"))
        await self._broadcast({"type": "ingested", "event": event.to_dict()})
        await self._broadcast({"type": "result",   "event": event.to_dict()})


# ─── Demo stream (kept for hackathon) ────────────────────

DEMO_QUERIES = [
    {"sql": "SELECT * FROM user_transactions WHERE amount > 10000", "source": "webhook", "metadata": {"app": "payments-api"}},
    {"sql": "SELECT user_id, email, ssn FROM users", "source": "sdk", "metadata": {"app": "data-pipeline"}},
    {"sql": "DELETE FROM logs", "source": "webhook", "metadata": {"app": "cleanup-cron"}},
    {"sql": "SELECT o.id, c.name, SUM(oi.total) FROM orders o JOIN customers c ON c.id = o.customer_id JOIN order_items oi ON oi.order_id = o.id WHERE o.created_at > '2024-01-01' GROUP BY o.id, c.name ORDER BY SUM(oi.total) DESC LIMIT 100", "source": "sdk", "metadata": {"app": "analytics-dash"}},
    {"sql": "SELECT * FROM products JOIN categories", "source": "webhook", "metadata": {"app": "catalog-service"}},
    {"sql": "UPDATE inventory SET stock = 0 WHERE product_id = 42", "source": "sdk", "metadata": {"app": "warehouse-api"}},
    {"sql": "SELECT customer_name, order_date FROM orders WHERE status = 'pending' ORDER BY order_date DESC LIMIT 50", "source": "sdk", "metadata": {"app": "ops-dashboard"}},
    {"sql": "DROP TABLE temp_staging;", "source": "webhook", "metadata": {"app": "etl-pipeline"}},
    {"sql": "SELECT COUNT(*) as total, status FROM payments GROUP BY status", "source": "sdk", "metadata": {"app": "finance-report"}},
    {"sql": "SELECT a.*, b.* FROM accounts a, balances b", "source": "webhook", "metadata": {"app": "reconciliation"}},
    # v3: Injection examples
    {"sql": "SELECT * FROM users WHERE username = '' OR 1=1 --'", "source": "webhook", "metadata": {"app": "auth-service"}},
    {"sql": "SELECT * FROM users WHERE id = 1 UNION SELECT username, password FROM admin_users", "source": "sdk", "metadata": {"app": "suspicious-client"}},
]


async def run_demo_stream(engine: IngestionEngine, interval: float = 4.0):
    """Simulate incoming queries for demo/hackathon."""
    idx = 0
    while True:
        try:
            q = DEMO_QUERIES[idx % len(DEMO_QUERIES)]
            await engine.ingest(q["sql"], source=q["source"], metadata=q["metadata"])
            idx += 1
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Demo stream error: %s", str(e))
            await asyncio.sleep(2)
