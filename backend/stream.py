"""
Arc Genesis — Stream Layer
Event-driven message bus with Redis Streams (production) or AsyncQueue (zero-config).
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import AsyncGenerator, Optional, Callable, Awaitable

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "")
STREAM_KEY = "arc:queries"
CONSUMER_GROUP = "arc-workers"
REVIEW_STREAM_STEPS = [
    "Analyzing query...",
    "Validating security...",
    "Checking joins...",
    "Mapping lineage...",
    "Estimating cost...",
    "Calling LLM...",
    "Generating decision...",
]


def format_sse(payload: dict, event: str | None = None) -> str:
    """Encode a JSON payload as a Server-Sent Event frame."""
    lines = []
    if event:
        lines.append(f"event: {event}")

    body = json.dumps(payload)
    for line in body.splitlines():
        lines.append(f"data: {line}")

    return "\n".join(lines) + "\n\n"


@dataclass
class StreamMessage:
    id: str
    sql: str
    source: str
    timestamp: str
    metadata: dict = field(default_factory=dict)
    execution_time_ms: int = 0
    rows_scanned: int = 0
    user_name: str = ""
    database_name: str = ""
    app_name: str = ""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "sql": self.sql,
            "source": self.source,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
            "execution_time_ms": self.execution_time_ms,
            "rows_scanned": self.rows_scanned,
            "user_name": self.user_name,
            "database_name": self.database_name,
            "app_name": self.app_name,
        }


class InMemoryStream:
    """
    Zero-config async stream using asyncio.Queue.
    Works everywhere, no external deps. Good for hackathon / single-instance.
    """

    def __init__(self, maxsize: int = 500):
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._consumer_task: Optional[asyncio.Task] = None
        logger.info("⚡ Stream layer: In-Memory AsyncQueue (zero-config)")

    async def publish(self, message: StreamMessage):
        """Publish a message to the stream."""
        try:
            self.queue.put_nowait(message)
        except asyncio.QueueFull:
            # Drop oldest on overflow
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self.queue.put_nowait(message)

    async def consume(self, handler: Callable[[StreamMessage], Awaitable[None]]):
        """Consume messages from the stream and process with handler."""
        while True:
            try:
                message = await self.queue.get()
                await handler(message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Stream consumer error: %s", str(e))
                await asyncio.sleep(0.5)

    def start_consumer(self, handler: Callable[[StreamMessage], Awaitable[None]]):
        """Start background consumer task."""
        self._consumer_task = asyncio.create_task(self.consume(handler))
        logger.info("🔄 Stream consumer started")

    async def stop_consumer(self):
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        logger.info("Stream consumer stopped")

    @property
    def pending_count(self) -> int:
        return self.queue.qsize()


class RedisStream:
    """
    Production-grade stream using Redis Streams.
    Supports consumer groups, acknowledgment, and multi-worker.
    """

    def __init__(self, redis_url: str, stream_key: str = STREAM_KEY, group: str = CONSUMER_GROUP):
        self.redis_url = redis_url
        self.stream_key = stream_key
        self.group = group
        self._redis = None
        self._consumer_task: Optional[asyncio.Task] = None

    async def _get_redis(self):
        if self._redis is None:
            try:
                import redis.asyncio as aioredis
                self._redis = aioredis.from_url(self.redis_url, decode_responses=True)
                # Create consumer group if not exists
                try:
                    await self._redis.xgroup_create(
                        self.stream_key, self.group, id="0", mkstream=True
                    )
                except Exception:
                    pass  # Group already exists
                logger.info("🔴 Stream layer: Redis Streams (%s)", self.redis_url)
            except ImportError:
                logger.error("redis package not installed. Falling back to in-memory.")
                raise
        return self._redis

    async def publish(self, message: StreamMessage):
        """Publish to Redis Stream."""
        r = await self._get_redis()
        data = {
            "id": message.id,
            "sql": message.sql,
            "source": message.source,
            "timestamp": message.timestamp,
            "metadata": json.dumps(message.metadata),
            "execution_time_ms": str(message.execution_time_ms),
            "rows_scanned": str(message.rows_scanned),
            "user_name": message.user_name,
            "database_name": message.database_name,
            "app_name": message.app_name,
        }
        await r.xadd(self.stream_key, data, maxlen=5000)

    async def consume(self, handler: Callable[[StreamMessage], Awaitable[None]]):
        """Consume from Redis Stream with consumer group."""
        r = await self._get_redis()
        consumer_name = f"worker-{os.getpid()}"

        while True:
            try:
                results = await r.xreadgroup(
                    self.group, consumer_name,
                    {self.stream_key: ">"},
                    count=10, block=2000,
                )
                for stream_name, messages in results:
                    for msg_id, data in messages:
                        try:
                            message = StreamMessage(
                                id=data.get("id", msg_id),
                                sql=data.get("sql", ""),
                                source=data.get("source", "redis"),
                                timestamp=data.get("timestamp", ""),
                                metadata=json.loads(data.get("metadata", "{}")),
                                execution_time_ms=int(data.get("execution_time_ms", 0)),
                                rows_scanned=int(data.get("rows_scanned", 0)),
                                user_name=data.get("user_name", ""),
                                database_name=data.get("database_name", ""),
                                app_name=data.get("app_name", ""),
                            )
                            await handler(message)
                            await r.xack(self.stream_key, self.group, msg_id)
                        except Exception as e:
                            logger.error("Failed to process message %s: %s", msg_id, str(e))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Redis consume error: %s", str(e))
                await asyncio.sleep(2)

    def start_consumer(self, handler: Callable[[StreamMessage], Awaitable[None]]):
        self._consumer_task = asyncio.create_task(self.consume(handler))
        logger.info("🔄 Redis stream consumer started")

    async def stop_consumer(self):
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        if self._redis:
            await self._redis.close()

    @property
    def pending_count(self) -> int:
        return 0  # Redis manages this


def create_stream() -> InMemoryStream | RedisStream:
    """Factory: create the appropriate stream backend."""
    if REDIS_URL:
        try:
            import redis.asyncio  # noqa: F401
            return RedisStream(REDIS_URL)
        except ImportError:
            logger.warning("Redis URL configured but redis package not installed. Using in-memory.")
    return InMemoryStream()
