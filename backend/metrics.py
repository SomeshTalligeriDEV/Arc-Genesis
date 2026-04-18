"""
Arc Genesis — Review Metrics
Small timing helpers for the hackathon review pipeline.
"""

import time
from dataclasses import dataclass, field
from typing import Awaitable, TypeVar

T = TypeVar("T")


def elapsed_ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


async def measure_async(awaitable: Awaitable[T]) -> tuple[T, int]:
    start = time.monotonic()
    result = await awaitable
    return result, elapsed_ms(start)


@dataclass
class ReviewMetrics:
    started_at: float = field(default_factory=time.monotonic)
    total_latency_ms: int = 0
    altimate_time_ms: int = 0
    llm_time_ms: int = 0

    @property
    def analysis_time_ms(self) -> int:
        return max(self.total_latency_ms - self.llm_time_ms, 0)

    def finish(self) -> None:
        self.total_latency_ms = elapsed_ms(self.started_at)

