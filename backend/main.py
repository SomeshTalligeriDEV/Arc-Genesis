"""
Arc Genesis — FastAPI Backend (v3)
Real-time AI Query Copilot with persistence, streaming, and warehouse integration.

Routes:
  POST /review       — Manual SQL review
  POST /ingest       — Webhook: receive queries from SDK/external systems
  POST /ingest/batch — Batch ingestion
  GET  /stream       — SSE: real-time event stream
  GET  /events       — Recent events list (supports filters)
  GET  /stats        — Dashboard statistics
  POST /ask          — Natural language Q&A
  POST /demo/start   — Start simulated query stream
  POST /demo/stop    — Stop simulated query stream
  GET  /health       — Health check
  GET  /traces       — Altimate trace sessions
  GET  /warehouse/status — Connected warehouse status
  GET  /threats      — Recent threats
  GET  /history      — Persisted query history
"""

import asyncio
import json
import logging
import re
import time
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from analyzer import analyze_sql, generate_fix, get_altimate_traces
from impact import analyze_impact
from ingestion import IngestionEngine, run_demo_stream
from llm import call_llm, ask_natural_language
from security import check_sql_security, ThreatLevel
from profiling import profile_query
from persistence import db
from warehouse import WarehousePoller
from stream import StreamMessage

# ─── Logging ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(name)s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("arc-genesis")

# ─── Globals ──────────────────────────────────────────────
engine = IngestionEngine()
demo_task: asyncio.Task | None = None
warehouse_poller: WarehousePoller | None = None


async def _full_analyze(sql: str) -> dict:
    """Complete pipeline: security → analyze → profile → impact → LLM → response."""
    start = time.monotonic()

    # 1. Security gate (includes injection detection)
    security = check_sql_security(sql)
    if security.level == ThreatLevel.BLOCKED:
        tables = list(set(
            re.findall(r'\bFROM\s+([a-zA-Z_]\w*)', sql, re.IGNORECASE) +
            re.findall(r'\bJOIN\s+([a-zA-Z_]\w*)', sql, re.IGNORECASE)
        ))
        blocked_impact = analyze_impact(tables, [], _get_query_type(sql))
        profiling = profile_query(sql)
        return {
            "status": "BLOCKED",
            "decision": "REJECT",
            "issues": [security.reason or "Dangerous SQL"],
            "impact": ["Query blocked — will NOT be analyzed"],
            "risk_level": "HIGH",
            "cost_score": 0,
            "reasoning": f"Security: {security.matched_pattern}",
            "explanation": "This query contains a dangerous operation that could destroy data or compromise security. It was blocked automatically.",
            "suggested_fix": None,
            "lineage": _extract_lineage(sql),
            "impact_analysis": blocked_impact.to_dict(),
            "profiling": profiling.to_dict(),
            "is_injection": security.is_injection,
            "injection_type": security.injection_type,
            "injection_patterns": security.injection_patterns,
            "risk_score": security.risk_score,
            "severity": security.severity.value,
            "duration_ms": _elapsed(start),
        }

    # 2. AST analysis
    analysis = await analyze_sql(sql)

    # 3. Data profiling
    profiling = profile_query(sql)

    # 4. Impact analysis
    issue_dicts = [i.to_dict() for i in analysis.issues]
    impact = analyze_impact(analysis.tables, issue_dicts, analysis.query_type)

    # 5. LLM decision
    review = await call_llm(sql, analysis.output)

    # 6. Auto-fix
    auto_fix = generate_fix(sql, analysis.issues)
    final_fix = review.suggested_fix if review.suggested_fix != "No changes needed" else auto_fix

    return {
        "status": "REVIEWED",
        "decision": review.decision.value,
        "issues": review.issues,
        "impact": review.impact,
        "suggested_fix": final_fix,
        "risk_level": review.risk_level.value,
        "cost_score": review.cost_score,
        "reasoning": review.reasoning,
        "explanation": review.explanation,
        "analysis_source": analysis.source,
        "query_type": analysis.query_type,
        "complexity": analysis.complexity_score,
        "tables": analysis.tables,
        "security_warning": security.reason if security.level == ThreatLevel.WARNING else None,
        "security_threats": security.threats if security.threats else None,
        "is_injection": security.is_injection,
        "injection_type": security.injection_type,
        "injection_patterns": security.injection_patterns,
        "risk_score": security.risk_score,
        "severity": security.severity.value,
        "lineage": _extract_lineage(sql),
        "impact_analysis": impact.to_dict(),
        "profiling": profiling.to_dict(),
        "original_sql": sql,
        "duration_ms": _elapsed(start),
        "source": review.source,
    }


# ─── App Setup ────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global warehouse_poller

    logger.info("⚡ Arc Genesis v3 starting — Production Pipeline")

    # Initialize persistence
    await db.initialize()
    logger.info("📦 Database ready")

    # Start ingestion engine
    await engine.start_worker(_full_analyze, db=db)

    # Start warehouse poller
    async def publish_to_engine(msg: StreamMessage):
        await engine.ingest_from_stream(msg)

    warehouse_poller = WarehousePoller(publish_to_engine)
    await warehouse_poller.start()

    yield

    # Shutdown
    if warehouse_poller:
        await warehouse_poller.stop()
    await engine.stop_worker()
    db.close()
    logger.info("Arc Genesis stopped")


app = FastAPI(
    title="Arc Genesis",
    description="Real-Time AI SQL Observability Platform — Detect, Analyze, Protect",
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Models ───────────────────────────────────────────────

class ReviewRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=50000)


class IngestRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=50000)
    source: str = Field(default="webhook")
    metadata: dict = Field(default_factory=dict)
    execution_time_ms: int = Field(default=0)
    rows_scanned: int = Field(default=0)
    user_name: str = Field(default="")
    database_name: str = Field(default="")
    app_name: str = Field(default="")


class IngestBatchRequest(BaseModel):
    queries: list[IngestRequest]


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    context: str = Field(default="")


# ─── Routes ───────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "arc-genesis",
        "version": "3.0.0",
        "features": [
            "impact-analysis", "real-time-ingestion", "auto-fix",
            "nl-query", "injection-detection", "data-profiling",
            "persistence", "warehouse-polling", "stream-backed",
        ],
        "engine": engine.get_stats(),
        "warehouse": warehouse_poller.status if warehouse_poller else None,
    }


@app.post("/review")
async def review_sql(req: ReviewRequest):
    """Manual SQL review (full pipeline)."""
    result = await _full_analyze(req.sql.strip())
    return result


@app.post("/ingest")
async def ingest_query(req: IngestRequest):
    """Webhook: receive a query for async analysis."""
    event = await engine.ingest(
        req.sql,
        source=req.source,
        metadata=req.metadata,
        execution_time_ms=req.execution_time_ms,
        rows_scanned=req.rows_scanned,
        user_name=req.user_name,
        database_name=req.database_name,
        app_name=req.app_name,
    )
    return {"status": "accepted", "event_id": event.id}


@app.post("/ingest/batch")
async def ingest_batch(req: IngestBatchRequest):
    """Webhook: receive multiple queries at once."""
    ids = []
    for q in req.queries[:20]:
        event = await engine.ingest(
            q.sql,
            source=q.source,
            metadata=q.metadata,
            execution_time_ms=q.execution_time_ms,
            rows_scanned=q.rows_scanned,
            user_name=q.user_name,
            database_name=q.database_name,
            app_name=q.app_name,
        )
        ids.append(event.id)
    return {"status": "accepted", "count": len(ids), "event_ids": ids}


@app.get("/stream")
async def sse_stream():
    """SSE: real-time event stream for connected dashboards."""
    subscriber = engine.subscribe()

    async def event_generator():
        yield f"data: {json.dumps({'type': 'init', 'stats': engine.get_stats(), 'events': engine.get_recent_events(20)})}\n\n"
        async for data in engine.sse_stream(subscriber):
            yield data

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/events")
async def get_events(
    limit: int = Query(default=50, le=200),
    decision: Optional[str] = Query(default=None),
    injection_only: bool = Query(default=False),
):
    """Get recent events with optional filters."""
    # Try persisted data first
    if db:
        try:
            persisted = await db.get_recent_queries(limit, decision, injection_only)
            if persisted:
                return {
                    "events": persisted,
                    "stats": engine.get_stats(),
                    "source": "database",
                }
        except Exception:
            pass

    # Fallback to in-memory
    events = engine.get_recent_events(min(limit, 100))
    if decision:
        events = [e for e in events if e.get("result", {}).get("decision") == decision]
    if injection_only:
        events = [e for e in events if e.get("result", {}).get("is_injection")]
    return {
        "events": events,
        "stats": engine.get_stats(),
        "source": "memory",
    }


@app.get("/stats")
async def get_stats():
    """Dashboard statistics (from persistence + live)."""
    live = engine.get_stats()
    if db:
        try:
            persisted = await db.get_stats()
            # Merge: use whichever is higher
            for k in persisted:
                if k in live:
                    live[k] = max(live[k], persisted[k])
                else:
                    live[k] = persisted[k]
        except Exception:
            pass
    return live


@app.post("/ask")
async def ask_question(req: AskRequest):
    """Natural language Q&A about SQL and data engineering."""
    answer = await ask_natural_language(req.question, req.context)
    return {"question": req.question, "answer": answer}


@app.post("/demo/start")
async def start_demo():
    """Start simulated query stream for hackathon demo."""
    global demo_task
    if demo_task and not demo_task.done():
        return {"status": "already_running"}
    demo_task = asyncio.create_task(run_demo_stream(engine, interval=4.0))
    return {"status": "started", "message": "Demo query stream started — queries arrive every 4s"}


@app.post("/demo/stop")
async def stop_demo():
    """Stop simulated query stream."""
    global demo_task
    if demo_task and not demo_task.done():
        demo_task.cancel()
        try:
            await demo_task
        except asyncio.CancelledError:
            pass
        demo_task = None
        return {"status": "stopped"}
    return {"status": "not_running"}


@app.get("/traces")
async def list_traces():
    return await get_altimate_traces()


@app.get("/warehouse/status")
async def warehouse_status():
    """Show connected warehouse adapters."""
    if warehouse_poller:
        return warehouse_poller.status
    return {"running": False, "adapters": [], "connected_count": 0}


@app.get("/threats")
async def get_threats(limit: int = Query(default=20, le=100)):
    """Get recent threats from persistence."""
    try:
        threats = await db.get_recent_threats(limit)
        return {"threats": threats, "count": len(threats)}
    except Exception:
        return {"threats": [], "count": 0}


@app.get("/history")
async def get_history(
    limit: int = Query(default=50, le=200),
    decision: Optional[str] = Query(default=None),
    injection_only: bool = Query(default=False),
):
    """Get persisted query history."""
    try:
        queries = await db.get_recent_queries(limit, decision, injection_only)
        return {"queries": queries, "count": len(queries)}
    except Exception as e:
        return {"queries": [], "count": 0, "error": str(e)}


# ─── Helpers ──────────────────────────────────────────────

def _elapsed(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


def _get_query_type(sql: str) -> str:
    upper = sql.strip().upper()
    for t in ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"]:
        if upper.startswith(t):
            return t
    return "UNKNOWN"


def _extract_lineage(sql: str) -> dict:
    from impact import analyze_impact as _ai

    tables = set()
    for m in re.findall(r'\bFROM\s+([a-zA-Z_]\w*(?:\.\w+)*)', sql, re.IGNORECASE):
        tables.add(m)
    for m in re.findall(r'\bJOIN\s+([a-zA-Z_]\w*(?:\.\w+)*)', sql, re.IGNORECASE):
        tables.add(m)

    if not tables:
        return {"nodes": [], "edges": []}

    impact = _ai(list(tables), [], "SELECT")
    service_lookup = {s.affected_table: s.service for s in impact.affected_services}

    nodes = []
    for t in sorted(tables):
        node = {"id": t, "label": t, "type": "source"}
        if t.lower() in service_lookup:
            node["service"] = service_lookup[t.lower()]
        nodes.append(node)

    nodes.append({"id": "result", "label": "Query Result", "type": "output"})

    # Build edges from JOINs
    edges = []
    join_pairs = re.findall(
        r'(\w+)\s+\w+\s+JOIN\s+(\w+)', sql, re.IGNORECASE
    )
    if join_pairs:
        for t1, t2 in join_pairs:
            edges.append({"from": t1, "to": t2, "type": "join"})
    # All tables flow to result
    for t in sorted(tables):
        edges.append({"from": t, "to": "result", "type": "flow"})

    return {"nodes": nodes, "edges": edges}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
