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
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from analyzer import analyze_sql, generate_fix, get_altimate_traces
from impact import analyze_impact
from ingestion import IngestionEngine, run_demo_stream
from llm import Decision, ReviewDecision, RiskLevel, call_llm, ask_natural_language
from metrics import ReviewMetrics, measure_async
from security import check_sql_security, ThreatLevel
from profiling import profile_query
from persistence import db
from warehouse import WarehousePoller
from stream import REVIEW_STREAM_STEPS, StreamMessage, format_sse

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
StepEmitter = Callable[[int, str], Awaitable[None]]


async def _emit_step(emit_step: StepEmitter | None, step_index: int) -> None:
    if emit_step:
        await emit_step(step_index, REVIEW_STREAM_STEPS[step_index])


def _severity_to_cost_score(severity: str) -> int:
    mapping = {
        "critical": 10,
        "high": 8,
        "medium": 6,
        "low": 3,
    }
    return mapping.get(severity.lower(), 5)


def _cost_band(cost_score: int) -> str:
    if cost_score >= 8:
        return "HIGH"
    if cost_score >= 5:
        return "MEDIUM"
    return "LOW"


def _compute_overall_risk_score(
    query_risk_score: float,
    security_risk_score: int,
    review_risk_level: str,
    decision: str,
) -> float:
    overall = max(query_risk_score, round(security_risk_score / 10, 1))
    if review_risk_level == "HIGH":
        overall = max(overall, 8.0)
    elif review_risk_level == "MEDIUM":
        overall = max(overall, 5.0)
    elif review_risk_level == "LOW":
        overall = max(overall, 2.0)

    if decision == "REJECT":
        overall = max(overall, 8.5)

    return round(min(overall, 10.0), 1)


def _build_structured_impact(tables: list[str], impact_result, cost_score: int) -> dict:
    downstream = [svc.service for svc in impact_result.affected_services]
    return {
        "tables": tables,
        "downstream": downstream or ["dashboard"],
        "cost": _cost_band(cost_score),
    }


def _build_blocked_review(sql: str, security, analysis, impact_result) -> ReviewDecision:
    issues: list[str] = []

    if security.reason:
        issues.append(security.reason)

    for issue in analysis.issues:
        if issue.message not in issues:
            issues.append(issue.message)

    if not issues:
        issues.append("Dangerous SQL blocked by the security policy")

    impact = [
        "Query blocked before it could reach the warehouse",
        impact_result.alert_message,
    ]
    if impact_result.business_impact:
        impact.append(impact_result.business_impact[0])

    suggested_fix = generate_fix(sql, analysis.issues)
    if suggested_fix == sql:
        suggested_fix = "No changes needed"

    return ReviewDecision(
        decision=Decision.REJECT,
        issues=issues[:5],
        impact=impact[:3],
        suggested_fix=suggested_fix,
        risk_level=RiskLevel.HIGH,
        cost_score=9,
        reasoning=f"Security gate blocked this query: {security.reason or security.matched_pattern or 'policy violation'}",
        explanation="Arc Genesis stopped this query before execution because it matched a destructive or injection-risk pattern.",
        source="security-gate",
    )


def _to_stream_review_payload(result: dict) -> dict:
    return {
        "type": "final",
        "status": result["status"],
        "decision": result["decision"],
        "risk_score": result["overall_risk_score"],
        "risk_score_max": 10,
        "risk_level": result["risk_level"],
        "cost_score": result["cost_score"],
        "issues": result["issues"],
        "impact": result["impact"],
        "suggested_fix": result["suggested_fix"],
        "latency_ms": result["total_latency_ms"],
        "total_latency_ms": result["total_latency_ms"],
        "analysis_time_ms": result["analysis_time_ms"],
        "altimate_time_ms": result["altimate_time_ms"],
        "llm_time_ms": result["llm_time_ms"],
        "security_risk_score": result["risk_score"],
        "query_risk_score": result["query_risk_score"],
        "overall_risk_score": result["overall_risk_score"],
        "source": result["source"],
        "reasoning": result["reasoning"],
        "explanation": result["explanation"],
        "original_sql": result["original_sql"],
        "is_injection": result["is_injection"],
        "injection_type": result["injection_type"],
        "injection_patterns": result["injection_patterns"],
        "severity": result["severity"],
        "impact_analysis": result["impact_analysis"],
        "profiling": result["profiling"],
        "lineage": result["lineage"],
    }


async def _full_analyze(sql: str, emit_step: StepEmitter | None = None) -> dict:
    """Complete pipeline: security → analyze → profile → impact → LLM → response."""
    sql = sql.strip()
    metrics = ReviewMetrics()

    await _emit_step(emit_step, 0)

    await _emit_step(emit_step, 1)
    security = check_sql_security(sql)

    await _emit_step(emit_step, 2)
    analysis = await analyze_sql(sql)
    issue_dicts = [i.to_dict() for i in analysis.issues]

    await _emit_step(emit_step, 3)
    lineage = _extract_lineage(sql)
    impact = analyze_impact(analysis.tables, issue_dicts, analysis.query_type)

    await _emit_step(emit_step, 4)
    profiling = profile_query(sql)
    query_risk_score = _compute_query_risk_score(sql, analysis.issues)

    await _emit_step(emit_step, 5)
    if security.level == ThreatLevel.BLOCKED:
        review = _build_blocked_review(sql, security, analysis, impact)
    else:
        review, metrics.llm_time_ms = await measure_async(call_llm(sql, analysis.output))

    await _emit_step(emit_step, 6)
    auto_fix = generate_fix(sql, analysis.issues)
    final_fix = review.suggested_fix if review.suggested_fix != "No changes needed" else auto_fix
    if final_fix.strip() == sql.strip():
        final_fix = "No changes needed"
    metrics.altimate_time_ms = analysis.altimate_time_ms
    metrics.finish()
    overall_risk_score = _compute_overall_risk_score(
        query_risk_score=query_risk_score,
        security_risk_score=security.risk_score,
        review_risk_level=review.risk_level.value,
        decision=review.decision.value,
    )
    structured_impact = _build_structured_impact(analysis.tables, impact, review.cost_score)

    return {
        "status": "BLOCKED" if security.level == ThreatLevel.BLOCKED else "REVIEWED",
        "decision": review.decision.value,
        "issues": review.issues,
        "impact_summary": review.impact,
        "impact": structured_impact,
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
        "query_risk_score": query_risk_score,
        "overall_risk_score": overall_risk_score,
        "severity": security.severity.value,
        "lineage": lineage,
        "impact_analysis": impact.to_dict(),
        "profiling": profiling.to_dict(),
        "original_sql": sql,
        "duration_ms": metrics.total_latency_ms,
        "latency_ms": metrics.total_latency_ms,
        "total_latency_ms": metrics.total_latency_ms,
        "analysis_time_ms": metrics.analysis_time_ms,
        "altimate_time_ms": metrics.altimate_time_ms,
        "llm_time_ms": metrics.llm_time_ms,
        "source": review.source,
        "processing_steps": REVIEW_STREAM_STEPS.copy(),
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

    # Start background auto-simulation (keeps feed alive when demo is off)
    auto_sim_task = asyncio.create_task(_auto_simulate())
    logger.info("🤖 Background simulation started")

    yield

    # Shutdown
    auto_sim_task.cancel()
    try:
        await auto_sim_task
    except asyncio.CancelledError:
        pass
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


@app.get("/stream-review")
async def stream_review(
    request: Request,
    sql: str = Query(..., min_length=1, max_length=50000),
):
    """SSE: stream one manual SQL review step-by-step."""
    cleaned_sql = sql.strip()

    async def event_generator():
        queue: asyncio.Queue[str] = asyncio.Queue()
        task: asyncio.Task | None = None

        async def emit_step(step_index: int, step: str):
            await queue.put(format_sse({
                "type": "step",
                "step_index": step_index,
                "step": step,
            }))

        try:
            yield format_sse({
                "type": "start",
                "steps": REVIEW_STREAM_STEPS,
                "sql": cleaned_sql,
            })
            task = asyncio.create_task(_full_analyze(cleaned_sql, emit_step=emit_step))

            while True:
                if await request.is_disconnected():
                    if task and not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                    break

                if task and task.done() and queue.empty():
                    result = task.result()
                    payload = _to_stream_review_payload(result)
                    yield format_sse(payload)
                    # ── Push to live feed ──
                    logger.info("📤 Event published to live feed: %s → %s", cleaned_sql[:40], result.get("decision"))
                    asyncio.create_task(engine.broadcast_result(cleaned_sql, "manual", result))
                    break

                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=0.1)
                    yield payload
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            if task and not task.done():
                task.cancel()
            raise
        except Exception as exc:
            logger.exception("stream-review failed")
            yield format_sse({
                "type": "error",
                "message": str(exc),
            })

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

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

def _generate_insight(sql: str, decision: str, risk_score: float, tables: list, issues: list) -> str:
    """One-sentence human insight derived deterministically from analysis results."""
    upper = sql.upper()

    if decision == "REJECT":
        if "JOIN" in upper and "ON" not in upper:
            pair = " × ".join(tables[:2]) if len(tables) >= 2 else "the joined tables"
            return f"This query produces a cartesian product — {pair} will multiply to potentially billions of rows, crashing your warehouse."
        if re.search(r'\bDELETE\b', upper):
            return "This DELETE has no WHERE clause — it will wipe every row in the table with no recovery path."
        if issues:
            first = issues[0].message if hasattr(issues[0], "message") else str(issues[0])
            return f"Critical issue detected: {first}"
        return "This query has structural problems that will cause incorrect results or a production outage."

    if decision == "WARNING":
        if re.search(r'\bSELECT\s+\*\b', upper):
            tbl = tables[0] if tables else "this table"
            return f"SELECT * on {tbl} transfers all columns — schema changes will silently break downstream consumers."
        if "ORDER BY" in upper and "LIMIT" not in upper:
            return "Sorting without LIMIT forces the entire result set into memory — latency grows linearly with table size."
        if "WHERE" not in upper:
            tbl = "the " + tables[0] + " table" if tables else "data volume"
            return f"No WHERE clause means a full table scan on every execution — cost scales with {tbl}."
        return f"This query will work but has performance concerns that will surface at production scale (risk {risk_score}/10)."

    # APPROVE
    if tables:
        return f"Query looks clean — efficient access pattern on {', '.join(tables[:2])}, safe to run in production."
    return "No issues detected — this query follows best practices and is safe for production."


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


def _compute_query_risk_score(sql: str, issues: list) -> float:
    """
    Deterministic 0-10 risk score based purely on query structure rules.
    Separate from security.risk_score (which is 0-100 injection-focused).
    """
    score = 0.0
    upper = sql.upper()

    if re.search(r'\bSELECT\s+\*\b', upper):
        score += 2
    if "WHERE" not in upper and re.search(r'\bFROM\b', upper):
        score += 3
    if "JOIN" in upper and "ON" not in upper:
        score += 4
    if re.search(r'\bFROM\s+\w+\s*,\s*\w+', upper):  # implicit cross join
        score += 4
    if "ORDER BY" in upper and "LIMIT" not in upper:
        score += 1
    if re.search(r"LIKE\s+'%", upper):
        score += 1
    # Each additional error-severity issue adds 0.5
    for issue in issues:
        if hasattr(issue, 'severity') and issue.severity == "error":
            score += 0.5

    level = "LOW" if score <= 3 else "MEDIUM" if score <= 7 else "HIGH"
    return round(min(score, 10.0), 1)


# ─── Background Auto-Simulation ───────────────────────────

_SIM_QUERIES = [
    ("SELECT * FROM payments WHERE amount > 5000", "system"),
    ("SELECT order_id, total FROM orders WHERE created_at > CURRENT_DATE - 7 LIMIT 100", "system"),
    ("SELECT u.email, COUNT(o.id) FROM users u JOIN orders o ON o.user_id = u.id GROUP BY u.email", "system"),
    ("SELECT * FROM user_transactions", "system"),
    ("SELECT product_id, SUM(quantity) FROM order_items GROUP BY product_id ORDER BY SUM(quantity) DESC LIMIT 20", "system"),
    ("SELECT * FROM customers JOIN orders", "system"),
    ("SELECT session_id, user_id FROM sessions WHERE expires_at < NOW()", "system"),
    ("SELECT COUNT(*) FROM logs WHERE level = 'ERROR' AND created_at > NOW() - INTERVAL '1 hour'", "system"),
]
_sim_idx = 0


async def _auto_simulate():
    """Push one background query every 8s to keep the feed alive."""
    global _sim_idx
    import random
    await asyncio.sleep(5)  # let server fully start first
    while True:
        try:
            # Only fire when the explicit demo isn't running
            if demo_task is None or demo_task.done():
                sql, source = _SIM_QUERIES[_sim_idx % len(_SIM_QUERIES)]
                _sim_idx += 1
                await engine.ingest(sql, source=source, metadata={"app": "auto-sim"})
                logger.info("🤖 Auto-sim: %s", sql[:50])
            await asyncio.sleep(random.uniform(7, 10))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("Auto-sim error: %s", e)
            await asyncio.sleep(5)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
