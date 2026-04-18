"use client";
import { useState, useEffect, useRef } from "react";
import LoadingScreen from "../LoadingScreen";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

const DEC = {
  APPROVE:  { icon: "✓", cls: "dec-approve" },
  REJECT:   { icon: "✗", cls: "dec-reject" },
  WARNING:  { icon: "!", cls: "dec-warning" },
  BLOCKED:  { icon: "✗", cls: "dec-reject" },
};

const SEV_CLS = { critical: "alert-critical", high: "alert-high", medium: "alert-medium" };
const IMP_CLS = { critical: "imp-critical", high: "imp-high", medium: "imp-medium" };

const TESTS = [
  { label: "Bad JOIN",    sql: "SELECT * FROM orders JOIN customers", type: "danger" },
  { label: "DROP TABLE",  sql: "DROP TABLE users;", type: "danger" },
  { label: "Cross Join",  sql: "SELECT a.*, b.* FROM accounts a, balances b", type: "danger" },
  { label: "No WHERE",    sql: "DELETE FROM transactions", type: "danger" },
  { label: "SQL Injection", sql: "SELECT * FROM users WHERE id = '' OR 1=1 --", type: "danger" },
  { label: "UNION Inject", sql: "SELECT name FROM users WHERE id=1 UNION SELECT password FROM admin_users", type: "danger" },
  { label: "Good Query",  sql: "SELECT order_id, customer_name, total\nFROM orders\nWHERE created_at > CURRENT_DATE - 7\nORDER BY total DESC\nLIMIT 100", type: "safe" },
  { label: "Complex",     sql: "SELECT o.order_id, c.customer_name,\n  SUM(oi.quantity * oi.unit_price) AS total\nFROM orders o\nJOIN customers c ON c.id = o.customer_id\nJOIN order_items oi ON oi.order_id = o.id\nWHERE o.created_at >= '2024-01-01'\nGROUP BY o.order_id, c.customer_name\nORDER BY total DESC\nLIMIT 50", type: "safe" },
];

const FILTERS = [
  { id: "all",       label: "All" },
  { id: "critical",  label: "Critical" },
  { id: "injection", label: "Injection" },
  { id: "rejected",  label: "Rejected" },
  { id: "warnings",  label: "Warnings" },
  { id: "approved",  label: "Approved" },
];

const CONNECTION_LABELS = {
  connecting: "🟡 Connecting…",
  connected: "🟢 Live",
  disconnected: "🔴 Disconnected",
};

const formatMs = (value) => (value > 0 ? `${value}ms` : "n/a");

// ═══════════════════════════════════════════════════════════════
// MAIN PAGE
// ═══════════════════════════════════════════════════════════════
export default function Home() {
  const [tab, setTab] = useState("dashboard");
  const [events, setEvents] = useState([]);
  const [stats, setStats] = useState({ total_ingested:0, total_analyzed:0, threats_blocked:0, warnings:0, approved:0, injections_detected:0 });
  const [selEvent, setSelEvent] = useState(null);
  const [demo, setDemo] = useState(false);
  const [connStatus, setConnStatus] = useState("connecting"); // connecting | connected | disconnected
  const [alerts, setAlerts] = useState([]);
  const [filter, setFilter] = useState("all");

  // Manual review
  const [sql, setSql] = useState("");
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);
  const [steps, setSteps] = useState([]);
  const [activeStep, setActiveStep] = useState(-1);
  const [history, setHistory] = useState([]);

  // Fetch recent reviews on mount and after each result
  const fetchHistory = () => {
    fetch(`${API}/history?limit=5`)
      .then(r => r.json())
      .then(d => setHistory(d.queries || []))
      .catch(() => {});
  };
  useEffect(() => { fetchHistory(); }, []);

  // NL
  const [q, setQ] = useState("");
  const [ans, setAns] = useState(null);
  const [asking, setAsking] = useState(false);

  const [copied, setCopied] = useState(false);
  const [time, setTime] = useState("");
  const [cursorOn, setCursorOn] = useState(true);
  const [whStatus, setWhStatus] = useState(null);
  const [sysStatus, setSysStatus] = useState({ eventsPerSec: 0, lastEventTime: null });
  const feedRef = useRef(null);
  const eventTimesRef = useRef([]); // timestamps of recent events for events/sec calc
  const reviewStreamRef = useRef(null);

  // Clock + cursor blink
  useEffect(() => {
    const t1 = setInterval(() => setTime(new Date().toLocaleTimeString("en-US", { hour12: false })), 1000);
    const t2 = setInterval(() => setCursorOn(v => !v), 530);
    return () => { clearInterval(t1); clearInterval(t2); };
  }, []);

  // Fetch warehouse status
  useEffect(() => {
    fetch(`${API}/warehouse/status`).then(r => r.json()).then(setWhStatus).catch(() => {});
    const t = setInterval(() => {
      fetch(`${API}/warehouse/status`).then(r => r.json()).then(setWhStatus).catch(() => {});
    }, 15000);
    return () => clearInterval(t);
  }, []);

  // ── Dashboard SSE ──
  useEffect(() => {
    setConnStatus("connecting");
    const es = new EventSource(`${API}/stream`);
    es.onopen = () => { setConnStatus("connected"); }
    es.onerror = () => setConnStatus("disconnected");
    es.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.type === "init") { setStats(msg.stats); setEvents(msg.events||[]); return; }
        if (msg.type === "ingested" || msg.type === "analyzing") {
          setEvents(prev => {
            const ex = prev.find(ev => ev.id === msg.event.id);
            if (ex) return prev.map(ev => ev.id === msg.event.id ? msg.event : ev);
            // Track event time for events/sec
            const now = Date.now();
            eventTimesRef.current = [...eventTimesRef.current.filter(t => now - t < 60000), now];
            const eps = eventTimesRef.current.filter(t => now - t < 10000).length / 10;
            setSysStatus({ eventsPerSec: Math.round(eps * 10) / 10, lastEventTime: now });
            // Auto-scroll feed to top
            if (feedRef.current) feedRef.current.scrollTop = 0;
            return [msg.event, ...prev].slice(0, 100);
          });
        }
        if (msg.type === "result") {
          setEvents(prev => prev.map(ev => ev.id === msg.event.id ? msg.event : ev));
          const d = msg.event.result?.decision;
          const ia = msg.event.result?.impact_analysis;
          if (d === "REJECT" || msg.event.result?.status === "BLOCKED") setSelEvent(msg.event);
          if (ia && (ia.overall_severity === "critical" || ia.overall_severity === "high")) {
            setAlerts(prev => [{ id: msg.event.id, message: ia.alert_message, severity: ia.overall_severity, sql: msg.event.sql, ts: Date.now() }, ...prev].slice(0, 8));
          }
          // Injection alert
          if (msg.event.result?.is_injection) {
            setAlerts(prev => [{
              id: msg.event.id + "-inj",
              message: `SQL Injection detected: ${msg.event.result.injection_type}`,
              severity: "critical",
              sql: msg.event.sql,
              ts: Date.now(),
            }, ...prev].slice(0, 8));
          }
          setStats(prev => {
            const s = { ...prev, total_analyzed: prev.total_analyzed + 1 };
            if (d === "REJECT" || msg.event.result?.status === "BLOCKED") s.threats_blocked++;
            else if (d === "WARNING") s.warnings++;
            else if (d === "APPROVE") s.approved++;
            if (msg.event.result?.is_injection) s.injections_detected = (s.injections_detected || 0) + 1;
            return s;
          });
        }
      } catch {}
    };
    return () => { es.close(); };
  }, []);

  useEffect(() => () => {
    if (reviewStreamRef.current) {
      reviewStreamRef.current.close();
      reviewStreamRef.current = null;
    }
  }, []);

  // ── Filter events ──
  const filteredEvents = events.filter(ev => {
    if (filter === "all") return true;
    const d = ev.result?.decision || ev.result?.status;
    const inj = ev.result?.is_injection;
    const sev = ev.result?.impact_analysis?.overall_severity;
    if (filter === "critical") return sev === "critical" || sev === "high";
    if (filter === "injection") return inj;
    if (filter === "rejected") return d === "REJECT" || d === "BLOCKED";
    if (filter === "warnings") return d === "WARNING";
    if (filter === "approved") return d === "APPROVE";
    return true;
  });

  // ── Handlers ──
  const toggleDemo = async () => {
    try {
      if (demo) { await fetch(`${API}/demo/stop`, { method: "POST" }); setDemo(false); }
      else { await fetch(`${API}/demo/start`, { method: "POST" }); setDemo(true); }
    } catch (e) { setErr(e.message); }
  };

  const [demoRunning, setDemoRunning] = useState(false);
  const [showLoader, setShowLoader] = useState(false);
  const demoAbortRef = useRef(false);

  const closeReviewStream = () => {
    if (reviewStreamRef.current) {
      reviewStreamRef.current.close();
      reviewStreamRef.current = null;
    }
  };

  // Returns a Promise that resolves when the final event arrives.
  // Accepts an optional sqlOverride so runDemo can pass queries directly.
  const review = (sqlOverride) => {
    const query = (sqlOverride ?? sql).trim();
    if (!query) return Promise.resolve();
    closeReviewStream();
    setLoading(true);
    setShowLoader(true);   // show real loader
    setResult(null);
    setErr(null);
    setSteps([]);
    setActiveStep(-1);

    return new Promise((resolve) => {
      const es = new EventSource(`${API}/stream-review?sql=${encodeURIComponent(query)}`);
      reviewStreamRef.current = es;
      let settled = false;

      const settle = (finalMsg, errMsg) => {
        if (settled) return;
        settled = true;
        es.close();
        reviewStreamRef.current = null;
        setShowLoader(false); // always hide loader on settle
        if (finalMsg) {
          setSteps(prev => prev.map(s => ({ ...s, done: true })));
          setActiveStep(-1);
          setResult(finalMsg);
          setLoading(false);
          setErr(null);
          fetchHistory();
        } else {
          setErr(errMsg || "Stream error");
          setLoading(false);
        }
        resolve();
      };

      es.onmessage = (e) => {
        try {
          const msg = JSON.parse(e.data);
          // First real event from backend — hide loader immediately
          setShowLoader(false);
          if (msg.type === "start") {
            setSteps((msg.steps || []).map((label) => ({ label, done: false })));
            setActiveStep(-1);
          } else if (msg.type === "step") {
            setSteps(prev => {
              const base = prev.length
                ? prev
                : (msg.steps || []).map((label) => ({ label, done: false }));
              return base.map((step, index) => ({
                ...step,
                label: step.label || msg.step,
                done: index < msg.step_index,
              }));
            });
            setActiveStep(msg.step_index);
          } else if (msg.type === "final") {
            settle(msg, null);
          } else if (msg.type === "error") {
            settle(null, msg.message || "Review failed");
          }
        } catch {
          settle(null, "Invalid stream payload");
        }
      };

      es.onerror = () => settle(null, "Stream disconnected — check backend is running");
    });
  };

  const DEMO_QUERIES = [
    "SELECT * FROM orders JOIN customers",
    "DROP TABLE users;",
    "SELECT order_id FROM orders WHERE created_at > CURRENT_DATE - 7",
  ];

  const runDemo = () => {
    setShowLoader(true);
    _startDemo();
  };

  const _startDemo = async () => {
    setDemoRunning(true);
    demoAbortRef.current = false;
    setTab("manual");
    for (const q of DEMO_QUERIES) {
      if (demoAbortRef.current) break;
      setSql(q);
      setResult(null);
      await review(q);
      if (demoAbortRef.current) break;
      await new Promise(res => setTimeout(res, 800));
    }
    setDemoRunning(false);
  };

  const stopDemo = () => { demoAbortRef.current = true; setDemoRunning(false); setShowLoader(false); closeReviewStream(); setLoading(false); };

  const ask = async () => {
    if (!q.trim()) return;
    setAsking(true); setAns(null);
    try {
      const r = await fetch(`${API}/ask`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ question: q.trim() }) });
      const d = await r.json();
      setAns(d.answer);
    } catch (e) { setAns("Error: " + e.message); }
    finally { setAsking(false); }
  };

  const copy = (t) => { navigator.clipboard.writeText(t); setCopied(true); setTimeout(() => setCopied(false), 2000); };
  const active = selEvent?.result || result;

  return (
    <div className="shell">
      {showLoader && <LoadingScreen />}
      <div className="grid-bg" />

      {/* ── Header ── */}
      <div className="term-header">
        <div className="header-brand">
          <div className="header-title">Arc Genesis</div>
          <div className="header-sub">Real-time AI SQL Observability</div>
        </div>
        <div className="term-badges">
          <div className={`badge badge-live conn-${connStatus}`}>
            <span className="dot"></span>
            {connStatus === "connected" ? "Live" : connStatus === "connecting" ? "Connecting" : "Disconnected"}
          </div>
          {whStatus?.connected_count > 0 && (
            <div className="badge" style={{ borderColor: "rgba(16,185,129,0.3)", color: "var(--approve)" }}>
              {whStatus.connected_count} warehouse{whStatus.connected_count > 1 ? "s" : ""}
            </div>
          )}
          <button className={`badge badge-btn ${demo ? "badge-stop" : ""}`} onClick={toggleDemo}>
            {demo ? "Stop Feed" : "Start Feed"}
          </button>
          <button
            className={`badge badge-btn ${demoRunning ? "badge-stop" : "badge-run-demo"}`}
            onClick={demoRunning ? stopDemo : runDemo}
            disabled={loading && !demoRunning}
          >
            {demoRunning ? "Stop Demo" : "Run Demo"}
          </button>
        </div>
      </div>

      {/* ── Alerts ── */}
      {alerts.length > 0 && (
        <div className="alerts">
          {alerts.slice(0, 3).map(a => (
            <div key={a.id} className={`alert ${SEV_CLS[a.severity] || ""}`}>
              <span className="alert-sev-dot"></span>
              <div className="alert-body">
                <div className="alert-msg">{a.message}</div>
                <div className="alert-sql">{a.sql}</div>
              </div>
              <button className="alert-x" onClick={() => setAlerts(p => p.filter(x => x.id !== a.id))}>✕</button>
            </div>
          ))}
        </div>
      )}

      {/* ── Stats ── */}
      <div className="stats-row">
        <div className="stat-cell"><div className="stat-num">{stats.total_ingested}</div><div className="stat-label">Ingested</div></div>
        <div className="stat-cell"><div className="stat-num">{stats.total_analyzed}</div><div className="stat-label">Analyzed</div></div>
        <div className="stat-cell threats"><div className="stat-num">{stats.threats_blocked}</div><div className="stat-label">Threats</div></div>
        <div className="stat-cell warnings"><div className="stat-num">{stats.warnings}</div><div className="stat-label">Warnings</div></div>
        <div className="stat-cell approved"><div className="stat-num">{stats.approved}</div><div className="stat-label">Approved</div></div>
        <div className="stat-cell" style={{ borderColor: "var(--reject)" }}><div className="stat-num" style={{ color: "var(--reject)" }}>{stats.injections_detected || 0}</div><div className="stat-label">Injections</div></div>
      </div>

      {/* ── System Status ── */}
      <div className="sys-status">
        <span className={`sys-dot ${connStatus}`}></span>
        <span className="sys-item">System <strong>OK</strong></span>
        <span className="sys-sep">·</span>
        <span className="sys-item">{sysStatus.eventsPerSec} <span className="sys-dim">events/sec</span></span>
        <span className="sys-sep">·</span>
        <span className="sys-item">Last event: <span className="sys-dim">{sysStatus.lastEventTime ? new Date(sysStatus.lastEventTime).toLocaleTimeString("en-US", { hour12: false }) : "—"}</span></span>
        <span className="sys-sep">·</span>
        <span className="sys-item sys-dim">{events.length} in buffer</span>
      </div>

      {/* ── Tabs ── */}
      <div className="tabs">
        {[["dashboard","Live Feed"],["manual","Review"],["ask","Ask AI"]].map(([id, label]) => (
          <button key={id} className={`tab ${tab === id ? "active" : ""}`} onClick={() => setTab(id)}>{label}</button>
        ))}
      </div>

      {/* ═══ DASHBOARD ═══ */}
      {tab === "dashboard" && (
        <div className="grid-2">
          <div className="panel">
            <div className="panel-head"><span>Live Query Feed</span><span>{filteredEvents.length} events</span></div>
            {/* Filters */}
            <div className="filter-row">
              {FILTERS.map(f => (
                <button key={f.id} className={`filter-btn ${filter === f.id ? "active" : ""}`} onClick={() => setFilter(f.id)}>{f.label}</button>
              ))}
            </div>
            {filteredEvents.length === 0 ? (
              <div className="empty"><div className="empty-icon">◎</div><div className="empty-text">{filter !== "all" ? "No events match this filter" : connStatus === "connected" ? "Waiting for queries — run a review or start the feed" : "Connecting to live feed..."}</div></div>
            ) : (
              <div className="ev-list" ref={feedRef}>
                {filteredEvents.map(ev => {
                  const d = ev.result?.decision || ev.result?.status;
                  const dc = DEC[d === "BLOCKED" ? "BLOCKED" : d];
                  const ia = ev.result?.impact_analysis;
                  const isInj = ev.result?.is_injection;
                  const srcCls = ev.source === "manual" ? "src-manual" : ev.source === "system" || ev.source === "auto-sim" ? "src-system" : ev.source === "demo" ? "src-demo" : "src-other";
                  const srcLabel = ev.source === "manual" ? "user" : ev.source === "system" ? "system" : ev.source === "auto-sim" ? "system" : ev.source;
                  return (
                    <div key={ev.id} className={`ev ev-slide ${selEvent?.id === ev.id ? "sel" : ""} ${ev.status === "ANALYZING" ? "analyzing" : ""} ${isInj ? "ev-injection" : ""}`} onClick={() => setSelEvent(ev)}>
                      <div className="ev-top">
                        <span className={`ev-src ${srcCls}`}>{srcLabel}</span>
                        {ev.metadata?.app && <span className="ev-app">{ev.metadata.app}</span>}
                        {isInj && <span className="inj-badge">Injection</span>}
                        {ia?.affected_services?.length > 0 && (
                          <span className="ev-svc">
                            {ia.affected_services.slice(0, 2).map(s => <span key={s.service} className={`svc-tag ${s.criticality}`}>{s.service}</span>)}
                          </span>
                        )}
                        {ev.status === "ANALYZING" && <span style={{ animation: "spin 1s linear infinite", fontSize: "0.8rem" }}>⟳</span>}
                        {dc && <span className="ev-icon" style={{ color: d === "APPROVE" ? "var(--approve)" : d === "WARNING" ? "var(--warning)" : "var(--reject)" }}>{dc.icon}</span>}
                      </div>
                      <pre className="ev-sql">{ev.sql}</pre>
                      {ev.status === "DONE" && ev.result && (
                        <div className="ev-meta">
                          <span style={{ color: d === "APPROVE" ? "var(--approve)" : d === "WARNING" ? "var(--warning)" : "var(--reject)", fontWeight: 600 }}>{d}</span>
                          {ev.result.risk_level && <span>Risk:{ev.result.risk_level}</span>}
                          {(ev.result.overall_risk_score ?? ev.result.risk_score) > 0 && <span>Score:{ev.result.overall_risk_score ?? ev.result.risk_score}</span>}
                          {ia?.overall_severity && <span>Impact:{ia.overall_severity}</span>}
                          {(ev.result.total_latency_ms ?? ev.result.duration_ms) > 0 && <span>{ev.result.total_latency_ms ?? ev.result.duration_ms}ms</span>}
                          {ev.execution_time_ms > 0 && <span>Exec:{ev.execution_time_ms}ms</span>}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          <div className="panel" style={{ position: "sticky", top: "1rem" }}>
            <div className="panel-head"><span>Analysis Detail</span></div>
            {!active ? (
              <div className="empty"><div className="empty-icon">◎</div><div className="empty-text">Select a query from the feed</div></div>
            ) : (
              <Detail r={active} copy={copy} copied={copied} />
            )}
          </div>
        </div>
      )}

      {/* ═══ MANUAL REVIEW ═══ */}
      {tab === "manual" && (
        <div className="manual-layout">
          <div className="grid-2">
            <div className="panel">
              <div className="panel-head"><span>SQL Query</span><span>{sql.length}/50000</span></div>
              <textarea className="sql-input" value={sql} onChange={e => setSql(e.target.value)} placeholder="-- paste SQL here" spellCheck={false} disabled={demoRunning} />
              <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.5rem" }}>
                <button className="btn btn-accent" onClick={() => review()} disabled={loading || !sql.trim() || demoRunning}>{demoRunning ? "Running Demo..." : loading ? "Analyzing..." : "Review"}</button>
                <button className="btn btn-ghost" onClick={() => { closeReviewStream(); setLoading(false); setSql(""); setResult(null); setErr(null); setSteps([]); setActiveStep(-1); }} disabled={demoRunning}>clear</button>
              </div>
              <div className="chips">
                {TESTS.map((t, i) => <button key={i} className={`chip ${t.type}`} onClick={() => { setSql(t.sql); setResult(null); }}>{t.label}</button>)}
              </div>
            </div>
            <div className="panel">
              <div className="panel-head"><span>Live Result</span><span>{loading ? "streaming" : result ? "complete" : "waiting"}</span></div>
              {(loading || steps.length > 0) && (
                <div className="live-stream-card">
                  <div className="live-stream-head">
                    <div className="live-stream-title">{loading ? "Analysis in progress" : "Analysis complete"}</div>
                    <div className={`live-stream-state ${loading ? "stream-running" : err ? "stream-error" : "stream-complete"}`}>
                      {loading ? "backend-driven SSE" : err ? "stream interrupted" : "decision ready"}
                    </div>
                  </div>
                  <div className="steps-panel">
                    {steps.map((s, i) => (
                      <div key={i} className={`step-row ${s.done ? "step-done" : i === activeStep ? "step-active" : "step-pending"}`}>
                        <span className="step-icon">
                          {s.done ? "✓" : i === activeStep ? <span className="step-spinner">⟳</span> : "○"}
                        </span>
                        <span className="step-label">{s.label}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {err && !loading && <div className="err">{err}</div>}
              {!loading && !result && !err && steps.length === 0 && <div className="empty"><div className="empty-icon">◎</div><div className="empty-text">Enter SQL and click review</div></div>}
              {result && !loading && <Detail r={result} copy={copy} copied={copied} />}
            </div>
          </div>

          {/* ── Impact Panel (shown after result) ── */}
          {result && !loading && result.impact && (
            <div className="panel impact-panel">
              <div className="panel-head"><span>Impact Analysis</span></div>
              <div className="impact-grid">
                <div className="impact-col">
                  <div className="impact-label">Affected Tables</div>
                  <div className="impact-tags">
                    {(result.impact.tables || []).length > 0
                      ? (result.impact.tables).map(t => <span key={t} className="tag tag-table">{t}</span>)
                      : <span className="dim-text">none detected</span>}
                  </div>
                </div>
                <div className="impact-col">
                  <div className="impact-label">Downstream Systems</div>
                  <div className="impact-tags">
                    {(result.impact.downstream || []).length > 0
                      ? (result.impact.downstream).map(d => <span key={d} className="tag tag-downstream">{d}</span>)
                      : <span className="dim-text">none</span>}
                  </div>
                </div>
                <div className="impact-col">
                  <div className="impact-label">Cost Impact</div>
                  <span className={`cost-badge cost-${(result.impact.cost || "").toLowerCase()}`}>
                    {result.impact.cost || "—"}
                  </span>
                </div>
              </div>
            </div>
          )}

          {/* ── Recent Reviews ── */}
          <div className="panel">
            <div className="panel-head"><span>Recent Reviews</span><button className="btn btn-ghost" style={{fontSize:"0.65rem",padding:"0.2rem 0.5rem"}} onClick={fetchHistory}>Refresh</button></div>
            {history.length === 0
              ? <div className="empty"><div className="empty-text">No reviews yet</div></div>
              : (
                <div className="history-list">
                  {history.map((h, i) => {
                    const dec = h.decision || "—";
                    const dc = DEC[dec] || {};
                    return (
                      <div key={h.id || i} className="history-row" onClick={() => setSql(h.sql_text || h.sql_preview || "")}>
                        <span className={`history-dec ${dc.cls || ""}`}>{dc.icon || "?"} {dec}</span>
                        <span className="history-sql">{(h.sql_preview || h.sql_text || "").slice(0, 60)}</span>
                        <span className="history-meta">
                          {(h.overall_risk_score ?? h.query_risk_score ?? h.risk_score) > 0 && <span>risk {h.overall_risk_score ?? h.query_risk_score ?? h.risk_score}</span>}
                          {h.latency_ms > 0 && <span>{h.latency_ms}ms</span>}
                          <span>{new Date(h.created_at).toLocaleTimeString()}</span>
                        </span>
                      </div>
                    );
                  })}
                </div>
              )}
          </div>
        </div>
      )}

      {/* ═══ ASK AI ═══ */}
      {tab === "ask" && (
        <div style={{ maxWidth: 640, margin: "0 auto" }}>
          <div className="panel">
            <div className="panel-head"><span>Ask Arc Genesis</span></div>
            <p style={{ color: "var(--text-dim)", fontSize: "0.8rem", marginBottom: "0.75rem" }}>Ask about SQL, performance, injection, data engineering.</p>
            <div style={{ display: "flex", gap: "0.5rem" }}>
              <input className="nl-input" value={q} onChange={e => setQ(e.target.value)} placeholder='"Why is SELECT * bad?"' onKeyDown={e => e.key === "Enter" && ask()} />
              <button className="btn btn-accent" onClick={ask} disabled={asking || !q.trim()}>{asking ? "..." : "ask"}</button>
            </div>
            <div className="chips">
              {["Why is SELECT * bad?", "How to optimize joins?", "What is SQL injection?", "What causes cartesian joins?", "How to detect injection?"].map(s => (
                <button key={s} className="chip" onClick={() => { setQ(s); setAns(null); }}>{s}</button>
              ))}
            </div>
            {ans && <div style={{ marginTop: "1rem" }}><div className="sec-title">Answer</div><div className="explain-text" style={{ padding: "0.6rem", background: "var(--bg)", borderRadius: 6, whiteSpace: "pre-wrap" }}>{ans}</div></div>}
          </div>
        </div>
      )}

      <div className="foot">Arc Genesis v3.0 · Real-time AI SQL Observability · {connStatus === "connected" ? "Connected" : connStatus === "connecting" ? "Connecting..." : "Disconnected"}{whStatus?.connected_count > 0 ? ` · ${whStatus.connected_count} warehouse(s)` : ""}</div>
    </div>
  );
}


// ═══════════════════════════════════════════════════════════════
// DETAIL PANEL
// ═══════════════════════════════════════════════════════════════
function Detail({ r, copy, copied }) {
  const dk = r.status === "BLOCKED" ? "BLOCKED" : r.decision;
  const dc = DEC[dk];
  const ia = r.impact_analysis;
  const pr = r.profiling;
  const totalLatency = r.total_latency_ms ?? r.latency_ms ?? r.duration_ms ?? 0;
  const analysisLatency = r.altimate_time_ms ?? 0;
  const aiLatency = r.llm_time_ms ?? 0;
  const overallRiskScore = r.overall_risk_score ?? (r.risk_score_max === 10 ? r.risk_score : null);
  const securityRiskScore = r.security_risk_score ?? (r.risk_score_max === 10 ? null : r.risk_score);
  const showQueryRisk = r.query_risk_score != null && r.query_risk_score !== overallRiskScore;

  return (
    <div style={{ animation: "slideIn 0.2s ease" }}>
      {/* Decision */}
      {dc && (
        <div className={`dec ${dc.cls}`}>
          <span style={{ fontSize: "1.2rem" }}>{dc.icon}</span>
          <span>{dk}</span>
          {overallRiskScore != null && <span className="risk-pill">Risk {overallRiskScore}/10</span>}
          {totalLatency > 0 && <span className="dec-ms">{totalLatency}ms</span>}
          {r.source && <span className="dec-ms">{r.source}</span>}
        </div>
      )}

      {/* Latency Metrics */}
      {(totalLatency > 0 || aiLatency > 0 || analysisLatency > 0) && (
        <div className="metrics-row">
          <div className="metric-box"><span className="metric-val">{formatMs(totalLatency)}</span><span className="metric-label">Total Latency</span></div>
          <div className="metric-box"><span className="metric-val">{formatMs(aiLatency)}</span><span className="metric-label">AI Processing</span></div>
          <div className="metric-box"><span className="metric-val">{formatMs(analysisLatency)}</span><span className="metric-label">Analysis Time</span></div>
        </div>
      )}

      {r.insight && (
        <div className="insight-box">
          <span className="insight-text">{r.insight}</span>
        </div>
      )}

      {/* Injection Alert */}
      {r.is_injection && (
        <div className="inj-alert">
          <span className="inj-badge" style={{alignSelf:"flex-start"}}>Injection</span>
          <div>
            <div style={{ fontWeight: 700 }}>SQL INJECTION DETECTED</div>
            <div style={{ fontSize: "0.75rem", opacity: 0.9 }}>Type: {r.injection_type} · Risk: {securityRiskScore ?? r.risk_score}/100 · Severity: {r.severity}</div>
            {r.injection_patterns?.length > 0 && (
              <div style={{ fontSize: "0.7rem", marginTop: "0.25rem", opacity: 0.8 }}>
                Patterns: {r.injection_patterns.join(", ")}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Impact Alert */}
      {ia?.alert_message && ia.overall_severity !== "low" && (
        <div className={`imp-alert ${IMP_CLS[ia.overall_severity] || ""}`}>
          <span className="alert-sev-dot"></span>
          <span>{ia.alert_message}</span>
        </div>
      )}

      {/* Indicators */}
      {r.risk_level && (
        <div className="indic">
          <div className="indic-box">
            <div className="indic-label">Risk</div>
            <div className={`indic-val ${r.risk_level}`}>{r.risk_level}</div>
          </div>
          <div className="indic-box">
            <div className="indic-label">Cost</div>
            <div className="indic-val">{r.cost_score}/10</div>
            <div className="cost-bar">
              {Array.from({ length: 10 }).map((_, i) => (
                <div key={i} className={`cost-seg ${i < r.cost_score ? "on" : ""} ${i < 4 ? "lo" : i < 7 ? "md" : "hi"}`}></div>
              ))}
            </div>
          </div>
          {overallRiskScore != null && (
            <div className="indic-box">
              <div className="indic-label">Risk Score</div>
              <div className={`indic-val ${overallRiskScore >= 8 ? "HIGH" : overallRiskScore >= 5 ? "MEDIUM" : "LOW"}`}>{overallRiskScore}/10</div>
            </div>
          )}
          {securityRiskScore > 0 && (
            <div className="indic-box">
              <div className="indic-label">Security Risk</div>
              <div className={`indic-val ${securityRiskScore >= 70 ? "HIGH" : securityRiskScore >= 40 ? "MEDIUM" : "LOW"}`}>{securityRiskScore}/100</div>
            </div>
          )}
          {showQueryRisk && (
            <div className="indic-box">
              <div className="indic-label">Query Risk</div>
              <div className={`indic-val ${r.query_risk_score >= 8 ? "HIGH" : r.query_risk_score >= 4 ? "MEDIUM" : "LOW"}`}>{r.query_risk_score}/10</div>
            </div>
          )}
        </div>
      )}

      {/* Data Profiling */}
      {pr && (
        <div className="prof-sec">
          <div className="sec-title">Query Profile</div>
          <div className="prof-grid">
            <div className="prof-item">
              <span className="prof-label">Row Estimate</span>
              <span className="prof-val">{pr.row_estimate?.toLocaleString()}</span>
            </div>
            <div className="prof-item">
              <span className="prof-label">Scan Type</span>
              <span className={`prof-val ${pr.scan_type === "full_table_scan" ? "prof-bad" : "prof-ok"}`}>{pr.scan_type?.replace(/_/g, " ")}</span>
            </div>
            <div className="prof-item">
              <span className="prof-label">Null Risk</span>
              <span className={`prof-val ${pr.null_risk === "high" ? "prof-bad" : pr.null_risk === "medium" ? "prof-warn" : "prof-ok"}`}>{pr.null_risk}</span>
            </div>
            <div className="prof-item">
              <span className="prof-label">Join Explosion</span>
              <span className={`prof-val ${pr.join_explosion_risk === "critical" ? "prof-bad" : pr.join_explosion_risk === "high" ? "prof-warn" : "prof-ok"}`}>{pr.join_explosion_risk}</span>
            </div>
            <div className="prof-item">
              <span className="prof-label">Index Usage</span>
              <span className={`prof-val ${pr.index_usage === "none" ? "prof-bad" : "prof-ok"}`}>{pr.index_usage}</span>
            </div>
            <div className="prof-item">
              <span className="prof-label">Memory</span>
              <span className={`prof-val ${pr.memory_impact === "critical" || pr.memory_impact === "high" ? "prof-bad" : "prof-ok"}`}>{pr.memory_impact}</span>
            </div>
          </div>
          {pr.warnings?.length > 0 && (
            <ul className="prof-warnings">{pr.warnings.map((w, i) => <li key={i}>{w}</li>)}</ul>
          )}
        </div>
      )}

      {/* Impact Analysis */}
      {ia?.affected_services?.length > 0 && (
        <div className="imp-sec">
          <div className="sec-title">Affected Services</div>
          <div className="svc-grid">
            {ia.affected_services.map(s => (
              <div key={s.service} className={`svc-card ${s.criticality}`}>
                <div className="svc-head">
                  <span className="svc-name">{s.service}</span>
                  <span className={`crit-badge ${s.criticality}`}>{s.criticality}</span>
                </div>
                <div className="svc-info">Table: <code>{s.table}</code></div>
                <div className="svc-info">Team: {s.team}</div>
              </div>
            ))}
          </div>

          {ia.predictions?.length > 0 && ia.predictions[0] !== "No structural issues detected" && (
            <div className="sec" style={{ marginTop: "0.5rem" }}>
              <div className="sec-title">Predictions</div>
              <ul className="sec-list">{ia.predictions.map((p, i) => <li key={i}>{p}</li>)}</ul>
            </div>
          )}

          {ia.business_impact?.length > 0 && ia.business_impact[0] !== "Minimal business impact expected" && (
            <div className="sec">
              <div className="sec-title">Business Impact</div>
              <ul className="sec-list">{ia.business_impact.map((b, i) => <li key={i}>{b}</li>)}</ul>
            </div>
          )}

          {ia.teams_to_notify?.length > 0 && (
            <div className="teams">
              <span className="teams-label">Notify:</span>
              {ia.teams_to_notify.map(t => <span key={t} className="team-tag">@{t}</span>)}
            </div>
          )}
        </div>
      )}

      {/* Explanation */}
      {r.explanation && (
        <div className="explain">
          <div className="sec-title">Explanation</div>
          <div className="explain-text">{r.explanation}</div>
        </div>
      )}

      {/* Reasoning */}
      {r.reasoning && r.reasoning !== r.explanation && (
        <div className="sec">
          <div className="sec-title">Technical Reasoning</div>
          <div className="explain-text" style={{ fontSize: "0.72rem", opacity: 0.8 }}>{r.reasoning}</div>
        </div>
      )}

      {/* Issues */}
      {r.issues?.length > 0 && (
        <div className="sec">
          <div className="sec-title">Issues</div>
          <ul className="sec-list">{r.issues.map((x, i) => <li key={i}>{x}</li>)}</ul>
        </div>
      )}

      {/* Before → After */}
      {r.suggested_fix && r.suggested_fix !== "No changes needed" && r.original_sql && (
        <div className="sec">
          <div className="sec-title">Suggested Fix</div>
          <div className="cmp">
            <div className="cmp-block before"><div className="cmp-label">Before</div><pre className="cmp-code">{r.original_sql}</pre></div>
            <div className="cmp-block after">
              <div className="cmp-label">After</div>
              <pre className="cmp-code">{r.suggested_fix}</pre>
              <div style={{ padding: "0.3rem 0.5rem" }}>
                <button className="btn btn-accent" style={{ fontSize: "0.65rem", padding: "0.25rem 0.5rem" }} onClick={() => copy(r.suggested_fix)}>
                  {copied ? "Copied" : "Copy Fix"}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Lineage */}
      {r.lineage?.nodes?.length > 0 && (
        <div className="sec">
          <div className="sec-title">Data Lineage</div>
          <div className="lineage">
            {r.lineage.nodes.map((n, i) => (
              <span key={n.id} style={{ display: "flex", alignItems: "center" }}>
                {i > 0 && <span className="lin-arrow">→</span>}
                <span className={`lin-node ${n.type}`}>
                  {n.type === "source" ? "◈" : "◉"} {n.label}
                  {n.service && <span className="lin-svc">{n.service}</span>}
                </span>
              </span>
            ))}
          </div>
          {/* Join edges */}
          {r.lineage.edges?.filter(e => e.type === "join").length > 0 && (
            <div style={{ marginTop: "0.4rem", fontSize: "0.7rem", color: "var(--text-dim)" }}>
              Joins: {r.lineage.edges.filter(e => e.type === "join").map(e => `${e.from} ⟷ ${e.to}`).join(", ")}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
