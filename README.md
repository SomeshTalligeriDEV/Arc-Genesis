# Arc Genesis

**Real-time AI SQL Observability System**

Stop bad SQL before it runs. Every query is analyzed, scored, and decided — before it touches your warehouse.

---

## What it does

- Intercepts SQL queries from any source
- Runs a 7-step analysis pipeline (security → AST → lineage → cost → LLM)
- Returns **APPROVE / REJECT / WARNING** with explanation and suggested fix
- Streams results live to a real-time dashboard

---

## Quick Start

### Backend

```bash
cd backend
pip install -r requirements.txt

# Add your API keys
echo "GROQ_API_KEY=your_key" > .env
echo "ALTIMATE_API_KEY=your_key" >> .env

python main.py
# → http://localhost:8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev
# → http://localhost:3000
```

---

## How it works

```
SQL → Security Gate → Altimate CLI + AST → Impact Mapping → Groq LLM → Decision
```

| Step | Tool | What it does |
|------|------|-------------|
| Security | `security.py` | Blocks DROP, TRUNCATE, injections instantly |
| Analysis | Altimate CLI + sqlparse | Detects 10+ anti-patterns |
| Lineage | `impact.py` | Maps tables → services → dashboards |
| Profiling | `profiling.py` | Estimates cost, scan type, row explosion risk |
| Decision | Groq → Gemini → Deterministic | Returns APPROVE/REJECT/WARNING |
| Fix | `analyzer.py` | Auto-generates corrected SQL |
| Persist | SQLite | Saves every query and result |

---

## API

```bash
# Review a query
curl -X GET "http://localhost:8000/stream-review?sql=SELECT+*+FROM+orders"

# Query history
curl http://localhost:8000/history

# Health check
curl http://localhost:8000/health
```

---

## Test Cases

| Query | Result |
|-------|--------|
| `SELECT * FROM orders JOIN customers` | REJECT — cartesian product |
| `DROP TABLE users` | BLOCKED — security gate |
| `SELECT order_id FROM orders WHERE created_at > CURRENT_DATE - 7 LIMIT 100` | APPROVE |
| `SELECT * FROM users WHERE id = '' OR 1=1 --` | BLOCKED — SQL injection |

---

## Stack

- **Frontend** — Next.js 16, React 19
- **Backend** — FastAPI, Python 3.11+
- **Analysis** — Altimate CLI, sqlparse
- **AI** — Groq (llama-3.3-70b), Gemini fallback, deterministic fallback
- **Storage** — SQLite
- **Streaming** — Server-Sent Events (SSE)

---

Built for the Altimate Hackathon.
