# ⚡ Arc Genesis — AI PR Reviewer for Data Engineering

AI-powered decision system that analyzes SQL queries before execution. Detects issues, estimates cost, extracts lineage, and returns **APPROVE / REJECT / WARNING** decisions.

> **We do NOT execute queries. We decide if they should run.**



## Architecture

```
User → Next.js UI → FastAPI → Security Gate → Altimate CLI → Gemini LLM → Decision
```

## Quick Start

### 1. Backend (FastAPI)

```bash
cd backend
pip install -r requirements.txt

# Set your Gemini API key
export GEMINI_API_KEY=your-key-here

# Start server
python main.py
# → running on http://localhost:8000
```

### 2. Frontend (Next.js)

```bash
cd frontend
npm install
npm run dev
# → running on http://localhost:3000
```

### 3. Verify

```bash
# Health check
curl http://localhost:8000/health

# Test review
curl -X POST http://localhost:8000/review \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM orders JOIN customers"}'
```

---

## API Reference

### `POST /review`

**Request:**
```json
{
  "sql": "SELECT * FROM orders JOIN customers"
}
```

**Response:**
```json
{
  "status": "REVIEWED",
  "decision": "REJECT",
  "issues": [
    "SELECT * detected — specify columns explicitly",
    "JOIN without ON clause — cartesian product risk"
  ],
  "impact": [
    "May transfer unnecessary data",
    "Exponential row multiplication — may crash warehouse"
  ],
  "suggested_fix": "SELECT col1, col2 FROM orders JOIN customers ON ...",
  "risk_level": "HIGH",
  "cost_score": 10,
  "reasoning": "Critical issues detected",
  "analysis_source": "altimate-cli",
  "lineage": {
    "nodes": [
      {"id": "orders", "label": "orders", "type": "source"},
      {"id": "customers", "label": "customers", "type": "source"},
      {"id": "result", "label": "Query Result", "type": "output"}
    ],
    "edges": [
      {"from": "orders", "to": "result"},
      {"from": "customers", "to": "result"}
    ]
  },
  "duration_ms": 2500
}
```

### `GET /health`

Returns service health status.

### `GET /traces`

Lists Altimate trace sessions.

---

## Project Structure

```
arc-genesis/
├── backend/
│   ├── main.py              # FastAPI app + /review route
│   ├── security.py          # SQL security gate (blocks DROP, TRUNCATE, etc.)
│   ├── analyzer.py          # Altimate CLI integration + fallback analyzer
│   ├── llm.py               # Gemini LLM integration + fallback decision engine
│   ├── requirements.txt     # Python dependencies
│   └── .env                 # API keys
├── frontend/
│   ├── src/app/
│   │   ├── layout.js        # Root layout with fonts + SEO
│   │   ├── page.js          # Main page with input, results, lineage
│   │   └── globals.css      # Full design system
│   ├── .env.local           # Frontend env
│   └── package.json
├── altimate-code.json       # Altimate configuration
├── AGENTS.md                # Project rules for AI agents
└── .altimate-code/
    ├── connections.json     # Warehouse connections
    └── skill/               # Custom skills
```

---

## Altimate Trace Support

```bash
# List traces
altimate-code trace list

# Export for submission
zip traces.zip ~/.local/share/altimate-code/traces/*.json
```

---

## Test Cases

| Query | Expected | Result |
|-------|----------|--------|
| `SELECT * FROM orders JOIN customers` | REJECT (cartesian join) | ✅ REJECT, cost 10/10 |
| `DROP TABLE users;` | BLOCKED (security) | ✅ BLOCKED |
| `SELECT order_id FROM orders WHERE created_at > CURRENT_DATE - 7` | APPROVE | ✅ APPROVE, cost 3/10 |

---

## Tech Stack

- **Frontend**: Next.js 16 (App Router)
- **Backend**: FastAPI + Python 3.11+
- **Analysis**: Altimate CLI (altimate-code)
- **AI**: Gemini API (with deterministic fallback)
- **No database, no auth** — hackathon-focused

---

Built for the Altimate Hackathon 🏆
