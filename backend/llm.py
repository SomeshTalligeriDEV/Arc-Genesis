"""
Arc Genesis — LLM Integration (v3)
Groq (primary) → Gemini (fallback) → Deterministic (last-resort).
Enhanced with explain, optimize, and cost prediction.
"""

import json
import logging
import os
import re
from dataclasses import dataclass
from enum import Enum

import httpx

logger = logging.getLogger(__name__)

LLM_TIMEOUT = 25
MAX_RETRIES = 1


class Decision(str, Enum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    WARNING = "WARNING"


class RiskLevel(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass
class ReviewDecision:
    decision: Decision
    issues: list[str]
    impact: list[str]
    suggested_fix: str
    risk_level: RiskLevel
    cost_score: int
    reasoning: str
    explanation: str
    source: str = "llm"

    def to_dict(self) -> dict:
        return {
            "decision": self.decision.value,
            "issues": self.issues,
            "impact": self.impact,
            "suggested_fix": self.suggested_fix,
            "risk_level": self.risk_level.value,
            "cost_score": self.cost_score,
            "reasoning": self.reasoning,
            "explanation": self.explanation,
            "source": self.source,
        }


_SYSTEM_PROMPT = """You are a senior data engineer reviewing SQL queries for production safety.

Given the original SQL and an automated analysis report, make a DECISION.

Respond ONLY in valid JSON:
{
  "decision": "APPROVE" | "REJECT" | "WARNING",
  "issues": ["issue 1", "issue 2"],
  "impact": ["business impact 1", "impact 2"],
  "suggested_fix": "corrected SQL query or 'No changes needed'",
  "risk_level": "HIGH" | "MEDIUM" | "LOW",
  "cost_score": <1-10 where 10 is most expensive>,
  "reasoning": "technical explanation of WHY this query is risky/safe",
  "explanation": "simple English explanation a junior dev would understand. Explain WHAT could go wrong in production."
}

Rules:
- REJECT: breaking issues (cartesian joins, missing conditions, injections, schema violations)
- WARNING: performance issues (SELECT *, no LIMIT, full scans)
- APPROVE: clean and production-safe
- Always provide a concrete optimized SQL fix when possible
- reasoning: explain WHY the query is problematic with specific technical details
- explanation: use analogies, concrete examples. If it's a cost issue, estimate the cost impact.
- cost_score: 1=trivial lookup, 5=moderate join, 10=warehouse-killer. Consider rows scanned and operations.
"""


def _parse_llm_response(text: str, source: str) -> ReviewDecision | None:
    """Parse JSON response from any LLM provider."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r'^```(?:json)?\n?', '', text)
        text = re.sub(r'\n?```$', '', text)

    try:
        parsed = json.loads(text)
        return ReviewDecision(
            decision=Decision(parsed.get("decision", "WARNING")),
            issues=parsed.get("issues", []),
            impact=parsed.get("impact", []),
            suggested_fix=parsed.get("suggested_fix", "No changes needed"),
            risk_level=RiskLevel(parsed.get("risk_level", "MEDIUM")),
            cost_score=min(10, max(1, int(parsed.get("cost_score", 5)))),
            reasoning=parsed.get("reasoning", ""),
            explanation=parsed.get("explanation", ""),
            source=source,
        )
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        logger.warning("Failed to parse LLM JSON: %s", str(e))
        return None


# ─── Groq (Primary) ──────────────────────────────────────

async def _call_groq_api(api_key: str, prompt: str) -> ReviewDecision | None:
    """Call Groq API with llama-3.3-70b-versatile."""
    url = "https://api.groq.com/openai/v1/chat/completions"

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 2048,
        "response_format": {"type": "json_object"},
    }

    timeout = httpx.Timeout(LLM_TIMEOUT, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            url,
            json=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )

        if resp.status_code == 429:
            logger.warning("Groq rate limited")
            return None
        if resp.status_code >= 400:
            logger.warning("Groq API error: %d — %s", resp.status_code, resp.text[:200])
            return None

        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"]
        return _parse_llm_response(text, "groq")


# ─── Gemini (Fallback) ───────────────────────────────────

async def _call_gemini_api(api_key: str, prompt: str, attempt: int = 0) -> ReviewDecision | None:
    """Call Gemini API."""
    models = ["gemini-2.0-flash", "gemini-1.5-flash"]
    model = models[min(attempt, len(models) - 1)]
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

    payload = {
        "contents": [{"parts": [{"text": _SYSTEM_PROMPT + "\n\n" + prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 2048,
            "responseMimeType": "application/json",
        },
    }

    timeout = httpx.Timeout(LLM_TIMEOUT, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=payload)

        if resp.status_code in (400, 401, 403):
            logger.warning("Gemini API key issue: %d", resp.status_code)
            return None
        if resp.status_code == 429:
            logger.warning("Gemini rate limited")
            return None

        resp.raise_for_status()
        data = resp.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        return _parse_llm_response(text, "gemini")


# ─── Main LLM Call ────────────────────────────────────────

async def call_llm(sql: str, analysis: str) -> ReviewDecision:
    """
    Call LLM with provider chain: Groq → Gemini → Deterministic.
    """
    prompt = f"""Analyze this SQL query and its automated analysis:

--- SQL ---
{sql}

--- ANALYSIS ---
{analysis}

Respond with JSON decision. Be specific about WHY this query is risky/safe and what the concrete business impact would be."""

    # 1. Try Groq
    groq_key = os.getenv("GROQ_API_KEY", "")
    if groq_key and len(groq_key) >= 10:
        try:
            result = await _call_groq_api(groq_key, prompt)
            if result:
                logger.debug("LLM decision from Groq: %s", result.decision.value)
                return result
        except Exception as e:
            logger.warning("Groq failed: %s", str(e))

    # 2. Try Gemini
    gemini_key = os.getenv("GEMINI_API_KEY", "")
    if gemini_key and len(gemini_key) >= 20 and gemini_key.startswith("AIza"):
        for attempt in range(MAX_RETRIES + 1):
            try:
                result = await _call_gemini_api(gemini_key, prompt, attempt)
                if result:
                    logger.debug("LLM decision from Gemini: %s", result.decision.value)
                    return result
            except Exception as e:
                logger.warning("Gemini attempt %d failed: %s", attempt + 1, str(e))

    # 3. Deterministic fallback
    logger.info("All LLM providers unavailable — using deterministic engine")
    return _deterministic_decision(sql, analysis)


def _deterministic_decision(sql: str, analysis: str) -> ReviewDecision:
    """
    High-quality deterministic fallback. Parses the analysis JSON
    and makes rule-based decisions that match LLM quality.
    """
    upper = sql.upper()
    issues: list[str] = []
    impact: list[str] = []
    risk = RiskLevel.LOW
    cost = 3
    decision = Decision.APPROVE

    # Parse structured analysis
    try:
        data = json.loads(analysis)
        for issue in data.get("issues", []):
            sev = issue.get("severity", "info")
            msg = issue.get("message", "")
            issues.append(msg)
            if sev == "error":
                decision = Decision.REJECT
                risk = RiskLevel.HIGH
                cost = max(cost, 9)
            elif sev == "warning" and decision != Decision.REJECT:
                decision = Decision.WARNING
                risk = RiskLevel.MEDIUM
                cost = max(cost, 6)
    except (json.JSONDecodeError, TypeError):
        pass

    # Impact analysis
    if "JOIN" in upper and "ON" not in upper:
        impact.append("Cartesian product will multiply rows exponentially — could crash the warehouse")
        cost = 10
    if "SELECT *" in upper:
        impact.append("All columns transferred — excess network I/O and schema-change fragility")
    if "ORDER BY" in upper and "LIMIT" not in upper:
        impact.append("Full result set sorted — CPU-intensive on large tables")
    if "FROM" in upper and "WHERE" not in upper and "LIMIT" not in upper:
        impact.append("Full table scan on every execution — scales linearly with table size")

    if not issues:
        issues.append("No significant issues detected")
    if not impact:
        impact.append("Query appears safe for production use")

    # Generate explanation
    if decision == Decision.REJECT:
        explanation = "This query has critical issues that will cause problems in production. The query structure is broken — it would produce incorrect results or overload the system. Think of it like trying to search every book in a library by reading them all cover-to-cover instead of using the card catalog."
    elif decision == Decision.WARNING:
        explanation = "This query works but has performance concerns. It might be slow on large datasets or use more resources than necessary. Like ordering everything on a menu when you only want one dish — it works, but it's wasteful."
    else:
        explanation = "This query looks clean and well-structured. It follows best practices and should run efficiently. It's like a well-written recipe — clear, specific, and efficient."

    # Auto-fix
    suggested_fix = sql
    if "SELECT *" in upper:
        suggested_fix = re.sub(r'\bSELECT\s+\*', 'SELECT col1, col2, col3 /* specify actual columns */', suggested_fix, flags=re.IGNORECASE)
    if "ORDER BY" in upper and "LIMIT" not in upper:
        suggested_fix = suggested_fix.rstrip().rstrip(';') + "\nLIMIT 1000;"

    return ReviewDecision(
        decision=decision,
        issues=issues,
        impact=impact,
        suggested_fix=suggested_fix if suggested_fix != sql else "No changes needed",
        risk_level=risk,
        cost_score=cost,
        reasoning=f"Deterministic analysis: {len(issues)} issue(s) found, cost estimated at {cost}/10",
        explanation=explanation,
        source="deterministic",
    )


# ─── Natural Language Q&A ─────────────────────────────────

async def ask_natural_language(question: str, context: str = "") -> str:
    """
    Natural language Q&A about queries and data engineering.
    Uses Groq → Gemini → template fallback.
    """
    prompt = f"""You are a friendly data engineering assistant. Answer this question in simple, clear English.

Question: {question}

Context: {context}

Keep your answer concise (3-5 sentences). Be practical and actionable. Use examples when helpful."""

    # Try Groq
    groq_key = os.getenv("GROQ_API_KEY", "")
    if groq_key and len(groq_key) >= 10:
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            payload = {
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "max_tokens": 512,
            }
            async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
                resp = await client.post(url, json=payload, headers={
                    "Authorization": f"Bearer {groq_key}",
                    "Content-Type": "application/json",
                })
                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"]
        except Exception as e:
            logger.warning("Groq NL query failed: %s", str(e))

    # Try Gemini
    gemini_key = os.getenv("GEMINI_API_KEY", "")
    if gemini_key and len(gemini_key) >= 20:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={gemini_key}"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.3, "maxOutputTokens": 512},
            }
            async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                data = resp.json()
                return data["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            logger.warning("Gemini NL query failed: %s", str(e))

    return _template_answer(question)


def _template_answer(question: str) -> str:
    """Template-based answers for common questions."""
    q = question.lower()
    if "fail" in q or "error" in q:
        return "Queries typically fail due to syntax errors, missing tables, or permission issues. Check the error message for the specific cause, and verify your table names and column references."
    if "slow" in q or "performance" in q:
        return "Slow queries are usually caused by full table scans (missing WHERE clause), inefficient JOINs, or sorting large datasets without LIMIT. Add indexes, filter early, and limit results."
    if "cost" in q or "expensive" in q:
        return "Query cost depends on data scanned, joins performed, and sorting. Reduce cost by selecting only needed columns, adding WHERE filters, and using LIMIT."
    if "select *" in q:
        return "SELECT * fetches all columns, which wastes bandwidth and breaks when the schema changes. Always specify the columns you need."
    if "inject" in q:
        return "SQL injection happens when user input is concatenated directly into queries. Always use parameterized queries ($1, ?, :param) instead of string interpolation. Arc Genesis can detect injection patterns like 1=1, UNION SELECT, and comment bypass."
    if "join" in q:
        return "JOINs combine rows from multiple tables. Always specify a JOIN condition (ON clause) — without it, you get a cartesian product that multiplies rows from both tables together. For 1M × 1M tables, that's 1 trillion rows."
    return "I can help with SQL optimization, security, injection detection, and best practices. Try asking about specific query issues, performance, or cost concerns. Arc Genesis analyzes your queries for 10+ anti-patterns."
