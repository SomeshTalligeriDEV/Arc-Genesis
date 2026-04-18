"""
Arc Genesis — SQL Analyzer (v2)
Combines Altimate CLI + sqlparse AST + heuristic rules.
Produces structured analysis with zero-hallucination deterministic output.
"""

import asyncio
import json
import logging
import re
import shutil
import time
from dataclasses import dataclass, field

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Parenthesis
from sqlparse.tokens import Keyword, DML

logger = logging.getLogger(__name__)

ALTIMATE_TIMEOUT = 30


@dataclass
class Issue:
    rule: str
    severity: str  # error | warning | info
    message: str
    fix: str | None = None

    def to_dict(self) -> dict:
        return {k: v for k, v in {
            "rule": self.rule,
            "severity": self.severity,
            "message": self.message,
            "fix": self.fix,
        }.items() if v is not None}


@dataclass
class AnalysisResult:
    success: bool
    output: str
    issues: list[Issue] = field(default_factory=list)
    tables: list[str] = field(default_factory=list)
    query_type: str = "UNKNOWN"
    complexity_score: int = 1
    source: str = "ast-analyzer"
    altimate_time_ms: int = 0
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "source": self.source,
            "query_type": self.query_type,
            "complexity_score": self.complexity_score,
            "tables": self.tables,
            "altimate_time_ms": self.altimate_time_ms,
            "issues_count": len(self.issues),
            "issues": [i.to_dict() for i in self.issues],
        }


def _find_altimate() -> str | None:
    return shutil.which("altimate") or shutil.which("altimate-code")


def _extract_tables(sql: str) -> list[str]:
    """Extract table names from SQL using regex (fast, reliable)."""
    tables = set()
    for match in re.findall(r'\bFROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)', sql, re.IGNORECASE):
        tables.add(match)
    for match in re.findall(r'\bJOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)', sql, re.IGNORECASE):
        tables.add(match)
    for match in re.findall(r'\bINTO\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)', sql, re.IGNORECASE):
        tables.add(match)
    for match in re.findall(r'\bUPDATE\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)', sql, re.IGNORECASE):
        tables.add(match)
    return sorted(tables)


def _get_query_type(sql: str) -> str:
    """Detect SQL statement type via sqlparse."""
    try:
        parsed = sqlparse.parse(sql)
        if parsed:
            t = parsed[0].get_type()
            return t if t else "UNKNOWN"
    except Exception:
        pass
    return "UNKNOWN"


def _count_complexity(sql: str) -> int:
    """Score 1-10 complexity based on structural analysis."""
    score = 1
    upper = sql.upper()

    if "JOIN" in upper: score += 2
    if upper.count("JOIN") > 2: score += 1
    if "SUBQUERY" in upper or upper.count("SELECT") > 1: score += 2
    if "GROUP BY" in upper: score += 1
    if "HAVING" in upper: score += 1
    if "UNION" in upper: score += 2
    if "WINDOW" in upper or "OVER(" in upper.replace(" ", ""): score += 1
    if "WITH" in upper and "AS" in upper: score += 1  # CTE

    return min(score, 10)


def _ast_analyze(sql: str) -> list[Issue]:
    """Deep AST-based analysis using sqlparse."""
    issues: list[Issue] = []
    upper = sql.upper()

    try:
        parsed = sqlparse.parse(sql)
    except Exception:
        return issues

    for stmt in parsed:
        tokens = list(stmt.flatten())

        # ── Rule 1: SELECT * ──
        if re.search(r'\bSELECT\s+\*\b', upper):
            issues.append(Issue(
                rule="no-select-star",
                severity="warning",
                message="SELECT * transfers all columns — breaks on schema changes, wastes bandwidth",
                fix="Replace SELECT * with specific column names",
            ))

        # ── Rule 2: JOIN without ON ──
        if "JOIN" in upper and re.search(r'\bJOIN\s+\w+\s*(?:WHERE|$|;)', upper):
            issues.append(Issue(
                rule="missing-join-condition",
                severity="error",
                message="JOIN without ON clause creates a cartesian product (row explosion)",
                fix="Add ON clause: JOIN table ON table.id = other_table.foreign_id",
            ))

        # ── Rule 3: Implicit cross join ──
        from_match = re.search(r'\bFROM\s+([\w.]+\s*,\s*[\w.]+)', upper)
        if from_match:
            issues.append(Issue(
                rule="implicit-cross-join",
                severity="error",
                message=f"Implicit cross join detected in FROM clause — use explicit JOIN",
                fix="Replace comma-separated tables with explicit JOIN ... ON syntax",
            ))

        # ── Rule 4: ORDER BY without LIMIT ──
        if "ORDER BY" in upper and "LIMIT" not in upper and "TOP " not in upper and "FETCH" not in upper:
            issues.append(Issue(
                rule="order-without-limit",
                severity="warning",
                message="ORDER BY without LIMIT sorts entire result set — expensive on large tables",
                fix="Add LIMIT clause to bound the result set",
            ))

        # ── Rule 5: Deeply nested subqueries ──
        subquery_depth = upper.count("SELECT") - 1
        if subquery_depth >= 2:
            issues.append(Issue(
                rule="deep-nesting",
                severity="warning",
                message=f"Query has {subquery_depth} levels of nesting — hard to optimize",
                fix="Refactor nested subqueries into CTEs (WITH ... AS)",
            ))

        # ── Rule 6: DISTINCT as a smell ──
        if "DISTINCT" in upper and "JOIN" in upper:
            issues.append(Issue(
                rule="distinct-join-smell",
                severity="info",
                message="DISTINCT with JOIN often indicates duplicate rows from incorrect join",
                fix="Check join conditions — DISTINCT may be masking a data issue",
            ))

        # ── Rule 7: Missing WHERE on large table operations ──
        if stmt.get_type() in ("SELECT", None):
            if "FROM" in upper and "WHERE" not in upper and "LIMIT" not in upper:
                if "JOIN" not in upper and "GROUP BY" not in upper:
                    issues.append(Issue(
                        rule="unbounded-scan",
                        severity="warning",
                        message="No WHERE or LIMIT — full table scan on every execution",
                        fix="Add WHERE clause to filter rows, or LIMIT to cap results",
                    ))

        # ── Rule 8: Function in WHERE (index killer) ──
        if re.search(r'WHERE\s+\w+\s*\(', upper):
            issues.append(Issue(
                rule="function-in-where",
                severity="warning",
                message="Function call in WHERE clause prevents index usage",
                fix="Move function to a computed column or restructure the predicate",
            ))

        # ── Rule 9: LIKE with leading wildcard ──
        if re.search(r"LIKE\s+'%", upper):
            issues.append(Issue(
                rule="leading-wildcard",
                severity="warning",
                message="LIKE with leading wildcard (%) cannot use indexes — full scan required",
                fix="Consider full-text search or restructure the query",
            ))

        # ── Rule 10: Multiple aggregations without GROUP BY ──
        agg_count = sum(1 for fn in ["SUM(", "COUNT(", "AVG(", "MAX(", "MIN("] if fn in upper)
        if agg_count >= 2 and "GROUP BY" not in upper:
            issues.append(Issue(
                rule="multi-agg-no-group",
                severity="info",
                message="Multiple aggregations without GROUP BY — returns a single row",
                fix="Add GROUP BY if you need per-group aggregations",
            ))

    return issues


async def analyze_sql(sql: str) -> AnalysisResult:
    """
    Full analysis pipeline:
    1. sqlparse AST analysis (always runs)
    2. Altimate CLI (if available, augments)
    """
    tables = _extract_tables(sql)
    query_type = _get_query_type(sql)
    complexity = _count_complexity(sql)
    issues = _ast_analyze(sql)

    # Try Altimate CLI augmentation
    altimate_output = ""
    source = "ast-analyzer"
    altimate_time_ms = 0
    altimate_bin = _find_altimate()

    if altimate_bin:
        altimate_start = time.monotonic()
        try:
            proc = await asyncio.create_subprocess_exec(
                altimate_bin, "check", "--format", "json",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(input=sql.encode()),
                timeout=ALTIMATE_TIMEOUT,
            )
            stdout_str = stdout.decode().strip()
            if stdout_str:
                altimate_output = stdout_str
                source = "ast+altimate"
        except (asyncio.TimeoutError, Exception) as e:
            logger.debug("Altimate CLI augmentation skipped: %s", str(e))
        finally:
            altimate_time_ms = int((time.monotonic() - altimate_start) * 1000)

    # Build structured output
    result_data = {
        "source": source,
        "query_type": query_type,
        "complexity_score": complexity,
        "tables": tables,
        "issues_count": len(issues),
        "issues": [i.to_dict() for i in issues],
    }

    if altimate_output:
        result_data["altimate_raw"] = altimate_output

    return AnalysisResult(
        success=True,
        output=json.dumps(result_data, indent=2),
        issues=issues,
        tables=tables,
        query_type=query_type,
        complexity_score=complexity,
        source=source,
        altimate_time_ms=altimate_time_ms,
    )


def generate_fix(sql: str, issues: list[Issue]) -> str:
    """
    Auto-generate a fixed version of the SQL based on detected issues.
    Deterministic, no LLM needed.
    """
    fixed = sql

    for issue in issues:
        if issue.rule == "no-select-star":
            # Replace SELECT * with placeholder columns
            fixed = re.sub(
                r'\bSELECT\s+\*\b',
                'SELECT\n  -- TODO: specify columns\n  col1,\n  col2,\n  col3',
                fixed,
                count=1,
                flags=re.IGNORECASE,
            )

        elif issue.rule == "order-without-limit":
            fixed = fixed.rstrip().rstrip(';') + "\nLIMIT 1000;"

        elif issue.rule == "unbounded-scan":
            # Add a safety LIMIT
            if "LIMIT" not in fixed.upper():
                fixed = fixed.rstrip().rstrip(';') + "\nLIMIT 1000;"

        elif issue.rule == "missing-join-condition":
            # Add ON placeholder
            fixed = re.sub(
                r'\bJOIN\s+(\w+)\s*(?=WHERE|$|;)',
                r'JOIN \1 ON \1.id = <table>.foreign_id ',
                fixed,
                flags=re.IGNORECASE,
            )

    return fixed


async def get_altimate_traces() -> dict:
    """Run altimate-code trace list."""
    altimate_bin = _find_altimate()
    if not altimate_bin:
        return {"success": False, "error": "altimate-code not found"}

    try:
        proc = await asyncio.create_subprocess_exec(
            altimate_bin, "trace", "list",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=ALTIMATE_TIMEOUT,
        )
        return {"success": True, "traces": stdout.decode().strip()}
    except Exception as e:
        return {"success": False, "error": str(e)}
