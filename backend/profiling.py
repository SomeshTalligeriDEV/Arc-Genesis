"""
Arc Genesis — Data Profiling Engine
Estimates query resource usage, scan types, null risk, and join explosion.
Uses real DB stats when available, heuristics as fallback.
"""

import logging
import re
from dataclasses import dataclass, field

import sqlparse

logger = logging.getLogger(__name__)

# ─── Table size estimates (fallback when no DB stats) ─────

_DEFAULT_TABLE_SIZES = {
    "users": 50000,
    "customers": 100000,
    "orders": 500000,
    "order_items": 2000000,
    "products": 10000,
    "categories": 500,
    "inventory": 10000,
    "payments": 800000,
    "transactions": 1000000,
    "user_transactions": 600000,
    "logs": 5000000,
    "sessions": 200000,
    "accounts": 30000,
    "balances": 30000,
}

_DEFAULT_ROW_ESTIMATE = 100000  # Unknown tables


@dataclass
class ProfilingResult:
    row_estimate: int = 0
    scan_type: str = "unknown"
    null_risk: str = "low"
    cardinality_estimate: int = 0
    join_explosion_risk: str = "none"
    index_usage: str = "unknown"
    memory_impact: str = "low"
    tables_involved: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "row_estimate": self.row_estimate,
            "scan_type": self.scan_type,
            "null_risk": self.null_risk,
            "cardinality_estimate": self.cardinality_estimate,
            "join_explosion_risk": self.join_explosion_risk,
            "index_usage": self.index_usage,
            "memory_impact": self.memory_impact,
            "tables_involved": self.tables_involved,
            "warnings": self.warnings,
        }


def _extract_tables(sql: str) -> list[str]:
    """Extract table names from SQL."""
    tables = set()
    for m in re.findall(r'\bFROM\s+([a-zA-Z_]\w*(?:\.\w+)*)', sql, re.IGNORECASE):
        tables.add(m.split(".")[-1].lower())
    for m in re.findall(r'\bJOIN\s+([a-zA-Z_]\w*(?:\.\w+)*)', sql, re.IGNORECASE):
        tables.add(m.split(".")[-1].lower())
    for m in re.findall(r'\bUPDATE\s+([a-zA-Z_]\w*(?:\.\w+)*)', sql, re.IGNORECASE):
        tables.add(m.split(".")[-1].lower())
    for m in re.findall(r'\bINTO\s+([a-zA-Z_]\w*(?:\.\w+)*)', sql, re.IGNORECASE):
        tables.add(m.split(".")[-1].lower())
    return sorted(tables)


def _get_table_size(table: str, db_stats: dict = None) -> int:
    """Get estimated row count for a table."""
    if db_stats and table in db_stats:
        return db_stats[table].get("row_count", _DEFAULT_ROW_ESTIMATE)
    return _DEFAULT_TABLE_SIZES.get(table, _DEFAULT_ROW_ESTIMATE)


def _estimate_scan_type(sql: str) -> str:
    """Classify scan type based on query structure."""
    upper = sql.upper()

    # Primary key / unique lookup
    if re.search(r'WHERE\s+\w+\s*=\s*\d+', upper) and "JOIN" not in upper:
        return "index_lookup"

    # Indexed range scan (WHERE with comparison operators)
    if re.search(r'WHERE\s+\w+\s*(>|<|>=|<=|BETWEEN)', upper):
        if "JOIN" not in upper:
            return "index_range_scan"
        return "index_range_scan_with_join"

    # WHERE equals (potential index scan)
    if "WHERE" in upper:
        if "LIKE" in upper and re.search(r"LIKE\s+'%", upper):
            return "full_table_scan"  # Leading wildcard = no index
        return "index_scan"

    # No WHERE at all
    if "WHERE" not in upper:
        if "LIMIT" in upper:
            limit_match = re.search(r'LIMIT\s+(\d+)', upper)
            if limit_match and int(limit_match.group(1)) <= 100:
                return "sequential_scan_limited"
        return "full_table_scan"

    return "sequential_scan"


def _estimate_null_risk(sql: str, tables: list[str]) -> str:
    """Estimate null risk from query pattern."""
    upper = sql.upper()
    risk_score = 0

    # LEFT/RIGHT/FULL OUTER JOIN → high null risk
    if re.search(r'\b(LEFT|RIGHT|FULL)\s+(OUTER\s+)?JOIN\b', upper):
        risk_score += 3

    # No COALESCE/IFNULL wrapping
    if ("LEFT JOIN" in upper or "RIGHT JOIN" in upper) and "COALESCE" not in upper and "IFNULL" not in upper:
        risk_score += 2

    # Aggregations without null handling
    if re.search(r'\b(SUM|AVG|COUNT)\s*\(', upper) and "COALESCE" not in upper:
        risk_score += 1

    # Subquery in SELECT (can return NULL)
    if upper.count("SELECT") > 1:
        risk_score += 1

    if risk_score >= 4:
        return "high"
    elif risk_score >= 2:
        return "medium"
    return "low"


def _estimate_join_explosion(sql: str, tables: list[str], db_stats: dict = None) -> tuple[str, list[str]]:
    """Estimate join explosion risk."""
    upper = sql.upper()
    warnings = []

    join_count = upper.count("JOIN")
    if join_count == 0 and "," in re.search(r'FROM\s+(.*?)(?:WHERE|ORDER|GROUP|LIMIT|$)', upper, re.DOTALL).group(1) if re.search(r'FROM\s+(.*?)(?:WHERE|ORDER|GROUP|LIMIT|$)', upper, re.DOTALL) else "":
        return "critical", ["Implicit cross join — cartesian product"]

    # Cross join / no ON
    if "JOIN" in upper and re.search(r'JOIN\s+\w+\s*(?:WHERE|$|;|GROUP|ORDER)', upper):
        total = 1
        for t in tables:
            total *= _get_table_size(t, db_stats)
        warnings.append(f"Missing JOIN condition — potential {total:,} row cartesian product")
        return "critical", warnings

    # Many-to-many risk
    if join_count >= 3:
        warnings.append(f"{join_count} JOINs — verify cardinality at each join point")
        return "high", warnings

    if join_count >= 2:
        return "medium", warnings

    return "none", warnings


def _estimate_cardinality(sql: str, tables: list[str], db_stats: dict = None) -> int:
    """Estimate result set cardinality."""
    upper = sql.upper()

    # Start with base table size
    base = max((_get_table_size(t, db_stats) for t in tables), default=_DEFAULT_ROW_ESTIMATE) if tables else _DEFAULT_ROW_ESTIMATE

    # LIMIT caps the result
    limit_match = re.search(r'LIMIT\s+(\d+)', upper)
    if limit_match:
        base = min(base, int(limit_match.group(1)))

    # GROUP BY reduces rows
    if "GROUP BY" in upper:
        base = base // 10  # Rough estimate: 10% of rows are unique groups

    # DISTINCT reduces
    if "DISTINCT" in upper:
        base = base // 5

    # WHERE filters (rough 30% selectivity)
    if "WHERE" in upper:
        where_conditions = upper.count(" AND ") + 1
        for _ in range(where_conditions):
            base = int(base * 0.3)

    # JOINs can multiply (but usually filtered)
    join_count = upper.count("JOIN")
    if join_count > 0 and "ON" in upper:
        base = int(base * 1.2 * join_count)  # Slight increase per join

    # Aggregation without GROUP BY → 1 row
    agg_funcs = ["SUM(", "COUNT(", "AVG(", "MAX(", "MIN("]
    if any(f in upper for f in agg_funcs) and "GROUP BY" not in upper:
        base = 1

    return max(1, base)


def _estimate_memory_impact(row_estimate: int, scan_type: str, join_count: int) -> str:
    """Estimate memory impact."""
    if scan_type == "full_table_scan" and row_estimate > 1000000:
        return "critical"
    if row_estimate > 500000 or (join_count > 2 and row_estimate > 100000):
        return "high"
    if row_estimate > 50000:
        return "medium"
    return "low"


def profile_query(sql: str, db_stats: dict = None) -> ProfilingResult:
    """
    Profile a SQL query for resource estimation.

    Args:
        sql: The SQL query to profile
        db_stats: Optional dict of {table_name: {row_count, avg_row_size, ...}}
                  from real database stats (pg_stats, information_schema)

    Returns:
        ProfilingResult with all estimations
    """
    tables = _extract_tables(sql)
    upper = sql.upper()
    warnings = []

    # Scan type
    scan_type = _estimate_scan_type(sql)
    if scan_type == "full_table_scan":
        warnings.append("Full table scan — consider adding WHERE clause or indexes")

    # Row estimate
    row_estimate = _estimate_cardinality(sql, tables, db_stats)

    # Null risk
    null_risk = _estimate_null_risk(sql, tables)
    if null_risk in ("medium", "high"):
        warnings.append(f"Null risk: {null_risk} — consider COALESCE/IFNULL wrapping")

    # Join explosion
    join_risk, join_warnings = _estimate_join_explosion(sql, tables, db_stats)
    warnings.extend(join_warnings)

    # Index usage
    if scan_type in ("index_lookup", "index_scan", "index_range_scan"):
        index_usage = "likely"
    elif scan_type == "full_table_scan":
        index_usage = "none"
    else:
        index_usage = "partial"

    # Memory impact
    join_count = upper.count("JOIN")
    memory_impact = _estimate_memory_impact(row_estimate, scan_type, join_count)

    # Cardinality
    cardinality = _estimate_cardinality(sql, tables, db_stats)

    # Additional warnings
    if "SELECT *" in upper and row_estimate > 10000:
        warnings.append(f"SELECT * on ~{row_estimate:,} rows — specify columns to reduce I/O")

    if "ORDER BY" in upper and "LIMIT" not in upper and row_estimate > 100000:
        warnings.append(f"Sorting ~{row_estimate:,} rows without LIMIT — memory intensive")

    return ProfilingResult(
        row_estimate=row_estimate,
        scan_type=scan_type,
        null_risk=null_risk,
        cardinality_estimate=cardinality,
        join_explosion_risk=join_risk,
        index_usage=index_usage,
        memory_impact=memory_impact,
        tables_involved=tables,
        warnings=warnings,
    )


async def get_real_table_stats(table_name: str, db_type: str = "postgresql", conn=None) -> dict:
    """
    Pull real statistics from a database.
    Returns dict with row_count, avg_row_size, null_frac per column, etc.
    """
    if conn is None:
        return {}

    try:
        if db_type == "postgresql":
            # pg_stats + pg_class for real row estimates
            row = await conn.fetchrow(
                """SELECT reltuples::bigint AS row_count,
                          pg_total_relation_size(c.oid) AS total_bytes
                   FROM pg_class c
                   JOIN pg_namespace n ON n.oid = c.relnamespace
                   WHERE c.relname = $1 AND n.nspname = 'public'""",
                table_name,
            )
            if row:
                return {
                    "row_count": row["row_count"],
                    "total_bytes": row["total_bytes"],
                    "source": "pg_stats",
                }

        elif db_type == "mysql":
            # information_schema.tables
            row = await conn.fetchone(
                """SELECT TABLE_ROWS as row_count,
                          DATA_LENGTH + INDEX_LENGTH as total_bytes
                   FROM information_schema.TABLES
                   WHERE TABLE_NAME = %s AND TABLE_SCHEMA = DATABASE()""",
                (table_name,),
            )
            if row:
                return {
                    "row_count": row[0],
                    "total_bytes": row[1],
                    "source": "information_schema",
                }
    except Exception as e:
        logger.debug("Could not get real stats for %s: %s", table_name, str(e))

    return {}
