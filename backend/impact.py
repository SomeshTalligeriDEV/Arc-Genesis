"""
Arc Genesis — Impact Analysis Engine
Maps SQL issues → real-world business impact.
The core differentiator: "This query will break your checkout flow."
"""

import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# ─── Service Map (loaded from config or defaults) ────────

_DEFAULT_SERVICE_MAP = {
    "orders": {"service": "payments-api", "team": "payments", "criticality": "high", "description": "Core order processing"},
    "order_items": {"service": "payments-api", "team": "payments", "criticality": "high", "description": "Line items for orders"},
    "customers": {"service": "auth-service", "team": "identity", "criticality": "high", "description": "Customer accounts and profiles"},
    "users": {"service": "auth-service", "team": "identity", "criticality": "critical", "description": "User authentication data"},
    "products": {"service": "catalog-service", "team": "catalog", "criticality": "medium", "description": "Product catalog"},
    "categories": {"service": "catalog-service", "team": "catalog", "criticality": "low", "description": "Product categories"},
    "inventory": {"service": "warehouse-api", "team": "logistics", "criticality": "high", "description": "Stock levels and availability"},
    "payments": {"service": "payments-api", "team": "payments", "criticality": "critical", "description": "Payment transactions"},
    "transactions": {"service": "payments-api", "team": "payments", "criticality": "critical", "description": "Financial transactions"},
    "user_transactions": {"service": "payments-api", "team": "payments", "criticality": "critical", "description": "User payment history"},
    "logs": {"service": "observability", "team": "platform", "criticality": "low", "description": "Application logs"},
    "sessions": {"service": "auth-service", "team": "identity", "criticality": "medium", "description": "User sessions"},
    "accounts": {"service": "finance-service", "team": "finance", "criticality": "critical", "description": "Financial accounts"},
    "balances": {"service": "finance-service", "team": "finance", "criticality": "critical", "description": "Account balances"},
}

# Impact templates keyed by analyzer rule
_IMPACT_RULES = {
    "no-select-star": {
        "prediction": "Schema changes will break this query silently",
        "business": "API responses may include sensitive fields (PII leak risk)",
        "latency": "+30-50% data transfer overhead",
        "severity": "medium",
    },
    "missing-join-condition": {
        "prediction": "Cartesian product — row count = table_A × table_B",
        "business": "Will crash or timeout any downstream API consuming this data",
        "latency": "Query may run for hours or exhaust warehouse memory",
        "severity": "critical",
    },
    "implicit-cross-join": {
        "prediction": "Implicit cross join produces exponential row multiplication",
        "business": "Analytics dashboards will show wildly incorrect numbers",
        "latency": "Warehouse costs spike — unbounded compute",
        "severity": "critical",
    },
    "order-without-limit": {
        "prediction": "Full result set sorted in memory before returning",
        "business": "API endpoints using this will have unpredictable response times",
        "latency": "+200-500% latency on tables > 1M rows",
        "severity": "medium",
    },
    "deep-nesting": {
        "prediction": "Query optimizer may fail to find efficient execution plan",
        "business": "Intermittent timeouts in production — hard to debug",
        "latency": "Execution time grows non-linearly with data volume",
        "severity": "medium",
    },
    "unbounded-scan": {
        "prediction": "Full table scan on every execution — no index used",
        "business": "Will slow down all queries sharing the same warehouse/database",
        "latency": "Latency scales linearly with table growth",
        "severity": "high",
    },
    "function-in-where": {
        "prediction": "Index bypass — database must evaluate function on every row",
        "business": "Query that was fast on dev data becomes slow on production volume",
        "latency": "+10x latency when table grows past 100K rows",
        "severity": "medium",
    },
    "leading-wildcard": {
        "prediction": "LIKE '%...' forces full table scan regardless of indexes",
        "business": "Search features become unusable as data grows",
        "latency": "Cannot be optimized — consider full-text search",
        "severity": "medium",
    },
    "distinct-join-smell": {
        "prediction": "DISTINCT is masking duplicate rows from incorrect join",
        "business": "Aggregations (SUM, COUNT) will produce wrong results",
        "latency": "Extra deduplication step on every execution",
        "severity": "high",
    },
    "multi-agg-no-group": {
        "prediction": "Multiple aggregations collapse to single row — likely not intended",
        "business": "Reports/dashboards will show misleading summary numbers",
        "latency": "Minimal — but results are wrong, which is worse",
        "severity": "medium",
    },
}


@dataclass
class ServiceImpact:
    service: str
    team: str
    criticality: str
    description: str
    affected_table: str


@dataclass
class ImpactResult:
    affected_services: list[ServiceImpact]
    predictions: list[str]
    business_impact: list[str]
    latency_impact: list[str]
    overall_severity: str  # critical | high | medium | low
    alert_message: str  # One-line alert for UI banner
    teams_to_notify: list[str]

    def to_dict(self) -> dict:
        return {
            "affected_services": [
                {
                    "service": s.service,
                    "team": s.team,
                    "criticality": s.criticality,
                    "description": s.description,
                    "table": s.affected_table,
                }
                for s in self.affected_services
            ],
            "predictions": self.predictions,
            "business_impact": self.business_impact,
            "latency_impact": self.latency_impact,
            "overall_severity": self.overall_severity,
            "alert_message": self.alert_message,
            "teams_to_notify": self.teams_to_notify,
        }


def _load_service_map() -> dict:
    """Load table→service map from config file, fallback to defaults."""
    config_path = Path(__file__).parent.parent / "service_map.json"
    if config_path.exists():
        try:
            with open(config_path) as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Failed to load service_map.json: %s", e)
    
    # Also check backend directory
    local_path = Path(__file__).parent / "service_map.json"
    if local_path.exists():
        try:
            with open(local_path) as f:
                return json.load(f)
        except Exception:
            pass

    return _DEFAULT_SERVICE_MAP


def analyze_impact(tables: list[str], issues: list[dict], query_type: str = "SELECT") -> ImpactResult:
    """
    Analyze the real-world impact of a SQL query.
    
    Args:
        tables: List of table names extracted from SQL
        issues: List of issue dicts from analyzer (with 'rule' and 'severity' keys)
        query_type: SQL statement type (SELECT, INSERT, UPDATE, DELETE)
    
    Returns:
        ImpactResult with affected services, predictions, and alert info
    """
    service_map = _load_service_map()

    # ── Map tables → services ──
    affected_services: list[ServiceImpact] = []
    seen_services = set()

    for table in tables:
        # Handle schema-qualified names (schema.table)
        table_name = table.split(".")[-1].lower()
        if table_name in service_map:
            info = service_map[table_name]
            svc_name = info["service"]
            if svc_name not in seen_services:
                affected_services.append(ServiceImpact(
                    service=svc_name,
                    team=info.get("team", "unknown"),
                    criticality=info.get("criticality", "medium"),
                    description=info.get("description", ""),
                    affected_table=table_name,
                ))
                seen_services.add(svc_name)

    # ── Map issues → impact predictions ──
    predictions: list[str] = []
    business_impact: list[str] = []
    latency_impact: list[str] = []
    max_severity = "low"
    severity_rank = {"low": 0, "medium": 1, "high": 2, "critical": 3}

    for issue in issues:
        rule = issue.get("rule", "")
        if rule in _IMPACT_RULES:
            impact = _IMPACT_RULES[rule]
            predictions.append(impact["prediction"])
            business_impact.append(impact["business"])
            latency_impact.append(impact["latency"])

            if severity_rank.get(impact["severity"], 0) > severity_rank.get(max_severity, 0):
                max_severity = impact["severity"]

    # Write-operation escalation
    if query_type in ("UPDATE", "DELETE", "INSERT") and affected_services:
        critical_services = [s for s in affected_services if s.criticality in ("critical", "high")]
        if critical_services:
            svc_names = ", ".join(s.service for s in critical_services)
            predictions.append(f"Write operation on critical service data ({svc_names})")
            business_impact.append(f"Data mutation affects live systems — {svc_names} may serve stale/corrupt data")
            max_severity = "critical" if max_severity != "critical" else max_severity

    # ── Build alert message ──
    if not affected_services and not predictions:
        alert_message = "No significant impact detected"
    elif max_severity == "critical":
        svc_list = ", ".join(s.service for s in affected_services[:3])
        alert_message = f"🚨 CRITICAL: This query will impact {svc_list} — potential production outage"
    elif max_severity == "high":
        alert_message = f"⚠️ HIGH RISK: Query affects {len(affected_services)} service(s) — performance degradation likely"
    elif max_severity == "medium":
        alert_message = f"⚡ WARNING: Query has optimization issues affecting {len(affected_services)} service(s)"
    else:
        alert_message = "✅ Low impact — query appears safe"

    # ── Teams to notify ──
    teams = sorted(set(s.team for s in affected_services))

    return ImpactResult(
        affected_services=affected_services,
        predictions=predictions if predictions else ["No structural issues detected"],
        business_impact=business_impact if business_impact else ["Minimal business impact expected"],
        latency_impact=latency_impact if latency_impact else ["No significant latency concerns"],
        overall_severity=max_severity,
        alert_message=alert_message,
        teams_to_notify=teams,
    )
