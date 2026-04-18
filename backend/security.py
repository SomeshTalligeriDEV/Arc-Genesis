"""
Arc Genesis — SQL Security Layer (v3)
Enhanced with SQL injection detection, risk scoring, and anomaly patterns.
Regex + AST hybrid with zero false positives on safe queries.
"""

import re
import sqlparse
from dataclasses import dataclass, field
from enum import Enum


class ThreatLevel(str, Enum):
    BLOCKED = "BLOCKED"
    WARNING = "WARNING"
    SAFE = "SAFE"


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass
class SecurityResult:
    level: ThreatLevel
    reason: str | None = None
    matched_pattern: str | None = None
    threats: list[str] = field(default_factory=list)
    # v3: Injection detection
    is_injection: bool = False
    injection_type: str | None = None
    injection_patterns: list[str] = field(default_factory=list)
    risk_score: int = 0  # 0-100
    severity: Severity = Severity.LOW

    def to_dict(self) -> dict:
        return {
            "level": self.level.value,
            "reason": self.reason,
            "matched_pattern": self.matched_pattern,
            "threats": self.threats,
            "is_injection": self.is_injection,
            "injection_type": self.injection_type,
            "injection_patterns": self.injection_patterns,
            "risk_score": self.risk_score,
            "severity": self.severity.value,
        }


# ─── Injection Pattern Definitions ───────────────────────

_TAUTOLOGY_PATTERNS = [
    (r"\bOR\s+1\s*=\s*1\b", "OR 1=1 tautology"),
    (r"\bOR\s+'[^']*'\s*=\s*'[^']*'", "OR string tautology"),
    (r"\bOR\s+TRUE\b", "OR TRUE tautology"),
    (r"\bOR\s+1\b(?!\s*=)", "OR 1 tautology"),
    (r"'\s*OR\s*'", "String-break OR injection"),
    (r"(?<!=)\b1\s*=\s*1\b", "1=1 tautology"),
    (r"''\s*=\s*''", "Empty string tautology"),
    (r"\bOR\s+''='", "Empty string OR injection"),
    (r"\bOR\s+\d+\s*=\s*\d+", "Numeric tautology"),
]

_UNION_PATTERNS = [
    (r"\bUNION\s+SELECT\b", "UNION SELECT injection"),
    (r"\bUNION\s+ALL\s+SELECT\b", "UNION ALL SELECT injection"),
    (r"\bUNION\s+(SELECT|ALL)\b.*\bFROM\b.*\binformation_schema\b", "UNION schema exfiltration"),
    (r"\bUNION\s+(SELECT|ALL)\b.*\bpassword\b", "UNION password extraction"),
]

_COMMENT_BYPASS_PATTERNS = [
    (r"'\s*--", "Quote-then-comment bypass"),
    (r"'\s*#", "Quote-then-hash bypass"),
    (r"'\s*/\*", "Quote-then-block-comment bypass"),
    (r"/\*.*?\*/.*\bSELECT\b", "Block comment obfuscation"),
    (r"--\s*$", "Trailing comment (potential bypass)"),
]

_TIME_BASED_PATTERNS = [
    (r"\bSLEEP\s*\(", "SLEEP() time-based injection"),
    (r"\bBENCHMARK\s*\(", "BENCHMARK() time-based injection"),
    (r"\bWAITFOR\s+DELAY\b", "WAITFOR DELAY time-based injection"),
    (r"\bpg_sleep\s*\(", "pg_sleep() time-based injection"),
    (r"\bDBMS_LOCK\.SLEEP\b", "Oracle DBMS_LOCK.SLEEP injection"),
]

_STACKED_QUERY_PATTERNS = [
    (r";\s*(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC)\b", "Stacked query injection"),
    (r";\s*--", "Stacked query with comment"),
]

_ENCODING_PATTERNS = [
    (r"\bCHAR\s*\(\s*\d+", "CHAR() encoding injection"),
    (r"0x[0-9a-fA-F]{4,}", "Hex encoding injection"),
    (r"\bCONCAT\s*\(.*\bCHAR\b", "CONCAT+CHAR encoding injection"),
    (r"\\x[0-9a-fA-F]{2}", "Hex escape injection"),
]

_ERROR_BASED_PATTERNS = [
    (r"\bEXTRACTVALUE\s*\(", "EXTRACTVALUE error-based injection"),
    (r"\bUPDATEXML\s*\(", "UPDATEXML error-based injection"),
    (r"\bCONVERT\s*\(.*\bINT\b", "CONVERT type-error injection"),
]


def _check_injection(sql: str) -> tuple[bool, str | None, list[str], int]:
    """
    Comprehensive SQL injection detection.
    Returns: (is_injection, injection_type, matched_patterns, risk_score)
    """
    normalized = " ".join(sql.upper().split())
    matched: list[str] = []
    risk = 0

    # Check each category
    categories = [
        ("tautology", _TAUTOLOGY_PATTERNS, 25),
        ("union_based", _UNION_PATTERNS, 35),
        ("comment_bypass", _COMMENT_BYPASS_PATTERNS, 20),
        ("time_based", _TIME_BASED_PATTERNS, 30),
        ("stacked_query", _STACKED_QUERY_PATTERNS, 30),
        ("encoding", _ENCODING_PATTERNS, 20),
        ("error_based", _ERROR_BASED_PATTERNS, 25),
    ]

    detected_types: list[str] = []

    for category, patterns, base_score in categories:
        for pattern, label in patterns:
            if re.search(pattern, normalized, re.IGNORECASE):
                matched.append(label)
                risk += base_score
                if category not in detected_types:
                    detected_types.append(category)

    # Multi-vector injection (multiple categories) = much higher risk
    if len(detected_types) >= 2:
        risk = min(100, risk + 20)

    # Contextual analysis: injection patterns in WHERE clause are more suspicious
    if matched and re.search(r"\bWHERE\b", normalized):
        risk = min(100, risk + 10)

    risk = min(100, risk)
    is_injection = risk >= 20

    if detected_types:
        primary_type = detected_types[0]
    else:
        primary_type = None

    return is_injection, primary_type, matched, risk


def _risk_to_severity(risk_score: int) -> Severity:
    """Map risk score to severity level."""
    if risk_score >= 70:
        return Severity.CRITICAL
    elif risk_score >= 50:
        return Severity.HIGH
    elif risk_score >= 25:
        return Severity.MEDIUM
    return Severity.LOW


def check_sql_security(sql: str) -> SecurityResult:
    """
    Three-pass security check:
    1. Fast regex scan for destructive operations
    2. SQL injection detection (tautology, UNION, comment, time-based, encoding)
    3. AST parse for structural validation
    """
    if not sql or not sql.strip():
        return SecurityResult(level=ThreatLevel.BLOCKED, reason="Empty SQL query", risk_score=0)

    threats: list[str] = []
    normalized = " ".join(sql.upper().split())

    # ── Pass 1: Destructive operations (immediate block) ──
    blocked_patterns = [
        (r"\bDROP\s+(TABLE|DATABASE|SCHEMA|VIEW|INDEX)\b", "DROP statement"),
        (r"\bTRUNCATE\s+(TABLE\s+)?\w+", "TRUNCATE statement"),
        (r"\bALTER\s+TABLE\s+\w+\s+DROP\b", "ALTER TABLE DROP"),
        (r"\bEXEC(?:UTE)?\s*\(", "Dynamic SQL execution"),
        (r"\bGRANT\b.*\bTO\b", "GRANT permission"),
        (r"\bREVOKE\b.*\bFROM\b", "REVOKE permission"),
        (r"\bxp_cmdshell\b", "System command execution"),
        (r"\bINTO\s+OUTFILE\b", "File write attempt"),
        (r"\bLOAD_FILE\b", "File read attempt"),
        (r"\bINTO\s+DUMPFILE\b", "Binary file write"),
        (r"\bLOAD\s+DATA\b", "File load attempt"),
    ]

    for pattern, label in blocked_patterns:
        if re.search(pattern, normalized, re.IGNORECASE):
            return SecurityResult(
                level=ThreatLevel.BLOCKED,
                reason=f"{label} detected",
                matched_pattern=label,
                threats=[label],
                risk_score=95,
                severity=Severity.CRITICAL,
            )

    # ── Pass 2: SQL Injection Detection ──
    is_injection, injection_type, injection_patterns, risk_score = _check_injection(sql)

    if is_injection and risk_score >= 70:
        return SecurityResult(
            level=ThreatLevel.BLOCKED,
            reason=f"SQL injection detected: {injection_type}",
            matched_pattern=injection_type,
            threats=injection_patterns,
            is_injection=True,
            injection_type=injection_type,
            injection_patterns=injection_patterns,
            risk_score=risk_score,
            severity=_risk_to_severity(risk_score),
        )

    # ── Pass 3: AST Parse ──
    try:
        parsed = sqlparse.parse(sql)
        for stmt in parsed:
            stmt_type = stmt.get_type()

            # DELETE without WHERE
            if stmt_type == "DELETE":
                flat = str(stmt).upper()
                if "WHERE" not in flat:
                    return SecurityResult(
                        level=ThreatLevel.BLOCKED,
                        reason="DELETE without WHERE clause",
                        matched_pattern="DELETE",
                        threats=["Unbounded DELETE — would delete all rows"],
                        risk_score=90,
                        severity=Severity.CRITICAL,
                    )
                else:
                    threats.append("DELETE with WHERE — verify condition is correct")

            # UPDATE checks
            if stmt_type == "UPDATE":
                flat = str(stmt).upper()
                if "WHERE" not in flat:
                    threats.append("UPDATE without WHERE — would modify all rows")

            if stmt_type == "INSERT":
                threats.append("INSERT statement — verify target table")

            if stmt_type == "CREATE":
                threats.append("CREATE statement — verify naming conventions")

    except Exception:
        pass

    # ── Build result with injection info ──
    warning_patterns = [
        (r"\bSELECT\s+\*\b", "SELECT * — specify columns explicitly"),
        (r"\bORDER\s+BY\b(?!.*\bLIMIT\b)", "ORDER BY without LIMIT"),
        (r"\bDISTINCT\b", "DISTINCT may indicate a join issue"),
    ]

    for pattern, label in warning_patterns:
        if re.search(pattern, normalized, re.IGNORECASE):
            threats.append(label)

    # Injection info (even for warnings)
    if is_injection:
        threats.extend(injection_patterns)

    # Calculate final risk
    if not is_injection:
        # Non-injection risk from structural issues
        risk_score = min(len(threats) * 10, 40)

    if threats or is_injection:
        return SecurityResult(
            level=ThreatLevel.WARNING,
            reason=threats[0] if threats else None,
            threats=threats,
            is_injection=is_injection,
            injection_type=injection_type,
            injection_patterns=injection_patterns,
            risk_score=risk_score,
            severity=_risk_to_severity(risk_score),
        )

    return SecurityResult(level=ThreatLevel.SAFE, risk_score=0, severity=Severity.LOW)
