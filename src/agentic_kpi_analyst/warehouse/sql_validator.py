"""SQL safety validator — enforces read-only access and approved table restrictions."""

from __future__ import annotations

import re

from agentic_kpi_analyst.config import Settings
from agentic_kpi_analyst.models import SQLValidationResult


# Dangerous keywords that indicate write operations
_FORBIDDEN_KEYWORDS = [
    r"\bINSERT\b",
    r"\bUPDATE\b",
    r"\bDELETE\b",
    r"\bDROP\b",
    r"\bCREATE\b",
    r"\bALTER\b",
    r"\bTRUNCATE\b",
    r"\bREPLACE\b",
    r"\bCOPY\b",
    r"\bATTACH\b",
    r"\bDETACH\b",
    r"\bEXPORT\b",
    r"\bIMPORT\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
    r"\bEXEC\b",
    r"\bEXECUTE\b",
    r"\bCALL\b",
    r"\bMERGE\b",
    r"\bUPSERT\b",
]

_FORBIDDEN_PATTERN = re.compile(
    "|".join(_FORBIDDEN_KEYWORDS),
    re.IGNORECASE,
)

# Pattern to extract table/view references from FROM and JOIN clauses
_TABLE_REF_PATTERN = re.compile(
    r"(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)


def validate_sql(sql: str, settings: Settings | None = None) -> SQLValidationResult:
    """Validate a SQL query for safety.

    Checks:
    1. Must be a SELECT statement (no writes)
    2. No forbidden keywords (INSERT, UPDATE, DELETE, etc.)
    3. Only references approved tables/views
    4. No multiple statements (semicolons)
    5. Adds row limit if not present
    """
    if settings is None:
        from agentic_kpi_analyst.config import get_settings
        settings = get_settings()

    errors: list[str] = []
    sanitized = sql.strip()

    # Remove trailing semicolons
    sanitized = sanitized.rstrip(";").strip()

    # Check for empty query
    if not sanitized:
        return SQLValidationResult(is_valid=False, errors=["Empty SQL query"])

    # Check for multiple statements
    # Simple heuristic: split on semicolons not inside strings
    if ";" in sanitized:
        errors.append("Multiple SQL statements are not allowed (semicolons found in query body)")

    # Must start with SELECT or WITH (for CTEs)
    first_word = sanitized.split()[0].upper() if sanitized.split() else ""
    if first_word not in ("SELECT", "WITH"):
        errors.append(f"Only SELECT queries are allowed. Found: {first_word}")

    # Check for forbidden keywords (outside of string literals)
    # Strip string literals for safety check
    sql_no_strings = re.sub(r"'[^']*'", "''", sanitized)
    matches = _FORBIDDEN_PATTERN.findall(sql_no_strings)
    if matches:
        errors.append(f"Forbidden SQL keywords found: {', '.join(set(m.upper() for m in matches))}")

    # Extract CTE names so they're not flagged as unknown tables
    cte_names: set[str] = set()
    cte_pattern = re.compile(r"\b(\w+)\s+AS\s*\(", re.IGNORECASE)
    if first_word == "WITH":
        cte_names = {m.lower() for m in cte_pattern.findall(sql_no_strings)}

    # Check table references
    referenced_tables = _TABLE_REF_PATTERN.findall(sql_no_strings)
    allowed = set(t.lower() for t in settings.allowed_tables)
    for table in referenced_tables:
        tl = table.lower()
        if tl not in allowed and tl not in cte_names:
            errors.append(f"Table '{table}' is not in the approved list. Allowed: {sorted(allowed)}")

    # Add LIMIT if not present
    if not re.search(r"\bLIMIT\b", sanitized, re.IGNORECASE):
        sanitized = f"{sanitized}\nLIMIT {settings.max_sql_rows}"

    if errors:
        return SQLValidationResult(is_valid=False, errors=errors, sanitized_sql=sanitized)

    return SQLValidationResult(is_valid=True, errors=[], sanitized_sql=sanitized)
