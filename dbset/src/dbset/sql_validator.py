from __future__ import annotations

import re

FORBIDDEN_KEYWORDS = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "EXECUTE",
    "EXEC",
]

FORBIDDEN_PATTERN = re.compile(
    r"\b(" + "|".join(FORBIDDEN_KEYWORDS) + r")\b",
    re.IGNORECASE,
)


class SQLValidationError(Exception):
    pass


def validate_readonly(sql: str) -> bool:
    """
    Check if SQL query is read-only (SELECT only).
    Returns True if valid, raises SQLValidationError if not.
    """
    sql_clean = sql.strip()

    if not sql_clean:
        raise SQLValidationError("Empty SQL query")

    if not sql_clean.upper().startswith("SELECT"):
        raise SQLValidationError("Query must start with SELECT")

    match = FORBIDDEN_PATTERN.search(sql_clean)
    if match:
        raise SQLValidationError(f"Forbidden keyword detected: {match.group(1)}")

    return True


def extract_table_names(sql: str) -> list[str]:
    """
    Extract table names from SQL query.
    Improved regex-based extraction for FROM and JOIN clauses.
    Handles table aliases and avoids matching FROM in function calls like EXTRACT(... FROM ...).
    """
    tables: list[str] = []

    # Remove function calls containing FROM (like EXTRACT, SUBSTRING, etc.)
    # to avoid false positives
    sql_cleaned = re.sub(
        r'\b(EXTRACT|SUBSTRING|POSITION|TRIM)\s*\([^)]*\bFROM\b[^)]*\)',
        '',
        sql,
        flags=re.IGNORECASE
    )

    # Match FROM tablename [alias] - capture only table name, not alias
    # Pattern explanation:
    # \bFROM\s+ - FROM keyword followed by whitespace
    # ([a-zA-Z_][a-zA-Z0-9_]*) - table name (captured)
    # (?:\s+[a-zA-Z_][a-zA-Z0-9_]*)? - optional alias (not captured)
    from_pattern = re.compile(
        r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+(?:AS\s+)?[a-zA-Z_][a-zA-Z0-9_]*)?',
        re.IGNORECASE
    )

    # Match JOIN tablename [alias] - capture only table name, not alias
    join_pattern = re.compile(
        r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+(?:AS\s+)?[a-zA-Z_][a-zA-Z0-9_]*)?',
        re.IGNORECASE
    )

    tables.extend(from_pattern.findall(sql_cleaned))
    tables.extend(join_pattern.findall(sql_cleaned))

    return list(set(tables))


def validate_tables_exist(sql: str, existing_tables: list[str]) -> list[str]:
    """
    Check if all tables referenced in SQL exist in the schema.
    Returns list of missing tables (empty if all exist).
    """
    referenced_tables = extract_table_names(sql)
    existing_lower = [t.lower() for t in existing_tables]

    missing = []
    for table in referenced_tables:
        if table.lower() not in existing_lower:
            missing.append(table)

    return missing
