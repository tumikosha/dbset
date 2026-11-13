"""Backend matrix for tests: SQLite always; libSQL when the libsql package is installed;
PostgreSQL and MySQL when their URLs are set.

    DBSET_PG_URL=postgresql://dbset:dbset@127.0.0.1:55432/dbset \\
    DBSET_MYSQL_URL=mysql://dbset:dbset@127.0.0.1:33306/dbset pytest

The make_db / make_async_db fixtures in conftest.py connect to these backends.
"""

import importlib.util
import os
import uuid

import pytest

_PG_URL = os.environ.get("DBSET_PG_URL", "")
_MYSQL_URL = os.environ.get("DBSET_MYSQL_URL", "")
HAS_LIBSQL = importlib.util.find_spec("libsql") is not None
_LIBSQL_SKIP = pytest.mark.skipif(not HAS_LIBSQL, reason="libsql not installed")


def _with_driver(url: str, scheme: str, driver: str) -> str:
    if not url:
        return ""
    _, _, rest = url.partition("://")
    return f"{scheme}+{driver}://{rest}"


def _param(url: str, backend: str, env: str):
    return pytest.param(url, id=backend, marks=pytest.mark.skipif(not url, reason=f"{env} not set"))


SYNC_URLS = [
    pytest.param("sqlite:///:memory:", id="sqlite"),
    pytest.param("sqlite+dbsetlibsql://", id="libsql", marks=_LIBSQL_SKIP),
    _param(_with_driver(_PG_URL, "postgresql", "psycopg2"), "postgres", "DBSET_PG_URL"),
    _param(_with_driver(_MYSQL_URL, "mysql", "pymysql"), "mysql", "DBSET_MYSQL_URL"),
]

ASYNC_URLS = [
    pytest.param("sqlite+aiosqlite:///:memory:", id="sqlite"),
    pytest.param("sqlite+dbsetaiolibsql://", id="libsql", marks=_LIBSQL_SKIP),
    _param(_with_driver(_PG_URL, "postgresql", "asyncpg"), "postgres", "DBSET_PG_URL"),
    _param(_with_driver(_MYSQL_URL, "mysql", "aiomysql"), "mysql", "DBSET_MYSQL_URL"),
]


def table_name() -> str:
    """A fresh table name: PostgreSQL and MySQL keep tables between runs."""
    return f"t_{uuid.uuid4().hex[:10]}"


def backend_of(url: str) -> str:
    return url.split(":", 1)[0].split("+", 1)[0]
