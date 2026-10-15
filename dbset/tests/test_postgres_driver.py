"""A postgresql:// URL without a driver works with whichever documented driver is installed.

SQLAlchemy 2.1 made psycopg (v3) the default driver of postgresql://, while dbset's
`postgres` extra installs psycopg2 and asyncpg; a plain URL then failed with
"No module named 'psycopg'".
"""

import pytest

from dbset import async_connect, connect
from dbset import connection
from dbset.connection import resolve_postgres_driver

from ._backends import _PG_URL

URL = "postgresql://u:p%40ss@h:5432/db"


def installed(*modules):
    return lambda name: name in modules


@pytest.mark.parametrize("is_async, modules, driver", [
    (False, ("psycopg",), None),                      # SQLAlchemy 2.1 default is there: URL unchanged
    (False, ("psycopg2",), "psycopg2"),               # only the extra's driver
    (False, ("psycopg2", "asyncpg"), "psycopg2"),
    (True, ("psycopg",), None),                       # psycopg does asyncio
    (True, ("asyncpg", "psycopg2"), "asyncpg"),
    (True, ("psycopg2",), None),                      # nothing async: leave it to SQLAlchemy's error
    (False, (), None),
])
def test_plain_url_gets_an_installed_driver(is_async, modules, driver):
    url = resolve_postgres_driver(URL, is_async=is_async, has_module=installed(*modules), default_driver="psycopg")
    assert url == (URL if driver is None else URL.replace("postgresql://", f"postgresql+{driver}://"))


def test_sqlalchemy_2_0_default_psycopg2_is_not_async():
    url = resolve_postgres_driver(URL, is_async=True, has_module=installed("psycopg2", "asyncpg"),
                                  default_driver="psycopg2")
    assert url.startswith("postgresql+asyncpg://")


@pytest.mark.parametrize("url", [
    "postgresql+psycopg2://u:p@h/db", "postgresql+asyncpg://u:p@h/db", "sqlite:///:memory:", "mysql://u:p@h/db",
])
def test_explicit_driver_or_other_database_unchanged(url):
    assert resolve_postgres_driver(url, is_async=False, has_module=installed("psycopg2")) == url


def test_password_is_kept_as_is():
    url = resolve_postgres_driver(URL, is_async=False, has_module=installed("psycopg2"), default_driver="psycopg")
    assert url == "postgresql+psycopg2://u:p%40ss@h:5432/db"


needs_pg = pytest.mark.skipif(not _PG_URL, reason="DBSET_PG_URL not set")


def _plain_pg_url() -> str:
    _, _, rest = _PG_URL.partition("://")
    return f"postgresql://{rest}"


@needs_pg
def test_connect_plain_url_without_psycopg(monkeypatch):
    real = connection._has_module
    monkeypatch.setattr(connection, "_has_module", lambda name: name != "psycopg" and real(name))
    db = connect(_plain_pg_url())
    assert db.engine.dialect.driver == "psycopg2"
    assert db.tables is not None
    db.close()


@needs_pg
async def test_async_connect_plain_url_without_psycopg(monkeypatch):
    real = connection._has_module
    monkeypatch.setattr(connection, "_has_module", lambda name: name != "psycopg" and real(name))
    db = await async_connect(_plain_pg_url())
    assert db.engine.dialect.driver == "asyncpg"
    assert await db.tables() is not None
    await db.close()
