"""libSQL (Turso's SQLite fork) dialects for SQLAlchemy.

libSQL is SQLite plus a native vector type (``F32_BLOB(dim)``), distance functions
(``vector_distance_cos``, ``vector_distance_l2``) and a DiskANN index
(``CREATE INDEX ... (libsql_vector_idx(col))``). dbset registers two dialects:

    sqlite+dbsetlibsql:///path/to/file.db       sync,  driver: the ``libsql`` package
    sqlite+dbsetaiolibsql:///path/to/file.db    async, the same connection run in a
                                                worker thread by ``aiosqlite``

Both report ``dialect.name == 'sqlite'``, so every SQLite code path in dbset applies;
only the vector code asks :func:`is_libsql`.

The names are dbset's own, so they never shadow the ``sqlite+libsql://`` dialects of the
``sqlalchemy-libsql`` package (a process may use both). dbset does not build on that
package: it uses the abandoned ``libsql-experimental`` and its async dialect does not run
(every statement raises ``AwaitRequired``).

Local files and ``:memory:`` only; a URL with a host (remote Turso) is refused.
"""

from __future__ import annotations

import types

from sqlalchemy import exc
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.sqlite.aiosqlite import AsyncAdapt_aiosqlite_dbapi, SQLiteDialect_aiosqlite
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite

#: ``dialect.driver`` values that mean a libSQL connection (the registered URL names
#: differ). ``libsql`` also covers the sync dialect of the ``sqlalchemy-libsql`` package.
LIBSQL_DRIVERS = frozenset({"libsql", "aiolibsql"})


def is_libsql(dialect) -> bool:
    """True when ``dialect`` (or an Engine/AsyncEngine) talks to libSQL."""
    dialect = getattr(dialect, "dialect", dialect)
    return dialect.name == "sqlite" and dialect.driver in LIBSQL_DRIVERS


def _reject_remote(url):
    if url.host:
        raise exc.ArgumentError(
            "dbset supports local libSQL databases only (sqlite+dbsetlibsql:///path.db or "
            f"sqlite+dbsetlibsql://); remote libSQL/Turso URLs are not supported: {url!r}"
        )


def _libsql_connect_args(args, kwargs):
    """Adapt pysqlite connect arguments to ``libsql.connect``."""
    kwargs = dict(kwargs)
    # libsql names it _check_same_thread. SQLAlchemy's pool may hand a connection to
    # another thread (and aiosqlite always runs it in its own), so it is off.
    kwargs.pop("check_same_thread", None)
    kwargs["_check_same_thread"] = False
    # libsql.connect takes no uri/detect_types/cached_statements.
    for key in ("uri", "detect_types", "cached_statements"):
        kwargs.pop(key, None)
    return args, kwargs


def _ischema_names(base):
    """SQLite's reflected type names plus F32_BLOB(dim), reflected as dbset's Vector(dim)."""
    from .vector import Vector

    return {**base.ischema_names, "F32_BLOB": Vector}


class SQLiteDialect_libsql(SQLiteDialect_pysqlite):
    """Sync libSQL dialect: pysqlite's SQL with the ``libsql`` DBAPI module."""

    driver = "libsql"
    supports_statement_cache = True
    ischema_names = _ischema_names(SQLiteDialect_pysqlite)

    @classmethod
    def import_dbapi(cls):
        import libsql

        return libsql

    def create_connect_args(self, url):
        _reject_remote(url)
        args, kwargs = super().create_connect_args(url)
        return _libsql_connect_args(args, kwargs)

    def on_connect(self):
        # pysqlite registers REGEXP with create_function; libsql connections lack it.
        return None


class SQLiteDialect_aiolibsql(SQLiteDialect_aiosqlite):
    """Async libSQL dialect: aiosqlite's worker thread around a ``libsql`` connection."""

    driver = "aiolibsql"
    supports_statement_cache = True
    ischema_names = _ischema_names(SQLiteDialect_aiosqlite)

    @classmethod
    def import_dbapi(cls):
        import sqlite3

        import aiosqlite
        import libsql

        # aiosqlite with connect() swapped for one that opens a libsql connection;
        # its exceptions, constants and Connection class are kept as they are.
        shim = types.ModuleType("dbset_aiolibsql")
        shim.__dict__.update({k: v for k, v in vars(aiosqlite).items() if not k.startswith("__")})

        def connect(database, iter_chunk_size=64, **kwargs):
            return aiosqlite.Connection(lambda: libsql.connect(database, **kwargs), iter_chunk_size)

        shim.connect = connect
        # sqlite3 constants, but the SQLite version of libSQL (as the sync dialect reports).
        sqlite_shim = types.ModuleType("dbset_libsql_sqlite3")
        sqlite_shim.__dict__.update({k: v for k, v in vars(sqlite3).items() if not k.startswith("__")})
        sqlite_shim.sqlite_version_info = libsql.sqlite_version_info
        sqlite_shim.sqlite_version = ".".join(map(str, libsql.sqlite_version_info))
        shim.sqlite_version_info = libsql.sqlite_version_info
        shim.sqlite_version = sqlite_shim.sqlite_version
        return AsyncAdapt_aiosqlite_dbapi(shim, sqlite_shim)

    def create_connect_args(self, url):
        _reject_remote(url)
        args, kwargs = super().create_connect_args(url)
        return _libsql_connect_args(args, kwargs)

    def on_connect(self):
        return None


def register() -> None:
    """Make ``sqlite+dbsetlibsql://`` and ``sqlite+dbsetaiolibsql://`` resolve to dbset's dialects."""
    registry.register("sqlite.dbsetlibsql", __name__, "SQLiteDialect_libsql")
    registry.register("sqlite.dbsetaiolibsql", __name__, "SQLiteDialect_aiolibsql")


register()
