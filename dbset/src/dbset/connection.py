"""Connection pooling and management for sync and async engines."""
from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator

from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .exceptions import ConnectionError


class AsyncConnectionPool:
    """
    Async connection pool wrapper for AsyncEngine.

    Provides context manager for acquiring connections with automatic cleanup.
    SQLAlchemy AsyncEngine handles the actual pooling.
    """

    def __init__(self, engine: AsyncEngine):
        """
        Initialize pool with async engine.

        Args:
            engine: SQLAlchemy AsyncEngine with built-in connection pool
        """
        self._engine = engine

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[AsyncConnection]:
        """
        Acquire connection from pool as async context manager.

        Yields:
            AsyncConnection from the pool

        Raises:
            ConnectionError: If connection cannot be acquired

        Examples:
            >>> async with pool.acquire() as conn:
            ...     result = await conn.execute(stmt)
        """
        try:
            async with self._engine.begin() as conn:
                yield conn
        except Exception as e:
            raise ConnectionError(f"Failed to acquire connection: {e}")

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        """
        Get connection without starting transaction.

        Use this when you need connection but don't want auto-commit.

        Yields:
            AsyncConnection without transaction

        Examples:
            >>> async with pool.connect() as conn:
            ...     result = await conn.execute(stmt)
        """
        try:
            async with self._engine.connect() as conn:
                yield conn
        except Exception as e:
            raise ConnectionError(f"Failed to connect: {e}")

    async def close(self):
        """
        Close all connections in pool and dispose engine.

        Should be called when shutting down application.
        """
        await self._engine.dispose()


class SyncConnectionPool:
    """
    Sync connection pool wrapper for Engine.

    Provides context manager for acquiring connections with automatic cleanup.
    SQLAlchemy Engine handles the actual pooling.
    """

    def __init__(self, engine: Engine):
        """
        Initialize pool with sync engine.

        Args:
            engine: SQLAlchemy Engine with built-in connection pool
        """
        self._engine = engine

    @contextmanager
    def acquire(self) -> Iterator:
        """
        Acquire connection from pool as context manager.

        Yields:
            Connection from the pool

        Raises:
            ConnectionError: If connection cannot be acquired

        Examples:
            >>> with pool.acquire() as conn:
            ...     result = conn.execute(stmt)
        """
        try:
            with self._engine.begin() as conn:
                yield conn
        except Exception as e:
            raise ConnectionError(f"Failed to acquire connection: {e}")

    @contextmanager
    def connect(self) -> Iterator:
        """
        Get connection without starting transaction.

        Use this when you need connection but don't want auto-commit.

        Yields:
            Connection without transaction

        Examples:
            >>> with pool.connect() as conn:
            ...     result = conn.execute(stmt)
        """
        try:
            with self._engine.connect() as conn:
                yield conn
        except Exception as e:
            raise ConnectionError(f"Failed to connect: {e}")

    def close(self):
        """
        Close all connections in pool and dispose engine.

        Should be called when shutting down application.
        """
        self._engine.dispose()


def create_pool_config(
    pool_size: int = 5,
    max_overflow: int = 10,
    pool_timeout: float = 30.0,
    pool_recycle: int = 3600,
) -> dict:
    """
    Create connection pool configuration for SQLAlchemy engine.

    Args:
        pool_size: Number of connections to maintain in pool (default: 5)
        max_overflow: Max connections beyond pool_size (default: 10)
        pool_timeout: Seconds to wait for connection (default: 30.0)
        pool_recycle: Recycle connections after N seconds (default: 3600)

    Returns:
        Dictionary of pool configuration for create_engine/create_async_engine

    Examples:
        >>> config = create_pool_config(pool_size=10, max_overflow=20)
        >>> engine = create_async_engine(url, **config)
    """
    return {
        'pool_size': pool_size,
        'max_overflow': max_overflow,
        'pool_timeout': pool_timeout,
        'pool_recycle': pool_recycle,
        'pool_pre_ping': True,  # Test connections before using
    }
