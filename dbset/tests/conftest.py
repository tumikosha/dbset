"""Backend fixtures: make_db / make_async_db connect to SQLite, PostgreSQL and MySQL (see _backends.py)
and drop the tables a test made when it ends (PostgreSQL and MySQL keep them between runs)."""

import pytest
from sqlalchemy import text

from dbset import async_connect, connect

from ._backends import ASYNC_URLS, SYNC_URLS, backend_of


@pytest.fixture(params=SYNC_URLS)
def make_db(request):
    """make_db(**connect_kwargs) -> Database on the parametrized backend; .backend is 'sqlite'/'postgresql'/'mysql'."""
    url = request.param
    made = []

    def make(**kwargs):
        db = connect(url, **kwargs)
        made.append(db)
        return db

    make.backend = backend_of(url)
    yield make
    for db in made:
        for name in db.tables:
            if name.startswith("t_"):
                with db.engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {name}"))
        db.close()


@pytest.fixture(params=ASYNC_URLS)
async def make_async_db(request):
    """The async twin of make_db."""
    url = request.param
    made = []

    async def make(**kwargs):
        db = await async_connect(url, **kwargs)
        made.append(db)
        return db

    make.backend = backend_of(url)
    yield make
    for db in made:
        for name in await db.tables():
            if name.startswith("t_"):
                async with db.engine.begin() as conn:
                    await conn.execute(text(f"DROP TABLE IF EXISTS {name}"))
        await db.close()
