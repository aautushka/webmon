import time
import pytest
import asyncio
import asyncpg

from webmon.pipeline import Pipeline
from webmon.database import Database, ConnectionDetails, count_records


rows = [
    {
        "url": "http://acme.com",
        "code": 200,
        "status": "completed",
        "ts": 1688704296.542176,
        "response_time_ms": 10,
    },
    {
        "url": "http://foo.com",
        "status": "error",
        "ts": 1688704296.542176,
        "response_time_ms": 0,
    },
]


@pytest.mark.asyncio
async def test_database():
    details = ConnectionDetails(
        user="newuser", password="password", host="localhost", database="webmon"
    )

    database = Database(details)
    pl = Pipeline.build(database)

    before = await count_records(details)

    pl.put(rows).put(rows).put(rows)
    await pl.put(None).wait_async()

    after = await count_records(details)

    assert 6 == after - before


@pytest.mark.asyncio
async def test_database_wrong_creds():
    details = ConnectionDetails(
        user="foo", password="bar", host="localhost", database="webmon"
    )

    pl = Pipeline.build(Database(details))

    await pl.put(rows).put(None).wait_async()


@pytest.mark.asyncio
async def test_database_connection_error():
    details = ConnectionDetails(
        user="foo", password="bar", host="localhost:80", database="webmon"
    )

    pl = Pipeline.build(Database(details))

    await pl.put(rows).put(None).wait_async()
