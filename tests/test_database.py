import time
import pytest
import asyncio
import asyncpg

from webmon.pipeline import Pipeline
from webmon.database import Database, ConnectionDetails


async def count_records(connection_details):
    conn = await asyncpg.connect(**connection_details._asdict())

    res = await conn.fetch("SELECT count(*) FROM webmon")
    await conn.close()

    return res[0]["count"]


@pytest.mark.asyncio
async def test_database():
    details = ConnectionDetails(
        user="newuser", password="password", host="localhost", database="webmon"
    )

    database = Database(details)
    pl = Pipeline.build(database)

    before = await count_records(details)

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

    pl.put(rows).put(rows).put(rows)
    await pl.put(None).wait_async()

    after = await count_records(details)

    assert 6 == after - before
