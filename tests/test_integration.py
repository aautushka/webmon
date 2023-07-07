import asyncio
import pytest

from webmon.pipeline import Pipeline
from webmon.monitor import monitor
from webmon.scheduler import schedule
from webmon.validator import validate
from webmon.database import Database, ConnectionDetails, count_records

from tests.server import start
from tests.pipeline_nodes import Store


@pytest.mark.asyncio
async def test_integration_no_database(aiohttp_server):
    server = await start(aiohttp_server)
    store = Store()
    pl = Pipeline.build(schedule, monitor, validate, store)

    req = [
        {"url": str(server.make_url("http200")), "schedule": 1},
        {
            "url": str(server.make_url("utf16?body=hello")),
            "schedule": 1,
            "regex": "hello",
        },
    ]

    await pl.put(req, None).wait_async()

    data = [x for batch in store.data for x in batch]
    assert len(data) >= 2
    assert [200] * len(data) == [x["code"] for x in data]
    assert "completed,regexok" in [x["status"] for x in data]


@pytest.mark.asyncio
async def test_database_integration(aiohttp_server):
    server = await start(aiohttp_server)

    details = ConnectionDetails(
        user="newuser",
        password="password",
        host="localhost",
        database="webmon",
        ssl="prefer",
    )
    pl = Pipeline.build(schedule, monitor, validate, Database(details))

    before = await count_records(details)

    req = [{"url": str(server.make_url("http200")), "schedule": 1}]
    await pl.put(req, None).wait_async()

    after = await count_records(details)
    assert after - before >= 1
