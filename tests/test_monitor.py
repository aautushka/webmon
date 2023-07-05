import time
import pytest
import asyncio

from webmon.pipeline import Pipeline
from webmon.monitor import monitor

from tests.server import start
from tests.pipeline_nodes import Store, relay


def make_pipeline() -> Pipeline:
    store = Store()
    return (Pipeline.build(monitor, store), store.data)


def make_batch(server, count=1, path="http200", schedule=1) -> dict:
    url = server.make_url(path)
    return [{"url": url, "schedule": schedule} for _ in range(0, count)]


@pytest.mark.asyncio
async def test_sequential_requests(aiohttp_server):
    server = await start(aiohttp_server)
    pl, output = make_pipeline()

    req = make_batch(server, 1)
    await pl.put(req, req, req, None).wait_async()

    assert [200, 200, 200] == [y["code"] for x in output for y in x]


@pytest.mark.asyncio
async def test_concurrent_requests(aiohttp_server):
    server = await start(aiohttp_server)
    pl, output = make_pipeline()

    req = make_batch(server, 100)
    await pl.put(req, None).wait_async()

    assert [200 for _ in range(0, 100)] == [y["code"] for x in output for y in x]


@pytest.mark.asyncio
async def test_no_early_termination(aiohttp_server):
    server = await start(aiohttp_server)
    pl, output = make_pipeline()

    req = make_batch(server, 100)
    pl.put(req)
    await asyncio.sleep(0.3)

    pl.put(None)
    await pl.wait_async()

    assert [200 for _ in range(0, 100)] == [y["code"] for x in output for y in x]


@pytest.mark.asyncio
async def test_time_successful_concurrent_request(aiohttp_server):
    server = await start(aiohttp_server)
    pl, output = make_pipeline()

    req = make_batch(server, 100, "sleep?ms=1000", 100)

    start_time = time.time()
    await pl.put(req, None).wait_async()
    total_time = time.time() - start_time

    assert total_time > 1 and total_time < 2
    assert [200 for _ in range(0, 100)] == [y["code"] for x in output for y in x]


@pytest.mark.asyncio
async def test_time_successful_concurrent_request(aiohttp_server):
    server = await start(aiohttp_server)
    pl, output = make_pipeline()

    req = make_batch(server, 100, "sleep?ms=1500", 100)

    start_time = time.time()
    await pl.put(req, None).wait_async()
    total_time = time.time() - start_time

    assert total_time > 1 and total_time < 2


@pytest.mark.asyncio
async def test_measure_time(aiohttp_server):
    server = await start(aiohttp_server)
    pl, output = make_pipeline()

    req = make_batch(server, 1, "sleep?ms=170")
    await pl.put(req, None).wait_async()

    network_time = output[0][0]["network_time_ms"]
    assert network_time > 170 and network_time < 200
