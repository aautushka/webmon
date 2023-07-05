from webmon.monitor import fetch_url
import pytest

from tests.server import start


@pytest.mark.asyncio
async def test_http_200(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("http200")
    resp = await fetch_url({"url": url})

    resp.pop("network_time_ms")
    assert {"url": url, "code": 200, "status": "completed", "body": "success"} == resp


@pytest.mark.asyncio
async def test_http_500(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("http500")
    resp = await fetch_url({"url": url})

    resp.pop("body")
    resp.pop("network_time_ms")
    assert {"url": url, "code": 500, "status": "completed"} == resp


@pytest.mark.asyncio
async def test_http_404(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("notfound")
    resp = await fetch_url({"url": url})

    resp.pop("body")
    resp.pop("network_time_ms")
    assert {"url": url, "code": 404, "status": "completed"} == resp


@pytest.mark.asyncio
async def test_client_timeout(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("sleep?ms=1500")
    resp = await fetch_url({"url": url, "schedule": 1})

    assert resp["network_time_ms"] > 1000 and resp["network_time_ms"] < 1100

    resp.pop("network_time_ms")
    assert {"url": url, "status": "client timeout", "schedule": 1} == resp
