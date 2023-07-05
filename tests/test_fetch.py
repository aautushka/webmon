from webmon.monitor import fetch_url
import aiohttp
import asyncio
import pytest
import time

from aiohttp import web


async def http200(request):
    return web.Response(text="success")


async def http500(request):
    raise Exception("server error")


async def sleep(request):
    ms = request.query.get("ms", 0)
    time.sleep(int(ms) / 1000)
    return web.Response(text="slept")


async def start(aiohttp_server):
    app = web.Application()
    for f in [http200, http500, sleep]:
        app.router.add_get(f"/{f.__name__}", f)

    return await aiohttp_server(app)


@pytest.mark.asyncio
async def test_http_200(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("http200")
    resp = await fetch_url({"url": url})

    assert {"url": url, "code": 200, "status": "completed"} == resp


@pytest.mark.asyncio
async def test_http_500(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("http500")
    resp = await fetch_url({"url": url})

    assert {"url": url, "code": 500, "status": "completed"} == resp


@pytest.mark.asyncio
async def test_http_404(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("notfound")
    resp = await fetch_url({"url": url})

    assert {"url": url, "code": 404, "status": "completed"} == resp


@pytest.mark.asyncio
async def test_client_timeout(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("sleep?ms=1500")
    resp = await fetch_url({"url": url, "schedule": 1})

    assert {"url": url, "status": "client timeout", "schedule": 1} == resp
