from webmon.monitor import fetch_url
import aiohttp
import asyncio
import pytest

from aiohttp import web

async def hello(request):
        return web.Response(text="hello")

async def fetch_async(request):
    async with aiohttp.ClientSession() as session:
        return await asyncio.gather(fetch_url(session, request))

@pytest.mark.asyncio
async def test_http(aiohttp_server):
    app = web.Application()
    app.router.add_get('/hello', hello)
    server = await aiohttp_server(app)

    resp = await fetch_async({"url": server.make_url('hello'), "schedule": 1})

    assert 1 == len(resp)
    assert 200 == resp[0]['code']
    assert 'completed' == resp[0]['status']

