from aiohttp import web
import asyncio


async def http200(request):
    return web.Response(text="success")


async def http500(request):
    raise Exception("server error")


async def sleep(request):
    ms = request.query.get("ms", 0)
    await asyncio.sleep(int(ms) / 1000)
    return web.Response(text="slept")


async def start(aiohttp_server):
    app = web.Application()
    for f in [http200, http500, sleep]:
        app.router.add_get(f"/{f.__name__}", f)

    return await aiohttp_server(app)
