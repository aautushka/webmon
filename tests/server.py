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


async def utf16(request):
    body = request.query.get("body", "").encode("utf-16")
    return web.Response(
        body=body, headers={"Content-Type": "text/plain; charset=utf-16"}
    )


async def binary(request):
    body = b"\xc3\x28"  # fyi this is invalid utf-8
    return web.Response(body=body, headers={"Content-Type": "application/octet-stream"})


async def redirect(request):
    path = request.query.get("url", "/")
    raise web.HTTPFound(path)


async def compressed(request):
    response = web.Response(text="compressed content")
    response.enable_compression()
    return response


async def start(aiohttp_server):
    app = web.Application()
    for f in [http200, http500, sleep, utf16, redirect, binary, compressed]:
        app.router.add_get(f"/{f.__name__}", f)

    return await aiohttp_server(app)
