import aiohttp
import asyncio
from webmon import util

# TODO
# error handling:
# * http error status
# * connection failure
# * connection terminated
# * ssl failure
# * server timeout
# * client timeout
# * regex failure


async def fetch_url(request: dict) -> dict:
    try:
        seconds = request.get("schedule", 300)
        timeout = aiohttp.ClientTimeout(total=seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(request["url"]) as response:
                return {**request, "status": "completed", "code": response.status}
    except asyncio.exceptions.TimeoutError:
        return {**request, "status": "client timeout"}
    except Exception as e:
        print(f"exception {e} of type {type(e)}")
        return {**request, "status": "exception"}


async def run_async(source, sink) -> None:
    tasks = []
    first_received = None
    while True:
        batch = None
        if not source.empty():
            batch = source.get()

        if batch:
            # for debugging
            if not first_received:
                first_received = util.now()

            tasks += [asyncio.create_task(fetch_url(x)) for x in batch]

        if tasks:
            done, inprogress = await asyncio.wait(tasks, timeout=0.1)
            tasks = [x for x in inprogress]

            if done:
                results = [x.result() for x in done]
                sink.put(results)

    await asyncio.sleep(0.5)


def monitor(source, sink) -> None:
    asyncio.run(run_async(source, sink))
