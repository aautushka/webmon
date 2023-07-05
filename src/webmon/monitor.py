import aiohttp
import asyncio
from webmon import util
import traceback

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
        print(f"incoming request {request}")
        seconds = request.get("schedule", 300)
        timeout = aiohttp.ClientTimeout(total=seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(request["url"]) as response:
                body = await response.text()
                return {
                    **request,
                    "status": "completed",
                    "code": response.status,
                    "body": body,
                }
    except asyncio.exceptions.TimeoutError:
        return {**request, "status": "client timeout"}
    except Exception as e:
        print(f"exception {e} of type {type(e)}")
        return {**request, "status": "exception"}


async def run_async(source, sink) -> None:
    try:
        tasks = []
        first_received = None
        terminate = False

        while not terminate:
            batch = None
            if not source.empty():
                batch = source.get()

                if batch == None:
                    terminate = True

            if batch:
                # for debugging
                if not first_received:
                    first_received = util.now()

                tasks += [asyncio.create_task(fetch_url(x)) for x in batch]

            if tasks:
                if not terminate:
                    done, inprogress = await asyncio.wait(tasks, timeout=0.1)
                    results = [x.result() for x in done]
                else:
                    done = tasks
                    results = await asyncio.gather(*tasks)
                    inprogress = []

                # print(f"done {len(done)} inprogress {len(inprogress)}")
                tasks = [x for x in inprogress]
                # print(f"{len(tasks)} yet to complete")

                if done:
                    sink.put(results)

            else:
                await asyncio.sleep(0.1)
    except Exception as e:
        print(f"exception in monitor {e} of type {type(e)}")
        traceback.print_exc()


def monitor(source, sink) -> None:
    asyncio.run(run_async(source, sink))
