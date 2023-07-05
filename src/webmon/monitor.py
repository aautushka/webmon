import aiohttp
import asyncio
from . import util
from . import constants
import traceback
import time


async def fetch_url(request: dict) -> dict:
    result = {**request}
    started = time.time()
    try:
        # print(f"incoming request {request}")
        seconds = request.get("schedule", constants.MAX_POLL_PERIOD_SEC)
        timeout = aiohttp.ClientTimeout(total=seconds)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(request["url"]) as response:
                body = await response.text()
                result.update(
                    {
                        "status": "completed",
                        "code": response.status,
                        "body": body,
                    }
                )
    except aiohttp.ClientError as e:
        result.update({"status": type(e).__name__})
    except asyncio.exceptions.TimeoutError:
        result.update({"status": "TimeoutError"})
    except Exception as e:
        result.update({"status": "unknown error"})

    result["network_time_ms"] = int((time.time() - started) * 1000)
    return result


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
