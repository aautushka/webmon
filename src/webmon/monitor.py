import aiohttp
import asyncio
from . import util
from . import constants
import traceback
import time
import logging


async def read_from_stream(stream: aiohttp.StreamReader, num_bytes: int) -> bytes:
    data = bytes()
    while chunk := await stream.read(num_bytes - len(data)):
        data += chunk

    return data


async def read_chunked_response(
    response: aiohttp.ClientResponse, num_bytes: int
) -> str:
    data = await read_from_stream(response.content, constants.MAX_CONTENT_LENGTH)

    return str(data, response.get_encoding())


async def fetch_url(request: dict) -> dict:
    result = {**request}
    started = time.time()
    try:
        seconds = request.get("schedule", constants.MAX_POLL_PERIOD_SEC)
        timeout = aiohttp.ClientTimeout(total=seconds)

        result["ts"] = util.now()
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(request["url"], allow_redirects=False) as response:
                result.update(
                    {
                        "status": "completed",
                        "code": response.status,
                    }
                )

                if "regex" in request and (
                    response.content_type.startswith("text/")
                    or response.charset is not None
                ):
                    content_len = response.headers.get("content-length", None)
                    if (
                        content_len is not None
                        and int(content_len) < constants.MAX_CONTENT_LENGTH
                    ):
                        result["body"] = await response.text()
                    else:
                        result["body"] = await read_chunked_response(
                            response, constants.MAX_CONTENT_LENGTH
                        )

    except aiohttp.ClientError as e:
        result.update({"status": type(e).__name__})
    except asyncio.exceptions.TimeoutError:
        result.update({"status": "TimeoutError"})
    except Exception as e:
        result.update({"status": "unknown error"})

    result["response_time_ms"] = int((time.time() - started) * 1000)
    return result


async def run_async(source, sink) -> None:
    tasks: list[asyncio.Task] = []
    terminate = False

    while not terminate:
        batch = None

        try:
            # i think we want to be smarter than this way of limiting resources
            # we can just start actively dropping incoming requests if we reach our limits
            # but also we do not want to allow some rogue site to consume all our capacity
            if len(tasks) < 2 * constants.MAX_CONNECTIONS and not source.empty():
                try:
                    batch = source.get_nowait()
                except:
                    # should not happen
                    pass

                if batch == None:
                    terminate = True

            if batch:
                tasks += [asyncio.create_task(fetch_url(x)) for x in batch]

            if tasks:
                if not terminate:
                    execute = tasks[: constants.MAX_CONNECTIONS]
                    onhold = tasks[constants.MAX_CONNECTIONS :]

                    done, pending = await asyncio.wait(execute, timeout=0.03)

                    inprogress = list(pending) + onhold
                    results = [x.result() for x in done]
                else:
                    results = await asyncio.gather(*tasks)
                    inprogress = []

                # print(f"done {len(done)} inprogress {len(inprogress)}")
                tasks = inprogress

                if results:
                    sink.put(results)

            else:
                await asyncio.sleep(0.03)
        except Exception as e:
            logging.error(f"Exception in monitor {e} of type {type(e)}")
            traceback.print_exc()


def monitor(source, sink) -> None:
    asyncio.run(run_async(source, sink))
