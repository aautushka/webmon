import aiohttp
import asyncio
from . import util
from . import constants
import traceback
import time
import logging

import resource


def set_max_file_limit() -> None:
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (2**14, resource.RLIM_INFINITY))
    except ValueError:  # not everybody allows to do that
        pass


async def read_from_stream(stream: aiohttp.StreamReader, num_bytes: int) -> bytes:
    """Read from stream up to the number of bytes passed in the argument."""
    data = bytes()
    while chunk := await stream.read(num_bytes - len(data)):
        data += chunk

    return data


async def read_chunked_response(
    response: aiohttp.ClientResponse, num_bytes: int
) -> str:
    """Read bytes from stream and decode to text if possible."""
    data = await read_from_stream(response.content, constants.MAX_CONTENT_LENGTH)

    return str(data, response.get_encoding())


async def fetch_with_session(session: aiohttp.ClientSession, request: dict) -> dict:
    """Issue a GET request to the URL specified in the request dict."""
    result = {**request}
    started = time.time()
    try:
        result["ts"] = util.now()
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
        # print('timeout')
        result.update({"status": "TimeoutError"})
    except Exception as e:
        # print(f'exception happend {e} of type {type(e)}')
        result.update({"status": "unknown error"})

    result["response_time_ms"] = int((time.time() - started) * 1000)
    return result


async def fetch_url(request: dict) -> dict:
    """Issue a GET request to the URL specified in the request dict."""
    seconds = request.get("schedule", constants.MAX_POLL_PERIOD_SEC)
    timeout = aiohttp.ClientTimeout(total=seconds)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        return await fetch_with_session(session, request)


class SessionPool:
    """
    Pool of aiohttp sessions, each of them configured with a different timeout.
    Given the max timeout of 10 seconds we might have a pool of 9 sessions (1 second to 10 seconds)
    """

    def __init__(self):
        self.sessions = {}
        self.sessions2 = {}

    def __call__(self, request: dict) -> aiohttp.ClientSession:
        """Construct or fetch a session based on URL schedule."""
        schedule = min(request["schedule"], constants.MAX_CONNECTION_TIMEOUT)
        if not schedule in self.sessions:
            timeout = aiohttp.ClientTimeout(total=schedule)
            connector = aiohttp.TCPConnector(
                limit=0, force_close=True, ttl_dns_cache=300, use_dns_cache=True
            )
            session = aiohttp.ClientSession(timeout=timeout, connector=connector)
            self.sessions[schedule] = session

        return self.sessions[schedule]

    async def close(self) -> None:
        """Gracefully shutdown all sessions with underlaying TCP connections."""
        tasks = [asyncio.create_task(v.close()) for k, v in self.sessions.items()]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.25)  # as per aiohttp documentation


def read_batch(pending: int, source) -> tuple[list[dict], bool]:
    """Read everything from queue until the queue is empty or we hit a limit."""
    result: list[dict] = []
    terminate = False
    # i think we want to be smarter than this way of limiting rescurces
    # we can just start actively dropping incoming requests if we reach our limits
    # but also we do not want to allow some rogue site to consume all our capacity
    while pending + len(result) < constants.MAX_CONNECTIONS and not source.empty():
        try:
            batch = source.get_nowait()
        except:
            # should not happen
            pass

        if batch == None:
            terminate = True
            break
        else:
            result += batch

    return (result, terminate)


async def run_async(source, sink) -> None:
    """
    Run pipeline node asynchronously.
    Read input messages from source queue.
    Write output messages to sink queue.
    """

    tasks: list[asyncio.Task] = []
    terminate = False

    pool = SessionPool()
    while not terminate:
        batch: list[dict] = []

        try:
            batch, terminate = read_batch(len(tasks), source)

            if batch:
                tasks += [
                    asyncio.create_task(fetch_with_session(pool(x), x)) for x in batch
                ]

            if tasks:
                if not terminate:
                    execute = tasks[: constants.MAX_CONNECTIONS]
                    onhold = tasks[constants.MAX_CONNECTIONS :]

                    done, pending = await asyncio.wait(execute, timeout=0.3)
                    # print(f"done {len(done)} pending {len(pending)}")

                    inprogress = list(pending) + onhold
                    results = [x.result() for x in done]
                else:
                    results = await asyncio.gather(*tasks)
                    inprogress = []

                tasks = inprogress

                if results:
                    sink.put(results)

            else:
                await asyncio.sleep(0.1)
        except Exception as e:
            logging.error(f"Exception in monitor {e} of type {type(e)}")
            traceback.print_exc()

    await pool.close()


def monitor(source, sink) -> None:
    set_max_file_limit()
    asyncio.run(run_async(source, sink))
