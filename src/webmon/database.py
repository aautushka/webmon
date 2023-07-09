import asyncio

import asyncpg  # type: ignore

import logging
from datetime import datetime

from typing import Optional
from typing import NamedTuple
from typing import Any

import traceback

from . import pipeline
from . import constants
from . import util


class ConnectionDetails(NamedTuple):
    """Database connection details: username, password, etc."""

    user: str
    password: str
    database: str
    host: str
    port: int = 5432
    ssl: str = "require"


async def create_table(conn) -> None:
    """Create database table for to put the measurements in."""
    await conn.fetch(
        """CREATE TABLE IF NOT EXISTS webmon (
                       id SERIAL PRIMARY KEY, 
                       url VARCHAR(2000), 
                       code INT, 
                       status VARCHAR(255), 
                       timestamp TIMESTAMP, 
                       response_time INT)"""
    )


async def insert_rows(pool, rows) -> None:
    """Insert measurements into the created database table."""
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.executemany(
                """
                INSERT INTO webmon(url, code, status, timestamp, response_time) 
                VALUES($1, $2, $3, $4, $5)
                """,
                rows,
            )


def get_all(source) -> tuple[list[dict], bool]:
    """Try reading every messages there is from the input queue."""
    items, last = pipeline.retrieve_everything(source)
    return ([x for batch in items for x in batch], last)


def convert_rows(rows: list[dict]) -> list[tuple]:
    """Convert measurements in bulk from dict to tuple to connect with the API."""
    res = [{**x, "ts": datetime.fromtimestamp(x["ts"])} for x in rows]
    order = ["url", "code", "status", "ts", "response_time_ms"]
    return [tuple([x.get(k, None) for k in order]) for x in res]


async def create_db_connection_pool(details: ConnectionDetails) -> Any:
    """Create database connection pool."""
    pool = None
    try:
        pool = await asyncpg.create_pool(
            **details._asdict(),
            min_size=1,
            max_size=max(1, constants.PG_POOL_SIZE),
        )

        async with pool.acquire() as connection:
            await create_table(connection)

    except asyncpg.PostgresError as e:
        pass
    except OSError:
        pass

    return pool


async def count_records(connection_details: ConnectionDetails) -> int:
    """Count rows in the measurements table."""
    conn = await asyncpg.connect(**connection_details._asdict())

    res = await conn.fetch("SELECT count(*) FROM webmon")
    await conn.close()

    return res[0]["count"]


class PoolFactory:
    """
    Database connection pool factory.
    Creates just one pool and caches it or returns nothing if unable to connect.
    """

    def __init__(self, connection_details: ConnectionDetails):
        self.details = connection_details
        self.pool = None
        self.next_attempt = util.now()
        self.timeout_sec = 10

    async def obtain(self) -> Optional[Any]:
        """Create pool or return from cache."""
        if self.pool:
            return self.pool

        if self.next_attempt <= util.now():
            self.pool = await create_db_connection_pool(self.details)
            if not self.pool:
                logging.error(
                    f"Database misconfiguration or connection error. Will attempt again in {self.timeout_sec} seconds."
                )
                self.next_attempt = util.now() + 10

        return self.pool

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.pool:
            await self.pool.close()


class DatabaseErrors:
    """Class that accumulates and prints database errors."""

    def __init__(self):
        self.count = 0
        self.report_count = 0
        self.report_time = 0
        self.report_period = 10

    def accumulate(self) -> None:
        """Increment the number of error."""
        self.count += 1

    def report(self) -> None:
        """Report the errors if a certain amount of time has passed."""
        if (
            self.count > self.report_count
            and util.now() - self.report_time > self.report_period
        ):
            logging.error(
                f"Encountered a number of database errors: {self.count - self.report_count}"
            )
            self.report_count = self.count
            self.report_time = util.now()


class Database:
    """
    Pipeline handler class that receives messages from the input queue
    and dumps them wholesale into the database.
    """

    def __init__(self, connection_details: ConnectionDetails):
        self.connection_details = connection_details

    def __call__(self, source, sink) -> None:
        """The handler itself. Goes async immediately."""
        asyncio.run(self.run_async(source, sink))

    async def run_async(self, source, sink) -> None:
        """Async pipeline handler."""
        terminated = False
        pending: list = []
        errors = DatabaseErrors()
        tasks: list = []

        async with PoolFactory(self.connection_details) as factory:
            pool = await factory.obtain()

            while not terminated or tasks:
                if not pool:
                    if terminated:
                        break
                    else:
                        pool = await factory.obtain()

                if not terminated:
                    rows, terminated = get_all(source)

                    if rows:
                        pending += convert_rows(rows)

                        if len(pending) > constants.MAX_DB_RECORDS:
                            oldest = len(pending) - constants.MAX_DB_RECORDS
                            logging.warning(
                                f"Too many measurements waiting for database insertion. Removing the oldest {oldest}"
                            )
                            pending = pending[oldest:]

                if pending and pool:
                    if len(tasks) < constants.PG_POOL_SIZE:
                        tasks.append(asyncio.create_task(insert_rows(pool, pending)))
                        pending = []

                if tasks:
                    done, incomplete = await asyncio.wait(tasks, timeout=1)
                    for task in done:
                        if task.exception():
                            errors.accumulate()
                            tasks.append(task)
                        errors.report()

                    tasks = list(incomplete)
                else:
                    await asyncio.sleep(1)
