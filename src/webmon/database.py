import asyncio
import asyncpg
from datetime import datetime

from typing import Optional
from typing import NamedTuple

import traceback

from . import pipeline
from . import constants


class ConnectionDetails(NamedTuple):
    user: str
    password: str
    database: str
    host: str


async def create_table(conn) -> None:
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
    try:
        async with pool.acquire() as connection:
            await connection.executemany(
                """
                INSERT INTO webmon(url, code, status, timestamp, response_time) 
                VALUES($1, $2, $3, $4, $5)
                """,
                rows,
            )
            # print(f"insert done {len(rows)}")
    except Exception as e:
        print(f"exception while inserting rows {e}")


def get_all(source) -> tuple[list[dict], bool]:
    items, last = pipeline.retrieve_everything(source)
    return ([x for batch in items for x in batch], last)


def convert_rows(rows: list[dict]) -> list[tuple]:
    res = [{**x, "ts": datetime.fromtimestamp(x["ts"])} for x in rows]
    order = ["url", "code", "status", "ts", "response_time_ms"]
    return [tuple([x.get(k, None) for k in order]) for x in res]


class Database:
    def __init__(self, connection_details: ConnectionDetails):
        self.connection_details = connection_details

    def __call__(self, source, sink):
        asyncio.run(self.run_async(source, sink))

    async def run_async(self, source, sink) -> None:
        terminated = False
        pending = []

        try:
            pool = await asyncpg.create_pool(
                **self.connection_details._asdict(),
                min_size=constants.PG_POOL_SIZE,
                max_size=constants.PG_POOL_SIZE,
            )

            async with pool.acquire() as connection:
                await create_table(connection)

            tasks: list = []
            while not terminated or tasks:
                if not terminated:
                    rows, terminated = get_all(source)

                    if rows:
                        pending += convert_rows(rows)

                if pending:
                    if len(tasks) < constants.PG_POOL_SIZE:
                        tasks.append(asyncio.create_task(insert_rows(pool, pending)))
                        pending = []

                if tasks:
                    _, incomplete = await asyncio.wait(tasks, timeout=1)
                    tasks = list(incomplete)
                else:
                    await asyncio.sleep(1)

        except Exception as e:
            print(f"Exception in database {e}")
            traceback.print_exc()
