from webmon.monitor import monitor
from webmon.scheduler import schedule
from webmon.validator import validate
from webmon.pipeline import Pipeline
from webmon.database import Database, ConnectionDetails

from typing import Optional, Any

import argparse
import json
import signal
import sys
import time

test_config = [
    {"url": "http://localhost:3000/test/test200", "schedule": 1, "regex": None},
    {"url": "http://localhost:3000/test/test404", "schedule": 5, "regex": None},
    {},
]

test_db = ConnectionDetails(
    user="newuser",
    password="password",
    host="localhost",
    database="webmon",
    ssl="prefer",
)


def print_to_console(source, sink) -> None:
    """Pipeline handler that prints its inputs to console."""
    while batch := source.get():
        for b in batch:
            # print([v for k, v in b.items()])
            print(f'{b["url"]:<50}\t{b["status"]}')
            pass
        sink.put(batch)


def run_pipeline(
    url_config: list[dict], db_config: Optional[ConnectionDetails]
) -> None:
    """Build and run the pipeline. This is crux of the matter."""
    pipeline = Pipeline.build(schedule, monitor, validate, print_to_console)

    if db_config:
        pipeline.then(Database(db_config))

    def consume(source, sink) -> None:
        while source.get():
            pass

    pipeline.then(consume)

    pipeline.put(url_config)
    pipeline.wait()


def load_config(json_or_file_path: str) -> Optional[list[dict]]:
    """Load config from JSON or file."""
    try:
        return json.loads(json_or_file_path)
    except Exception as e:
        pass

    try:
        with open(json_or_file_path, "r") as f:
            return json.loads(f.read())
    except Exception as e:
        return None


def configuration_from_args(
    args,
) -> Optional[tuple[list[Any], Optional[ConnectionDetails]]]:
    """Choose the pipeline configuration based on command line arguments."""
    if args.test:
        return (test_config, test_db)

    if not args.config:
        print("ERROR: --config is required to contain to the config file")
        return None

    config = load_config(args.config)
    if not config:
        print(f'ERROR: unable to read config json from "{args.config}"')
        return None

    dbconfig = {
        k: getattr(args, k, None) for k in ["user", "password", "database", "host"]
    }

    if not any(dbconfig.values()):
        print(
            f'WARNING: running without the database because the database is not configured, see --help"'
        )
        return (config, None)
    elif not all(dbconfig.values()):
        print(f'WARNING: missing database configuration paramters, see --help"')
        return None

    return (config, ConnectionDetails(**dbconfig, ssl=args.ssl, port=args.port))  # type: ignore


def parse_args(args_list: Optional[list[str]] = None):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(prog="webmon", description="Web monitoring tool")

    parser.add_argument(
        "--test",
        help="run webmon in test mode with hardcoded config and database",
        action="store_true",
    )

    parser.add_argument("--config", action="store", help="path to config files")

    parser.add_argument("--user", action="store", help="database username")

    parser.add_argument("--password", action="store", help="database password")

    parser.add_argument("--host", action="store", help="database host")

    parser.add_argument(
        "--port", action="store", type=int, default=5432, help="database port"
    )

    parser.add_argument("--database", action="store", help="database name")

    parser.add_argument(
        "--ssl", action="store", default="require", type=str, help="posgres SSL mode"
    )

    args = parser.parse_args(args_list)
    return args


def main() -> int:
    args = parse_args()
    conf = configuration_from_args(args)
    if not conf:
        return 1

    run_pipeline(*conf)
    return 0
