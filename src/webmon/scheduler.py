import time
import logging
import random

from typing import Optional, Generator

from . import util
from . import constants


def try_fetch_config(source) -> Generator[Optional[list[dict]], None, None]:
    """Try reading input queue untile sentinel is found."""
    while True:
        request = None
        if not source.empty():
            try:
                request = source.get_nowait()
                if request and not isinstance(request, list):
                    request = None
                if not request:
                    break
            except:
                # should not happen
                pass

        yield request


def schedule(source, sink, period=0.05) -> None:
    """
    Pipeline handler, reads from source, processes and puts outputs to sink.
    Receives configs with URLs and schedules HTTP requests, watches over time and schedule.
    """
    config: dict = {}

    for request in try_fetch_config(source):
        tick(request, config, sink)
        time.sleep(period)


def validate_config(config: dict) -> dict:
    """Validate single URL config."""
    if not isinstance(config, dict):
        return False

    checks = [
        ("url", str, False, None, None),
        (
            "schedule",
            int,
            False,
            constants.MIN_POLL_PERIOD_SEC,
            constants.MAX_POLL_PERIOD_SEC,
        ),
        ("regex", str, True, None, None),
    ]

    cleaned = {}
    for check in checks:
        field, field_type, optional, min_value, max_value = check
        if optional and (not field in config or not config[field]):
            continue

        if (
            not field in config
            or not isinstance(config[field], field_type)
            or (min_value is not None and config[field] < min_value)
            or (max_value is not None and config[field] > max_value)
        ):
            logging.warning(f"Wrong '{field}' config: {config}")
            return {}

        cleaned[field] = config[field]

    return cleaned


def reload_config(request: list, config: dict) -> None:
    """Reload config (but rather append) and start timing requests for overrides from scratch."""
    new_config = {
        x["url"]: {**validate_config(x)} for x in request if validate_config(x)
    }

    if new_config:
        now = util.now()

        def randomize_time():
            """generate time offset in fraction of seconds  based on max  connections per sec"""
            connections_per_second = constants.MAX_POLL_PERIOD_SEC
            maxrange = len(new_config.keys()) // connections_per_second * 1000

            if maxrange > 0:
                return random.randrange(0, maxrange) / 1000

            return 0

        # we want to randomize time as to avoid peaks and spread the load evenly
        new_config = {
            k: {**v, "ts": now + randomize_time()} for k, v in new_config.items()
        }

    config.update(new_config)


def tick(request: Optional[list], config: dict, sink) -> None:
    """Check if we are on schedule and issue HTTP requests."""
    if request:
        reload_config(request, config)

    now = util.now()
    batch = []
    for k, v in config.items():
        if now > v["ts"]:
            v["ts"] += v["schedule"]
            batch.append(v)

    if batch:
        if sink.qsize() < 2 * len(config.keys()):
            sink.put(batch)
        else:
            logging.warning(f"Have to drop a batch of {len(batch)}, running busy")
