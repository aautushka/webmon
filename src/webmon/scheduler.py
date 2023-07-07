import time
import logging

from typing import Optional

from . import util
from . import constants


def schedule(source, sink) -> None:
    config: dict = {}
    terminate = False

    while not terminate:
        request = None
        if not source.empty():
            try:
                request = source.get_nowait()
                if request and not isinstance(request, list):
                    request = None
                if not request:
                    terminate = True
            except:
                # should not happen
                pass

        tick(request, config, sink)
        time.sleep(constants.MIN_POLL_PERIOD_SEC)


def validate_config(config: dict) -> dict:
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
    now = util.now()
    new_config = {
        x["url"]: {**validate_config(x), "ts": now}
        for x in request
        if validate_config(x)
    }
    config.update(new_config)


def tick(request: Optional[list], config: dict, sink) -> None:
    if request:
        reload_config(request, config)

    now = util.now()
    batch = []
    for k, v in config.items():
        if now > v["ts"]:
            v["ts"] += v["schedule"]
            batch.append(v)

    if batch:
        if sink.qsize() < 2:
            sink.put(batch)
        else:
            logging.warning(f"Have to drop a batch of {len(batch)}, running busy")
