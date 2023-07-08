import time
import logging

from webmon.pipeline import Pipeline
from webmon.scheduler import schedule
from webmon.monitor import monitor

import webmon.constants as constants
from tests.pipeline_nodes import Store, Sleep

# i'll have a bunch of warnings in my tests i do not want them to spoil my console output
logger = logging.getLogger()
logger.disabled = True


def make_pipeline() -> tuple[Pipeline, list[dict]]:
    store = Store()
    return (Pipeline.build(monitor, store), store.data)


def test_schedule():
    store = Store()
    pl = Pipeline.build(schedule, store)

    pl.put([{"url": "http://acme.com", "schedule": 1}], None).wait()
    assert 1 <= len(store.data) and len(store.data) <= 2


def test_uber_fast_schedule():
    # sorry, just could not stand long running tests
    constants.MIN_POLL_PERIOD_SEC = 0

    store = Store()
    pl = Pipeline.build(lambda x, y: schedule(x, y, 0), store)

    pl.put([{"url": "http://acme.com", "schedule": 0}])
    time.sleep(0.1)

    pl.put(None).wait()

    assert len(store.data) > 100


def test_drop_because_pipeline_is_busy():
    # sorry, just could not stand long running tests
    constants.MIN_POLL_PERIOD_SEC = 0

    store = Store()
    pl = Pipeline.build(lambda x, y: schedule(x, y, 0), Sleep(0.2), store)

    pl.put([{"url": "http://acme.com", "schedule": 0}])
    time.sleep(0.1)

    pl.put(None).wait()
    assert len(store.data) < 10
