from webmon.pipeline import Pipeline
from tests.pipeline_nodes import Store, relay


def test_single_node():
    store = Store()
    pl = Pipeline.build(store)
    pl.put("hello").put(None).wait()

    assert ["hello"] == store.data


def test_pair_of_nodes():
    store = Store()
    pl = Pipeline.build(relay, store)
    pl.put("hello").put(None).wait()

    assert ["hello"] == store.data


def test_multiple_nodes():
    store = Store()
    pl = Pipeline.build(relay, relay, relay, relay, relay, relay, store)
    pl.put("hello").put(None).wait()

    assert ["hello"] == store.data
