from webmon import Pipeline


def relay(source, sink):
    while message := source.get():
        sink.put(message)

    sink.put(None)


class Store:
    def __init__(self):
        self.data = []

    def __call__(self, source, sink):
        while message := source.get():
            self.data.append(message)


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
