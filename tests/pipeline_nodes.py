import time


def relay(source, sink):
    while message := source.get():
        sink.put(message)


class Store:
    def __init__(self):
        self.data = []

    def __call__(self, source, sink):
        while message := source.get():
            self.data.append(message)
            sink.put(message)


class Sleep:
    def __init__(self, secs):
        self.secs = secs

    def __call__(self, source, sink):
        while message := source.get():
            time.sleep(self.secs)
            sink.put(message)
