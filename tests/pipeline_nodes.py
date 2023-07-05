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
