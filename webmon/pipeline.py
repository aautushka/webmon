from queue import SimpleQueue
from threading import Thread


def make_queue():
    return SimpleQueue()


class Pipeline:
    def __init__(self):
        self.queues = []
        self.threads = []

    def add_node(self, handler):
        source = self.queues[-2]
        sink = self.queues[-1]

        thread = Thread(target=self.run, args=[source, sink, handler])
        thread.start()

        self.threads.append(thread)

    def run(self, source, sink, handler):
        handler(source, sink)

    def first(self, handler):
        if self.queues:
            raise Exception("already have the first node")

        self.queues += [make_queue(), make_queue()]
        self.add_node(handler)
        return self

    def then(self, handler):
        if not self.queues:
            raise Exception("no first node")

        self.queues += [make_queue()]
        self.add_node(handler)
        return self

    def wait(self) -> None:
        [x.join() for x in self.threads]

    def put(self, message):
        if not self.queues:
            raise Exception("the pipeline is not properly constructed")

        self.queues[0].put(message)
        return self

    def build(*args):
        if not args:
            raise Exception("unable to create an empty pipeline")

        pipeline = Pipeline()
        pipeline.first(args[0])
        [pipeline.then(x) for x in args[1:]]
        return pipeline