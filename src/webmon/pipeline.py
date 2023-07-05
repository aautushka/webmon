from queue import SimpleQueue
from threading import Thread
import asyncio


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
        try:
            handler(source, sink)
            sink.put(None)
        except Exception as e:
            print(f"exception {e} of type {type(e)} in {handler.__name__}")
            traceback.print_exc()

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

    async def wait_async(self) -> None:
        while self.threads:
            self.threads[-1].join(timeout=0)
            if not self.threads[-1].is_alive():
                self.threads = self.threads[:-1]
            await asyncio.sleep(0.1)

    def put(self, *messages):
        if not self.queues:
            raise Exception("the pipeline is not properly constructed")

        [self.queues[0].put(message) for message in messages]
        return self

    def build(*args):
        if not args:
            raise Exception("unable to create an empty pipeline")

        pipeline = Pipeline()
        pipeline.first(args[0])
        [pipeline.then(x) for x in args[1:]]
        return pipeline
