from typing import Callable

from queue import SimpleQueue
from queue import Empty

from threading import Thread
import asyncio
import traceback


def make_queue():
    return SimpleQueue()


def retrieve_everything(queue) -> tuple[list, bool]:
    """Retrieve everything fom the queue in one go."""
    result = []
    last = False

    try:
        while not queue.empty():
            item = queue.get_nowait()
            if item:
                result.append(item)
            else:
                last = True
                break
    except Empty:
        pass

    return (result, last)


class Pipeline:
    """
    Multi threaded pipeline with message queues connecting individual threads.
    Each pipeline node has a handler function and a pair of queues (intput and output)
    associated with it.
    """

    def __init__(self):
        self.queues = []
        self.threads = []

    def add_node(self, handler: Callable) -> None:
        source = self.queues[-2]
        sink = self.queues[-1]

        thread = Thread(target=self.run, args=[source, sink, handler])
        thread.start()

        self.threads.append(thread)

    def run(self, source, sink, handler: Callable) -> None:
        handler(source, sink)
        sink.put(None)

    def first(self, handler: Callable):
        if self.queues:
            raise Exception("already have the first node")

        self.queues += [make_queue(), make_queue()]
        self.add_node(handler)
        return self

    def then(self, handler: Callable):
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

    def build(*args: Callable):
        if not args:
            raise Exception("unable to create an empty pipeline")

        pipeline = Pipeline()
        pipeline.first(args[0])
        [pipeline.then(x) for x in args[1:]]
        return pipeline
