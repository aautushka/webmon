import re
from contextlib import contextmanager

from typing import Optional

from multiprocessing.pool import Pool
from multiprocessing import cpu_count


class RegexLibrary:
    """Saves compile regexp object. Supposed to save a tiny bit of CPU time."""

    def __init__(self):
        self.lib = {}

    def __call__(self, regex: str) -> Optional[re.Pattern]:
        if regex in self.lib:
            return self.lib[regex]

        try:
            compiled = re.compile(regex)
        except re.error:
            compiled = None

        self.lib[regex] = compiled
        return compiled


def append_status(request: dict, status: str):
    if "status" in request:
        request["status"] += f",{status}"
    else:
        request["status"] = status


def search_regex(regex: Optional[re.Pattern], request: dict) -> dict:
    """Search regex and update status."""
    if regex is not None and regex.search(request["body"]):
        append_status(request, "regexok")
    else:
        append_status(request, "regexfail")

    # optimization: we don't need to copy memory one more time
    # in my tests just this one line saves 20%
    request.pop("body")
    return request


@contextmanager
def create_pool():
    """Create process pool."""
    try:
        # leave one core alone
        pool = Pool(processes=max(cpu_count() - 1, 1))
        yield pool
    finally:
        pool.close()
        pool.join()


def validate(source, sink) -> None:
    """
    Pipeline handler that either passes the messages through becuase no regex needed
    or asks another process to do the heavy loading.
    """

    library = RegexLibrary()

    with create_pool() as pool:
        while batch := source.get():
            out = []
            for message in batch:
                pending = False
                if regex := message.get("regex", None):
                    if body := message.get("body", None):
                        if library(regex):
                            pending = True

                            def forward(request: dict):
                                sink.put([request])

                            pool.apply_async(
                                search_regex,
                                [library(regex), message],
                                callback=forward,
                            )
                        else:
                            append_status(message, "regexfail")
                    else:
                        append_status(message, "regexfail")

                if not pending:
                    out.append(message)

            if out:
                sink.put(out)
