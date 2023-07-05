import re
from multiprocessing.pool import Pool
from multiprocessing import cpu_count


class RegexLibrary:
    def __init__(self):
        self.lib = {}

    def __call__(self, regex):
        if regex in self.lib:
            return self.lib[regex]

        try:
            compiled = re.compile(regex)
        except re.error:
            compiled = None

        self.lib[regex] = compiled
        return compiled


def append_status(message, status):
    if "status" in message:
        message["status"] += f",{status}"
    else:
        message["status"] = status


def search_regex(message):
    library = RegexLibrary()

    if library(message["regex"]).search(message["body"]):
        append_status(message, "regexok")
    else:
        append_status(message, "regexfail")

    # optimization: we don't need to copy memory one more time
    # in my tests just this one line saves 20%
    message.pop("body")
    return message


def validate(source, sink) -> None:
    library = RegexLibrary()

    pool = Pool(processes=max(cpu_count() - 1, 1))  # leave one core alone

    while batch := source.get():
        for message in batch:
            pending = False
            if regex := message.get("regex", None):
                if body := message.get("body", None):
                    if library(regex):
                        pending = True
                        pool.apply_async(
                            search_regex, [message], callback=lambda x: sink.put(x)
                        )
                    else:
                        append_status(message, "regexfail")

                    # if library(regex) and library(regex).search(body):
                    #     append_status(message, "regexok")
                    # else:
                    #     append_status(message, "regexfail")
                else:
                    append_status(message, "regexfail")

            if not pending:
                sink.put(message)

    pool.close()
    pool.join()
