import re


class RegexLibrary:
    def __init__(self):
        self.lib = {}

    def __call__(self, regex):
        cached = self.lib.get(regex, None)
        if cached in self.lib:
            return regex

        compiled = re.compile(regex)
        self.lib[regex] = compiled
        return compiled


def append_status(message, status):
    if "status" in message:
        message["status"] += f",{status}"
    else:
        message["status"] = status


def validate(source, sink) -> None:
    library = RegexLibrary()

    while batch := source.get():
        print(f"batch {batch}")
        for message in batch:
            if regex := message.get("regex", None):
                if body := message.get("body", None):
                    if not library(regex).match(body):
                        append_status(message, "regexfail")
                    else:
                        append_status(message, "regexok")
                else:
                    append_status(message, "regexfail")

            sink.put(message)
