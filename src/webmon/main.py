from webmon.monitor import monitor
from webmon.scheduler import schedule
from webmon.validator import validate
from webmon.pipeline import Pipeline
from webmon.database import Database, ConnectionDetails

config = [
    {"url": "http://localhost:3000/test/test200", "schedule": 1, "regex": None},
    {"url": "http://localhost:3000/test/test404", "schedule": 5, "regex": None},
    {},
]


def print_to_console(source, sink):
    while batch := source.get():
        for b in batch:
            print([v for k, v in b.items()])

        sink.put(batch)


def main():
    details = ConnectionDetails(
        user="newuser",
        password="password",
        host="localhost",
        database="webmon",
        ssl="prefer",
    )

    # details = ConnectionDetails(
    #     user="avnadmin",
    #     password="******",
    #     host="pg-144798e6-autushka-7fec.aivencloud.com",
    #     database="defaultdb",
    #     port=25105,
    # )

    pipeline = Pipeline.build(
        schedule, monitor, validate, print_to_console, Database(details)
    )

    pipeline.put(config)
    pipeline.wait()


if __name__ == "__main__":
    main()
