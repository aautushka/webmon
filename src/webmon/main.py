from webmon.monitor import monitor
from webmon.reporter import report
from webmon.scheduler import schedule
from webmon.validator import validate
from webmon.pipeline import Pipeline

config = [
    {"url": "http://localhost:3000/test/test200", "schedule": 1, "regex": None},
    {"url": "http://localhost:3000/test/test404", "schedule": 5, "regex": None},
    {},
]


def main():
    pipeline = Pipeline.build(schedule, monitor, validate, report)

    pipeline.put(config)
    pipeline.wait()


if __name__ == "__main__":
    main()
