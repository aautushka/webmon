from monitor import monitor
from reporter import report
from scheduler import schedule
from pipeline import Pipeline

config = [
    {"url": "http://localhost:3000/test/test200", "schedule": 1, "regex": None},
    {"url": "http://localhost:3000/test/test404", "schedule": 5, "regex": None},
]


def main():
    pipeline = Pipeline()
    pipeline.first(schedule).then(monitor).then(report)

    pipeline.put(config)
    pipeline.wait()


if __name__ == "__main__":
    main()
