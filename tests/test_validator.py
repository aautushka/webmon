from webmon.pipeline import Pipeline
from webmon.validator import validate
from tests.pipeline_nodes import Store
import time
import os


def make_request(body: str, regex: str) -> list[dict]:
    return [{"body": body, "regex": regex}]


def run_test(*requests: dict) -> list[dict]:
    store = Store()
    pl = Pipeline.build(validate, store)
    pl.put(*requests, None).wait()

    print(store.data)
    return [x for batch in store.data for x in batch]


def run_first(*requests: dict) -> dict:
    return run_test(*requests)[0]


def test_regex():
    result = run_first(make_request("abc", "abc"))
    assert {"regex": "abc", "status": "regexok"} == result

    assert "regexfail" == run_first(make_request("abc", "xyz"))["status"]

    assert "regexfail" == run_first([{"regex": "a"}])["status"]

    assert "ok" == run_first([{"body": "a", "status": "ok"}])["status"]

    assert "ok" == run_first([{"status": "ok"}])["status"]

    assert (
        "ok,regexfail"
        == run_first([{"regex": "a", "body": "b", "status": "ok"}])["status"]
    )

    assert "regexfail" == run_first([{"body": "abc", "regex": "[a-Z]"}])["status"]


def test_large_request():
    text = open("data/huge.txt", "r").read()
    regex = "[\\w\\d\\s]+ i \\+ i"

    start = time.time()
    res = run_first([{"body": text, "regex": regex}])
    print(f"took {time.time() - start}")

    assert "regexok" == res["status"]

    start = time.time()
    iterations = 100
    res = run_test([{"body": text, "regex": regex} for _ in range(0, iterations)])
    print(f"took {time.time() - start}")

    assert ["regexok" for _ in range(0, iterations)] == [x["status"] for x in res]
