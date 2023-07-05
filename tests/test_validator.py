from webmon.pipeline import Pipeline
from webmon.validator import validate
from tests.pipeline_nodes import Store
import time
import os


def make_request(body, regex):
    return [{"body": body, "regex": regex}]


def run_test(*requests):
    store = Store()
    pl = Pipeline.build(validate, store)
    pl.put(*requests, None).wait()

    return store.data


def test_regex():
    result = run_test(make_request("abc", "abc"))
    assert [{"regex": "abc", "status": "regexok"}] == result

    assert "regexfail" == run_test(make_request("abc", "xyz"))[0]["status"]

    assert "regexfail" == run_test([{"regex": "a"}])[0]["status"]

    assert "ok" == run_test([{"body": "a", "status": "ok"}])[0]["status"]

    assert "ok" == run_test([{"status": "ok"}])[0]["status"]

    assert (
        "ok,regexfail"
        == run_test([{"regex": "a", "body": "b", "status": "ok"}])[0]["status"]
    )

    assert "regexfail" == run_test([{"body": "abc", "regex": "[a-Z]"}])[0]["status"]


def test_large_request():
    text = open("data/huge.txt", "r").read()
    regex = "[\\w\\d\\s]+ i \\+ i"

    start = time.time()
    res = run_test([{"body": text, "regex": regex}])
    assert "regexok" == res[0]["status"]
    print(f"took {time.time() - start}")

    start = time.time()
    res = run_test([{"body": text, "regex": regex} for _ in range(0, 10)])
    assert ["regexok" for _ in range(0, 10)] == [x["status"] for x in res]
    print(f"took {time.time() - start}")
