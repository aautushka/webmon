from webmon.pipeline import Pipeline
from webmon.validator import validate
from tests.pipeline_nodes import Store


def make_request(body, regex):
    return [{"body": body, "regex": regex}]


def run_test(*requests):
    store = Store()
    pl = Pipeline.build(validate, store)
    pl.put(*requests, None).wait()

    return store.data


def test_regex():
    result = run_test(make_request("abc", "abc"))
    assert [{"body": "abc", "regex": "abc", "status": "regexok"}] == result

    assert "regexfail" == run_test(make_request("abc", "xyz"))[0]["status"]

    assert "regexfail" == run_test([{"regex": "a"}])[0]["status"]

    assert "ok" == run_test([{"body": "a", "status": "ok"}])[0]["status"]

    assert "ok" == run_test([{"status": "ok"}])[0]["status"]

    assert (
        "ok,regexfail"
        == run_test([{"regex": "a", "body": "b", "status": "ok"}])[0]["status"]
    )
