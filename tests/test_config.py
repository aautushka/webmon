from webmon.scheduler import validate_config
import webmon.constants as constants


def test_valid_config():
    assert validate_config(
        {"url": "http://acme.com", "schedule": constants.MIN_POLL_PERIOD_SEC}
    )
    assert validate_config(
        {"url": "http://acme.com", "schedule": constants.MAX_POLL_PERIOD_SEC}
    )
    assert validate_config({"url": "http://acme.com", "schedule": 1, "regex": None})
    assert validate_config({"url": "http://acme.com", "schedule": 1, "regex": "abc"})


def test_cleanup_config():
    config = {"url": "http://acme.com", "schedule": 1}
    assert config == validate_config(config)
    assert config == validate_config({**config, "junk": 123})
    assert config == validate_config({**config, "regex": None})
    assert {**config, "regex": "123"} == validate_config({**config, "regex": "123"})


def test_invalid_config():
    assert not validate_config([])
    assert not validate_config({"url": "http://acme.com"})
    assert not validate_config({"schedule": constants.MAX_POLL_PERIOD_SEC})
    assert not validate_config(
        {"url": "http://acme.com", "schedule": constants.MAX_POLL_PERIOD_SEC + 1}
    )
    assert not validate_config(
        {"url": "http://acme.com", "schedule": constants.MIN_POLL_PERIOD_SEC - 1}
    )
    assert not validate_config({"url": "http://acme.com", "schedule": "1"})
    assert not validate_config({"url": None, "schedule": 1})
    assert not validate_config({"url": "http://acme.com", "schedule": None})
    assert not validate_config({"url": str.encode("http://acme.com"), "schedule": "1"})
    assert not validate_config({"url": "http://acme.com", "schedule": 1, "regex": 123})
