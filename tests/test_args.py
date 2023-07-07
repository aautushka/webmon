from webmon.main import parse_args, configuration_from_args
from webmon.database import ConnectionDetails
from typing import Optional, Any
import tempfile
import json


def load(*args: str) -> Optional[tuple[list[Any], Optional[ConnectionDetails]]]:
    return configuration_from_args(
        parse_args(
            list(
                args,
            )
        )
    )


def test_insufficient_configuration():
    assert not load("--host=localhost")


def test_run_in_test_mode():
    urls, db = load("--test")
    assert db
    assert urls
    assert db.host == "localhost"


def test_wrong_config():
    assert not load("--config=foobarbaz")

    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(b"text")
        tmp.flush()

        assert not load(f"--config={tmp.name}")


def test_valid_config_file():
    with tempfile.NamedTemporaryFile() as tmp:
        config = [{"url": "http://acme.com", "schedule": 1}]
        tmp.write(bytes(json.dumps(config), "utf-8"))
        tmp.flush()

        urls, db = load(f"--config={tmp.name}")
        assert urls == config
        assert not db


def test_inline_config():
    config = [{"url": "http://acme.com", "schedule": 1}]
    urls, db = load("--config", json.dumps(config))
    assert urls == config
    assert not db


def test_insufficient_db_config():
    config = [{"url": "http://acme.com", "schedule": 1}]
    assert not load("--config", json.dumps(config), "--user", "testuser")


def test_sufficient_db_config():
    config = [{"url": "http://acme.com", "schedule": 1}]
    urls, db = load(
        "--config",
        json.dumps(config),
        "--user=u",
        "--password=p",
        "--host=h",
        "--database=db",
    )
    assert urls and db

    assert db == ConnectionDetails(
        user="u", password="p", host="h", database="db", ssl="require"
    )

    assert not load("--user=u", "--password=p", "--host=h", "--database=db")
