from webmon.monitor import fetch_url
import webmon.constants as constants
import pytest

from tests.server import start


@pytest.mark.asyncio
async def test_http_200(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("http200")
    resp = await fetch_url({"url": url})

    resp = {k: resp[k] for k in ["url", "code", "status"]}
    assert {"url": url, "code": 200, "status": "completed"} == resp


@pytest.mark.asyncio
async def test_http_500(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("http500")
    resp = await fetch_url({"url": url})

    resp = {k: resp[k] for k in ["url", "code", "status"]}
    assert {"url": url, "code": 500, "status": "completed"} == resp


@pytest.mark.asyncio
async def test_http_404(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("notfound")
    resp = await fetch_url({"url": url})

    resp = {k: resp[k] for k in ["url", "code", "status"]}
    assert {"url": url, "code": 404, "status": "completed"} == resp


@pytest.mark.asyncio
async def test_client_timeout(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("sleep?ms=1500")
    resp = await fetch_url({"url": url, "schedule": 1})

    assert resp["response_time_ms"] > 1000 and resp["response_time_ms"] < 1100

    resp = {k: resp[k] for k in ["url", "schedule", "status"]}
    assert {"url": url, "status": "TimeoutError", "schedule": 1} == resp


@pytest.mark.asyncio
async def test_connection_error(aiohttp_server):
    resp = await fetch_url({"url": "http://localhost:", "schedule": 1})
    assert "ClientConnectorError" == resp["status"]


@pytest.mark.asyncio
async def test_protocol_error(aiohttp_server):
    server = await start(aiohttp_server)

    url = str(server.make_url("http200"))
    url = url.replace("http://", "https://")
    resp = await fetch_url({"url": url})

    assert "ClientConnectorSSLError" == resp["status"]


@pytest.mark.asyncio
async def test_convert_content_to_utf8(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("utf16?body=こんにちは")
    resp = await fetch_url({"url": url, "regex": "abc"})

    assert "こんにちは" == resp["body"]


@pytest.mark.asyncio
async def test_do_not_read_body_if_regex_is_not_configured(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("utf16?body=こんにちは")
    resp = await fetch_url({"url": url})

    assert not "body" in resp
    assert 200 == resp["code"]


@pytest.mark.asyncio
async def test_do_not_follow_redirect(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("redirect?url=https://acme.com")
    resp = await fetch_url({"url": url})

    assert 302 == resp["code"]


@pytest.mark.asyncio
async def test_ignore_binary_content(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("binary?body=%c3%28")
    resp = await fetch_url({"url": url, "regex": "abc"})

    assert 200 == resp["code"]
    assert not "body" in resp
    assert "regex" in resp


@pytest.mark.asyncio
async def test_compressed_content(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("compressed")
    resp = await fetch_url({"url": url, "regex": "abc"})

    assert "compressed content" == resp["body"]


@pytest.mark.asyncio
async def test_trim_huge_responses(aiohttp_server):
    server = await start(aiohttp_server)

    url = server.make_url("huge")
    resp = await fetch_url({"url": url, "regex": "abc"})

    assert constants.MAX_CONTENT_LENGTH == len(resp["body"])
    assert 200 == resp["code"]
    assert "regex" in resp
