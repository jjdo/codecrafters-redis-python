from app.cmd import execute
from app.resp import Array, BulkString
import pytest


@pytest.fixture
def storage(monkeypatch):
    storage = {}
    monkeypatch.setattr("app.cmd.storage", storage)
    yield storage


def test_ping():
    ping_cmd = Array([BulkString("PING")])
    reply = execute(ping_cmd)
    assert reply == b"+PONG\r\n"


def test_echo():
    ping_cmd = Array([BulkString("ECHO"), BulkString("Hello!")])
    reply = execute(ping_cmd)
    assert reply == b"$6\r\nHello!\r\n"


def test_set_get(storage):
    set_cmd = Array([BulkString("SET"), BulkString("name"), BulkString("Poirot")])
    reply = execute(set_cmd)
    assert reply == b"+OK\r\n"
    assert storage["name"] == "Poirot"

    get_cmd = Array([BulkString("GET"), BulkString("name")])
    reply = execute(get_cmd)
    assert reply == b"$6\r\nPoirot\r\n"


def test_get_missing(storage):
    get_cmd = Array([BulkString("GET"), BulkString("name")])
    reply = execute(get_cmd)
    assert reply == b"$-1\r\n"
