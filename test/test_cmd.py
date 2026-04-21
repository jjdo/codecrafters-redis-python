from app.cmd import execute, Stored, InvalidCommand, time_ms
from app.resp import Array, BulkString, dump
import pytest


@pytest.fixture
def storage(monkeypatch):
    storage = {}
    monkeypatch.setattr("app.cmd.storage", storage)
    yield storage


def test_ping():
    ping_cmd = Array([BulkString("PING")])
    reply = execute(ping_cmd)
    assert dump(reply) == b"+PONG\r\n"


def test_echo():
    ping_cmd = Array([BulkString("ECHO"), BulkString("Hello!")])
    reply = execute(ping_cmd)
    assert dump(reply) == b"$6\r\nHello!\r\n"


def test_set_get(storage):
    set_cmd = Array(
        [BulkString("SET"), BulkString("name"), BulkString("Hercules Poirot")]
    )
    reply = execute(set_cmd)
    assert dump(reply) == b"+OK\r\n"
    assert storage["name"].value == "Hercules Poirot"

    get_cmd = Array([BulkString("GET"), BulkString("name")])
    reply = execute(get_cmd)
    assert dump(reply) == b"$15\r\nHercules Poirot\r\n"


def test_get_missing(storage):
    get_cmd = Array([BulkString("GET"), BulkString("name")])
    reply = execute(get_cmd)
    assert dump(reply) == b"$-1\r\n"


def test_stored():
    assert Stored("a").expired(time_ms()) is False
    assert Stored("a", expiry_ms=100).expired(time_ms() + 50) is False
    assert Stored("a", expiry_ms=100).expired(time_ms() + 100) is True


def test_rpush(storage):
    rpush_cmd = Array(
        [
            BulkString("RPUSH"),
            BulkString("sleuths"),
            BulkString("Sherlock Holmes"),
            BulkString("Hercules Poirot"),
        ]
    )
    reply = execute(rpush_cmd)
    assert dump(reply) == b":2\r\n"
    assert storage["sleuths"].value == ["Sherlock Holmes", "Hercules Poirot"]

    # Append more
    rpush_cmd = Array(
        [
            BulkString("RPUSH"),
            BulkString("sleuths"),
            BulkString("Pepe Carvalho"),
        ]
    )
    reply = execute(rpush_cmd)
    assert dump(reply) == b":3\r\n"
    assert storage["sleuths"].value == [
        "Sherlock Holmes",
        "Hercules Poirot",
        "Pepe Carvalho",
    ]


def test_rpush_invalid(storage):
    with pytest.raises(InvalidCommand):
        rpush_cmd = Array([BulkString("RPUSH"), BulkString("name")])
        execute(rpush_cmd)
