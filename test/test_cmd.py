from app.cmd import execute, InvalidCommand
from app.resp import (
    Array,
    ArrayNull,
    BulkString,
    BulkNullString,
    SimpleString,
    Integer,
    dump,
)
import pytest


def redis_cmd(cmd: str, *args: str) -> Array:
    return Array((cmd, *(map(BulkString, args))))


def test_ping():
    ping_cmd = Array([BulkString("PING")])
    assert execute(ping_cmd) == SimpleString("PONG")


def test_echo():
    echo_cmd = Array([BulkString("ECHO"), BulkString("Hello!")])
    assert execute(echo_cmd) == BulkString("Hello!")


def test_set_get(storage):
    set_cmd = Array(
        [BulkString("SET"), BulkString("name"), BulkString("Hercules Poirot")]
    )
    assert execute(set_cmd) == SimpleString("OK")
    assert storage.get("name") == "Hercules Poirot"

    get_cmd = Array([BulkString("GET"), BulkString("name")])
    assert execute(get_cmd) == BulkString("Hercules Poirot")


def test_get_missing(storage):
    get_cmd = Array([BulkString("GET"), BulkString("name")])
    assert execute(get_cmd) == BulkNullString()


def test_rpush(storage):
    rpush_cmd = Array(
        [
            BulkString("RPUSH"),
            BulkString("sleuths"),
            BulkString("Sherlock Holmes"),
            BulkString("Hercules Poirot"),
        ]
    )
    assert execute(rpush_cmd) == Integer(2)
    assert storage.get("sleuths") == ["Sherlock Holmes", "Hercules Poirot"]

    # Append more
    rpush_cmd = Array(
        [
            BulkString("RPUSH"),
            BulkString("sleuths"),
            BulkString("Pepe Carvalho"),
        ]
    )
    assert execute(rpush_cmd) == Integer(3)
    assert storage["sleuths"].value == [
        "Sherlock Holmes",
        "Hercules Poirot",
        "Pepe Carvalho",
    ]


def test_rpush_invalid(storage):
    with pytest.raises(InvalidCommand):
        execute(Array([BulkString("RPUSH"), BulkString("name")]))


@pytest.fixture
def sleuths(storage):
    assert execute(
        Array(
            [
                BulkString("RPUSH"),
                BulkString("sleuths"),
                BulkString("Sherlock Holmes"),
                BulkString("Hercules Poirot"),
                BulkString("Pepe Carvalho"),
            ]
        )
    ) == Integer(3)
    yield storage


def test_lrange(sleuths):
    lrange_cmd = redis_cmd("LRANGE", "sleuths", "0", "2")
    assert execute(lrange_cmd) == Array(
        [
            BulkString("Sherlock Holmes"),
            BulkString("Hercules Poirot"),
            BulkString("Pepe Carvalho"),
        ]
    )

    lrange_cmd = Array(
        [
            BulkString("LRANGE"),
            BulkString("sleuths"),
            Integer(0),
            Integer(1),
        ]
    )
    assert execute(lrange_cmd) == Array(
        [
            BulkString("Sherlock Holmes"),
            BulkString("Hercules Poirot"),
        ]
    )


def test_lrange_missing(storage):
    lrange_cmd = Array(
        [
            BulkString("LRANGE"),
            BulkString("sleuths"),
            Integer(0),
            Integer(2),
        ]
    )
    assert execute(lrange_cmd) == ArrayNull()


def test_lrange_start_index_ge_length(sleuths):
    lrange_cmd = Array(
        [
            BulkString("LRANGE"),
            BulkString("sleuths"),
            Integer(100),
            Integer(102),
        ]
    )
    assert execute(lrange_cmd) == ArrayNull()


def test_lrange_start_index_ge_end_index(sleuths):
    lrange_cmd = Array(
        [
            BulkString("LRANGE"),
            BulkString("sleuths"),
            Integer(2),
            Integer(0),
        ]
    )
    assert execute(lrange_cmd) == ArrayNull()


def test_lrange_end_index_ge_length(sleuths):
    lrange_cmd = Array(
        [
            BulkString("LRANGE"),
            BulkString("sleuths"),
            Integer(0),
            Integer(100),
        ]
    )
    assert execute(lrange_cmd) == Array(
        [
            BulkString("Sherlock Holmes"),
            BulkString("Hercules Poirot"),
            BulkString("Pepe Carvalho"),
        ]
    )
