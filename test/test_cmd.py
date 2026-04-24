from app.cmd import execute, InvalidCommand
from app.resp import (
    Array,
    ArrayNull,
    BulkString,
    BulkNullString,
    SimpleString,
    Integer,
)
import pytest


def redis_cmd(*args: str) -> Array:
    return Array(list((map(BulkString, args))))


def test_ping():
    ping_cmd = redis_cmd("PING")
    assert execute(ping_cmd) == SimpleString("PONG")


def test_echo():
    echo_cmd = redis_cmd("ECHO", "Hello!")
    assert execute(echo_cmd) == BulkString("Hello!")


def test_set_get(storage):
    set_cmd = redis_cmd("SET", "name", "Hercules Poirot")
    assert execute(set_cmd) == SimpleString("OK")
    assert storage.get("name") == "Hercules Poirot"

    get_cmd = redis_cmd("GET", "name")
    assert execute(get_cmd) == BulkString("Hercules Poirot")


def test_get_missing(storage):
    get_cmd = redis_cmd("GET", "name")
    assert execute(get_cmd) == BulkNullString()


def test_rpush(storage):
    rpush_cmd = redis_cmd(
        "RPUSH",
        "sleuths",
        "Sherlock Holmes",
        "Hercules Poirot",
    )
    assert execute(rpush_cmd) == Integer(2)
    assert storage.get("sleuths") == ["Sherlock Holmes", "Hercules Poirot"]

    # Append more
    rpush_cmd = redis_cmd(
        "RPUSH",
        "sleuths",
        "Pepe Carvalho",
    )
    assert execute(rpush_cmd) == Integer(3)
    assert storage["sleuths"].value == [
        "Sherlock Holmes",
        "Hercules Poirot",
        "Pepe Carvalho",
    ]


def test_rpush_invalid(storage):
    with pytest.raises(InvalidCommand):
        execute(redis_cmd("RPUSH", "name"))


@pytest.fixture
def sleuths(storage):
    assert execute(
        redis_cmd(
            "RPUSH",
            "sleuths",
            "Sherlock Holmes",
            "Hercules Poirot",
            "Pepe Carvalho",
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

    lrange_cmd = redis_cmd("LRANGE", "sleuths", "0", "1")
    assert execute(lrange_cmd) == Array(
        [
            BulkString("Sherlock Holmes"),
            BulkString("Hercules Poirot"),
        ]
    )


def test_lrange_missing(storage):
    lrange_cmd = redis_cmd("LRANGE", "sleuths", "0", "2")
    assert execute(lrange_cmd) == Array([])


def test_lrange_start_index_ge_length(sleuths):
    lrange_cmd = redis_cmd("LRANGE", "sleuths", "100", "102")
    assert execute(lrange_cmd) == Array([])


def test_lrange_start_index_ge_end_index(sleuths):
    lrange_cmd = redis_cmd("LRANGE", "sleuths", "2", "0")
    assert execute(lrange_cmd) == Array([])


def test_lrange_end_index_ge_length(sleuths):
    lrange_cmd = redis_cmd("LRANGE", "sleuths", "0", "100")
    assert execute(lrange_cmd) == Array(
        [
            BulkString("Sherlock Holmes"),
            BulkString("Hercules Poirot"),
            BulkString("Pepe Carvalho"),
        ]
    )
