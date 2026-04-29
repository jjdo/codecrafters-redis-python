from threading import Thread
import time
from app.cmd import execute, InvalidCommand
from app.resp import (
    Array,
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


def test_lrange_negative(sleuths):
    lrange_cmd = redis_cmd("LRANGE", "sleuths", "-2", "-1")
    assert execute(lrange_cmd) == Array(
        [
            BulkString("Hercules Poirot"),
            BulkString("Pepe Carvalho"),
        ]
    )

    lrange_cmd = redis_cmd("LRANGE", "sleuths", "1", "-1")
    assert execute(lrange_cmd) == Array(
        [
            BulkString("Hercules Poirot"),
            BulkString("Pepe Carvalho"),
        ]
    )


def test_lpush(storage):
    lpush_cmd = redis_cmd("LPUSH", "letters", "a", "b", "c")
    assert execute(lpush_cmd) == Integer(3)
    assert storage.get("letters") == ["c", "b", "a"]


def test_llen(sleuths):
    llen_cmd = redis_cmd("LLEN", "sleuths")
    assert execute(llen_cmd) == Integer(3)


def test_llen_missing():
    llen_cmd = redis_cmd("LLEN", "does_not_exist")
    assert execute(llen_cmd) == Integer(0)


def test_lpop(sleuths):
    lpop_cmd = redis_cmd("LPOP", "sleuths")
    assert execute(lpop_cmd) == BulkString("Sherlock Holmes")
    assert len(sleuths.get("sleuths")) == 2

    lpop_cmd = redis_cmd("LPOP", "sleuths")
    assert execute(lpop_cmd) == BulkString("Hercules Poirot")
    assert len(sleuths.get("sleuths")) == 1

    lpop_cmd = redis_cmd("LPOP", "sleuths")
    assert execute(lpop_cmd) == BulkString("Pepe Carvalho")
    assert len(sleuths.get("sleuths")) == 0

    lpop_cmd = redis_cmd("LPOP", "sleuths")
    assert execute(lpop_cmd) == BulkNullString()


def test_lpop_missing():
    lpop_cmd = redis_cmd("LPOP", "does_not_exist")
    assert execute(lpop_cmd) == BulkNullString()


def test_lpop_n(sleuths):
    lpop_cmd = redis_cmd("LPOP", "sleuths", "2")
    assert execute(lpop_cmd) == Array(
        [
            BulkString("Sherlock Holmes"),
            BulkString("Hercules Poirot"),
        ]
    )
    assert len(sleuths.get("sleuths")) == 1


def test_lpop_all(sleuths):
    lpop_cmd = redis_cmd("LPOP", "sleuths", "100")
    assert execute(lpop_cmd) == Array(
        [
            BulkString("Sherlock Holmes"),
            BulkString("Hercules Poirot"),
            BulkString("Pepe Carvalho"),
        ]
    )
    assert len(sleuths.get("sleuths")) == 0


def test_blpop(storage):
    def send_rpush():
        time.sleep(1)
        execute(redis_cmd("RPUSH", "sleuths", "Sherlock Holmes"))

    rpush = Thread(name="RPUSH", target=send_rpush)
    rpush.start()

    blpop_cmd = redis_cmd("BLPOP", "sleuths", "0")
    assert execute(blpop_cmd) == Array(
        [
            BulkString("sleuths"),
            BulkString("Sherlock Holmes"),
        ]
    )


def test_blpop_multiple(storage):
    blpop1_ret = None
    blpop2_ret = None

    def blpop_1():
        nonlocal blpop1_ret
        blpop1_ret = execute(redis_cmd("BLPOP", "sleuths", "0"))

    def blpop_2():
        nonlocal blpop2_ret
        blpop2_ret = execute(redis_cmd("BLPOP", "sleuths", "0"))

    blpop_1 = Thread(name="BLPOP_1", target=blpop_1, daemon=True)
    blpop_1.start()
    blpop_2 = Thread(name="BLPOP_2", target=blpop_2, daemon=True)
    blpop_2.start()

    def send_rpush():
        time.sleep(1)
        execute(redis_cmd("RPUSH", "sleuths", "Sherlock Holmes"))

    rpush = Thread(name="RPUSH", target=send_rpush)
    rpush.start()

    blpop_1.join(timeout=2)
    blpop_2.join(timeout=2)

    assert blpop1_ret == Array(
        [
            BulkString("sleuths"),
            BulkString("Sherlock Holmes"),
        ]
    )
    assert blpop2_ret is None
