import io
from app.resp import (
    RESP,
    RESPError,
    RESPEot,
    RESPBuffered,
    RESPStream,
    Array,
    ArrayNull,
    BulkString,
    BulkNullString,
    SimpleString,
)

import pytest


def resp_stream(data: bytes) -> RESPStream:
    return RESPBuffered(io.BytesIO(data))


def test_invalid_type():
    resp = RESP()
    with pytest.raises(RESPError):
        resp.parse(resp_stream(b"@5\r\nhello\r\n"))


def test_simple_string():
    resp = RESP()
    r = resp.parse(resp_stream(b"+hello\r\n"))
    assert r == SimpleString("hello")


def test_simple_string_empty():
    resp = RESP()
    with pytest.raises((RESPEot, RESPError)):
        resp.parse(resp_stream(b"+\r\n"))


def test_simple_string_dump():
    type_ = SimpleString("hello")
    assert type_.dump() == b"+hello\r\n"


def test_bulk_string():
    resp = RESP()
    r = resp.parse(resp_stream(b"$5\r\nhello\r\n"))
    assert r == BulkString("hello")


def test_bulk_string_empty():
    resp = RESP()
    r = resp.parse(resp_stream(b"$0\r\n\r\n"))
    assert r == BulkString("")


@pytest.mark.parametrize(
    "bulk_string",
    [
        b"$\r\nhello\r\n",  # missing length
        b"$5\r\n\r\n",  # missing string literal
        b"$5\r\nh\r\n",  # wrong string literal length
        b"$5\r\n",  # missing tail
        b"$5",  # missing tail
        b"$",  # missing tail
    ],
)
def test_bulk_string_invlid(bulk_string):
    resp = RESP()
    with pytest.raises((RESPEot, RESPError)):
        resp.parse(resp_stream(bulk_string))


def test_bulk_string_dump():
    array = BulkString("A bulk string")
    assert array.dump() == b"$13\r\nA bulk string\r\n"


def test_bulk_null_string():
    resp = RESP()
    r = resp.parse(resp_stream(b"$-1\r\n"))
    assert r == BulkNullString()


def test_bulk_null_string_dump():
    type_ = BulkNullString()
    assert type_.dump() == b"$-1\r\n"


def test_array():
    resp = RESP()
    r = resp.parse(resp_stream(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"))
    assert r == Array([BulkString("hello"), BulkString("world")])


def test_array_null():
    resp = RESP()
    r = resp.parse(resp_stream(b"*-1\r\n"))
    assert r == ArrayNull()


def test_array_empty():
    resp = RESP()
    r = resp.parse(resp_stream(b"*0\r\n"))
    assert r == Array([])


def test_array_dump():
    type_ = Array([BulkString("name"), BulkString("Poirot")])
    assert type_.dump() == b"*2\r\n$4\r\nname\r\n$6\r\nPoirot\r\n"
