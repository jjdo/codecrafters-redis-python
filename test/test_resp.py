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
)

import pytest


def resp_stream(data: bytes) -> RESPStream:
    return RESPBuffered(io.BytesIO(data))


def test_invalid_type():
    resp = RESP()
    with pytest.raises(RESPError):
        resp.parse(resp_stream(b"@5\r\nhello\r\n"))


def test_bulk_string():
    resp = RESP()
    r = resp.parse(resp_stream(b"$5\r\nhello\r\n"))
    assert r == BulkString("hello")


def test_null_bulk_string():
    resp = RESP()
    r = resp.parse(resp_stream(b"$-1\r\n"))
    assert r == BulkNullString()


def test_empty_bulk_string():
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
def test_invalid_bulk_string(bulk_string):
    resp = RESP()
    with pytest.raises((RESPEot, RESPError)):
        resp.parse(resp_stream(bulk_string))


def test_array():
    resp = RESP()
    r = resp.parse(resp_stream(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"))
    assert r == Array([BulkString("hello"), BulkString("world")])


def test_null_array():
    resp = RESP()
    r = resp.parse(resp_stream(b"*-1\r\n"))
    assert r == ArrayNull()


def test_empty_array():
    resp = RESP()
    r = resp.parse(resp_stream(b"*0\r\n"))
    assert r == Array([])
