import asyncio
import socket
from app.async_resp import (
    RESP,
    RESPError,
    RESPEot,
    RESPStream,
    Array,
    ArrayNull,
    BulkString,
    BulkNullString,
    SimpleString,
)

import pytest


@pytest.fixture
def resp_stream():
    # Simulate a StreamReader
    class MockStreamReader:
        def __init__(self, data: bytes):
            self.data = data
            self.p = 0

        async def read(self, n: int) -> bytes:
            if self.at_eof():
                return b""
            if (self.p + n) > len(self.data):
                self.p = len(self.data)
                return self.data
            else:
                d = self.data[self.p : self.p + n]
                self.p += n
                return d

        def feed_eof(self):
            self.p = len(self.data)

        def at_eof(self) -> bool:
            return self.p >= len(self.data)

    def f(data: bytes):
        return RESPStream(MockStreamReader(data))

    yield f


@pytest.mark.asyncio
async def test_invalid_type(resp_stream):
    resp = RESP()
    with pytest.raises(RESPError):
        await resp.parse(resp_stream(b"@5\r\nhello\r\n"))


@pytest.mark.asyncio
async def test_simple_string(resp_stream):
    resp = RESP()
    r = await resp.parse(resp_stream(b"+hello\r\n"))
    assert r == SimpleString("hello")


@pytest.mark.asyncio
async def test_simple_string_empty(resp_stream):
    resp = RESP()
    with pytest.raises((RESPEot, RESPError)):
        await resp.parse(resp_stream(b"+\r\n"))


def test_simple_string_dump():
    type_ = SimpleString("hello")
    assert type_.dump() == b"+hello\r\n"


@pytest.mark.asyncio
async def test_bulk_string(resp_stream):
    resp = RESP()
    r = await resp.parse(resp_stream(b"$5\r\nhello\r\n"))
    assert r == BulkString("hello")


@pytest.mark.asyncio
async def test_bulk_string_empty(resp_stream):
    resp = RESP()
    r = await resp.parse(resp_stream(b"$0\r\n\r\n"))
    assert r == BulkString("")


@pytest.mark.asyncio
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
async def test_bulk_string_invlid(bulk_string, resp_stream):
    resp = RESP()
    with pytest.raises((RESPEot, RESPError)):
        await resp.parse(resp_stream(bulk_string))


def test_bulk_string_dump():
    array = BulkString("A bulk string")
    assert array.dump() == b"$13\r\nA bulk string\r\n"


@pytest.mark.asyncio
async def test_bulk_null_string(resp_stream):
    resp = RESP()
    r = await resp.parse(resp_stream(b"$-1\r\n"))
    assert r == BulkNullString()


def test_bulk_null_string_dump():
    type_ = BulkNullString()
    assert type_.dump() == b"$-1\r\n"


@pytest.mark.asyncio
async def test_array(resp_stream):
    resp = RESP()
    r = await resp.parse(resp_stream(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"))
    assert r == Array([BulkString("hello"), BulkString("world")])


@pytest.mark.asyncio
async def test_array_null(resp_stream):
    resp = RESP()
    r = await resp.parse(resp_stream(b"*-1\r\n"))
    assert r == ArrayNull()


@pytest.mark.asyncio
async def test_array_empty(resp_stream):
    resp = RESP()
    r = await resp.parse(resp_stream(b"*0\r\n"))
    assert r == Array([])


def test_array_dump():
    type_ = Array([BulkString("name"), BulkString("Poirot")])
    assert type_.dump() == b"*2\r\n$4\r\nname\r\n$6\r\nPoirot\r\n"
