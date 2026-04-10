from io import BytesIO
from app.resp import (
    RESPError,
    RESPEot,
    RESPStream,
    Array,
    ArrayNull,
    BulkString,
    BulkNullString,
    SimpleString,
    parse,
    dump,
)

import pytest


@pytest.fixture
def resp_stream():
    # Simulate a StreamReader
    class MockStreamReader:
        def __init__(self, data: bytes):
            self.data = BytesIO(data)
            self.eof = False

        async def read(self, n: int) -> bytes:
            r = self.data.read(n)
            if len(r) == 0 and n > 0:
                self.eof = True
            return r

        def feed_eof(self):
            self.eof = True

        def at_eof(self) -> bool:
            return self.eof

    def f(data: bytes):
        return RESPStream(MockStreamReader(data))

    yield f


@pytest.mark.asyncio
async def test_invalid_type(resp_stream):
    with pytest.raises(RESPError):
        await parse(resp_stream(b"@5\r\nhello\r\n"))


@pytest.mark.asyncio
async def test_simple_string(resp_stream):
    r = await parse(resp_stream(b"+hello\r\n"))
    assert r == SimpleString("hello")


@pytest.mark.asyncio
async def test_simple_string_empty(resp_stream):
    with pytest.raises((RESPEot, RESPError)):
        await parse(resp_stream(b"+\r\n"))


def test_simple_string_dump():
    assert dump(SimpleString("hello")) == b"+hello\r\n"


@pytest.mark.asyncio
async def test_bulk_string(resp_stream):
    r = await parse(resp_stream(b"$5\r\nhello\r\n"))
    assert r == BulkString("hello")


@pytest.mark.asyncio
async def test_bulk_string_empty(resp_stream):
    r = await parse(resp_stream(b"$0\r\n\r\n"))
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
    with pytest.raises((RESPEot, RESPError)):
        await parse(resp_stream(bulk_string))


def test_bulk_string_dump():
    assert dump(BulkString("A bulk string")) == b"$13\r\nA bulk string\r\n"


@pytest.mark.asyncio
async def test_bulk_null_string(resp_stream):
    r = await parse(resp_stream(b"$-1\r\n"))
    assert r == BulkNullString()


def test_bulk_null_string_dump():
    assert dump(BulkNullString()) == b"$-1\r\n"


@pytest.mark.asyncio
async def test_array(resp_stream):
    r = await parse(resp_stream(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"))
    assert r == Array([BulkString("hello"), BulkString("world")])


@pytest.mark.asyncio
async def test_array_null(resp_stream):
    r = await parse(resp_stream(b"*-1\r\n"))
    assert r == ArrayNull()


@pytest.mark.asyncio
async def test_array_empty(resp_stream):
    r = await parse(resp_stream(b"*0\r\n"))
    assert r == Array([])


def test_array_dump():
    assert (
        dump(Array([BulkString("name"), BulkString("Poirot")]))
        == b"*2\r\n$4\r\nname\r\n$6\r\nPoirot\r\n"
    )
