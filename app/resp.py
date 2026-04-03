"""RESP parser"""

from abc import ABC, abstractmethod
from enum import IntEnum, auto
from typing import Any, Iterable
import io
import re


def parse(data: RESPStream) -> bytes:
    return RESP().parse(data)


# Exceptions ------------------------------------------------------------------


class RESPError(ValueError):
    """RESP parsing error."""

    pass


class RESPEot(RESPError):
    """EOT before end of RESP type parsing."""

    pass


# Data streams ----------------------------------------------------------------


class RESPStream(ABC):
    @abstractmethod
    def read_byte(self) -> bytes:
        pass

    @abstractmethod
    def read_delimited(self) -> bytes:
        pass


class RESPBuffered(RESPStream):
    """Reads RESP data from a buffered binary stream (i.e. io.IOBytes)"""

    def __init__(self, buff: io.BufferedIOBase):
        self.buff = buff

    def read_byte(self) -> bytes:
        r = self.buff.read(1)
        if not r:
            raise RESPEot
        return r

    def read_delimited(self) -> bytes:
        r = b""
        while not r.endswith(b"\r\n"):
            r += self.read_byte()
        return r


class RESPSocket(RESPStream):
    """Reads RESP data from a TCP socket."""

    def __init__(self, skt):
        self.skt = skt

    def read_byte(self) -> bytes:
        r = self.skt.recv(1)
        if not r:
            raise RESPEot
        return r

    def read_delimited(self) -> bytes:
        r = b""
        while not r.endswith(b"\r\n"):
            r += self.read_byte()
        return r


# RESP types -----------------------------------------------------------------


class RESPTypeKind(IntEnum):
    SIMPLE_STR = auto()
    SIMPLE_ERROR = auto()
    INTEGER = auto()
    BULK_STR = auto()
    BULK_NULL_STR = auto()
    ARRAY = auto()
    ARRAY_NULL = auto()
    NULL = auto()
    BOOLEAN = auto()
    DOUBLE = auto()
    BIG_NUMBER = auto()
    BULK_ERROR = auto()
    VERBATIM_STR = auto()
    MAP = auto()
    ATTRIBUTE = auto()
    SET = auto()
    PUSH = auto()


class RESPType(ABC):
    __slots__ = ()

    def __init_subclass__(cls, type_: RESPTypeKind):
        super().__init_subclass__()
        cls.type_ = type_

    def __eq__(self, other: RESPType) -> bool:
        return self.type == other.type and self.value == other.value

    def __repr__(self) -> str:
        return f"<RESPType.{self.type.name} {self.value}>"

    @property
    def type(self) -> RESPTypeKind:
        return self.type_

    @property
    @abstractmethod
    def value(self) -> Any:
        pass

    @abstractmethod
    def dump(self) -> bytes:
        pass


class SimpleString(RESPType, type_=RESPTypeKind.SIMPLE_STR):
    __slots__ = ("_value",)

    def __init__(self, value: str):
        self._value = value

    @property
    def value(self) -> str:
        return self._value

    def dump(self) -> bytes:
        return f"+{self.value}\r\n".encode("utf8")


class Array(RESPType, type_=RESPTypeKind.ARRAY):
    __slots__ = ("_items",)

    def __init__(self, items: Iterable[RESPType]):
        self._items = items

    def __getitem__(self, i: int) -> RESPType:
        return self._items[i]

    @property
    def value(self) -> Iterable[RESPType]:
        return self._items

    def dump(self) -> bytes:
        prefix = f"*{len(self._items)}\r\n".encode("utf8")
        return b"".join((prefix, *map(lambda i: i.dump(), self._items)))


class ArrayNull(RESPType, type_=RESPTypeKind.ARRAY_NULL):
    __slots__ = ()

    @property
    def value(self) -> None:
        return None

    def dump(self) -> bytes:
        return b"*-1\r\n"


class BulkString(RESPType, type_=RESPTypeKind.BULK_STR):
    __slots__ = ("_value",)

    def __init__(self, value: str):
        self._value = value

    @property
    def value(self) -> str:
        return self._value

    def dump(self) -> bytes:
        return f"${len(self.value)}\r\n{self.value}\r\n".encode("utf8")


class BulkNullString(RESPType, type_=RESPTypeKind.BULK_NULL_STR):
    __slots__ = ()

    @property
    def value(self) -> None:
        return None

    def dump(self) -> bytes:
        return b"$-1\r\n"


# Parser ----------------------------------------------------------------------

# Grammar (https://redis.io/docs/latest/develop/reference/protocol-spec/)
#
# data          := type CRLF
# type          := simple_string
#               | simple_error
#               | integer
#               | bulk_string
#               | array
#               | null
#               | boolean
#               | double
#               | big_number
#               | bulk_error
#               | verbatim_str
#               | map
#               | attribute
#               | set
#               | push
#
# simple_string := '+' STR_LITERAL
# simple_error  := '-' (error_kind (' '|'\n'))? STR_LITERAL
# integer       := ':' INT_LITERAL
# bulk_string   := '$' ((UINT_LITERAL CRLF STR_LITERAL) | '-1')   // '-1' denotes the bulk null strings in RESP2
# array         := * (UINT_LITERAL CRLF type*) | '-1')    // '-1' denotes the null array in RESP2
# null          := '_'
# boolean       := '#' ('t'|'f')
# double        := ',' INT_LITERAL ('.'UINT_LITERAL)?(('E'|'e') INT_LITERAL)?
#               | [-]?'inf'
#               | 'nan'
# big_number    := '(' INT_LITERAL
# bulk_error    := '!' UINT_LITERAL CRLF (error_kind (' '|'\n'))? STR_LITERAL
# verbatim_str  := '=' UINT_LITERAL CRLF encoding ':' STR_LITERAL
# map           := '%' UINT_LITERAL CRLF (type type)+
# attribute     := '|' // Same as map
# set           := '~' UINT_LITERAL CRLF type*
# push          := '>' UINT_LITERAL CRLF type*
#
# error_kind    := TODO
# encoding      := [a-z]{3}
#
# STR_LITERAL   := [^\r\n]*
# INT_LITERAL   := [+-]?[0-9]+
# UINT_LITERAL  := [0-9]+
# CRLF          := '\r\n'

RE_STR_LITERAL = re.compile(rb"([^\r\n]*)\r\n")
RE_INT_LITERAL = re.compile(rb"([+-]?[0-9]+)\r\n")
RE_UINT_LITERAL = re.compile(rb"([0-9]+)\r\n")


class RESP:
    __slots__ = ("data",)

    """RESP parser"""

    def __init__(self):
        self.data = None

    # Parse **one** type.
    def parse(self, data: RESPStream) -> bytes:
        self.data = data
        return self.type_()

    # Rules -------------------------------------------------------------------

    def type_(self) -> RESPType:
        match self.data.read_byte():
            case b"+":
                return self.simple_string()
            case "-":
                return self.simple_error()
            case b":":
                return self.integer()
            case b"$":
                return self.bulk_string()
            case b"*":
                return self.array()
            case b"_":
                return self.null()
            case b"#":
                return self.boolean()
            case b",":
                return self.double()
            case b"(":
                return self.big_number()
            case b"!":
                return self.bulk_error()
            case b"=":
                return self.verbatim_string()
            case b"%":
                return self.map()
            case b"|":
                return self.attribute()
            case b"~":
                return self.set()
            case b">":
                return self.push()
            case other:
                self.raise_error(f"Invalid REDIS type {other}")

    def int_(
        self,
    ) -> bytes | None:
        d = self.data.read_delimited()
        if m := RE_INT_LITERAL.match(d):
            return m.group(1)
        else:
            return None

    def uint_(
        self,
    ) -> bytes | None:
        d = self.data.read_delimited()
        if m := RE_UINT_LITERAL.match(d):
            return m.group(1)
        else:
            return None

    def is_unsigned(self, int_literal: bytes) -> bool:
        return not int_literal.startswith((b"+", b"-"))

    def str_(self) -> bytes:
        d = self.data.read_delimited()
        if m := RE_STR_LITERAL.match(d):
            return m.group(1)
        else:
            return None

    def simple_string(self) -> SimpleString:
        if not (s := self.str_()):
            self.raise_error("Expected string literal")
        return SimpleString(s.decode("utf8"))

    def bulk_string(self) -> BulkString:
        if v := self.int_():
            if self.is_unsigned(v):
                if (length := int(v)) > 0:
                    s = self.str_()
                    if len(s) != length:
                        self.raise_error(f"Expected string literal of length {length}")
                    s = s.decode("utf8")
                else:
                    s = ""
                return BulkString(s)
            elif int(v) == -1:
                return BulkNullString()

        self.raise_error(f"Invalid bulk string format {self.data}")

    def array(self) -> dict:
        if v := self.int_():
            if self.is_unsigned(v):
                if (n_items := int(v)) > 0:
                    try:
                        items = [self.type_() for _ in range(n_items)]
                    except RESPEot:
                        self.raise_error(f"Expected string array of length {n_items}")
                else:
                    items = []
                return Array(items)
            elif int(v) == -1:
                return ArrayNull()

        self.raise_error(f"Invalid array format {self.data}")

    def raise_error(self, msg: str):
        raise RESPError(msg)

    # Not implemented ---------------------------------------------------------

    def not_implemented(self):
        raise NotImplementedError

    simple_error = integer = null = boolean = double = big_number = bulk_error = (
        verbatim_string
    ) = map = attribute = set = push = not_implemented
