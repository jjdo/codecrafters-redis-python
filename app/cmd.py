"""REDIS commands"""

from typing import Any
from time import time_ns
from app.resp import Array, SimpleString, BulkString, BulkNullString


class Stored:
    def __init__(self, value: Any, expiry_ms: int = 0):
        self.value = value
        self.expiry(expiry_ms)

    def expiry(self, millisecs: int):
        self.expiry_ = 0 if not millisecs else (time_ms() + millisecs)

    def expired(self, now: int = 0) -> bool:
        if not self.expiry_:
            return False
        now = now or time_ms()
        return (now - self.expiry_) >= 0


def time_ms() -> int:
    return time_ns() // 1000_000


storage: dict[str, Stored] = {}


def execute(cmd: Array) -> bytes:
    match cmd[0].value.upper():
        case "PING":
            return SimpleString("PONG").dump()
        case "ECHO":
            return BulkString(cmd[1].value).dump()
        case "SET":
            key = cmd[1].value
            value = cmd[2].value
            if len(cmd) > 3:
                # Options
                match cmd[3].value:
                    case "EX":
                        expiry = int(cmd[4].value) * 1000
                    case "FX":
                        expiry = int(cmd[4].value)
                    case _:
                        raise NotImplementedError
                storage[key] = Stored(value, expiry)
            else:
                storage[key] = Stored(value)
            return SimpleString("OK").dump()
        case "GET":
            key = cmd[1].value
            if (stored := storage.get(key)) and not stored.expired():
                return BulkString(stored.value).dump()
            else:
                return BulkNullString().dump()
        case _:
            raise NotImplementedError
