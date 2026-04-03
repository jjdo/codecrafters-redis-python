"""REDIS commands"""

from app.resp import Array, SimpleString, BulkString, BulkNullString

storage = {}


def execute(cmd: Array) -> bytes:
    match cmd[0].value.upper():
        case "PING":
            return SimpleString("PONG").dump()
        case "ECHO":
            return BulkString(cmd[1].value).dump()
        case "SET":
            key = cmd[1].value
            value = cmd[2].value
            storage[key] = value
            return SimpleString("OK").dump()
        case "GET":
            key = cmd[1].value
            if value := storage.get(key):
                return BulkString(value).dump()
            else:
                return BulkNullString().dump()
