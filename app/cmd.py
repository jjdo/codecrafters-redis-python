"""REDIS commands"""

from app.resp import (
    RESPType,
    Array,
    SimpleString,
    BulkString,
    BulkNullString,
    Integer,
)
from app.storage import Storage


class InvalidCommand(Exception):
    pass


storage = Storage()


def execute(cmd: Array) -> RESPType:
    # cmd[0] is the command name
    match cmd[0].value.upper():
        case "PING":
            return SimpleString("PONG")
        case "ECHO":
            return BulkString(cmd[1].value)
        case "SET":
            key = cmd[1].value
            value = cmd[2].value
            if len(cmd) > 3:
                # Options
                match cmd[3].value:
                    case "EX":
                        expiry_ms = int(cmd[4].value) * 1000
                    case "PX":
                        expiry_ms = int(cmd[4].value)
                    case _:
                        raise NotImplementedError
            else:
                expiry_ms = 0
            storage.set(key, value, expiry_ms)
            return SimpleString("OK")
        case "GET":
            key = cmd[1].value
            if value := storage.get(key):
                return BulkString(value)
            else:
                return BulkNullString()
        case "RPUSH":
            # Expect at least one key and one value.
            if len(cmd) > 2:
                values = map(lambda v: v.value, cmd[2:])
                if (key := cmd[1].value) not in storage:
                    slist = list(values)
                    storage.set(key, slist)
                else:
                    slist = storage.get(key)
                    slist.extend(values)
                return Integer(len(slist))
            raise InvalidCommand("RPUSH: expected a key and at least one value")
        case _:
            raise NotImplementedError
