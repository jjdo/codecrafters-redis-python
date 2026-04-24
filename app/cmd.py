"""REDIS commands"""

from app.resp import (
    RESPType,
    Array,
    ArrayNull,
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
    # cmd[1:n] are the command arguments. These are always bulk strings.
    match cmd[0].value.upper():
        case "PING":
            return ping()
        case "ECHO":
            return echo(args(cmd))
        case "SET":
            return set(args(cmd))
        case "GET":
            return get(args(cmd))
        case "RPUSH":
            return rpush(args(cmd))
        case "LRANGE":
            return lrange(args(cmd))
        case _:
            raise NotImplementedError


def args(cmd: Array) -> Array:
    return cmd[1:]


def ping() -> RESPType:
    return SimpleString("PONG")


def echo(args: Array) -> RESPType:
    return BulkString(args[0].value)


def set(args: Array) -> RESPType:
    key = args[0].value
    value = args[1].value
    if len(args) > 2:
        # Options
        match args[2].value:
            case "EX":
                expiry_ms = int(args[3].value) * 1000
            case "PX":
                expiry_ms = int(args[3].value)
            case _:
                raise NotImplementedError
    else:
        expiry_ms = 0
    storage.set(key, value, expiry_ms)
    return SimpleString("OK")


def get(args: Array) -> RESPType:
    key = args[0].value
    if value := storage.get(key):
        return BulkString(value)
    else:
        return BulkNullString()


def rpush(args: Array) -> RESPType:
    # Expect at least one key and one value.
    if len(args) < 2:
        raise InvalidCommand("RPUSH: expected a key and at least one value")

    values = map(lambda v: v.value, args[1:])
    if (key := args[0].value) not in storage:
        slist = list(values)
        storage.set(key, slist)
    else:
        slist = storage.get(key)
        slist.extend(values)
    return Integer(len(slist))


def lrange(args: Array) -> RESPType:
    if len(args) < 3:
        raise InvalidCommand("LRANGE: expected key and two indexes")

    key = args[0].value
    if not (slist := storage.get(key)):
        return ArrayNull()

    start, stop = map(int, args[1].value, args[2].value)
    if start > stop or start > len(slist):
        return ArrayNull()

    return Array(list(map(BulkString, slist[start : (stop + 1)])))
