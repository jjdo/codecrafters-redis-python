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
        case "RPUSH" | "LPUSH" as push_cmd:
            return apush(args(cmd), push_cmd)
        case "LRANGE":
            return lrange(args(cmd))
        case "LLEN":
            return llen(args(cmd))
        case _:
            raise NotImplementedError


def args(cmd: Array) -> Array:
    return cmd[1:]


def ping() -> SimpleString:
    return SimpleString("PONG")


def echo(args: Array) -> BulkString:
    return BulkString(args[0].value)


def set(args: Array) -> SimpleString:
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


def get(args: Array) -> BulkString | BulkNullString:
    key = args[0].value
    if value := storage.get(key):
        return BulkString(value)
    else:
        return BulkNullString()


def apush(args: Array, cmd: str) -> Integer:
    # Expect at least one key and one value.
    if len(args) < 2:
        raise InvalidCommand("RPUSH: expected a key and at least one value")

    values = list(map(lambda v: v.value, args[1:]))
    if cmd == "LPUSH":
        values.reverse()

    key = args[0].value
    if (slist := storage.get(key)) is None:  # Does not exist
        slist = values
        storage.set(key, slist)
    else:
        if cmd == "LPUSH":
            slist = values + slist
        else:
            slist = slist + values
        storage.set(key, slist)
    return Integer(len(slist))


def lrange(args: Array) -> Array:
    if len(args) < 3:
        raise InvalidCommand("LRANGE: expected key and two indexes")

    key = args[0].value
    if not (slist := storage.get(key)):  # Does not exist or empty
        return Array([])

    start, stop = map(int, (args[1].value, args[2].value))
    if start < 0:
        start = max(0, len(slist) + start)
    if stop < 0:
        stop = max(0, len(slist) + stop)
    if start >= stop or start > len(slist):
        return Array([])

    return Array(list(map(BulkString, slist[start : (stop + 1)])))


def llen(args: Array) -> Integer:
    key = args[0].value
    if not (slist := storage.get(key)):  # Does not exist or empty
        return Integer(0)

    return Integer(len(slist))
