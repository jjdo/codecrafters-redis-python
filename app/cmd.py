"""REDIS commands"""

from app.resp import Array

storage = {}


def execute(cmd: Array) -> bytes:
    match cmd[0].value.upper():
        case "PING":
            return b"+PONG\r\n"
        case "ECHO":
            return bulk_string(cmd[1].value)
        case "SET":
            key = cmd[1].value
            value = cmd[2].value
            storage[key] = value
            return simple_string("OK")
        case "GET":
            key = cmd[1].value
            if value := storage.get(key):
                return bulk_string(value)
            else:
                return bulk_null_string()


# Fuctions to generate replies - WIP


def simple_string(value: str) -> bytes:
    return f"+{value}\r\n".encode("utf8")


def bulk_string(value: str) -> bytes:
    return f"${len(value)}\r\n{value}\r\n".encode("utf8")


def bulk_null_string() -> bytes:
    return b"$-1\r\n"
