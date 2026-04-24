from typing import Any
from time import time_ns


class Stored:
    """A stored value with optional expiration in milliseconds."""

    __slots__ = "value", "expiry_"

    def __init__(self, value: Any, expiry_ms: int = 0):
        self.value = value
        self.expiry(expiry_ms)

    def __repr__(self) -> str:
        return f"Stored(value={self.value}, expiry_ms={self.expiry_})"

    def expiry(self, millisecs: int):
        self.expiry_ = 0 if not millisecs else (time_ms() + millisecs)

    def expired(self, now: int = 0) -> bool:
        if not self.expiry_:
            return False
        now = now or time_ms()
        return (now - self.expiry_) >= 0


def time_ms() -> int:
    return time_ns() // 1000_000


class Storage(dict[str, Stored]):
    def set(self, key: str, value: Any, expiry_ms: int = 0):
        """Inserts/updates a value."""
        self[key] = Stored(value, expiry_ms)

    def get(self, key: str) -> Any:
        """Returns the value with the given key if exists and is not expired."""
        match self.pop_if_expired(key):
            case (stored, False):
                return stored.value
            case (_, True) | None:
                return None

    def pop_if_expired(self, key: str) -> tuple[Stored, bool] | None:
        """If the stored element with the given key has expired, remove it from
        the storage.

        Returns:
            - (Stored, True) if the element was removed because it was expired.
            - (Stored, False) if the element was not removed.
            - None if the element does not exist.
        """
        try:
            stored = self[key]
            if expired := stored.expired():
                del self[key]
            return (stored, expired)
        except KeyError:
            return None
