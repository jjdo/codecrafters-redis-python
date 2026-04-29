"""In-memory storage"""

from enum import IntEnum, auto
from queue import Queue
from threading import Event
from typing import Any, Iterable
from time import time_ns


class Stored:
    """A stored value with optional expiration in milliseconds."""

    __slots__ = (
        "value",
        "expiry_",
    )

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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.observers = Observers()

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


class Op(IntEnum):
    PUSH = auto()


type ObserverKey = tuple[Op, str]


class Observer(Event):
    pass


class Observers(dict):
    def add(self, op: Op, keys: Iterable[str]) -> Observer:
        """Registers an observer on the given Op and keys."""
        match op:
            case Op.PUSH:
                observer = OnPush(len(keys))
                for key in keys:
                    self.setdefault((Op.PUSH, key), []).append(observer)
                return observer
            case other:
                raise ValueError(f"Invalid Op {other}")

    def notify(self, op: Op, key: str):
        """Notify the first observer on the given pair (Op, key)."""
        if observers := self.pop((op, key), None):
            # Get first observer on the key
            observer = observers[0]
            observer.notify(key)


class OnPush(Observer):
    __slots__ = ("q_",)

    def __init__(self, num_keys: int):
        super().__init__()
        self.q_ = Queue(num_keys)

    def wait(self, timeout: int) -> str | None:
        """Waits until a value is pushed in any of the keys.

        Returns the first key where a value was pushed."""
        match super().wait(timeout):
            case True:
                return self.q_.get_nowait()
            case False:
                return None

    def notify(self, key: str):
        """Notifies that a value has been pushed in key."""
        self.q_.put_nowait(key)
        super().set()
