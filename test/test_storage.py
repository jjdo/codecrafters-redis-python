import time

from app.storage import Stored, time_ms


def test_stored():
    assert Stored("a").expired(time_ms()) is False
    assert Stored("a", expiry_ms=100).expired(time_ms() + 50) is False
    assert Stored("a", expiry_ms=100).expired(time_ms() + 100) is True


def test_storage_set_and_get(storage):
    """Test setting and getting a value without expiration."""
    storage.set("key1", "value1")
    assert storage.get("key1") == "value1"


def test_storage_get_nonexistent_key(storage):
    """Test getting a key that doesn't exist returns None."""
    assert storage.get("nonexistent") is None


def test_storage_set_overwrites_existing_value(storage):
    """Test that setting a key overwrites the previous value."""
    storage.set("key1", "value1")
    storage.set("key1", "value2")
    assert storage.get("key1") == "value2"


def test_storage_set_multiple_keys(storage):
    """Test setting and getting multiple different keys."""
    storage.set("key1", "value1")
    storage.set("key2", "value2")
    storage.set("key3", "value3")
    assert storage.get("key1") == "value1"
    assert storage.get("key2") == "value2"
    assert storage.get("key3") == "value3"


def test_storage_set_with_various_value_types(storage):
    """Test setting values of different types."""
    storage.set("string", "hello")
    storage.set("int", 42)
    storage.set("float", 3.14)
    storage.set("list", [1, 2, 3])
    storage.set("dict", {"nested": "value"})

    assert storage.get("string") == "hello"
    assert storage.get("int") == 42
    assert storage.get("float") == 3.14
    assert storage.get("list") == [1, 2, 3]
    assert storage.get("dict") == {"nested": "value"}


def test_storage_get_expired_value_returns_none(storage):
    """Test that getting an expired value returns None and removes it."""
    storage.set("key1", "value1", expiry_ms=100)

    # Before expiration
    assert storage.get("key1") == "value1"

    # Wait for expiration
    time.sleep(0.2)

    # After expiration
    assert storage.get("key1") is None
    assert "key1" not in storage


def test_storage_get_removes_expired_key_from_storage(storage):
    """Test that getting an expired key removes it from storage."""
    storage.set("key1", "value1", expiry_ms=1)

    # Wait for expiration and get
    time.sleep(0.01)
    storage.get("key1")

    assert "key1" not in storage


def test_storage_pop_if_expired_returns_none_for_nonexistent(storage):
    """Test pop_if_expired returns None for non-existent key."""
    result = storage.pop_if_expired("nonexistent")
    assert result is None


def test_storage_pop_if_expired_returns_tuple_false_for_valid(storage):
    """Test pop_if_expired returns (Stored, False) for valid non-expired key."""
    storage.set("key1", "value1")
    result = storage.pop_if_expired("key1")

    assert result is not None
    stored, expired = result
    assert stored.value == "value1"
    assert expired is False
    assert "key1" in storage  # Key should still exist


def test_storage_pop_if_expired_returns_tuple_true_for_expired(storage):
    """Test pop_if_expired returns (Stored, True) for expired key."""
    storage.set("key1", "value1", expiry_ms=1)

    time.sleep(0.01)
    result = storage.pop_if_expired("key1")

    assert result is not None
    stored, expired = result
    assert stored.value == "value1"
    assert expired is True
    assert "key1" not in storage  # Key should be removed


def test_storage_dict_behavior(storage):
    """Test that Storage inherits dict behavior."""
    storage.set("key1", "value1")

    # Test len
    assert len(storage) == 1

    # Test in operator
    assert "key1" in storage
    assert "nonexistent" not in storage

    # Test keys
    assert list(storage.keys()) == ["key1"]


def test_storage_multiple_operations_sequence(storage):
    """Test a sequence of operations."""
    # Set and get
    storage.set("user:1", "Alice")
    assert storage.get("user:1") == "Alice"

    # Update
    storage.set("user:1", "Bob")
    assert storage.get("user:1") == "Bob"

    # Set another
    storage.set("user:2", "Charlie")
    assert storage.get("user:2") == "Charlie"
    assert storage.get("user:1") == "Bob"

    # Get non-existent
    assert storage.get("user:3") is None
