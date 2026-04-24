from app.storage import Storage
import pytest


@pytest.fixture
def storage(monkeypatch):
    storage = Storage()
    monkeypatch.setattr("app.cmd.storage", storage)
    yield storage
