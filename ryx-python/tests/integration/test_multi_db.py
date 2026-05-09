"""
Integration tests for multi-database support.
"""

import pytest
from ryx import ryx_core
from ryx.models import Model
from ryx.fields import CharField, IntField
from ryx.router import BaseRouter, set_router
from ryx.exceptions import DoesNotExist


# Define models for multi-db testing
class User(Model):
    name = CharField()
    age = IntField()


class Log(Model):
    message = CharField()

    class Meta:
        database = "logs_db"


class TestRouter(BaseRouter):
    def db_for_read(self, model, **hints):
        if model == User:
            return "user_db"
        return None

    def db_for_write(self, model, **hints):
        if model == User:
            return "user_db"
        return None


@pytest.fixture(autouse=True)
async def setup_multi_db():
    """Set up multiple databases for the module."""
    urls = {
        "default": "sqlite::memory:",
        "user_db": "sqlite::memory:",
        "logs_db": "sqlite::memory:",
    }
    await ryx_core.setup(urls, 10, 1, 30, 600, 1800)

    # Create tables manually on all pools to ensure they exist for routing tests
    for alias in urls:
        await ryx_core.raw_execute(
            f"CREATE TABLE {User._meta.table_name} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            alias=alias,
        )
        await ryx_core.raw_execute(
            f"CREATE TABLE {Log._meta.table_name} (id INTEGER PRIMARY KEY, message TEXT)",
            alias=alias,
        )
    yield
    # No explicit teardown needed for in-memory sqlite pools as they are replaced by next setup


@pytest.mark.asyncio
async def test_using_explicit_routing():
    """Test that .using(alias) routes queries to the correct database."""
    # Clear tables (manual cleanup for this specific test)
    await ryx_core.raw_execute(f"DELETE FROM {User._meta.table_name}", alias="default")
    await ryx_core.raw_execute(f"DELETE FROM {User._meta.table_name}", alias="user_db")

    await User.objects.create(name="Default User", age=30)
    await User.objects.using("user_db").create(name="UserDB User", age=25)

    # Verify Default DB
    default_users = await User.objects.all()
    assert len(default_users) == 1
    assert default_users[0].name == "Default User"

    # Verify UserDB DB
    user_db_users = await User.objects.using("user_db").all()
    assert len(user_db_users) == 1
    assert user_db_users[0].name == "UserDB User"


@pytest.mark.asyncio
async def test_meta_database_routing():
    """Test that Model.Meta.database routes queries automatically."""
    # Clear tables
    await ryx_core.raw_execute(f"DELETE FROM {Log._meta.table_name}", alias="default")
    await ryx_core.raw_execute(f"DELETE FROM {Log._meta.table_name}", alias="logs_db")

    # Log should go to logs_db by default
    await Log.objects.create(message="Log entry 1")

    # Verify it's in logs_db
    logs_db_logs = await Log.objects.using("logs_db").all()
    assert len(logs_db_logs) == 1
    assert logs_db_logs[0].message == "Log entry 1"

    # Verify it's NOT in default db
    default_logs = await Log.objects.using("default").all()
    assert len(default_logs) == 0


@pytest.mark.asyncio
async def test_dynamic_router_routing():
    """Test that the configured Router routes queries dynamically."""
    set_router(TestRouter())

    # Clear User tables
    await ryx_core.raw_execute(f"DELETE FROM {User._meta.table_name}", alias="default")
    await ryx_core.raw_execute(f"DELETE FROM {User._meta.table_name}", alias="user_db")

    # Router should route User to user_db
    await User.objects.create(name="Routed User", age=40)

    # Verify it's in user_db
    user_db_users = await User.objects.using("user_db").filter(name="Routed User").all()
    assert len(user_db_users) == 1
    assert user_db_users[0].name == "Routed User"

    # Verify it's NOT in default db
    default_users = await User.objects.using("default").filter(name="Routed User").all()
    assert len(default_users) == 0

    # Reset router for other tests
    set_router(None)
