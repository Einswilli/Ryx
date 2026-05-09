import asyncio
from ryx import ryx_core
from ryx.models import Model
from ryx.fields import CharField, IntField
from ryx.router import BaseRouter, set_router
# from ryx.exceptions import DoesNotExist


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


async def main():
    urls = {
        "default": "sqlite::memory:",
        "user_db": "sqlite::memory:",
        "logs_db": "sqlite::memory:",
    }
    await ryx_core.setup(urls, 10, 1, 30, 600, 1800)

    # Create tables manually
    for alias in urls:
        # Use ryx_core.raw_execute to create tables on specific pools
        await ryx_core.raw_execute(
            f"CREATE TABLE {User._meta.table_name} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            alias=alias,
        )
        await ryx_core.raw_execute(
            f"CREATE TABLE {Log._meta.table_name} (id INTEGER PRIMARY KEY, message TEXT)",
            alias=alias,
        )

    # Test .using()
    await User.objects.create(name="Default User", age=30)
    await User.objects.using("user_db").create(name="UserDB User", age=25)
    print("Explicit using: OK")

    # Test Meta.database
    await Log.objects.create(message="Log entry 1")
    log = await Log.objects.get(message="Log entry 1")
    print(f"Meta database: OK ({log.message})")

    # Test Router
    set_router(TestRouter())
    await User.objects.create(name="Routed User", age=40)
    user = await User.objects.using("user_db").get(name="Routed User")
    print(f"Dynamic router: OK ({user.name})")


if __name__ == "__main__":
    asyncio.run(main())
