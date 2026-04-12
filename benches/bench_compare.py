"""
Benchmark helper to compare Ryx vs SQLAlchemy on SQLite/Postgres.

Usage:
  RYX_DATABASE_URL=sqlite:///bench.db python benches/bench_compare.py
  RYX_DATABASE_URL=postgresql://user:pass@localhost:5432/db python benches/bench_compare.py

This script aims to reproduce the 10k-row table of operations:
  - bulk_create
  - filter_query
  - aggregate
  - bulk_update
  - bulk_delete

It prints a table similar to the one shared in the thread.
"""

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Callable, List

import sqlalchemy as sa
from sqlalchemy import Column, Integer, String, Boolean, select, func, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker

from ryx import ryx_core as _core
from ryx.bulk import bulk_create, bulk_update, bulk_delete

Base = declarative_base()


class Item(Base):
    __tablename__ = "bench_items"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    active = Column(Boolean, default=True)
    value = Column(Integer)


@dataclass
class BenchResult:
    name: str
    ryx_orm: float
    sa_orm: float
    sa_core: float
    ryx_raw: float


async def setup_sa_engine(url: str):
    engine = create_async_engine(url, future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    return engine


async def setup_ryx(url: str):
    # One alias: default
    await _core.setup({"default": url})
    return url


async def bench_bulk_create(session_maker, count: int = 10_000):
    # SA ORM
    async with session_maker() as s:
        objs = [Item(name=f"i{i}", active=True, value=i) for i in range(count)]
        t0 = time.perf_counter()
        s.add_all(objs)
        await s.commit()
        sa_time = time.perf_counter() - t0
    # SA Core
    async with session_maker() as s:
        t0 = time.perf_counter()
        await s.execute(
            Item.__table__.insert(),
            [{"name": f"c{i}", "active": True, "value": i} for i in range(count)],
        )
        await s.commit()
        sa_core_time = time.perf_counter() - t0
    # Ryx ORM (via bulk_create)
    from ryx.models import Model, IntegerField, BooleanField, CharField

    class RItem(Model):
        name = CharField()
        active = BooleanField()
        value = IntegerField()

        class Meta:
            table = "bench_items"

    await _core.raw_execute('DELETE FROM "bench_items"', None)
    items = [RItem(name=f"r{i}", active=True, value=i) for i in range(count)]
    t0 = time.perf_counter()
    await bulk_create(RItem, items, batch_size=1000)
    ryx_time = time.perf_counter() - t0

    # Ryx raw
    await _core.raw_execute('DELETE FROM "bench_items"', None)
    t0 = time.perf_counter()
    values = ", ".join([f"('{i}', true, {i})" for i in range(count)])
    await _core.raw_execute(
        f'INSERT INTO "bench_items" ("name","active","value") VALUES {values}', None
    )
    ryx_raw_time = time.perf_counter() - t0
    return BenchResult("bulk_create", ryx_time, sa_time, sa_core_time, ryx_raw_time)


async def bench_filter(session_maker):
    async def sa_orm():
        async with session_maker() as s:
            t0 = time.perf_counter()
            res = await s.execute(select(Item).where(Item.value > 5000))
            _ = res.scalars().all()
            return time.perf_counter() - t0

    async def sa_core():
        async with session_maker() as s:
            t0 = time.perf_counter()
            res = await s.execute(select(Item).where(Item.value > 5000))
            _ = res.fetchall()
            return time.perf_counter() - t0

    async def ryx_orm():
        from ryx.models import Model, IntegerField, BooleanField, CharField

        class RItem(Model):
            name = CharField()
            active = BooleanField()
            value = IntegerField()

            class Meta:
                table = "bench_items"

        qs = RItem.objects.filter(value__gt=5000)
        t0 = time.perf_counter()
        _ = await qs
        return time.perf_counter() - t0

    async def ryx_raw():
        t0 = time.perf_counter()
        _ = await _core.raw_fetch(
            'SELECT * FROM "bench_items" WHERE "value" > 5000', None
        )
        return time.perf_counter() - t0

    sa_orm_t, sa_core_t, ryx_t, ryx_raw_t = await asyncio.gather(
        sa_orm(), sa_core(), ryx_orm(), ryx_raw()
    )
    return BenchResult("filter_query", ryx_t, sa_orm_t, sa_core_t, ryx_raw_t)


async def bench_aggregate(session_maker):
    async def sa_orm():
        async with session_maker() as s:
            t0 = time.perf_counter()
            _ = await s.execute(select(func.count()).select_from(Item))
            return time.perf_counter() - t0

    async def sa_core():
        async with session_maker() as s:
            t0 = time.perf_counter()
            _ = await s.execute(select(func.count()).select_from(Item))
            return time.perf_counter() - t0

    async def ryx_orm():
        from ryx.models import Model, IntegerField, BooleanField, CharField

        class RItem(Model):
            name = CharField()
            active = BooleanField()
            value = IntegerField()

            class Meta:
                table = "bench_items"

        t0 = time.perf_counter()
        _ = await RItem.objects.count()
        return time.perf_counter() - t0

    async def ryx_raw():
        t0 = time.perf_counter()
        _ = await _core.raw_fetch('SELECT COUNT(*) FROM "bench_items"', None)
        return time.perf_counter() - t0

    sa_orm_t, sa_core_t, ryx_t, ryx_raw_t = await asyncio.gather(
        sa_orm(), sa_core(), ryx_orm(), ryx_raw()
    )
    return BenchResult("aggregate", ryx_t, sa_orm_t, sa_core_t, ryx_raw_t)


async def bench_bulk_update(session_maker):
    async def sa_orm():
        async with session_maker() as s:
            t0 = time.perf_counter()
            await s.execute(
                sa.update(Item).where(Item.id <= 10000).values(active=False, value=sa.text("value+1"))
            )
            await s.commit()
            return time.perf_counter() - t0

    async def sa_core():
        async with session_maker() as s:
            t0 = time.perf_counter()
            await s.execute(
                Item.__table__.update()
                .where(Item.c.id <= 10000)
                .values(active=False, value=sa.text("value+1"))
            )
            await s.commit()
            return time.perf_counter() - t0

    async def ryx_orm():
        from ryx.models import Model, IntegerField, BooleanField, CharField

        class RItem(Model):
            name = CharField()
            active = BooleanField()
            value = IntegerField()

            class Meta:
                table = "bench_items"

        from ryx.bulk import bulk_update as ryx_bulk_update

        # fetch instances to update
        items = await RItem.objects.filter(pk__lte=10000)
        for it in items:
            it.active = False
            it.value = it.value + 1
        t0 = time.perf_counter()
        await ryx_bulk_update(RItem, items, fields=["active", "value"], batch_size=1000)
        return time.perf_counter() - t0

    async def ryx_raw():
        t0 = time.perf_counter()
        await _core.raw_execute(
            'UPDATE "bench_items" SET "active" = FALSE, "value" = "value" + 1 WHERE "id" <= 10000',
            None,
        )
        return time.perf_counter() - t0

    sa_orm_t, sa_core_t, ryx_t, ryx_raw_t = await asyncio.gather(
        sa_orm(), sa_core(), ryx_orm(), ryx_raw()
    )
    return BenchResult("bulk_update", ryx_t, sa_orm_t, sa_core_t, ryx_raw_t)


async def bench_bulk_delete(session_maker):
    async def sa_orm():
        async with session_maker() as s:
            t0 = time.perf_counter()
            await s.execute(sa.delete(Item).where(Item.id <= 10000))
            await s.commit()
            return time.perf_counter() - t0

    async def sa_core():
        async with session_maker() as s:
            t0 = time.perf_counter()
            await s.execute(Item.__table__.delete().where(Item.c.id <= 10000))
            await s.commit()
            return time.perf_counter() - t0

    async def ryx_orm():
        from ryx.models import Model, IntegerField, BooleanField, CharField

        class RItem(Model):
            name = CharField()
            active = BooleanField()
            value = IntegerField()

            class Meta:
                table = "bench_items"

        items = await RItem.objects.filter(pk__lte=10000)
        from ryx.bulk import bulk_delete as ryx_bulk_delete

        t0 = time.perf_counter()
        await ryx_bulk_delete(RItem, items, batch_size=1000)
        return time.perf_counter() - t0

    async def ryx_raw():
        t0 = time.perf_counter()
        await _core.raw_execute('DELETE FROM "bench_items" WHERE "id" <= 10000', None)
        return time.perf_counter() - t0

    sa_orm_t, sa_core_t, ryx_t, ryx_raw_t = await asyncio.gather(
        sa_orm(), sa_core(), ryx_orm(), ryx_raw()
    )
    return BenchResult("bulk_delete", ryx_t, sa_orm_t, sa_core_t, ryx_raw_t)


async def main():
    url = os.environ.get("RYX_DATABASE_URL", "sqlite:///bench.db")
    engine = await setup_sa_engine(url)
    await setup_ryx(url)

    Session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    # Seed data for non-create benches
    async with Session() as s:
        await s.execute(Item.__table__.insert(), [{"name": f"seed{i}", "active": True, "value": i} for i in range(10_000)])
        await s.commit()

    benches: List[Callable[[], BenchResult]] = [
        lambda: bench_bulk_create(Session),
        lambda: bench_filter(Session),
        lambda: bench_aggregate(Session),
        lambda: bench_bulk_update(Session),
        lambda: bench_bulk_delete(Session),
    ]

    results: List[BenchResult] = []
    for bench in benches:
        results.append(await bench())

    # Print table
    print("\n" + "=" * 70)
    print("BENCHMARK SUMMARY (times in seconds, lower is better)")
    print("=" * 70)
    print(f"{'Operation':<20} | {'Ryx ORM':>12} | {'SQLA ORM':>14} | {'SQLA Core':>15} | {'Ryx raw':>12}")
    print("-" * 70)
    for r in results:
        print(
            f"{r.name:<20} | {r.ryx_orm:12.4f} | {r.sa_orm:14.4f} | {r.sa_core:15.4f} | {r.ryx_raw:12.4f}"
        )
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
