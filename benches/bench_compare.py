"""
Ryx ORM — Benchmark vs SQLAlchemy (inspired by examples/13_benchmark_sqlalchemy.py)

Measures (N=10_000):
  - bulk_create
  - filter_query (category + is_active, order + limit)
  - aggregate (count, sum, avg)
  - bulk_update (price += 100 where is_active=1)
  - bulk_delete (category = 'B')

Supports SQLite and Postgres depending on RYX_DATABASE_URL.
"""

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Dict

import ryx
from ryx import Model, CharField, IntField
from ryx.migrations import MigrationRunner
from ryx.executor_helpers import raw_fetch, raw_execute


N = 10_000
DEFAULT_SQLITE = "sqlite://bench.sqlite3?mode=rwc"


def sa_async_url_from_env(url: str) -> str:
    _url = url
    if url.startswith("sqlite://"):
        # sqlalchemy async driver
        _url = url.replace("sqlite://", "sqlite+aiosqlite:///", 1).removesuffix('?mode=rwc')
    if url.startswith("postgres://"):
        _url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    if url.startswith("postgresql://"):
        _url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return _url


class RyxItem(Model):
    class Meta:
        table_name = "bench_items"

    name = CharField(max_length=100)
    category = CharField(max_length=50)
    price = IntField(default=0)
    is_active = IntField(default=1)


@dataclass
class Row:
    bulk_create: float
    filter_query: float
    aggregate: float
    bulk_update: float
    bulk_delete: float


async def bench_ryx(url: str) -> Row:
    await ryx.setup(url)
    runner = MigrationRunner([RyxItem])
    await runner.migrate()

    # bulk_create
    items = [
        RyxItem(
            name=f"Item {i}",
            category="A" if i % 2 == 0 else "B",
            price=i * 10,
            is_active=1 if i % 3 != 0 else 0,
        )
        for i in range(N)
    ]
    t0 = time.monotonic()
    await RyxItem.objects.bulk_create(items, batch_size=1000)
    t_bulk_create = time.monotonic() - t0

    # filter_query
    t0 = time.monotonic()
    await RyxItem.objects.filter(category="A", is_active=1).order_by("-price")[:50]
    t_filter = time.monotonic() - t0

    # aggregate
    t0 = time.monotonic()
    await RyxItem.objects.filter(category="A").aggregate(
        total=ryx.Count("id"),
        total_price=ryx.Sum("price"),
        avg_price=ryx.Avg("price"),
    )
    t_agg = time.monotonic() - t0

    # bulk_update (price += 100 where active)
    active = await RyxItem.objects.filter(is_active=1)
    for it in active:
        it.price += 100
    t0 = time.monotonic()
    await RyxItem.objects.bulk_update(active, ["price"], batch_size=1000)
    t_update = time.monotonic() - t0

    # bulk_delete (category B)
    t0 = time.monotonic()
    await RyxItem.objects.filter(category="B").delete()
    t_delete = time.monotonic() - t0

    return Row(t_bulk_create, t_filter, t_agg, t_update, t_delete)


async def bench_ryx_raw(url: str) -> Row:
    # assumes table exists and filled by Ryx bench
    # bulk_create raw
    values = ", ".join(
        [
            f"('Raw {i}','A', {i*10}, 1)"
            for i in range(N)
        ]
    )
    t0 = time.monotonic()
    await raw_execute(
        f'INSERT INTO "bench_items" ("name","category","price","is_active") VALUES {values}',
        None,
    )
    t_bulk_create = time.monotonic() - t0

    t0 = time.monotonic()
    await raw_fetch(
        'SELECT * FROM "bench_items" WHERE "category" = \'A\' AND "is_active" = 1 ORDER BY "price" DESC LIMIT 50',
        None,
    )
    t_filter = time.monotonic() - t0

    t0 = time.monotonic()
    await raw_fetch(
        'SELECT COUNT(*) AS total, SUM("price") AS total_price, AVG("price") AS avg_price FROM "bench_items" WHERE "category" = \'A\'',
        None,
    )
    t_agg = time.monotonic() - t0

    t0 = time.monotonic()
    await raw_execute(
        'UPDATE "bench_items" SET "price" = "price" + 100 WHERE "is_active" = 1',
        None,
    )
    t_update = time.monotonic() - t0

    t0 = time.monotonic()
    await raw_execute('DELETE FROM "bench_items" WHERE "category" = \'B\'', None)
    t_delete = time.monotonic() - t0

    return Row(t_bulk_create, t_filter, t_agg, t_update, t_delete)


async def bench_sqlalchemy(url: str) -> Dict[str, Row]:
    try:
        from sqlalchemy import (
            Column,
            Integer,
            String,
            select,
            func,
            update,
            delete,
        )
        from sqlalchemy.orm import DeclarativeBase, sessionmaker
        from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    except ImportError:
        print("SQLAlchemy not installed; skipping.")
        return {}

    async_url = sa_async_url_from_env(url)
    engine = create_async_engine(async_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession)

    class Base(DeclarativeBase):
        pass

    class SAItem(Base):
        __tablename__ = "sa_items"
        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(String(100), nullable=False)
        category = Column(String(50), nullable=False)
        price = Column(Integer, default=0)
        is_active = Column(Integer, default=1)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    def sa_seed_values():
        return [
            dict(
                name=f"Item {i}",
                category="A" if i % 2 == 0 else "B",
                price=i * 10,
                is_active=1 if i % 3 != 0 else 0,
            )
            for i in range(N)
        ]

    # ORM bulk_create
    t0 = time.monotonic()
    async with async_session() as session:
        session.add_all([SAItem(**v) for v in sa_seed_values()])
        await session.commit()
    sa_orm_create = time.monotonic() - t0

    # ORM filter
    t0 = time.monotonic()
    async with async_session() as session:
        stmt = (
            select(SAItem)
            .where(SAItem.category == "A", SAItem.is_active == 1)
            .order_by(SAItem.price.desc())
            .limit(50)
        )
        res = await session.execute(stmt)
        res.scalars().all()
    sa_orm_filter = time.monotonic() - t0

    # ORM aggregate
    t0 = time.monotonic()
    async with async_session() as session:
        stmt = select(
            func.count(SAItem.id),
            func.sum(SAItem.price),
            func.avg(SAItem.price),
        ).where(SAItem.category == "A")
        await session.execute(stmt)
    sa_orm_agg = time.monotonic() - t0

    # ORM bulk_update
    t0 = time.monotonic()
    async with async_session() as session:
        stmt = (
            update(SAItem)
            .where(SAItem.is_active == 1)
            .values(price=SAItem.price + 100)
        )
        await session.execute(stmt)
        await session.commit()
    sa_orm_update = time.monotonic() - t0

    # ORM bulk_delete
    t0 = time.monotonic()
    async with async_session() as session:
        stmt = delete(SAItem).where(SAItem.category == "B")
        await session.execute(stmt)
        await session.commit()
    sa_orm_delete = time.monotonic() - t0

    # Core: re-seed
    async with engine.begin() as conn:
        await conn.execute(delete(SAItem))
        await conn.execute(SAItem.__table__.insert(), sa_seed_values())

    # Core filter
    t0 = time.monotonic()
    async with async_session() as session:
        stmt = (
            select(SAItem)
            .where(SAItem.category == "A", SAItem.is_active == 1)
            .order_by(SAItem.price.desc())
            .limit(50)
        )
        res = await session.execute(stmt)
        res.fetchall()
    sa_core_filter = time.monotonic() - t0

    # Core aggregate
    t0 = time.monotonic()
    async with async_session() as session:
        stmt = select(
            func.count(SAItem.id),
            func.sum(SAItem.price),
            func.avg(SAItem.price),
        ).where(SAItem.category == "A")
        await session.execute(stmt)
    sa_core_agg = time.monotonic() - t0

    # Core bulk_update
    t0 = time.monotonic()
    async with async_session() as session:
        stmt = (
            SAItem.__table__.update()
            .where(SAItem.__table__.c.is_active == 1)
            .values(price=SAItem.__table__.c.price + 100)
        )
        await session.execute(stmt)
        await session.commit()
    sa_core_update = time.monotonic() - t0

    # Core bulk_delete
    t0 = time.monotonic()
    async with async_session() as session:
        stmt = SAItem.__table__.delete().where(SAItem.__table__.c.category == "B")
        await session.execute(stmt)
        await session.commit()
    sa_core_delete = time.monotonic() - t0

    await engine.dispose()

    orm_row = Row(sa_orm_create, sa_orm_filter, sa_orm_agg, sa_orm_update, sa_orm_delete)
    core_row = Row(sa_orm_create, sa_core_filter, sa_core_agg, sa_core_update, sa_core_delete)
    return {"orm": orm_row, "core": core_row}


def print_table(ryx_row: Row, sa_rows: Dict[str, Row], raw_row: Row):
    print("\n" + "=" * 70)
    print("BENCHMARK SUMMARY (seconds, lower is better)")
    print("=" * 70)
    print(f"{'Operation':<18} | {'Ryx ORM':>10} | {'SA ORM':>10} | {'SA Core':>10} | {'Ryx raw':>10}")
    print("-" * 70)
    ops = ["bulk_create", "filter_query", "aggregate", "bulk_update", "bulk_delete"]
    for op in ops:
        print(
            f"{op:<18} | "
            f"{getattr(ryx_row, op):10.4f} | "
            f"{getattr(sa_rows['orm'], op):10.4f} | "
            f"{getattr(sa_rows['core'], op):10.4f} | "
            f"{getattr(raw_row, op):10.4f}"
        )
    print("=" * 70)


async def main():
    url = os.environ.get("RYX_DATABASE_URL", DEFAULT_SQLITE)
    print(f"Using database URL: {url}")

    # Fresh table for Ryx benchmarks
    ryx_row = await bench_ryx(url)

    # Seed again for raw benchmarks
    await raw_execute('DELETE FROM "bench_items"', None)
    raw_row = await bench_ryx_raw(url)

    sa_rows = await bench_sqlalchemy(url)
    if not sa_rows:
        print("SQLAlchemy benches skipped.")
        return

    print_table(ryx_row, sa_rows, raw_row)


if __name__ == "__main__":
    asyncio.run(main())
