"""
Ryx ORM — Example 13: Performance Comparison with SQLAlchemy

This example benchmarks Ryx against SQLAlchemy in four modes:
  1. Ryx ORM      — high-level async ORM with Rust-powered queries
  2. SQLAlchemy ORM — high-level async ORM (classic Python)
  3. SQLAlchemy Core — mid-level expression language
  4. Ryx raw_execute/raw_fetch — low-level raw SQL via Rust executor

Each test measures:
  - Bulk insert time (1000 rows)
  - Filtered query time (WHERE + ORDER BY + LIMIT)
  - Aggregate query time (COUNT, SUM, AVG)
  - Bulk update time
  - Bulk delete time

Run with:
    uv run python examples/13_benchmark_sqlalchemy.py

Note: SQLAlchemy must be installed. Install it with:
    uv add sqlalchemy[asyncio] aiosqlite
"""

import asyncio
import os
import time
from pathlib import Path

import ryx
from ryx import Model, CharField, IntField, DateTimeField
from ryx.migrations import MigrationRunner
from ryx.executor_helpers import raw_fetch, raw_execute


DB_PATH = Path(__file__).parent.parent / "ryx_bench.sqlite3"
DATABASE_URL = f"sqlite://{DB_PATH}?mode=rwc"
os.environ["RYX_DATABASE_URL"] = DATABASE_URL

N = 1000  # Number of rows for bulk operations


#
#  BENCHMARK HELPERS
#
class BenchTimer:
    """Context manager that records elapsed time and prints it."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.elapsed = 0.0

    def __enter__(self) -> "BenchTimer":
        self.start = time.monotonic()
        return self

    def __exit__(self, *args) -> None:
        self.elapsed = time.monotonic() - self.start
        print(f"  {self.label}: {self.elapsed:.4f}s")


def timed(label: str) -> BenchTimer:
    return BenchTimer(label)


#
#  RYX MODELS
#
class RyxItem(Model):
    class Meta:
        table_name = "ryx_items"

    name = CharField(max_length=100)
    category = CharField(max_length=50)
    price = IntField(default=0)
    is_active = IntField(default=1)
    created_at = DateTimeField(auto_now_add=True, null=True, blank=True)


#
#  RYX ORM BENCHMARK
#
async def bench_ryx_orm() -> dict:
    """Benchmark Ryx ORM operations."""
    print("\n" + "=" * 60)
    print("Ryx ORM")
    print("=" * 60)

    await ryx.setup(DATABASE_URL)
    runner = MigrationRunner([RyxItem])
    await runner.migrate()

    results = {}

    # 1. Bulk insert
    items = [
        RyxItem(
            name=f"Item {i}",
            category="A" if i % 2 == 0 else "B",
            price=i * 10,
            is_active=1 if i % 3 != 0 else 0,
        )
        for i in range(N)
    ]
    with timed("bulk_create") as t:
        await RyxItem.objects.bulk_create(items)
    results["bulk_create"] = t.elapsed

    # 2. Filtered query
    with timed("filter + order + limit") as t:
        await RyxItem.objects.filter(category="A", is_active=1).order_by("-price")[:50]
    results["filter_query"] = t.elapsed

    # 3. Aggregate
    with timed("aggregate (count, sum, avg)") as t:
        await RyxItem.objects.filter(category="A").aggregate(
            total=ryx.Count("id"),
            total_price=ryx.Sum("price"),
            avg_price=ryx.Avg("price"),
        )
    results["aggregate"] = t.elapsed

    # 4. Bulk update
    active_items = await RyxItem.objects.filter(is_active=1)
    for item in active_items:
        item.price += 100
    with timed("bulk_update") as t:
        await RyxItem.objects.bulk_update(active_items, ["price"])
    results["bulk_update"] = t.elapsed

    # 5. Bulk delete
    with timed("bulk_delete") as t:
        await RyxItem.objects.filter(category="B").delete()
    results["bulk_delete"] = t.elapsed

    return results


#
#  SQLALCHEMY ORM BENCHMARK
#
async def bench_sqlalchemy_orm() -> dict:
    """Benchmark SQLAlchemy ORM (async) operations."""
    try:
        from sqlalchemy import Column, Integer, String, select, func, update, delete
        from sqlalchemy.orm import DeclarativeBase, sessionmaker
        from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    except ImportError:
        print("\n⚠️  SQLAlchemy not installed — skipping ORM benchmark.")
        print("   Install with: uv add sqlalchemy[asyncio] aiosqlite")
        return {}

    print("\n" + "=" * 60)
    print("SQLAlchemy ORM (async)")
    print("=" * 60)

    engine = create_async_engine(f"sqlite+aiosqlite:///{DB_PATH}", echo=False)
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

    results = {}

    # 1. Bulk insert
    with timed("bulk_create") as t:
        async with async_session() as session:
            session.add_all(
                [
                    SAItem(
                        name=f"Item {i}",
                        category="A" if i % 2 == 0 else "B",
                        price=i * 10,
                        is_active=1 if i % 3 != 0 else 0,
                    )
                    for i in range(N)
                ]
            )
            await session.commit()
    results["bulk_create"] = t.elapsed

    # 2. Filtered query
    with timed("filter + order + limit") as t:
        async with async_session() as session:
            stmt = (
                select(SAItem)
                .where(SAItem.category == "A", SAItem.is_active == 1)
                .order_by(SAItem.price.desc())
                .limit(50)
            )
            result = await session.execute(stmt)
            result.scalars().all()
    results["filter_query"] = t.elapsed

    # 3. Aggregate
    with timed("aggregate (count, sum, avg)") as t:
        async with async_session() as session:
            stmt = select(
                func.count(SAItem.id).label("total"),
                func.sum(SAItem.price).label("total_price"),
                func.avg(SAItem.price).label("avg_price"),
            ).where(SAItem.category == "A")
            await session.execute(stmt)
    results["aggregate"] = t.elapsed

    # 4. Bulk update
    with timed("bulk_update") as t:
        async with async_session() as session:
            stmt = (
                update(SAItem)
                .where(SAItem.is_active == 1)
                .values(price=SAItem.price + 100)
            )
            await session.execute(stmt)
            await session.commit()
    results["bulk_update"] = t.elapsed

    # 5. Bulk delete
    with timed("bulk_delete") as t:
        async with async_session() as session:
            stmt = delete(SAItem).where(SAItem.category == "B")
            await session.execute(stmt)
            await session.commit()
    results["bulk_delete"] = t.elapsed

    await engine.dispose()
    return results


#
#  SQLALCHEMY CORE BENCHMARK
#
async def bench_sqlalchemy_core() -> dict:
    """Benchmark SQLAlchemy Core (async) operations."""
    try:
        from sqlalchemy import (
            Table,
            Column,
            Integer,
            String,
            MetaData,
            select,
            func,
            insert,
            update,
            delete,
        )
        from sqlalchemy.ext.asyncio import create_async_engine
    except ImportError:
        print("\n⚠️  SQLAlchemy not installed — skipping Core benchmark.")
        print("   Install with: uv add sqlalchemy[asyncio] aiosqlite")
        return {}

    print("\n" + "=" * 60)
    print("SQLAlchemy Core (async)")
    print("=" * 60)

    engine = create_async_engine(f"sqlite+aiosqlite:///{DB_PATH}", echo=False)
    metadata = MetaData()

    core_items = Table(
        "core_items",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(100), nullable=False),
        Column("category", String(50), nullable=False),
        Column("price", Integer, default=0),
        Column("is_active", Integer, default=1),
    )

    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
        await conn.run_sync(metadata.create_all)

    results = {}

    # 1. Bulk insert
    with timed("bulk_create") as t:
        async with engine.begin() as conn:
            await conn.execute(
                insert(core_items),
                [
                    {
                        "name": f"Item {i}",
                        "category": "A" if i % 2 == 0 else "B",
                        "price": i * 10,
                        "is_active": 1 if i % 3 != 0 else 0,
                    }
                    for i in range(N)
                ],
            )
    results["bulk_create"] = t.elapsed

    # 2. Filtered query
    with timed("filter + order + limit") as t:
        async with engine.connect() as conn:
            stmt = (
                select(core_items)
                .where(core_items.c.category == "A", core_items.c.is_active == 1)
                .order_by(core_items.c.price.desc())
                .limit(50)
            )
            result = await conn.execute(stmt)
            result.fetchall()
    results["filter_query"] = t.elapsed

    # 3. Aggregate
    with timed("aggregate (count, sum, avg)") as t:
        async with engine.connect() as conn:
            stmt = select(
                func.count(core_items.c.id).label("total"),
                func.sum(core_items.c.price).label("total_price"),
                func.avg(core_items.c.price).label("avg_price"),
            ).where(core_items.c.category == "A")
            await conn.execute(stmt)
    results["aggregate"] = t.elapsed

    # 4. Bulk update
    with timed("bulk_update") as t:
        async with engine.begin() as conn:
            stmt = (
                update(core_items)
                .where(core_items.c.is_active == 1)
                .values(price=core_items.c.price + 100)
            )
            await conn.execute(stmt)
    results["bulk_update"] = t.elapsed

    # 5. Bulk delete
    with timed("bulk_delete") as t:
        async with engine.begin() as conn:
            stmt = delete(core_items).where(core_items.c.category == "B")
            await conn.execute(stmt)
    results["bulk_delete"] = t.elapsed

    await engine.dispose()
    return results


#
#  RYX RAW SQL BENCHMARK
#
async def bench_ryx_raw() -> dict:
    """Benchmark Ryx raw_execute / raw_fetch — lowest level, no ORM overhead."""
    print("\n" + "=" * 60)
    print("Ryx raw_execute / raw_fetch")
    print("=" * 60)

    results = {}

    # Ensure pool is connected (reconnect if previous benchmark closed it)
    if not ryx.is_connected():
        await ryx.setup(DATABASE_URL)

    # Create table
    await raw_execute(
        "CREATE TABLE IF NOT EXISTS raw_items ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  name TEXT NOT NULL,"
        "  category TEXT NOT NULL,"
        "  price INTEGER DEFAULT 0,"
        "  is_active INTEGER DEFAULT 1"
        ")"
    )

    # 1. Bulk insert — single multi-row INSERT
    rows = []
    for i in range(N):
        cat = "A" if i % 2 == 0 else "B"
        active = 1 if i % 3 != 0 else 0
        rows.append(f"('Item {i}', '{cat}', {i * 10}, {active})")
    sql = f"INSERT INTO raw_items (name, category, price, is_active) VALUES {', '.join(rows)}"
    with timed("bulk_create") as t:
        await raw_execute(sql)
    results["bulk_create"] = t.elapsed

    # 2. Filtered query
    with timed("filter + order + limit") as t:
        await raw_fetch(
            "SELECT * FROM raw_items WHERE category='A' AND is_active=1 "
            "ORDER BY price DESC LIMIT 50"
        )
    results["filter_query"] = t.elapsed

    # 3. Aggregate
    with timed("aggregate (count, sum, avg)") as t:
        await raw_fetch(
            "SELECT COUNT(*) as total, SUM(price) as total_price, "
            "AVG(price) as avg_price FROM raw_items WHERE category='A'"
        )
    results["aggregate"] = t.elapsed

    # 4. Bulk update
    with timed("bulk_update") as t:
        await raw_execute(
            "UPDATE raw_items SET price = price + 100 WHERE is_active = 1"
        )
    results["bulk_update"] = t.elapsed

    # 5. Bulk delete
    with timed("bulk_delete") as t:
        await raw_execute("DELETE FROM raw_items WHERE category = 'B'")
    results["bulk_delete"] = t.elapsed

    # Cleanup
    await raw_execute("DROP TABLE IF EXISTS raw_items")

    return results


#
#  SUMMARY TABLE
#
def print_summary(all_results: dict) -> None:
    """Print a comparison table of all benchmarks."""
    print("\n" + "=" * 70)
    print("BENCHMARK SUMMARY (times in seconds, lower is better)")
    print("=" * 70)

    operations = [
        "bulk_create",
        "filter_query",
        "aggregate",
        "bulk_update",
        "bulk_delete",
    ]
    backends = list(all_results.keys())

    # Header
    header = f"{'Operation':<20s}"
    for b in backends:
        short = b[:16]
        header += f" | {short:>16s}"
    print(header)
    print("-" * len(header))

    # Rows
    for op in operations:
        row = f"{op:<20s}"
        for b in backends:
            val = all_results[b].get(op)
            if val is not None:
                row += f" | {val:>16.4f}"
            else:
                row += f" | {'N/A':>16s}"
        print(row)

    print("=" * 70)
    print("\nNote: SQLAlchemy results require sqlalchemy + aiosqlite installed.")
    print("Install with: uv add sqlalchemy[asyncio] aiosqlite")


#
#  MAIN
#
async def main() -> None:
    print("Ryx ORM — Example 13: Performance Comparison with SQLAlchemy")
    print(f"Database: {DATABASE_URL}")
    print(f"Rows per test: {N}")

    # Clean previous benchmark DB
    if DB_PATH.exists():
        DB_PATH.unlink()

    all_results = {}

    # 1. Ryx ORM
    all_results["Ryx ORM"] = await bench_ryx_orm()

    # Drop Ryx tables before SQLAlchemy benchmarks
    await raw_execute("DROP TABLE IF EXISTS ryx_items")
    await raw_execute("DROP TABLE IF EXISTS ryx_migrations")

    # 2. SQLAlchemy ORM
    all_results["SQLAlchemy ORM"] = await bench_sqlalchemy_orm()

    # Drop SQLAlchemy tables
    await raw_execute("DROP TABLE IF EXISTS sa_items")

    # 3. SQLAlchemy Core
    all_results["SQLAlchemy Core"] = await bench_sqlalchemy_core()

    # Drop SQLAlchemy Core tables
    await raw_execute("DROP TABLE IF EXISTS core_items")

    # 4. Ryx raw SQL
    all_results["Ryx raw"] = await bench_ryx_raw()

    # Print summary
    print_summary(all_results)

    # Cleanup
    if DB_PATH.exists():
        DB_PATH.unlink()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
