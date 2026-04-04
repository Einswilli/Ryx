"""
Ryx ORM — Example 03: Querying & Filters

This example covers:
  - All built-in lookups (exact, gt, gte, lt, lte, contains, icontains, …)
  - Q objects: AND, OR, NOT, complex nesting
  - Mixing Q objects with keyword arguments
  - exclude()
  - Ordering (ASC, DESC, multi-field)
  - Pagination (slicing, limit/offset)
  - distinct()
  - Query introspection (.query)
  - Custom lookups via @lookup decorator

Run with:
    uv run python examples/03_querying_and_filters.py
"""

import asyncio
import os
from pathlib import Path

import ryx
from ryx import Model, CharField, IntField, BooleanField, DateTimeField, Q
from ryx.migrations import MigrationRunner


DB_PATH = Path(__file__).parent.parent / "ryx_examples.sqlite3"
DATABASE_URL = f"sqlite://{DB_PATH}?mode=rwc"
os.environ["RYX_DATABASE_URL"] = DATABASE_URL


#
#  MODELS
#
class Product(Model):
    class Meta:
        table_name = "ex3_products"
        ordering = ["name"]

    name = CharField(max_length=100)
    category = CharField(max_length=50)
    price = IntField(default=0)
    is_available = IntField(default=1)
    rating = IntField(default=0, min_value=0, max_value=5)


async def setup() -> None:
    await ryx.setup(DATABASE_URL)
    runner = MigrationRunner([Product])
    await runner.migrate()

    # Clean and seed
    await Product.objects.bulk_delete()

    products = [
        Product(
            name="Laptop Pro",
            category="Electronics",
            price=1200,
            is_available=True,
            rating=5,
        ),
        Product(
            name="Laptop Lite",
            category="Electronics",
            price=600,
            is_available=True,
            rating=4,
        ),
        Product(
            name="Wireless Mouse",
            category="Electronics",
            price=25,
            is_available=True,
            rating=3,
        ),
        Product(
            name="Python Cookbook",
            category="Books",
            price=45,
            is_available=True,
            rating=5,
        ),
        Product(
            name="Rust in Action",
            category="Books",
            price=50,
            is_available=False,
            rating=4,
        ),
        Product(
            name="Desk Chair",
            category="Furniture",
            price=300,
            is_available=True,
            rating=3,
        ),
        Product(
            name="Standing Desk",
            category="Furniture",
            price=800,
            is_available=False,
            rating=4,
        ),
        Product(
            name="Notebook", category="Books", price=5, is_available=True, rating=2
        ),
    ]
    await Product.objects.bulk_create(products)


#
#  LOOKUPS
#
async def demo_lookups() -> None:
    print("\n" + "=" * 60)
    print("Built-in Lookups")
    print("=" * 60)

    # exact — default when no lookup is specified
    results = await Product.objects.filter(name="Laptop Pro")
    print(f"exact:          {len(results)} → {[p.name for p in results]}")

    # gt / gte / lt / lte — numeric comparisons
    expensive = await Product.objects.filter(price__gt=100)
    print(f"gt(100):        {len(expensive)} → {[p.name for p in expensive]}")

    affordable = await Product.objects.filter(price__lte=50)
    print(f"lte(50):        {len(affordable)} → {[p.name for p in affordable]}")

    # range — BETWEEN inclusive
    mid_range = await Product.objects.filter(price__range=(20, 100))
    print(f"range(20,100):  {len(mid_range)} → {[p.name for p in mid_range]}")

    # contains / icontains — case-sensitive / insensitive substring
    books = await Product.objects.filter(category__icontains="book")
    print(f"icontains(book):{len(books)} → {[p.name for p in books]}")

    # startswith / endswith
    laptops = await Product.objects.filter(name__startswith="Laptop")
    print(f"startswith(L):  {len(laptops)} → {[p.name for p in laptops]}")

    # in — membership in a list
    prices = await Product.objects.filter(price__in=[5, 25, 45])
    print(f"in([5,25,45]):  {len(prices)} → {[p.name for p in prices]}")

    # isnull — check for NULL
    # (all our products have non-null fields, so this returns 0)
    null_names = await Product.objects.filter(name__isnull=True)
    print(f"isnull(True):   {len(null_names)} products with NULL name")


#
#  Q OBJECTS
#
async def demo_q_objects() -> None:
    print("\n" + "=" * 60)
    print("Q Objects — OR, AND, NOT, Nesting")
    print("=" * 60)

    # OR — combine with |
    cheap_or_furniture = await Product.objects.filter(
        Q(price__lte=50) | Q(category="Furniture")
    )
    print(
        f"price<=50 OR Furniture: {len(cheap_or_furniture)} → {[p.name for p in cheap_or_furniture]}"
    )

    # AND — combine with &
    cheap_and_available = await Product.objects.filter(
        Q(price__lte=50) & Q(is_available=True)
    )
    print(
        f"price<=50 AND available: {len(cheap_and_available)} → {[p.name for p in cheap_and_available]}"
    )

    # NOT — invert with ~
    not_books = await Product.objects.filter(~Q(category="Books"))
    print(f"NOT Books: {len(not_books)} → {[p.name for p in not_books]}")

    # Complex nesting
    # (Electronics OR Furniture) AND (available AND rating >= 3)
    results = await Product.objects.filter(
        (Q(category="Electronics") | Q(category="Furniture"))
        & Q(is_available=True)
        & Q(rating__gte=3)
    )
    print(f"Complex: {len(results)} → {[p.name for p in results]}")

    # Q objects mixed with kwargs (kwargs are AND-ed with the Q tree)
    results = await Product.objects.filter(
        Q(price__gte=500) | Q(price__lte=10),
        is_available=True,
    )
    print(f"Q + kwargs: {len(results)} → {[p.name for p in results]}")


#
#  EXCLUDE
#
async def demo_exclude() -> None:
    print("\n" + "=" * 60)
    print("exclude()")
    print("=" * 60)

    # Simple exclude
    not_books = await Product.objects.exclude(category="Books")
    print(f"exclude(Books): {len(not_books)} → {[p.name for p in not_books]}")

    # Chained: filter then exclude
    available_not_electronics = await Product.objects.filter(is_available=True).exclude(
        category="Electronics"
    )
    print(
        f"available, not Electronics: {len(available_not_electronics)} → {[p.name for p in available_not_electronics]}"
    )


#
#  ORDERING
#
async def demo_ordering() -> None:
    print("\n" + "=" * 60)
    print("order_by()")
    print("=" * 60)

    # Ascending
    by_name = await Product.objects.order_by("name")
    print(f"ASC name:  {[p.name for p in by_name[:3]]}…")

    # Descending
    by_price_desc = await Product.objects.order_by("-price")
    print(f"DESC price: {[p.name for p in by_price_desc[:3]]}")

    # Multi-field ordering
    by_cat_then_price = await Product.objects.order_by("category", "-price")
    print("category ASC, price DESC:")
    for p in by_cat_then_price:
        print(f"  {p.category:15s} ${p.price:4d}  {p.name}")


#
#  PAGINATION
#
async def demo_pagination() -> None:
    print("\n" + "=" * 60)
    print("Pagination — Slicing, limit(), offset()")
    print("=" * 60)

    all_products = await Product.objects.order_by("-price")

    # Slicing — [:n] returns first n
    top3 = await Product.objects.order_by("-price")[:3]
    print(f"[:3]  → {[p.name for p in top3]}")

    # Slicing — [start:stop] returns a range
    middle = await Product.objects.order_by("-price")[2:5]
    print(f"[2:5] → {[p.name for p in middle]}")

    # Single index — [n] returns the instance at position n
    third = await Product.objects.order_by("-price")[2]
    print(f"[2]   → {third.name} (${third.price})")

    # limit() / offset() — chainable methods
    page2 = await Product.objects.order_by("-price").limit(3).offset(3)
    print(f"limit(3).offset(3) → {[p.name for p in page2]}")


#
#  DISTINCT & QUERY INTROSPECTION
#
async def demo_distinct_and_query() -> None:
    print("\n" + "=" * 60)
    print("distinct() & Query Introspection")
    print("=" * 60)

    # distinct() — removes duplicate rows
    qs = Product.objects.filter(category__icontains="book").distinct()
    results = await qs
    print(f"distinct books: {len(results)}")

    # .query — see the compiled SQL
    print("\nCompiled SQL:")
    print(f"  {qs.query}")

    # Build a complex query and inspect it
    complex_qs = (
        Product.objects.filter(Q(price__gt=100) | Q(rating=5))
        .exclude(category="Furniture")
        .order_by("-price")
    )
    print("\nComplex query SQL:")
    print(f"  {complex_qs.query}")


#
#  CUSTOM LOOKUPS
#
# Register a custom lookup: __mod (SQL modulo)
# The decorator uses the function's docstring as the SQL template.
# {col} is replaced with the quoted column name, ? is the value placeholder.
@ryx.lookup("mod")
def mod_lookup():
    """({col} % ?) = 0"""


async def demo_custom_lookup() -> None:
    print("\n" + "=" * 60)
    print("Custom Lookup — @lookup decorator")
    print("=" * 60)

    # Find products whose price is divisible by 100
    round_prices = await Product.objects.filter(price__mod=100)
    print(f"price__mod=100: {[p.name for p in round_prices]}")

    # List all available lookups
    lookups = ryx.available_lookups()
    print(f"\nAll registered lookups: {sorted(lookups)}")


#
#  MAIN
#
async def main() -> None:
    print("Ryx ORM — Example 03: Querying & Filters")
    await setup()

    await demo_lookups()
    await demo_q_objects()
    await demo_exclude()
    await demo_ordering()
    await demo_pagination()
    await demo_distinct_and_query()
    await demo_custom_lookup()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
