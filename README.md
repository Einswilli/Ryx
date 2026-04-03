# Ryx ORM

**Django-style Python ORM. Powered by Rust.**

Ryx gives you the query API you love — `.filter()`, `Q` objects, aggregations, relationships — with the raw performance of a compiled Rust core. Async-native. Zero event-loop blocking.

```python
import ryx
from ryx import Model, CharField, IntField, BooleanField, DateTimeField, Q, Count, Sum

class Post(Model):
    title = CharField(max_length=200)
    slug = CharField(max_length=210, unique=True)
    views = IntField(default=0)
    active = BooleanField(default=True)
    created = DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created"]

# Setup once
await ryx.setup("postgres://user:pass@localhost/mydb")

# Query like Django, run like Rust
posts = await (
    Post.objects
        .filter(Q(active=True) | Q(views__gte=1000))
        .exclude(title__startswith="Draft")
        .order_by("-views")
        .limit(20)
)

# Aggregations
stats = await Post.objects.aggregate(
    total=Count("id"), avg_views=Avg("views"), top=Max("views"),
)

# Transactions with savepoints
async with ryx.transaction():
    post = await Post.objects.create(title="Atomic post", slug="atomic")
    await post.save()
```

## Why Ryx

| | Django ORM | SQLAlchemy | **Ryx** |
|---|---|---|---|
| **API** | Ergonomic | Verbose | **Ergonomic** |
| **Runtime** | Sync Python | Async Python | **Async Rust** |
| **GIL blocking** | Yes | Yes | **Zero** |
| **Backends** | All | All | **PG · MySQL · SQLite** |
| **Migrations** | Built-in | Alembic | **Built-in** |

## Quick Start

```bash
pip install maturin
maturin develop          # compile Rust + install
```

```python
import asyncio, ryx
from ryx import Model, CharField

class Article(Model):
    title = CharField(max_length=200)

async def main():
    await ryx.setup("sqlite:///app.db")
    await ryx.migrate([Article])
    await Article.objects.create(title="Hello Ryx")
    print(await Article.objects.all())

asyncio.run(main())
```

## Key Features

- **30+ field types** — from `AutoField` to `JSONField`, with validation built in
- **Q objects** — complex `AND` / `OR` / `NOT` expressions with nesting
- **Aggregations** — `Count`, `Sum`, `Avg`, `Min`, `Max` with `GROUP BY` and `HAVING`
- **Relationships** — `ForeignKey`, `OneToOneField`, `ManyToManyField` with `select_related` / `prefetch_related`
- **Transactions** — async context managers with nested savepoints
- **Signals** — `pre_save`, `post_save`, `pre_delete`, `post_delete` and more
- **Migrations** — autodetect schema changes, generate and apply
- **Validation** — field-level + model-level, collects all errors before raising
- **Sync/async bridge** — use from sync or async code seamlessly
- **CLI** — `python -m ryx migrate`, `makemigrations`, `shell`, `inspectdb`

## Architecture

```
Python (Models · QuerySet · Fields · Signals)
    ↓ PyO3
Rust  (AST · Compiler · Executor · Pool)
    ↓ sqlx
Database (PostgreSQL · MySQL · SQLite)
```

Your Python queries are compiled to SQL in Rust, executed by sqlx, and decoded back — all without blocking the Python event loop.

## Documentation

Full documentation with guides, API reference, and examples: **[docs](docs/)**

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, architecture details, and contribution guidelines.

## License

Python code: AGPL-3.0 · Rust code: MIT OR Apache-2.0
