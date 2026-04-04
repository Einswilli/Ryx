"""
Ryx ORM — Example 11: Migrations

This example covers:
  - MigrationRunner — auto-detect schema changes and apply them
  - DDLGenerator — generate raw SQL for schema operations
  - Autodetector — compare models to current state, generate operations
  - SchemaState — introspect and compare database schemas
  - detect_backend — auto-detect Postgres/MySQL/SQLite from URL
  - Per-model Meta.managed — skip migration for externally-managed tables
  - Dry-run mode — preview changes without applying

Run with:
    uv run python examples/11_migrations.py
"""

import asyncio
import os
from pathlib import Path

import ryx
from ryx import Model, CharField, IntField, BooleanField, DateTimeField, TextField
from ryx import Index
from ryx.migrations import MigrationRunner, Autodetector, DDLGenerator, detect_backend
from ryx.migrations.state import (
    project_state_from_models,
    diff_states,
    SchemaState,
    TableState,
    ColumnState,
)


DB_PATH = Path(__file__).parent.parent / "ryx_examples.sqlite3"
DATABASE_URL = f"sqlite://{DB_PATH}?mode=rwc"
os.environ["RYX_DATABASE_URL"] = DATABASE_URL


#
#  MODELS — Initial version
#
class User(Model):
    """User model — will evolve through migration examples."""

    class Meta:
        table_name = "ex11_users"
        indexes = [
            Index(fields=["email"], name="ex11_user_email_idx", unique=True),
        ]

    username = CharField(max_length=50, unique=True)
    email = CharField(max_length=200)
    is_active = BooleanField(default=True)
    created_at = DateTimeField(auto_now_add=True)


class Article(Model):
    class Meta:
        table_name = "ex11_articles"

    title = CharField(max_length=200)
    body = TextField(null=True, blank=True)
    author_id = IntField()  # Simple FK without relationship for migration demo
    views = IntField(default=0)


#
#  EXTERNALLY MANAGED MODEL — no auto-migration
#
class ExternalTable(Model):
    """A model backed by an externally-managed table.

    Meta.managed=False tells Ryx to never CREATE or DROP this table.
    Useful for views, legacy tables, or tables managed by another system.
    """

    class Meta:
        table_name = "ex11_external"
        managed = False

    id = IntField(primary_key=True)
    data = CharField(max_length=200)


async def setup() -> None:
    await ryx.setup(DATABASE_URL)


#
#  DETECT BACKEND
#
async def demo_detect_backend() -> None:
    print("\n" + "=" * 60)
    print("Backend Detection")
    print("=" * 60)

    urls = [
        "sqlite://db.sqlite3",
        "postgres://user:pass@localhost/mydb",
        "mysql://user:pass@localhost/mydb",
        "postgresql://user:pass@localhost/mydb",
    ]

    for url in urls:
        backend = detect_backend(url)
        print(f"  {url:50s} → {backend}")


#
#  DDL GENERATOR
#
async def demo_ddl_generator() -> None:
    print("\n" + "=" * 60)
    print("DDL Generator — Raw SQL Generation")
    print("=" * 60)

    gen = DDLGenerator(backend="sqlite")

    # CREATE TABLE — build a TableState with ColumnState objects
    users_table = TableState(name="users")
    users_table.add_column(
        ColumnState(name="id", db_type="INTEGER", nullable=False, primary_key=True)
    )
    users_table.add_column(ColumnState(name="username", db_type="TEXT", nullable=False))
    users_table.add_column(ColumnState(name="email", db_type="TEXT", nullable=False))
    users_table.add_column(
        ColumnState(name="is_active", db_type="INTEGER", nullable=False, default="1")
    )

    sql = gen.create_table(users_table)
    print(f"CREATE TABLE:\n  {sql}")

    # ADD COLUMN
    bio_col = ColumnState(name="bio", db_type="TEXT", nullable=True)
    sql = gen.add_column("users", bio_col)
    print(f"\nADD COLUMN:\n  {sql}")

    # CREATE INDEX
    sql = gen.create_index_from_fields(
        "users", ["email"], "user_email_idx", unique=True
    )
    print(f"\nCREATE INDEX:\n  {sql}")

    # DROP TABLE
    sql = gen.drop_table("old_table")
    print(f"\nDROP TABLE:\n  {sql}")


#
#  SCHEMA STATE & DIFF
#
async def demo_schema_state() -> None:
    print("\n" + "=" * 60)
    print("Schema State & Diff Engine")
    print("=" * 60)

    # Build target state from models
    target = project_state_from_models([User, Article])
    print("Target schema from models:")
    for table_name, table in target.tables.items():
        print(f"  {table_name}:")
        for col_name, col in table.columns.items():
            pk = " [PK]" if col.primary_key else ""
            null = " NULL" if col.nullable else " NOT NULL"
            print(f"    {col_name}: {col.db_type}{null}{pk}")

    # Create an empty current state
    current = SchemaState(tables={})

    # Diff
    changes = diff_states(current, target)
    print("\nChanges needed (empty DB → models):")
    for change in changes:
        print(f"  [{change.kind.value}] {change.description}")


#
#  AUTODETECTOR
#
async def demo_autodetector() -> None:
    print("\n" + "=" * 60)
    print("Autodetector — Detect Changes from Models")
    print("=" * 60)

    # Create an autodetector for our models
    detector = Autodetector([User, Article], app_label="ex11")

    # Detect operations needed
    operations = detector.detect()
    print(f"Detected {len(operations)} operations:")
    for op in operations:
        print(f"  {op.__class__.__name__}: {op}")


#
#  MIGRATION RUNNER
#
async def demo_migration_runner() -> None:
    print("\n" + "=" * 60)
    print("MigrationRunner — Apply Schema Changes")
    print("=" * 60)

    # Run migrations for initial models
    runner = MigrationRunner([User, Article])
    await runner.migrate()
    print("Initial migration applied: User, Article tables created")

    # Verify tables exist
    tables = await User.objects.filter().count()
    print(f"User table exists (count={tables})")
    tables = await Article.objects.filter().count()
    print(f"Article table exists (count={tables})")


#
#  SCHEMA EVOLUTION
#
class UserV2(Model):
    """User model with new fields — simulates a schema evolution."""

    class Meta:
        table_name = "ex11_users"
        indexes = [
            Index(fields=["email"], name="ex11_user_email_idx", unique=True),
            Index(fields=["username"], name="ex11_user_username_idx"),
        ]

    username = CharField(max_length=50, unique=True)
    email = CharField(max_length=200)
    is_active = BooleanField(default=True)
    # New fields
    display_name = CharField(max_length=100, null=True, blank=True)
    bio = TextField(null=True, blank=True)
    last_login = DateTimeField(null=True, blank=True)
    created_at = DateTimeField(auto_now_add=True)


async def demo_schema_evolution() -> None:
    print("\n" + "=" * 60)
    print("Schema Evolution — Adding New Columns")
    print("=" * 60)

    # Detect changes between current DB and new model definition
    runner = MigrationRunner([UserV2, Article])
    await runner.migrate()
    print("Schema evolved: added display_name, bio, last_login to users")

    # Verify new columns
    user = await UserV2.objects.create(
        username="testuser2",
        email="test2@example.com",
        display_name="Test User",
        bio="A bio",
    )
    print(
        f"User columns work: username={user.username}, display_name={user.display_name}"
    )


#
#  MANAGED = FALSE
#
async def demo_managed_false() -> None:
    print("\n" + "=" * 60)
    print("Meta.managed=False — Externally Managed Tables")
    print("=" * 60)

    # MigrationRunner skips managed=False models
    runner = MigrationRunner([UserV2, Article, ExternalTable])
    await runner.migrate()
    print("MigrationRunner skipped ExternalTable (managed=False)")

    # The model still works for querying if the table exists
    print(f"ExternalTable._meta.managed = {ExternalTable._meta.managed}")
    print(f"ExternalTable._meta.table_name = {ExternalTable._meta.table_name}")


#
#  MAIN
#
async def main() -> None:
    print("Ryx ORM — Example 11: Migrations")
    await setup()

    await demo_detect_backend()
    await demo_ddl_generator()
    await demo_schema_state()
    await demo_autodetector()
    await demo_migration_runner()
    await demo_schema_evolution()
    await demo_managed_false()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
