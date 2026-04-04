"""
Ryx ORM — Example 09: Signals

This example covers:
  - Signal class — publish/subscribe pattern
  - Built-in signals: pre_save, post_save, pre_delete, post_delete,
    pre_update, post_update, pre_bulk_delete, post_bulk_delete
  - @receiver decorator — concise receiver registration
  - Signal.connect() / Signal.disconnect() — programmatic control
  - sender filtering — only fire for specific models
  - Signal.send() — fire signals and await all receivers concurrently
  - Signal firing order for save/delete operations

Run with:
    uv run python examples/09_signals.py
"""

import asyncio
import os
from datetime import datetime
from pathlib import Path

import ryx
from ryx import (
    Model,
    CharField,
    IntField,
    BooleanField,
    DateTimeField,
    Signal,
    receiver,
    pre_save,
    post_save,
    pre_delete,
    post_delete,
    pre_update,
    post_update,
    pre_bulk_delete,
    post_bulk_delete,
)
from ryx.migrations import MigrationRunner


DB_PATH = Path(__file__).parent.parent / "ryx_examples.sqlite3"
DATABASE_URL = f"sqlite://{DB_PATH}?mode=rwc"
os.environ["RYX_DATABASE_URL"] = DATABASE_URL


#
#  MODELS
#
class Article(Model):
    class Meta:
        table_name = "ex9_articles"

    title = CharField(max_length=200)
    body = CharField(max_length=500, null=True, blank=True)
    views = IntField(default=0)
    is_published = BooleanField(default=False)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True, null=True, blank=True)


#
#  EVENT LOG — tracks all signal firings
#
event_log: list[str] = []


def log(event: str) -> None:
    """Record a signal event with a timestamp."""
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    event_log.append(f"[{ts}] {event}")


#
#  SIGNAL RECEIVERS — using @receiver decorator
#
@receiver(pre_save, sender=Article)
async def article_pre_save(sender, instance, created, **kwargs):
    """Run before an Article is saved."""
    action = "INSERT" if created else "UPDATE"
    log(f"pre_save({action}) → {instance.title}")


@receiver(post_save, sender=Article)
async def article_post_save(sender, instance, created, **kwargs):
    """Run after an Article is saved."""
    action = "Created" if created else "Updated"
    log(f"post_save({action}) → {instance.title} (pk={instance.pk})")


@receiver(pre_delete, sender=Article)
async def article_pre_delete(sender, instance, **kwargs):
    """Run before an Article is deleted."""
    log(f"pre_delete → {instance.title} (pk={instance.pk})")


@receiver(post_delete, sender=Article)
async def article_post_delete(sender, instance, **kwargs):
    """Run after an Article is deleted."""
    log(f"post_delete → {instance.title} removed")


@receiver(pre_update, sender=Article)
async def article_pre_update(sender, queryset, fields, **kwargs):
    """Run before a bulk QuerySet.update()."""
    log(f"pre_update → fields={fields}")


@receiver(post_update, sender=Article)
async def article_post_update(sender, queryset, updated_count, fields, **kwargs):
    """Run after a bulk QuerySet.update()."""
    log(f"post_update → {updated_count} rows, fields={fields}")


@receiver(pre_bulk_delete, sender=Article)
async def article_pre_bulk_delete(sender, queryset, **kwargs):
    """Run before a bulk QuerySet.delete()."""
    log(f"pre_bulk_delete → queryset query: {queryset.query}")


@receiver(post_bulk_delete, sender=Article)
async def article_post_bulk_delete(sender, queryset, deleted_count, **kwargs):
    """Run after a bulk QuerySet.delete()."""
    log(f"post_bulk_delete → {deleted_count} rows deleted")


#
#  GLOBAL RECEIVER (no sender filter)
#
@receiver(post_save)
async def global_post_save(sender, instance, created, **kwargs):
    """Fires for ALL models on post_save."""
    log(f"global post_save → {sender.__name__}.{instance}")


#
#  PROGRAMMATIC SIGNAL REGISTRATION
#
async def custom_audit_receiver(sender, instance, created, **kwargs):
    """Audit receiver registered programmatically."""
    log(f"AUDIT: Article {'created' if created else 'updated'} — {instance.title}")


#
#  CUSTOM SIGNAL
#
# Define a custom signal for article publication
article_published = Signal("article_published")


@receiver(article_published, sender=Article)
async def on_article_published(sender, instance, **kwargs):
    """Handle article publication event."""
    log(f"ARTICLE PUBLISHED: '{instance.title}' is now live!")


#
#  DEMOS
#
async def setup() -> None:
    await ryx.setup(DATABASE_URL)
    runner = MigrationRunner([Article])
    await runner.migrate()

    # Clean
    await Article.objects.bulk_delete()

    # Register the programmatic receiver
    post_save.connect(custom_audit_receiver, sender=Article)


async def demo_create_signals() -> None:
    print("\n" + "=" * 60)
    print("Signals on Create")
    print("=" * 60)

    event_log.clear()

    article = await Article.objects.create(
        title="Hello World",
        body="My first article",
        is_published=True,
    )

    for event in event_log:
        print(f"  {event}")


async def demo_update_signals() -> None:
    print("\n" + "=" * 60)
    print("Signals on Update (instance.save)")
    print("=" * 60)

    event_log.clear()

    article = await Article.objects.get(title="Hello World")
    article.title = "Hello World (Updated)"
    await article.save()

    for event in event_log:
        print(f"  {event}")


async def demo_bulk_update_signals() -> None:
    print("\n" + "=" * 60)
    print("Signals on Bulk Update (QuerySet.update)")
    print("=" * 60)

    event_log.clear()

    count = await Article.objects.filter().update(views=100)

    for event in event_log:
        print(f"  {event}")


async def demo_delete_signals() -> None:
    print("\n" + "=" * 60)
    print("Signals on Delete")
    print("=" * 60)

    event_log.clear()

    article = await Article.objects.get(title__startswith="Hello")
    await article.delete()

    for event in event_log:
        print(f"  {event}")


async def demo_bulk_delete_signals() -> None:
    print("\n" + "=" * 60)
    print("Signals on Bulk Delete")
    print("=" * 60)

    # Create articles to delete
    await Article.objects.create(title="Temp 1")
    await Article.objects.create(title="Temp 2")

    event_log.clear()

    deleted = await Article.objects.filter(title__startswith="Temp").delete()

    for event in event_log:
        print(f"  {event}")


async def demo_custom_signal() -> None:
    print("\n" + "=" * 60)
    print("Custom Signal — article_published")
    print("=" * 60)

    event_log.clear()

    article = await Article.objects.create(
        title="Breaking News",
        is_published=True,
    )

    # Fire the custom signal
    await article_published.send(sender=Article, instance=article)

    for event in event_log:
        print(f"  {event}")


async def demo_disconnect() -> None:
    print("\n" + "=" * 60)
    print("Disconnecting a Receiver")
    print("=" * 60)

    event_log.clear()

    # Disconnect the global receiver
    removed = post_save.disconnect(global_post_save)
    print(f"Disconnected global_post_save: {removed}")

    # Create an article — global receiver should NOT fire
    await Article.objects.create(title="Silent Article")

    for event in event_log:
        print(f"  {event}")

    # Reconnect for other demos
    post_save.connect(global_post_save)


#
#  MAIN
#
async def main() -> None:
    print("Ryx ORM — Example 09: Signals")
    await setup()

    await demo_create_signals()
    await demo_update_signals()
    await demo_bulk_update_signals()
    await demo_delete_signals()
    await demo_bulk_delete_signals()
    await demo_custom_signal()
    await demo_disconnect()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
