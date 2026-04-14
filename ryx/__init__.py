from __future__ import annotations

# Import the compiled Rust extension directly to avoid circular import
import ryx.ryx_core as _core
import os


# ORM core
from ryx.models import Constraint, Index, Model
from ryx.fields import (
    ArrayField,
    AutoField,
    BigAutoField,
    BigIntField,
    BinaryField,
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    DecimalField,
    DurationField,
    EmailField,
    FloatField,
    ForeignKey,
    IntField,
    IPAddressField,
    JSONField,
    ManyToManyField,
    NullBooleanField,
    OneToOneField,
    PositiveIntField,
    SlugField,
    SmallAutoField,
    SmallIntField,
    TextField,
    TimeField,
    URLField,
    UUIDField,
)
from ryx.queryset import (
    Avg,
    Count,
    Max,
    Min,
    Q,
    QuerySet,
    RawAgg,
    Sum,
    async_to_sync,
    run_async,
    run_sync,
    sync_to_async,
)
from ryx.validators import (
    ChoicesValidator,
    EmailValidator,
    FunctionValidator,
    MaxLengthValidator,
    MaxValueValidator,
    MinLengthValidator,
    MinValueValidator,
    NotBlankValidator,
    NotNullValidator,
    RangeValidator,
    RegexValidator,
    URLValidator,
    ValidationError,
    Validator,
)
from ryx.signals import (
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
from ryx.transaction import transaction, get_active_transaction
from ryx.descriptors import (
    ForwardDescriptor,
    ReverseFKDescriptor,
    ManyToManyDescriptor,
    ReverseFKManager,
    ManyToManyManager,
)
from ryx.bulk import bulk_create, bulk_update, bulk_delete, stream
from ryx import cache as cache_module
from ryx.cache import (
    AbstractCache,
    MemoryCache,
    configure_cache,
    invalidate,
    invalidate_model,
    invalidate_all,
    get_cache,
)
from ryx.migrations.ddl import DDLGenerator, generate_schema_ddl, detect_backend
from ryx.migrations.autodetect import Autodetector
from ryx.exceptions import (
    RyxError,
    DatabaseError,
    DoesNotExist,
    MultipleObjectsReturned,
    PoolNotInitialized,
)


# Setup
async def setup(
    urls: str | dict, # str | dict to maintain backward.
    *,
    max_connections: int = 10,
    min_connections: int = 1,
    connect_timeout: int = 30,
    idle_timeout: int = 600,
    max_lifetime: int = 1800,
) -> None:
    """Initialize the ryx connection pool. Call once at startup."""
    
    # For old versions wrap the url with a dict
    if isinstance(urls, str):
        urls = {'default': urls} 

    await _core.setup(
        urls,
        max_connections=max_connections,
        min_connections=min_connections,
        connect_timeout=connect_timeout,
        idle_timeout=idle_timeout,
        max_lifetime=max_lifetime,
    )


def register_lookup(name: str, sql_template: str) -> None:
    """Register a custom lookup operator (process-global)."""
    _core.register_lookup(name, sql_template)


def available_lookups() -> list[str]:
    """Return all registered lookup names (built-in + custom)."""
    return _core.available_lookups()


def list_lookups() -> list[str]:
    """Return all built-in lookup names (for auto-discovery)."""
    return list(_core.list_lookups())


def available_transforms() -> list[str]:
    """Return all built-in transform names (for auto-discovery)."""
    return list(_core.list_transforms())


def is_connected(db_alias: str = 'default') -> bool:
    return _core.is_connected(db_alias)


def pool_stats() -> dict:
    return _core.pool_stats()


def lookup(name: str):
    """Decorator shortcut for registering a lookup."""

    def decorator(sql_template_or_fn):
        if isinstance(sql_template_or_fn, str):
            register_lookup(name, sql_template_or_fn)
            return sql_template_or_fn
        doc = sql_template_or_fn.__doc__
        if doc:
            register_lookup(name, doc.strip())
        return sql_template_or_fn

    return decorator


__version__: str = _core.__version__

__all__ = [
    # Setup
    "setup",
    "register_lookup",
    "available_lookups",
    "is_connected",
    "pool_stats",
    "lookup",
    "list_lookups",
    "list_transforms",
    # Model
    "Model",
    "Index",
    "Constraint",
    # Fields
    "ArrayField",
    "AutoField",
    "BigAutoField",
    "BigIntField",
    "BinaryField",
    "BooleanField",
    "CharField",
    "DateField",
    "DateTimeField",
    "DecimalField",
    "DurationField",
    "EmailField",
    "FloatField",
    "ForeignKey",
    "IntField",
    "IPAddressField",
    "JSONField",
    "ManyToManyField",
    "NullBooleanField",
    "OneToOneField",
    "PositiveIntField",
    "SlugField",
    "SmallAutoField",
    "SmallIntField",
    "TextField",
    "TimeField",
    "URLField",
    "UUIDField",
    # QuerySet
    "QuerySet",
    "Q",
    # Aggregates
    "Count",
    "Sum",
    "Avg",
    "Min",
    "Max",
    "RawAgg",
    # Sync/async helpers
    "sync_to_async",
    "async_to_sync",
    "run_sync",
    "run_async",
    # Validators
    "ValidationError",
    "Validator",
    "FunctionValidator",
    "NotNullValidator",
    "NotBlankValidator",
    "MaxLengthValidator",
    "MinLengthValidator",
    "MinValueValidator",
    "MaxValueValidator",
    "RangeValidator",
    "RegexValidator",
    "EmailValidator",
    "URLValidator",
    "ChoicesValidator",
    # Signals
    "Signal",
    "receiver",
    "pre_save",
    "post_save",
    "pre_delete",
    "post_delete",
    "pre_update",
    "post_update",
    "pre_bulk_delete",
    "post_bulk_delete",
    # Exceptions
    "ryxError",
    "DatabaseError",
    "DoesNotExist",
    "MultipleObjectsReturned",
    "PoolNotInitialized",
    "ValidationError",
    # Transactions
    "transaction",
    "get_active_transaction",
    # Descriptors / relations
    "ForwardDescriptor",
    "ReverseFKDescriptor",
    "ManyToManyDescriptor",
    "ReverseFKManager",
    "ManyToManyManager",
    # Bulk operations
    "bulk_create",
    "bulk_update",
    "bulk_delete",
    "stream",
    # Cache
    "AbstractCache",
    "MemoryCache",
    "configure_cache",
    "invalidate",
    "invalidate_model",
    "invalidate_all",
    "get_cache",
    # Migrations
    "DDLGenerator",
    "generate_schema_ddl",
    "detect_backend",
    "Autodetector",
    # Version
    "__version__",
]

# ---
# Optional auto-initialize (can be disabled with RYX_AUTO_INITIALIZE=0|no|false|n)
# ---
_AUTO_INIT_DONE = False


def _should_auto_init() -> bool:
    return os.getenv("RYX_AUTO_INITIALIZE", "1").lower() not in ("0", "false", "n", "no")


def _discover_urls_from_env() -> dict:
    urls = {}
    for key, val in os.environ.items():
        if key.startswith("RYX_DB_") and key.endswith("_URL"):
            alias = key.removeprefix("RYX_DB_").removesuffix("_URL").lower()
            urls[alias] = val
    if "default" not in urls:
        env_url = os.environ.get("RYX_DATABASE_URL")
        if env_url:
            urls["default"] = env_url
    return urls


def _discover_config_file():
    try:
        from ryx.cli.config_loader import find_config_file, load_config_file
    except Exception:
        return {}
    path = find_config_file()
    if not path:
        return {}
    try:
        return load_config_file(path) or {}
    except Exception:
        return {}


def _auto_setup():
    global _AUTO_INIT_DONE
    if _AUTO_INIT_DONE or not _should_auto_init():
        return

    urls = _discover_urls_from_env()
    pool_cfg = {}
    cfg = _discover_config_file()
    if cfg:
        urls.update(cfg.get("urls", {}) or {})
        pool_cfg = cfg.get("pool", {}) or {}

    if not urls:
        return

    try:
        from ryx.queryset import run_sync

        run_sync(
            setup(
                urls,
                max_connections=pool_cfg.get("max_conn", 10),
                min_connections=pool_cfg.get("min_conn", 1),
                connect_timeout=pool_cfg.get("connect_timeout", 30),
                idle_timeout=pool_cfg.get("idle_timeout", 600),
                max_lifetime=pool_cfg.get("max_lifetime", 1800),
            )
        )
        _AUTO_INIT_DONE = True
    except Exception:
        # Fail silently to avoid breaking imports; user can call setup manually.
        pass


_auto_setup()
