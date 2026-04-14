from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from ryx.cli.config_loader import find_config_file, load_config_file


@dataclass
class ResolvedConfig:
    urls: Dict[str, str]
    pool: Dict
    models: List[str]
    db_alias: str
    config_path: Optional[Path]


def parse_urls_arg(urls_arg: Optional[str]) -> Dict[str, str]:
    if not urls_arg:
        return {}
    result = {}
    parts = [p.strip() for p in urls_arg.split(",") if p.strip()]
    for part in parts:
        if "=" not in part:
            continue
        alias, url = part.split("=", 1)
        result[alias.strip()] = url.strip()
    return result


def collect_env_urls() -> Dict[str, str]:
    urls = {}
    for k, v in os.environ.items():
        if k.startswith("RYX_DB_") and k.endswith("_URL"):
            alias = k.removeprefix("RYX_DB_").removesuffix("_URL").lower()
            urls[alias] = v
    if "default" not in urls and os.getenv("RYX_DATABASE_URL"):
        urls["default"] = os.environ["RYX_DATABASE_URL"]
    return urls


def resolve_config(args) -> ResolvedConfig:
    # 1) CLI urls
    urls: Dict[str, str] = parse_urls_arg(getattr(args, "urls", None))
    if getattr(args, "url", None):
        urls["default"] = args.url
        # keep backward compat with code paths expecting RYX_DATABASE_URL
        os.environ["RYX_DATABASE_URL"] = args.url

    # 2) env
    env_urls = collect_env_urls()
    for k, v in env_urls.items():
        urls.setdefault(k, v)

    # 3) config file
    cfg_path = None
    cfg = {}
    if getattr(args, "config", None):
        cfg_path = Path(args.config)
        if cfg_path.exists():
            cfg = load_config_file(cfg_path) or {}
    else:
        cfg_path = find_config_file()
        if cfg_path:
            cfg = load_config_file(cfg_path) or {}
    file_urls = cfg.get("urls", {}) if isinstance(cfg.get("urls"), dict) else {}
    for k, v in file_urls.items():
        urls.setdefault(k, v)

    pool = cfg.get("pool", {}) if isinstance(cfg.get("pool"), dict) else {}
    models = []
    if getattr(args, "models", None):
        models = args.models if isinstance(args.models, list) else [args.models]
    else:
        files = None
        if isinstance(cfg.get("models"), dict):
            files = cfg.get("models", {}).get("files")
        if files:
            models = files

    db_alias = getattr(args, "db", None) or "default"

    return ResolvedConfig(urls=urls, pool=pool, models=models, db_alias=db_alias, config_path=cfg_path)


__all__ = ["ResolvedConfig", "resolve_config", "parse_urls_arg", "collect_env_urls"]
