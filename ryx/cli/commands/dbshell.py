from __future__ import annotations

import argparse
import subprocess
import sys

from ryx.cli.commands.base import Command
from ryx.cli.config import get_config
from ryx.cli.config_context import resolve_config


class DbShellCommand(Command):
    """Connect directly to the database via its native CLI tool."""

    name = "dbshell"
    help = "Connect to the database via its CLI tool"
    description = (
        "Open the database's native command-line interface. "
        "Supports psql (PostgreSQL), mysql (MySQL), and sqlite3."
    )

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--command",
            "-c",
            metavar="CMD",
            help="Execute command and exit (non-interactive)",
        )

    async def execute(self, args: argparse.Namespace) -> int:
        cfg = getattr(args, "resolved_config", None) or resolve_config(args)
        urls = cfg.urls
        url = urls.get(getattr(args, "db", None) or cfg.db_alias, urls.get("default")) if urls else None

        if not url:
            self._print_missing_url()
            return 1

        return self._run_shell(url, args)

    def _run_shell(self, url: str, args: argparse.Namespace) -> int:
        """Run the appropriate database shell."""

        if url.startswith("postgres"):
            cmd = ["psql", url]
            if getattr(args, "command", None):
                cmd.extend(["-c", args.command])
            return subprocess.run(cmd).returncode

        elif url.startswith("mysql"):
            cmd = ["mysql", "--url", url]
            if getattr(args, "command", None):
                cmd.extend(["-e", args.command])
            return subprocess.run(cmd).returncode

        elif url.startswith("sqlite"):
            db_path = url.removeprefix("sqlite:///").removeprefix("sqlite://")
            cmd = ["sqlite3", db_path]
            if getattr(args, "command", None):
                cmd.extend([args.command])
            return subprocess.run(cmd).returncode
        else:
            print(f"[ryx] Don't know which CLI tool to use for: {url}")
            return 1

    def _print_missing_url(self) -> None:
        print(
            "[ryx] No database URL found.\n"
            "  Set RYX_DATABASE_URL environment variable, or\n"
            "  pass --url postgres://user:pass@host/db"
        )


# Legacy function for backward compatibility
async def cmd_dbshell(args) -> None:
    cmd = DbShellCommand()
    await cmd.execute(args)
