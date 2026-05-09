from __future__ import annotations

import argparse
import asyncio
import sys

from ryx.cli.commands.base import Command


class VersionCommand(Command):
    """Print ryx version."""

    name = "version"
    help = "Print ryx version"
    description = "Display the installed ryx ORM version"

    async def execute(self, args: argparse.Namespace) -> int:
        try:
            from ryx import __version__

            verbose = getattr(args, "verbose", False)

            print(f"ryx ORM {__version__}")

            if verbose:
                try:
                    import ryx.ryx_core as _core

                    print(f"  Rust core: {_core.__version__}")
                except Exception:
                    pass

        except Exception:
            print("ryx ORM (version unknown)")
        return 0

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--verbose",
            "-v",
            action="store_true",
            help="Show additional version info (Rust core version)",
        )


async def cmd_version(args) -> None:
    """Print ryx version."""
    cmd = VersionCommand()
    await cmd.execute(args)
