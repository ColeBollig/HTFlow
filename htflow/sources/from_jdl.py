from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import htcondor2

from ._registry import set_default_handler


def active(args: argparse.Namespace) -> bool:
    return args.jdl is not None


def resolve(args: argparse.Namespace) -> List[Path]:
    return [Path(f) for f in args.jdl]


def handle_file(path: Path) -> List[Path]:
    """Validate that a file parses as an HTCondor submit description.

    Used as the default directory-scan handler for any extension without a
    more specific parser registered (e.g. '.sub', '.txt', or no extension).
    """
    with open(path, "r") as f:
        content = f.read()

    try:
        htcondor2.Submit(content)
    except ValueError as e:
        print(f"Skipping '{path}': not a valid HTCondor submit file ({e})")
        return []

    return [path]


set_default_handler(handle_file)
