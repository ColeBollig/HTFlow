from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from ._registry import register


def active(args: argparse.Namespace) -> bool:
    return args.jdl is not None


def resolve(args: argparse.Namespace) -> List[Path]:
    return [Path(f) for f in args.jdl]


def handle_file(path: Path) -> List[Path]:
    return [path]


register(".sub", handle_file)
