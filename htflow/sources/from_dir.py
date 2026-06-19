from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List

from ._errors import InputError
from ._registry import FILE_HANDLERS

logger = logging.getLogger(__name__)


def active(args: argparse.Namespace) -> bool:
    return args.dir is not None


def resolve(args: argparse.Namespace) -> List[Path]:
    files = []
    for dir_str in args.dir:
        dir_path = Path(dir_str)
        if not dir_path.is_dir():
            raise InputError(f"Not a directory: {dir_path}")

        found = []
        for entry in sorted(dir_path.iterdir()):
            if entry.is_file() and entry.suffix in FILE_HANDLERS:
                found.extend(FILE_HANDLERS[entry.suffix](entry))

        if not found:
            logger.warning("No supported files found in directory: %s", dir_path)

        files.extend(found)

    return files
