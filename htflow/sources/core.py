from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List

from ._errors import InputError
from . import from_jdl, from_dir

logger = logging.getLogger(__name__)

CLI_RESOLVERS = [
    from_jdl,
    from_dir,
]


def collect_jdl_files(args: argparse.Namespace) -> List[Path]:
    active_resolvers = [r for r in CLI_RESOLVERS if r.active(args)]

    if not active_resolvers:
        raise InputError("at least one input source must be specified")

    seen: set = set()
    files: List[Path] = []

    for r in active_resolvers:
        for path in r.resolve(args):
            if path in seen:
                logger.warning("Duplicate JDL file ignored: %s", path)
            else:
                seen.add(path)
                files.append(path)

    if not files:
        raise InputError("no JDL files found from the provided input sources")

    return files
