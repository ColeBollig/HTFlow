# Copyright 2026 Center for High Throughput Computing (CHTC)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import argparse
import fcntl
import logging
import shutil
import sys

from htflow.engines.engine import Engine
from htflow.exit_codes import EXIT_ENGINE_ACTIVE

logger = logging.getLogger(__name__)


def add_parser(name: str, subparsers: argparse._SubParsersAction, common_parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Register the cleanup subcommand. No --jdl/--dir/etc — cleanup doesn't build a dataflow."""
    return subparsers.add_parser(
        name,
        formatter_class=argparse.RawTextHelpFormatter,
        help="Remove the engine working directory (flowman/)",
    )


def run(args: argparse.Namespace) -> None:
    workdir = Engine.work_dir()
    if not workdir.exists():
        print("Nothing to clean up (no flowman/ directory found).")
        return

    lock_fp = None
    if Engine.lock_file().exists():
        try:
            lock_fp = open(Engine.lock_file(), "w")
            fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            if lock_fp:
                lock_fp.close()
            logger.error("Cannot clean up: an engine is currently running.")
            sys.exit(EXIT_ENGINE_ACTIVE)

    shutil.rmtree(workdir)
    if lock_fp:
        lock_fp.close()
    print("Cleaned up flowman/ directory.")
