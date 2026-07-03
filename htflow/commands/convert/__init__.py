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
import logging

from htflow.dataflow import HTCondorDataFlow

logger = logging.getLogger(__name__)


def add_parser(name: str, subparsers: argparse._SubParsersAction, common_parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Register the convert subcommand"""
    conv_p = subparsers.add_parser(
        name,
        parents=[common_parser],
        formatter_class=argparse.RawTextHelpFormatter,
        help="Convert dataflow to an HTCondor DAG file",
    )
    conv_p.add_argument(
        "filename",
        nargs="?",
        type=str,
        default=None,
        metavar="FILE",
        help="Output DAG filename (default: dataflow.dag)"
    )
    return conv_p


def run(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    if args.filename is not None:
        df.filename = args.filename

    logger.debug("Converting dataflow to dag file: %s", df.filename)

    path = df.write()
    print(f"Dataflow written to DAG file: {path}")
