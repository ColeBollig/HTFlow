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

from htflow.dataflow import HTCondorDataFlow
from .._discovery import discover

_VIEWS = discover(__path__, __name__, required_attrs=("run",))


def add_parser(name: str, subparsers: argparse._SubParsersAction, common_parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Register the show subcommand"""
    show_p = subparsers.add_parser(
        name,
        parents=[common_parser],
        formatter_class=argparse.RawTextHelpFormatter,
        help="Inspect aspects about dataflow",
    )
    view_lines = "\n".join(f"    {n} - {(m.__doc__ or '').strip()}" for n, m in _VIEWS.items())
    show_p.add_argument(
        "subcmd",
        choices=list(_VIEWS),
        metavar="view",
        help=f"\nView options:\n{view_lines}\n",
    )
    return show_p


def run(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    _VIEWS[args.subcmd].run(df, args)
