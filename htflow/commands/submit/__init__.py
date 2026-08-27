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

# Each backend is a flat module (e.g. htcondor.py) implementing the same
# add_parser/run contract as a top-level command -- unlike show's views,
# backends get their own real subparser (and therefore their own flags),
# not just a value of a choices positional.
BACKENDS = discover(__path__, __name__, required_attrs=("add_parser", "run"))


def add_parser(name: str, subparsers: argparse._SubParsersAction, common_parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Register the submit subcommand"""
    submit_p = subparsers.add_parser(
        name,
        formatter_class=argparse.RawTextHelpFormatter,
        help="Submit the workflow to a backend as a managed job",
    )
    backend_subparsers = submit_p.add_subparsers(dest="backend", required=True, metavar="backend")
    for bname, backend in BACKENDS.items():
        backend.add_parser(bname, backend_subparsers, common_parser)
    return submit_p


def run(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    BACKENDS[args.backend].run(df, args)
