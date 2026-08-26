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

import sys
import logging
import argparse
import traceback
import json
from pathlib import Path
from typing import Tuple, Callable

from htflow.dataflow import HTCondorDataFlow, AssumptionError
from htflow.config import ExecutionConfig
from htflow.sources import collect_jdl_files, InputError
from htflow.exit_codes import EXIT_SETUP_FAILURE
from htflow.utils.naming import DEFAULT_HASH_LENGTH, validate_hash_length
from htflow import commands

logger = logging.getLogger(__name__)


def setup_logging(args: argparse.Namespace) -> None:
    """Setup global logger for htflow"""
    # Disable logging
    if args.no_log:
        logging.disable(logging.CRITICAL)
        return

    # Setup logger
    level = getattr(logging, args.log_level)
    handler = logging.FileHandler(args.log_file) if args.log_file else logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s"))
    logging.getLogger().setLevel(level)
    logging.getLogger().addHandler(handler)


def parse_args() -> Tuple[argparse.Namespace, Callable[[HTCondorDataFlow, argparse.Namespace], None]]:
    parser = argparse.ArgumentParser(
        prog = "htflow",
        description = "HTFlow — dataflow runner for HTCondor",
        epilog = "Developed by the Center for High Throughput Computing (CHTC) at UW-Madison",
    )

    # Logging options
    log_group = parser.add_argument_group("logging")
    log_output = log_group.add_mutually_exclusive_group()
    log_output.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        metavar="LEVEL",
        action="store",
        type=str.upper,
        help="Logging level (DEBUG/INFO/WARNING/ERROR/CRITICAL, default: INFO)",
    )
    log_output.add_argument(
        "--no-log",
        action="store_true",
        default=False,
        help="Disable all logging output",
    )
    log_group.add_argument(
        "--log-file",
        default=None,
        metavar="PATH",
        help="Write log output to this file instead of stdout",
    )

    # Command line options common to all actions
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "--jdl",
        nargs="+",
        action="append",
        required=False,
        default=None,
        metavar="PATH",
        help="One or more HTCondor submit files to process (may be repeated)",
    )
    common_parser.add_argument(
        "--dir", "--directory", "-d",
        dest="dir",
        nargs="+",
        action="append",
        default=None,
        metavar="DIR",
        help="Directory to scan for supported dataflow input sources (may be repeated)",
    )
    common_parser.add_argument(
        "--job-shapes",
        dest="job_shapes",
        action="store",
        type=str,
        default=None,
        metavar="PATH",
        help="Path to JSON file containing special job type shapes",
    )
    common_parser.add_argument(
        "--node-name-length",
        dest="node_name_length",
        action="store",
        type=int,
        default=DEFAULT_HASH_LENGTH,
        metavar="LENGTH",
        help=argparse.SUPPRESS,
    )
    path_resolution = common_parser.add_mutually_exclusive_group()
    path_resolution.add_argument(
        "--relative-to-source",
        dest="relative_to_source",
        action="store_true",
        default=False,
        help=(
            "Resolve relative paths in each submit file against the submit "
            "file's own directory instead of the current working directory. "
            "For 'execute', the task is run from its JDL's directory. For "
            "'convert', a DAGMan DIR clause is added pointing at the JDL's "
            "directory."
        ),
    )
    path_resolution.add_argument(
        "--resolve-from",
        dest="resolve_from",
        action="store",
        type=str,
        default=None,
        metavar="PATH",
        help=(
            "Resolve relative paths in each submit file against this fixed "
            "absolute directory instead of the current working directory. "
            "For 'execute', the task is run from this directory. For "
            "'convert', a DAGMan DIR clause is added pointing at this "
            "directory. PATH must be an absolute path to an existing "
            "directory. Mutually exclusive with --relative-to-source."
        ),
    )

    # Actions commands (i.e. execute, convert, show, etc). Each command module
    # under htflow.commands registers its own subparser.
    subparsers = parser.add_subparsers(dest="command")
    for name, cmd in commands.COMMANDS.items():
        cmd.add_parser(name, subparsers, common_parser)

    # No args so display usage
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(EXIT_SETUP_FAILURE)

    # Process and validate arguments

    args = parser.parse_args()

    # --jdl/--dir use action="append" so they can be repeated; flatten the
    # resulting list-of-lists back into a flat list of strings.
    flatten = lambda groups: [item for group in groups for item in group]

    if hasattr(args, "jdl") and args.jdl is not None:
        args.jdl = flatten(args.jdl)
    if hasattr(args, "dir") and args.dir is not None:
        args.dir = flatten(args.dir)

    if getattr(args, "resolve_from", None) is not None:
        resolve_from = Path(args.resolve_from)
        if not resolve_from.is_absolute():
            parser.error(f"--resolve-from must be an absolute path: {args.resolve_from}")
        if not resolve_from.is_dir():
            parser.error(f"--resolve-from is not a directory: {args.resolve_from}")
        args.resolve_from = resolve_from

    if hasattr(args, "node_name_length"):
        try:
            validate_hash_length(args.node_name_length)
        except ValueError as e:
            parser.error(f"--node-name-length: {e}")

    if args.command not in commands.CMD_TO_FUNCTION:
        parser.print_help()
        sys.exit(EXIT_SETUP_FAILURE)

    action = commands.CMD_TO_FUNCTION[args.command]

    if hasattr(args, "jdl"):
        try:
            args.jdl = collect_jdl_files(args)
        except InputError as e:
            parser.error(str(e))

    return (args, action)


def main() -> None:
    args, action = parse_args()
    setup_logging(args)

    try:
        if not hasattr(args, "jdl"):
            # Commands without --jdl/--dir in their parser (e.g. cleanup) don't
            # build a dataflow, so their run() only takes args.
            action(args)
            return

        config = ExecutionConfig(
            relative_to_source=args.relative_to_source,
            resolve_from=args.resolve_from,
            node_name_length=args.node_name_length,
        )
        df = HTCondorDataFlow(files=args.jdl, config=config)
        if args.job_shapes:
            with open(args.job_shapes, "r") as f:
                df.shapes = json.load(f)

        action(df, args)
    except AssumptionError as e:
        logger.critical(f"Invalid dataflow: {e}")
        sys.exit(EXIT_SETUP_FAILURE)
    except Exception as e:
        logger.critical("Uncaught Exception %s", traceback.format_exc())
        sys.exit(EXIT_SETUP_FAILURE)


if __name__ == "__main__":
    main()
