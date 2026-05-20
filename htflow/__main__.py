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
import time
import signal
import argparse
import importlib
import inspect
import textwrap
from typing import Tuple, Callable

from htflow.dataflow import HTCondorDataFlow, AssumptionError
from htflow.dag import Dag
from htflow.engines.engine import Engine

EXIT_SETUP_FAILURE = 125

def _load_engine(name: str) -> type:
    """Dynamically load an Engine subclass by name or fully qualified path.

    Short name (e.g. "manual"):
        Imports htflow.engines.<name> and returns the first concrete Engine subclass found.
    """
    try:
        module = importlib.import_module(f"htflow.engines.{name}")
        candidates = [
            cls for _, cls in inspect.getmembers(module, inspect.isclass)
            if issubclass(cls, Engine) and cls is not Engine and not inspect.isabstract(cls)
        ]
        if not candidates:
            raise ImportError(f"No concrete Engine subclass found in htflow.engines.{name}")
        cls = candidates[0]
    except ModuleNotFoundError as e:
        raise ImportError(str(e)) from e

    if not (isinstance(cls, type) and issubclass(cls, Engine)):
        raise TypeError(f"'{name}' does not resolve to an Engine subclass")

    return cls


def cmd_execute(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    try:
        engine_cls = _load_engine(args.engine)
    except (ImportError, TypeError) as e:
        print(f"ERROR: could not load engine '{args.engine}': {e}", file=sys.stderr)
        sys.exit(EXIT_SETUP_FAILURE)

    dag = df.generate()
    engine = engine_cls(dag)

    def _handle_signal(signum, frame):
        nonlocal engine
        engine.Cleanup()
        print("Interrupted — cleaned up.", file=sys.stderr)
        sys.exit(1)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # TODO: Rescue/recovery

    engine.Bootstrap()

    while (ec := engine.Terminate()) is None:
        engine.Execute()
        engine.Update()

        time.sleep(args.interval)

    sys.exit(ec)


def cmd_convert(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    if args.filename is not None:
        df.filename = args.filename
    path = df.write()
    print(f"Dataflow written to DAG file: {path}")


def cmd_show_files(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    df.generate()
    roots, intermediate, leafs = df.groupings

    print("Root files (external inputs, not produced here):")
    for f in roots:
        print(f"  {f}")

    print("Intermediate files (produced and consumed):")
    for f in intermediate:
        print(f"  {f}")

    print("Leaf files (produced, not consumed):")
    for f in leafs:
        print(f"  {f}")


CMD_EXECUTE = "execute"
CMD_CONVERT = "convert"
CMD_SHOW = "show"
SUBCMD_SHOW_FILES = "files"

CMD_TO_FUNCTION = {
    CMD_EXECUTE: cmd_execute,
    CMD_CONVERT: cmd_convert,
    CMD_SHOW: {
        SUBCMD_SHOW_FILES: cmd_show_files
    },
}


def parse_args() -> Tuple[argparse.Namespace, Callable[[HTCondorDataFlow, argparse.Namespace], None]]:
    parser = argparse.ArgumentParser(
        prog = "htflow",
        description = "HTFlow — dataflow runner for HTCondor",
        epilog = "Developed by the Center for High Throughput Computing (CHTC) at UW-Madison",
    )

    # Command line options common to all actions
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "--jdl",
        nargs="+",
        required=True,
        metavar="PATH",
        help="One or more HTCondor submit files to process",
    )

    # Actions commands (i.e. execute, convert, show, etc)
    subparsers = parser.add_subparsers(dest="command")

    def _add_sub_parser(action: str, info: str) -> argparse.ArgumentParser:
        nonlocal subparsers
        return subparsers.add_parser(
            action,
            parents=[common_parser],
            formatter_class=argparse.RawTextHelpFormatter,
            help=info,
        )

    # Execute: Command to execute dataflow in a specified manner (via a specific engine)
    exec_p = _add_sub_parser(CMD_EXECUTE, "Execute the workflow using an engine")
    exec_p.add_argument(
        "engine",
        choices=[
            "manual",
        ],
        help=textwrap.dedent(
            """
            Engine types:
                manual - Manually spawn/execute dataflow tasks
            """
        ),
    )
    exec_p.add_argument(
        "--interval",
        type=float,
        default=1.0,
        metavar="SECONDS",
        help="Polling interval in seconds (default: 1.0)"
    )

    # Convert: Command to translate dataflow into DAG file for DAGMan to execute
    conv_p = _add_sub_parser(CMD_CONVERT, "Convert dataflow to an HTCondor DAG file")
    conv_p.add_argument(
        "filename",
        nargs="?",
        type=str,
        default=None,
        metavar="FILE",
        help="Output DAG filename (default: dataflow.dag)"
    )

    # Show: Command to do introspection of various aspects of the dataflow
    show_p = _add_sub_parser(CMD_SHOW, "Inspect aspects about dataflow")
    show_p.add_argument(
        "subcmd",
        choices=[
            SUBCMD_SHOW_FILES,
        ],
        metavar="view",
        help=textwrap.dedent(
            """
            View options:
                files - Show list of root, intermediate, and leaf files in dataflow
            """
        )
    )

    # No args so display usage
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(EXIT_SETUP_FAILURE)

    # Process and validate arguments

    args = parser.parse_args()

    class UnknownCommandError(Exception):
        def __init__(self, msg: str = None) -> None:
            super().__init__(msg if msg is not None else "Unknown command")

    def __resolve_command() -> Callable[[HTCondorDataFlow, argparse.Namespace], None]:
        nonlocal args
        if args.command not in CMD_TO_FUNCTION:
            raise UnknownCommandError

        if isinstance(CMD_TO_FUNCTION[args.command], dict):
            SUB_CMDS = CMD_TO_FUNCTION[args.command]
            if args.subcmd not in SUB_CMDS:
                raise UnknownCommandError
            return SUB_CMDS[args.subcmd]
        else:
            return CMD_TO_FUNCTION[args.command]

    try:
        action = __resolve_command()
    except UnknownCommandError:
        parser.print_help()
        sys.exit(EXIT_SETUP_FAILURE)

    return (args, action)


def main() -> None:
    args, action = parse_args()
    df = HTCondorDataFlow(files=args.jdl)

    try:
        action(df, args)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(EXIT_SETUP_FAILURE)


if __name__ == "__main__":
    main()
