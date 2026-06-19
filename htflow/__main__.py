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
import logging
import argparse
import importlib
import inspect
import textwrap
import traceback
import json
import shutil
import fcntl
from pathlib import Path
from typing import Tuple, Callable

from htflow.dataflow import HTCondorDataFlow, AssumptionError
from htflow.dag import Dag
from htflow.engines.engine import Engine, EngineExecutionError
from htflow.sources import collect_jdl_files, InputError

EXIT_SETUP_FAILURE = 125
EXIT_ENGINE_ACTIVE = 75

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


def _load_engine(name: str) -> type:
    """Dynamically load an Engine subclass by name or fully qualified path.

    Short name (e.g. "manual"):
        Imports htflow.engines.<name> and returns the first concrete Engine subclass found.
    """
    try:
        logger.debug("Attempting to dynamically load htflow.engines.%s", name)
        module = importlib.import_module(f"htflow.engines.{name}")
        candidates = [
            cls for _, cls in inspect.getmembers(module, inspect.isclass)
            if issubclass(cls, Engine) and cls is not Engine and not inspect.isabstract(cls)
        ]
        if not candidates:
            raise ImportError(f"No concrete Engine subclass found in htflow.engines.{name}")

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Dynamically loaded class candidates:")
            for i, c in enumerate(candidates):
                logger.debug("\t[%2d] %s", i, c.__name__)

        cls = candidates[0]
    except ModuleNotFoundError as e:
        raise ImportError(str(e)) from e

    if not (isinstance(cls, type) and issubclass(cls, Engine)):
        raise TypeError(f"'{name}' does not resolve to an Engine subclass")

    logger.debug("Selected dynamically loaded class: %s", cls.__name__)

    return cls


def cmd_execute(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    try:
        engine_cls = _load_engine(args.engine)
    except (ImportError, TypeError) as e:
        logger.error("could not load engine '%s': %s", args.engine, e)
        sys.exit(EXIT_SETUP_FAILURE)

    dag = df.generate()
    engine = None

    def _handle_signal(signum, frame):
        nonlocal engine
        if engine is not None:
            engine.Cleanup()
        logger.info("Interrupted — cleaned up.")
        sys.exit(1)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        engine = engine_cls(dag)
    except EngineExecutionError as e:
        logger.error("Engine startup failed: %s", e)
        sys.exit(EXIT_ENGINE_ACTIVE)

    engine.Recover()
    engine.Bootstrap()

    while (ec := engine.Terminate()) is None:
        engine.Execute()
        engine.Update()

        time.sleep(args.interval)

    sys.exit(ec)


def cmd_cleanup(args: argparse.Namespace) -> None:
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


def cmd_convert(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    if args.filename is not None:
        df.filename = args.filename

    logger.debug("Converting dataflow to dag file: %s", df.filename)

    path = df.write()
    print(f"Dataflow written to DAG file: {path}")


def cmd_show_files(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    logger.debug("Displaying data flow files")

    df.generate()

    file_groups = dict()
    for f in df.mapping.keys():
        protocol = "cedar"
        f_str = str(f)

        if "://" in f_str:
            protocol = f_str[:f_str.find("://")].lower()

        if protocol in file_groups:
            file_groups[protocol].append(f)
        else:
            file_groups[protocol] = [ f ]

    first = True
    for protocol, files in file_groups.items():
        if not first:
            print("")

        print(f"{protocol.upper()} files in dataflow")
        print(" Gen | Consumers | File")
        print("-----+-----------+----->")
        for f in files:
            src, dependencies = df.mapping[f]
            gen = "-" if src is None else "T"
            consumers = len(dependencies)
            print(f"  {gen}  | {consumers:>9} | {f}")

        first = False

def _cmd_show_files(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    logger.debug("Displaying data flow files")

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


def cmd_show_types(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    logger.debug("Displaying list of defined job types")

    types = df.types
    if len(types) > 0:
        print("Defined Job Types:")
        for t in types:
            print(f"   - {t}")
    else:
        print("No job types defined in provided JDL files")


CMD_EXECUTE = "execute"
CMD_CONVERT = "convert"
CMD_CLEANUP = "cleanup"
CMD_SHOW = "show"
SUBCMD_SHOW_FILES = "files"
SUBCMD_SHOW_TYPES = "types"

CMD_TO_FUNCTION = {
    CMD_EXECUTE: cmd_execute,
    CMD_CONVERT: cmd_convert,
    CMD_CLEANUP: cmd_cleanup,
    CMD_SHOW: {
        SUBCMD_SHOW_FILES: cmd_show_files,
        SUBCMD_SHOW_TYPES: cmd_show_types,
    },
}


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
        required=False,
        default=None,
        metavar="PATH",
        help="One or more HTCondor submit files to process",
    )
    common_parser.add_argument(
        "--dir", "--directory", "-d",
        dest="dir",
        nargs="+",
        default=None,
        metavar="DIR",
        help="Directory to scan for supported HTCondor submit files",
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

    # Cleanup: Command to remove the engine working directory
    subparsers.add_parser(
        CMD_CLEANUP,
        formatter_class=argparse.RawTextHelpFormatter,
        help="Remove the engine working directory (flowman/)",
    )

    # Show: Command to do introspection of various aspects of the dataflow
    show_p = _add_sub_parser(CMD_SHOW, "Inspect aspects about dataflow")
    show_p.add_argument(
        "subcmd",
        choices=[
            SUBCMD_SHOW_FILES,
            SUBCMD_SHOW_TYPES,
        ],
        metavar="view",
        help=textwrap.dedent(
            """
            View options:
                files - Show list of root, intermediate, and leaf files in dataflow
                types - Show list of job types defined within list of JDL files
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
        if action is cmd_cleanup:
            action(args)
            return

        df = HTCondorDataFlow(files=args.jdl)
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
