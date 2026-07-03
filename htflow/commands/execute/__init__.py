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
import importlib
import inspect
import logging
import signal
import sys
import textwrap
import time

from htflow.dataflow import HTCondorDataFlow
from htflow.engines.engine import Engine, EngineExecutionError
from htflow.exit_codes import EXIT_SETUP_FAILURE, EXIT_ENGINE_ACTIVE

logger = logging.getLogger(__name__)


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


def add_parser(name: str, subparsers: argparse._SubParsersAction, common_parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Register the execute subcommand"""
    exec_p = subparsers.add_parser(
        name,
        parents=[common_parser],
        formatter_class=argparse.RawTextHelpFormatter,
        help="Execute the workflow using an engine",
    )
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
    return exec_p


def run(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
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
        engine = engine_cls(dag, config=df.config)
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
