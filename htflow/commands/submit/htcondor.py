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
import os
import shutil
import sys
import textwrap
from pathlib import Path
from typing import List, Union

import htcondor2

from htflow.dataflow import HTCondorDataFlow
from htflow.engines.engine import Engine
from htflow.exit_codes import EXIT_SETUP_FAILURE

logger = logging.getLogger(__name__)


# Forwarded from the submitter's own environment so the job can find the
# same htcondor2/classad2 bindings and config -- only valid when the job
# is guaranteed to run in the submitter's own environment (local universe
# always; vanilla universe only outside --no-shared-fs).
_SHARED_ENVIRONMENT_GETENV = "CONDOR_CONFIG,_CONDOR_*,PATH,PYTHONPATH,TZ,HOME,USER,LANG,LC_ALL,ASAN_OPTIONS,LSAN_OPTIONS"


def _local_universe_defaults(args: argparse.Namespace, df: HTCondorDataFlow) -> dict:
    """monitor: local universe always runs on the AP."""
    return {
        "universe": "local",
        "should_transfer_files": "NO",
        "initialdir": str(Path.cwd()),
        "getenv": _SHARED_ENVIRONMENT_GETENV,
    }


def _collision_check(paths: List[Union[Path, str]]) -> None:
    """Fail fast on two distinct source paths flattening to the same basename."""
    seen = {}
    for p in paths:
        name = Path(p).name
        prior = seen.get(name)
        if prior is not None and prior != p:
            logger.error("--no-shared-fs: '%s' and '%s' would both transfer as '%s'", prior, p, name)
            sys.exit(EXIT_SETUP_FAILURE)
        seen[name] = p


def _no_shared_fs_transfer(args: argparse.Namespace, df: HTCondorDataFlow) -> dict:
    """manual + --no-shared-fs: transfer root/JDL/job-shapes files in, leaf
    files + flowman/ back out. Intermediate files stay local -- every node
    runs as a subprocess of this same job."""
    roots, _, leafs = df.groupings

    # transfer_input_files resolves relative paths against wherever the job
    # is actually SUBMITTED from, which can differ from htflow's own cwd
    # here (e.g. a flowman node submitted later by DAGMan) -- resolve() to
    # absolute now, while htflow's cwd is still the right base.
    inputs = [Path(p).resolve() for p in args.jdl]
    if args.job_shapes:
        inputs.append(Path(args.job_shapes).resolve())
    # roots is Path (local) or a validated URL str -- resolve() only the
    # Path ones; wrapping a URL in Path() corrupts it (collapses the "//").
    inputs += [p.resolve() if isinstance(p, Path) else p for p in roots]

    _collision_check(inputs)

    outputs = [p.name for p in leafs if isinstance(p, Path)]
    outputs.append(str(Engine.work_dir()))

    return {
        "should_transfer_files": "YES",
        "transfer_input_files": ",".join(str(p) for p in inputs),
        "transfer_output_files": ",".join(outputs),
    }


def _vanilla_universe_defaults(args: argparse.Namespace, df: HTCondorDataFlow) -> dict:
    """manual: vanilla universe assumes a shared filesystem/environment,
    unless --no-shared-fs opts into real file transfer instead."""
    desc = {"universe": "vanilla"}

    if args.no_shared_fs:
        desc.update(_no_shared_fs_transfer(args, df))
    else:
        desc["should_transfer_files"] = "NO"
        desc["initialdir"] = str(Path.cwd())
        desc["getenv"] = _SHARED_ENVIRONMENT_GETENV

    if args.container:
        desc["container_image"] = _submit_string(args.container)

    return desc


MODE_DEFAULTS = {
    "manual": _vanilla_universe_defaults,
    "monitor": _local_universe_defaults,
}


def add_parser(name: str, subparsers: argparse._SubParsersAction, common_parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Register the htcondor submit backend"""
    htcondor_p = subparsers.add_parser(
        name,
        parents=[common_parser],
        formatter_class=argparse.RawTextHelpFormatter,
        help="Submit the workflow as an HTCondor job running an htflow engine",
    )
    htcondor_p.add_argument(
        "--mode",
        required=True,
        choices=list(MODE_DEFAULTS),
        help=textwrap.dedent(
            """
            Engine mode to run inside the submitted job:
                manual  - runs 'htflow execute manual' as a vanilla universe job
                monitor - runs 'htflow execute monitor' as a local universe job
            """
        ),
    )
    htcondor_p.add_argument(
        "--interval",
        type=float,
        default=1.0,
        metavar="SECONDS",
        help="Polling interval in seconds, passed through to the inner 'htflow execute' (default: 1.0)",
    )
    htcondor_p.add_argument(
        "--no-shared-fs",
        dest="no_shared_fs",
        action="store_true",
        default=False,
        help="--mode manual only. Don't assume a shared filesystem: transfer root/JDL/job-shapes files in, leaf files and flowman/ back out.",
    )
    htcondor_p.add_argument(
        "--container",
        default=None,
        metavar="IMAGE",
        help="--mode manual only. Run the job inside this container image (sets 'container_image').",
    )
    htcondor_p.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Print the generated submit description instead of submitting it",
    )
    htcondor_p.add_argument(
        "--submit-output",
        dest="submit_output",
        default=None,
        metavar="PATH",
        help="Path for the submitted job's own stdout (default: flowman/submit.<mode>.debug)",
    )
    htcondor_p.add_argument(
        "--submit-error",
        dest="submit_error",
        default=None,
        metavar="PATH",
        help="Path for the submitted job's own stderr (default: same as --submit-output)",
    )
    htcondor_p.add_argument(
        "--submit-log",
        dest="submit_log",
        default=None,
        metavar="PATH",
        help="Path for the submitted job's own HTCondor event log (default: flowman/submit.<mode>.log)",
    )
    htcondor_p.add_argument(
        "-a", "--append",
        dest="append",
        action="append",
        nargs=2,
        metavar=("KEY", "VALUE"),
        default=None,
        help="Add a raw 'KEY = VALUE' submit command, applied after everything "
             "else -- overrides any key htflow itself would set. Repeatable.",
    )
    htcondor_p.add_argument(
        "-p", "--prepend",
        dest="prepend",
        action="append",
        nargs=2,
        metavar=("KEY", "VALUE"),
        default=None,
        help="Add a raw 'KEY = VALUE' submit command, applied before everything "
             "else -- htflow's own defaults win on a collision. Repeatable.",
    )
    return htcondor_p


def _is_windows() -> bool:
    """Indirection so tests can monkeypatch this instead of the real
    os.name, which leaks into pytest's own internals."""
    return os.name == "nt"


def _python_name() -> str:
    """Windows' official Python installer registers only 'python.exe', not
    'python3.exe' -- same PEP 394 gotcha tests.yml's matrix already works
    around for the same reason."""
    return "python" if _is_windows() else "python3"


def _submit_string(value: str) -> str:
    """Wrap a value in HTCondor's double-quoted submit-language string literal syntax."""
    return '"' + value.replace('"', '""') + '"'


def _submit_arguments(args: List[str]) -> str:
    """Build an HTCondor submit-language 'new' arguments syntax string from
    a list -- doubled double-quotes escape a literal double quote, and an
    argument containing whitespace or a single quote gets wrapped in single
    quotes (with any literal single quote inside doubled the same way)."""
    parts = []
    for arg in args:
        escaped = arg.replace('"', '""')
        if not escaped or any(c.isspace() for c in escaped) or "'" in escaped:
            escaped = "'" + escaped.replace("'", "''") + "'"
        parts.append(escaped)
    return '"' + " ".join(parts) + '"'


def _inner_execute_arguments(args: argparse.Namespace, transferred: bool) -> List[str]:
    """Reconstruct the 'htflow execute <mode>' command line. Under
    --no-shared-fs, --jdl/--job-shapes use the transferred basename instead."""
    render = (lambda p: Path(p).name) if transferred else (lambda p: str(Path(p).resolve()))

    parts = ["execute", args.mode, "--interval", str(args.interval)]

    for jdl in args.jdl:
        parts += ["--jdl", render(jdl)]

    if args.job_shapes:
        parts += ["--job-shapes", render(args.job_shapes)]

    if args.relative_to_source:
        parts.append("--relative-to-source")
    elif args.resolve_from:
        parts += ["--resolve-from", str(args.resolve_from)]

    parts += ["--node-name-length", str(args.node_name_length)]

    return parts


def _build_submit(args: argparse.Namespace, df: HTCondorDataFlow) -> htcondor2.Submit:
    mode = args.mode

    # Invoked as 'python -m htflow ...' rather than the 'htflow'
    # console-script entry point: that's a POSIX shebang script on
    # Linux/macOS but a compiled .exe launcher on Windows, while 'python
    # -m htflow' means the same thing everywhere. transfer_executable is a
    # plain boolean (unlike the should_transfer_files enum) -- needs
    # "false", not "NO".
    python_path = shutil.which(_python_name())
    if python_path is None:
        logger.error("could not locate the '%s' executable on PATH", _python_name())
        sys.exit(EXIT_SETUP_FAILURE)

    # No mkdir here -- deferred to run() after the --dry-run check, so a
    # dry run touches nothing on disk.
    workdir = Engine.work_dir()

    output_path = args.submit_output or str(workdir / f"submit.{mode}.debug")
    error_path = args.submit_error or output_path
    log_path = args.submit_log or str(workdir / f"submit.{mode}.log")

    transferred = mode == "manual" and args.no_shared_fs
    inner_args = _inner_execute_arguments(args, transferred)

    # --prepend seeds the dict first (htflow's own keys below win ties);
    # --append is applied last (wins every tie, incl. MODE_DEFAULTS).
    desc = dict(args.prepend) if args.prepend else {}

    desc.update({
        "executable": python_path,
        "arguments": _submit_arguments(["-m", "htflow"] + inner_args),
        "transfer_executable": "false",
        "batch_name": f"flowman-{mode}+$(ClusterId)",
        "output": output_path,
        "error": error_path,
        "log": log_path,
    })
    desc.update(MODE_DEFAULTS[mode](args, df))

    if args.append:
        desc.update(dict(args.append))

    return htcondor2.Submit(desc)


def run(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    if args.mode != "manual" and (args.no_shared_fs or args.container):
        logger.error("--no-shared-fs and --container only apply to --mode manual")
        sys.exit(EXIT_SETUP_FAILURE)

    df.generate()  # fail fast, before touching the schedd

    desc = _build_submit(args, df)
    desc.setSubmitMethod(1000)

    if args.dry_run:
        print(str(desc))
        return

    # Only created once we're actually submitting -- default output/error/log
    # (and --no-shared-fs's returned manual.state) live here.
    Engine.work_dir().mkdir(exist_ok=True)

    schedd = htcondor2.Schedd()
    result = schedd.submit(desc)

    schedd.reschedule()

    print(f"Submitted '{args.mode}' engine as HTCondor cluster {result.cluster()} ({desc.expand('universe')} universe)")
