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
import shlex
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


def _local_universe_defaults(args: argparse.Namespace, df: HTCondorDataFlow) -> dict:
    """monitor: local universe always runs on the AP."""
    return {
        "universe": "local",
        "should_transfer_files": "NO",
        "initialdir": str(Path.cwd()),
        "getenv": "CONDOR_CONFIG,_CONDOR_*,PATH,PYTHONPATH,TZ,HOME,USER,LANG,LC_ALL,ASAN_OPTIONS,LSAN_OPTIONS",
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

    inputs = [Path(p) for p in args.jdl]
    if args.job_shapes:
        inputs.append(Path(args.job_shapes))
    # roots is already Path (local) or a validated URL str (df.generate()
    # enforces ALLOWED_PROTOCOLS) -- pass through as-is, never re-wrap a URL
    # in Path() or it corrupts (collapses the "//"). HTCondor's own plugin
    # mechanism handles a URL transfer_input_files entry directly.
    inputs += roots

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
    return htcondor_p


def _submit_string(value: str) -> str:
    """Wrap a value in HTCondor's double-quoted submit-language string literal syntax."""
    return '"' + value.replace('"', '""') + '"'


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

    if shutil.which("htflow") is None:
        logger.error("could not locate the 'htflow' executable on PATH")
        sys.exit(EXIT_SETUP_FAILURE)

    workdir = Engine.work_dir()
    workdir.mkdir(exist_ok=True)

    transferred = mode == "manual" and args.no_shared_fs
    command = ["htflow"] + _inner_execute_arguments(args, transferred)

    desc = {
        # 'shell' builds executable/arguments/transfer_executable for us.
        # Unlike 'arguments', its value isn't quote-stripped -- do NOT wrap
        # it in _submit_string(), or the shell sees one big quoted word.
        "shell": shlex.join(command),
        "batch_name": f"flowman-{mode}+$(ClusterId)",
        "output": str(workdir / f"submit.{mode}.debug"),
        "error": str(workdir / f"submit.{mode}.debug"),
        "log": str(workdir / f"submit.{mode}.log"),
    }
    desc.update(MODE_DEFAULTS[mode](args, df))

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

    schedd = htcondor2.Schedd()
    result = schedd.submit(desc)

    print(f"Submitted '{args.mode}' engine as HTCondor cluster {result.cluster()} ({desc.expand('universe')} universe)")
