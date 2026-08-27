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

# Integration tests for `htflow submit htcondor` -- unlike
# tests/test_cli.py's TestSubmitHtcondor (--dry-run only, never touches a
# Schedd), these actually submit the wrapper job and wait for it (and, for
# --mode monitor, the inner job it in turn submits) to run to completion on
# a live local HTCondor Schedd. Gated by the `condor_schedd` fixture
# (tests/conftest.py), which skips (or, with HTFLOW_REQUIRE_CONDOR=1,
# fails) when no Schedd is reachable -- see test_monitor.py's module
# docstring for the same skip/fail behavior.
#
# `htflow submit htcondor` itself only ever *launches* a job -- its own
# process exit code is 0 as soon as the schedd accepts the submission, long
# before the wrapper job (and whatever it does) has finished. So unlike
# test_cli.py/test_execute.py/test_monitor.py, "did the workflow succeed or
# fail" here is never read from run_submit()'s own return code -- it's read
# from the wrapper HTCondor job's real ExitCode via condor_schedd.history(),
# which is exactly what proves htflow execute <mode>'s own process exit
# code round-trips correctly through a real HTCondor job.

import sys
import re
import time
import signal
import logging
import pytest
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch

import htcondor2

from htflow.__main__ import main
from htflow.commands.submit import htcondor as submit_htcondor
from htflow.engines.monitor import ATTR_MANAGER_ID

pytestmark = pytest.mark.usefixtures("condor_schedd")

# Generous hard ceiling, not the expected time (see test_monitor.py's own
# WATCHDOG_SECONDS comment) -- --mode manual's wrapper job is vanilla
# universe and needs a real matchmaking cycle against the pool's one slot,
# which (observed locally) takes on the order of 10-20s, on top of the
# task itself running and the schedd writing history.
WATCHDOG_SECONDS = 150

CLUSTER_RE = re.compile(r"cluster (\d+)")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@contextmanager
def _watchdog(seconds):
    """Hard-fail if the wrapped block doesn't finish within `seconds`."""
    def _handler(signum, frame):
        raise TimeoutError(f"submit htcondor test exceeded its {seconds}s watchdog timeout")

    old_handler = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def run_submit(*args, log_file, capsys):
    """Invoke `htflow submit htcondor` (never --dry-run here) with DEBUG
    logging to log_file. Returns (exit_code, stdout) -- exit_code is
    'submit' launching the job, not the job's own eventual result."""
    argv = [
        "htflow",
        "--log-level", "DEBUG",
        "--log-file", str(log_file),
        "submit", "htcondor",
        *args,
    ]
    with patch.object(sys, "argv", argv):
        try:
            main()
            code = 0
        except SystemExit as e:
            code = e.code
    return code, capsys.readouterr().out


def _submitted_cluster_id(stdout: str) -> int:
    m = CLUSTER_RE.search(stdout)
    assert m, f"expected 'Submitted ... cluster <id> ...' in stdout, got: {stdout!r}"
    return int(m.group(1))


def _wait_for_history(condor_schedd, constraint: str, timeout: float, expect: int = 1):
    """Poll condor_schedd.history() until at least `expect` ads matching
    `constraint` show up (a job only appears in history once it has fully
    left the live queue), then return them."""
    deadline = time.time() + timeout
    ads = []
    while time.time() < deadline:
        ads = list(condor_schedd.history(constraint=constraint, match=-1))
        if len(ads) >= expect:
            return ads
        time.sleep(1)
    raise TimeoutError(f"expected >= {expect} history ad(s) for {constraint!r} within {timeout}s, got {len(ads)}")


@contextmanager
def _cleanup_on_exit(condor_schedd, cluster_id: int):
    """Best-effort removal of the wrapper job and anything it tagged with
    ManagerId == cluster_id, in case the test body raises (assertion
    failure, watchdog timeout) before the run completed and cleared itself
    naturally -- same spirit as test_monitor.py's cleanup_condor_jobs."""
    try:
        yield
    finally:
        try:
            result = condor_schedd.act(
                htcondor2.JobAction.Remove,
                f"ClusterId == {cluster_id} || {ATTR_MANAGER_ID} == {cluster_id}",
            )
            logging.getLogger(__name__).info("cleanup(cluster=%d): %s", cluster_id, result)
        except Exception as e:
            logging.getLogger(__name__).warning("cleanup(cluster=%d) FAILED: %s", cluster_id, e)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def task_script():
    return Path(__file__).parent / "task.py"


@pytest.fixture
def make_jdl(tmp_path, task_script):
    """A leaf JDL for --mode manual: never touches real HTCondor itself --
    ManualEngine (running inside the vanilla-universe wrapper job) just
    reads executable/arguments and spawns it as a local subprocess."""
    def _make(name, task_id, *, exit_code=0, log="exec.log"):
        lines = [
            f"executable = {sys.executable}",
            f"arguments = {str(task_script)} --id {task_id} --log {log} --exit-code {exit_code}",
            "queue",
        ]
        p = tmp_path / f"{name}.sub"
        p.write_text("\n".join(lines) + "\n")
        return p
    return _make


@pytest.fixture
def make_condor_jdl(tmp_path, task_script):
    """A leaf JDL for --mode monitor: MonitorEngine (running inside the
    local-universe wrapper job) submits this as a real second HTCondor job.
    universe = local keeps it on the submit machine too, so this test
    doesn't pay for a second round of matchmaking on top of the wrapper's
    own. periodic_remove is the same held-job safety net test_monitor.py's
    make_condor_jdl uses."""
    def _make(name, task_id, *, exit_code=0, log="exec.log"):
        lines = [
            "universe = local",
            f"executable = {sys.executable}",
            f"arguments = {str(task_script)} --id {task_id} --log {log} --exit-code {exit_code}",
            f"log = {name}.private.log",
            f"output = {name}.out",
            f"error = {name}.err",
            "periodic_remove = JobStatus == 5",
            "queue",
        ]
        p = tmp_path / f"{name}.sub"
        p.write_text("\n".join(lines) + "\n")
        return p
    return _make


@pytest.fixture
def htflow_log(tmp_path):
    return tmp_path / "htflow.log"


@pytest.fixture(autouse=True)
def isolated_workdir(tmp_path, monkeypatch):
    """Each test runs from its own tmp_path so flowman/ (and --dry-run-free
    submissions' `initialdir`) are isolated per-test."""
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def manual_mode_getenv(monkeypatch):
    """Test-only: --mode manual's shipped default sets no getenv (assumes
    the target pool provides PATH itself). This dev pool's job environment
    has no PATH either, so inject one here purely so these tests can find
    'htflow' -- does NOT change the shipped default."""
    original = submit_htcondor._vanilla_universe_defaults

    def patched(args, df):
        desc = original(args, df)
        desc.setdefault("getenv", "PATH,PYTHONPATH,CONDOR_CONFIG")
        return desc

    monkeypatch.setitem(submit_htcondor.MODE_DEFAULTS, "manual", patched)


@pytest.fixture
def make_transfer_jdl(tmp_path):
    """A leaf JDL for --no-shared-fs tests: 'cat's its input file(s) into a
    single output file, so the wrapper job's own transfer_input_files /
    transfer_output_files round-trip is exercised end-to-end."""
    def _make(name, *, inputs, output, exit_code=0, directory=None):
        cat = " ".join(inputs)
        lines = [
            "executable = /bin/sh",
            f"arguments = \"-c 'cat {cat} > {output} && exit {exit_code}'\"",
            f"transfer_input_files = {','.join(inputs)}",
            f"transfer_output_files = {output}",
            "queue",
        ]
        d = directory or tmp_path
        p = d / f"{name}.sub"
        p.write_text("\n".join(lines) + "\n")
        return p
    return _make


@pytest.fixture(autouse=True)
def reset_logging():
    """Remove all root logger handlers after each test -- setup_logging()
    adds a new FileHandler every run_submit() call."""
    yield
    root = logging.getLogger()
    for handler in root.handlers[:]:
        try:
            handler.close()
        except Exception:
            pass
        root.removeHandler(handler)
    root.setLevel(logging.WARNING)
    logging.disable(logging.NOTSET)


# ---------------------------------------------------------------------------
# --mode manual: vanilla-universe wrapper, no leaf-level HTCondor submission
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("manual_mode_getenv")
class TestSubmitHtcondorManual:
    def test_success(self, make_jdl, tmp_path, htflow_log, capsys, condor_schedd):
        a = make_jdl("a", task_id=1)
        with _watchdog(WATCHDOG_SECONDS):
            code, out = run_submit("--jdl", str(a), "--mode", "manual", log_file=htflow_log, capsys=capsys)
        assert code == 0

        cluster_id = _submitted_cluster_id(out)
        with _cleanup_on_exit(condor_schedd, cluster_id):
            ads = _wait_for_history(condor_schedd, f"ClusterId == {cluster_id}", WATCHDOG_SECONDS)

        assert ads[0]["ExitCode"] == 0
        assert (tmp_path / "exec.log").read_text().strip() == "1"

        state = tmp_path / "flowman" / "manual.state"
        assert state.exists()
        assert "FINISHED" in state.read_text()

    def test_failure(self, make_jdl, tmp_path, htflow_log, capsys, condor_schedd):
        a = make_jdl("a", task_id=1, exit_code=1)
        with _watchdog(WATCHDOG_SECONDS):
            code, out = run_submit("--jdl", str(a), "--mode", "manual", log_file=htflow_log, capsys=capsys)
        # 'submit' itself only launches the job -- it succeeds regardless of
        # whether the engine it launches goes on to fail.
        assert code == 0

        cluster_id = _submitted_cluster_id(out)
        with _cleanup_on_exit(condor_schedd, cluster_id):
            ads = _wait_for_history(condor_schedd, f"ClusterId == {cluster_id}", WATCHDOG_SECONDS)

        # ManualEngine.EXIT_FAILURE, propagated as the wrapper job's own exit code.
        assert ads[0]["ExitCode"] == 1
        assert (tmp_path / "exec.log").read_text().strip() == "1"  # the task still ran once

        state = tmp_path / "flowman" / "manual.state"
        assert not state.exists() or "FINISHED" not in state.read_text()


# ---------------------------------------------------------------------------
# --mode manual --no-shared-fs: real transfer_input_files/transfer_output_files
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("manual_mode_getenv")
class TestSubmitHtcondorNoSharedFs:
    def test_transfers_root_in_and_leaf_out(self, make_transfer_jdl, tmp_path, htflow_log, capsys, condor_schedd):
        (tmp_path / "ext.txt").write_text("hello from root\n")
        a = make_transfer_jdl("a", inputs=["ext.txt"], output="out.txt")

        with _watchdog(WATCHDOG_SECONDS):
            code, out = run_submit("--jdl", str(a), "--mode", "manual", "--no-shared-fs", log_file=htflow_log, capsys=capsys)
        assert code == 0

        cluster_id = _submitted_cluster_id(out)
        with _cleanup_on_exit(condor_schedd, cluster_id):
            ads = _wait_for_history(condor_schedd, f"ClusterId == {cluster_id}", WATCHDOG_SECONDS)

        assert ads[0]["ExitCode"] == 0
        assert (tmp_path / "out.txt").read_text() == "hello from root\n"

        state = tmp_path / "flowman" / "manual.state"
        assert state.exists()
        assert "FINISHED" in state.read_text()

    def test_failure_propagates_exit_code(self, make_transfer_jdl, tmp_path, htflow_log, capsys, condor_schedd):
        (tmp_path / "ext.txt").write_text("hello\n")
        a = make_transfer_jdl("a", inputs=["ext.txt"], output="out.txt", exit_code=1)

        with _watchdog(WATCHDOG_SECONDS):
            code, out = run_submit("--jdl", str(a), "--mode", "manual", "--no-shared-fs", log_file=htflow_log, capsys=capsys)
        assert code == 0

        cluster_id = _submitted_cluster_id(out)
        with _cleanup_on_exit(condor_schedd, cluster_id):
            ads = _wait_for_history(condor_schedd, f"ClusterId == {cluster_id}", WATCHDOG_SECONDS)

        assert ads[0]["ExitCode"] == 1

    def test_intermediate_file_never_transferred(self, make_transfer_jdl, tmp_path, htflow_log, capsys, condor_schedd):
        """a produces mid.txt, b consumes it and produces final.txt -- mid.txt
        is neither a root nor a leaf, so it's never declared for transfer;
        it only has to exist locally, since both nodes run as subprocesses
        of this same wrapper job."""
        (tmp_path / "ext.txt").write_text("root content\n")
        a = make_transfer_jdl("a", inputs=["ext.txt"], output="mid.txt")
        b = make_transfer_jdl("b", inputs=["mid.txt"], output="final.txt")

        with _watchdog(WATCHDOG_SECONDS):
            code, out = run_submit("--jdl", str(a), str(b), "--mode", "manual", "--no-shared-fs", log_file=htflow_log, capsys=capsys)
        assert code == 0

        cluster_id = _submitted_cluster_id(out)
        with _cleanup_on_exit(condor_schedd, cluster_id):
            ads = _wait_for_history(condor_schedd, f"ClusterId == {cluster_id}", WATCHDOG_SECONDS)

        assert ads[0]["ExitCode"] == 0
        assert (tmp_path / "final.txt").read_text() == "root content\n"
        assert not (tmp_path / "mid.txt").exists()  # never declared for transfer back

    def test_works_with_dir_input(self, tmp_path, htflow_log, capsys, condor_schedd):
        jobs = tmp_path / "jobs"
        jobs.mkdir()
        (tmp_path / "ext.txt").write_text("via --dir\n")
        (jobs / "a.sub").write_text(
            "executable = /bin/sh\n"
            "arguments = \"-c 'cat ext.txt > out.txt'\"\n"
            "transfer_input_files = ext.txt\n"
            "transfer_output_files = out.txt\n"
            "queue\n"
        )

        with _watchdog(WATCHDOG_SECONDS):
            code, out = run_submit("--dir", "jobs", "--mode", "manual", "--no-shared-fs", log_file=htflow_log, capsys=capsys)
        assert code == 0

        cluster_id = _submitted_cluster_id(out)
        with _cleanup_on_exit(condor_schedd, cluster_id):
            ads = _wait_for_history(condor_schedd, f"ClusterId == {cluster_id}", WATCHDOG_SECONDS)

        assert ads[0]["ExitCode"] == 0
        assert (tmp_path / "out.txt").read_text() == "via --dir\n"


# ---------------------------------------------------------------------------
# --mode monitor: local-universe wrapper that submits a real second job
# ---------------------------------------------------------------------------

class TestSubmitHtcondorMonitor:
    def test_success(self, make_condor_jdl, tmp_path, htflow_log, capsys, condor_schedd):
        a = make_condor_jdl("a", task_id=1)
        with _watchdog(WATCHDOG_SECONDS):
            code, out = run_submit("--jdl", str(a), "--mode", "monitor", log_file=htflow_log, capsys=capsys)
        assert code == 0

        cluster_id = _submitted_cluster_id(out)
        with _cleanup_on_exit(condor_schedd, cluster_id):
            wrapper_ads = _wait_for_history(condor_schedd, f"ClusterId == {cluster_id}", WATCHDOG_SECONDS)
            inner_ads = _wait_for_history(condor_schedd, f"{ATTR_MANAGER_ID} == {cluster_id}", WATCHDOG_SECONDS)

        assert wrapper_ads[0]["ExitCode"] == 0
        assert len(inner_ads) == 1
        assert inner_ads[0]["ExitCode"] == 0
        # The self-submission batch-naming convention documented in
        # docs/engines.md's MonitorEngine constructor section.
        assert inner_ads[0]["JobBatchName"] == f"flowman-monitor+{cluster_id}"

        assert (tmp_path / "exec.log").read_text().strip() == "1"
        shared_log = tmp_path / "flowman" / "dataflow.shared.log"
        assert shared_log.exists()
        assert shared_log.stat().st_size > 0

    def test_failure(self, make_condor_jdl, tmp_path, htflow_log, capsys, condor_schedd):
        a = make_condor_jdl("a", task_id=1, exit_code=1)
        with _watchdog(WATCHDOG_SECONDS):
            code, out = run_submit("--jdl", str(a), "--mode", "monitor", log_file=htflow_log, capsys=capsys)
        assert code == 0

        cluster_id = _submitted_cluster_id(out)
        with _cleanup_on_exit(condor_schedd, cluster_id):
            wrapper_ads = _wait_for_history(condor_schedd, f"ClusterId == {cluster_id}", WATCHDOG_SECONDS)
            inner_ads = _wait_for_history(condor_schedd, f"{ATTR_MANAGER_ID} == {cluster_id}", WATCHDOG_SECONDS)

        # MonitorEngine.EXIT_FAILURE, propagated as the wrapper job's own exit code.
        assert wrapper_ads[0]["ExitCode"] == 1
        assert inner_ads[0]["ExitCode"] == 1
        assert (tmp_path / "exec.log").read_text().strip() == "1"  # the task still ran once

    def test_inner_job_tagged_with_manager_id(self, make_condor_jdl, htflow_log, capsys, condor_schedd):
        """Regression test for the manager_id/My.ManagerId str() fix in
        htflow/engines/monitor.py -- before that fix, assigning the raw int
        ClusterId raised 'value must be a string' inside the wrapper job and
        the inner job was never submitted at all (wrapper ExitCode == 1,
        zero inner history ads)."""
        a = make_condor_jdl("a", task_id=1)
        with _watchdog(WATCHDOG_SECONDS):
            code, out = run_submit("--jdl", str(a), "--mode", "monitor", log_file=htflow_log, capsys=capsys)
        assert code == 0

        cluster_id = _submitted_cluster_id(out)
        with _cleanup_on_exit(condor_schedd, cluster_id):
            _wait_for_history(condor_schedd, f"ClusterId == {cluster_id}", WATCHDOG_SECONDS)
            inner_ads = _wait_for_history(condor_schedd, f"{ATTR_MANAGER_ID} == {cluster_id}", WATCHDOG_SECONDS)

        assert len(inner_ads) == 1
        assert inner_ads[0][ATTR_MANAGER_ID] == cluster_id
