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

# Integration tests for MonitorEngine ("htflow execute monitor"). Unlike
# test_execute.py (manual engine, local subprocesses only), these tests
# submit real jobs to a live local HTCondor Schedd -- they are gated by the
# `condor_schedd` fixture (tests/conftest.py), which skips (or, with
# HTFLOW_REQUIRE_CONDOR=1, fails) when no Schedd is reachable.

import sys
import time
import signal
import logging
import threading
import pytest
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch

import htcondor2

from htflow.__main__ import main
from htflow.utils.directory import ChangeDir
from htflow.utils.naming import hash_name

pytestmark = pytest.mark.usefixtures("condor_schedd")

# Real Schedd round trips are much slower than manual.py's local subprocess
# polling (test_execute.py uses 0.01s) -- no need to hammer the daemon.
POLL_INTERVAL = "0.5"

# Hard ceiling on a single test's wall-clock time. This is new code talking
# to a real daemon: if a bug reintroduces a hang (e.g. Terminate() never
# seeing an empty active_nodes set), this turns it into a clean test failure
# instead of hanging the whole CI job.
WATCHDOG_SECONDS = 90


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@contextmanager
def _watchdog(seconds):
    """Hard-fail if the wrapped block doesn't finish within `seconds`."""
    def _handler(signum, frame):
        raise TimeoutError(f"monitor engine test exceeded its {seconds}s watchdog timeout")

    old_handler = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def run_monitor(*args, log_file, cwd=None):
    """Invoke `htflow execute monitor` with DEBUG logging to log_file.

    Argument order mirrors test_execute.py's run_execute(): the positional
    'engine' arg ("monitor") precedes --jdl to prevent nargs="+" from
    consuming it.
    """
    argv = [
        "htflow",
        "--log-level", "DEBUG",
        "--log-file", str(log_file),
        "execute", "monitor",
        "--interval", POLL_INTERVAL,
        *args,
    ]
    with _watchdog(WATCHDOG_SECONDS):
        with patch.object(sys, "argv", argv):
            try:
                if cwd is not None:
                    with ChangeDir(cwd):
                        main()
                else:
                    main()
                return 0
            except SystemExit as e:
                return e.code


def exec_order(exec_log_path):
    """Return list of task IDs in the order they were written to exec_log_path."""
    if not exec_log_path.exists():
        return []
    return [int(line) for line in exec_log_path.read_text().splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def task_script():
    return Path(__file__).parent / "task.py"


@pytest.fixture
def make_condor_jdl(tmp_path, task_script):
    """Build a JDL that HTCondor can actually run: universe = local keeps this
    on the submit machine (no matchmaking/startd needed), and each job gets
    its own private `log` -- proving MonitorEngine's shared `dagman_log`
    (see htflow/engines/monitor.py) *adds* to it rather than replacing it."""
    def _make(name, task_id, *, exit_code=0, inputs=None, outputs=None, log="exec.log", count=None):
        # count materializes a real HTCondor job factory rather than a single
        # proc. A plain fixed `queue N` does NOT set one up (no
        # ATTR_JOB_MATERIALIZE_DIGEST_FILE), so it would never produce the
        # CLUSTER_SUBMIT/CLUSTER_REMOVE events MonitorEngine's factory-node
        # handling depends on -- max_materialize is what actually triggers it
        # (SubmitHash::want_factory_submit(), submit_utils.cpp), no itemdata
        # needed. $(Process) is each proc's 0-based index within the cluster;
        # exit_code may itself be "$(Process)" to vary per proc.
        proc_id = "$(Process)" if count else str(task_id)
        lines = [
            "universe = local",
            # HTCondor does not do a shell-style $PATH lookup for `executable` --
            # a bare "python3" is resolved relative to the job's IWD (and fails
            # with ENOENT there), not searched on $PATH. Use the exact
            # interpreter running this test suite instead.
            f"executable = {sys.executable}",
            f"arguments = {str(task_script)} --id {proc_id} --log {log} --exit-code {exit_code}",
            f"log = {name}.private.log",
            f"output = {name}.out",
            f"error = {name}.err",
            # Test-only safety net: MonitorEngine doesn't handle JOB_HELD (a
            # hold would otherwise hang the test forever, waiting out its own
            # watchdog). This is deliberately NOT in monitor.py itself -- a
            # real workflow's held job should stay held for inspection, not
            # get silently auto-removed by the engine.
            "periodic_remove = JobStatus == 5",
        ]
        if count:
            lines.append(f"max_materialize = {count}")
        if inputs:
            lines.append(f"transfer_input_files = {','.join(inputs)}")
        if outputs:
            lines.append(f"transfer_output_files = {','.join(outputs)}")
        lines.append(f"queue {count}" if count else "queue")
        p = tmp_path / f"{name}.sub"
        p.write_text("\n".join(lines) + "\n")
        return p
    return _make


@pytest.fixture
def htflow_log(tmp_path):
    return tmp_path / "htflow.log"


@pytest.fixture
def exec_log(tmp_path):
    return tmp_path / "exec.log"


@pytest.fixture(autouse=True)
def isolated_workdir(tmp_path, monkeypatch):
    """Each test runs from its own tmp_path so flowman/ (and the batch name
    MonitorEngine derives from cwd) are isolated per-test."""
    monkeypatch.chdir(tmp_path)


@pytest.fixture(autouse=True)
def cleanup_condor_jobs(tmp_path, condor_schedd):
    """Best-effort removal of anything this test submitted, keyed by the same
    hash_name(cwd) MonitorEngine uses as its default batch name -- so a test
    that fails/times out doesn't leave jobs sitting in the queue (a growing
    queue slows down the schedd's own per-cycle housekeeping, which is a
    plausible cause of tests getting progressively slower across a run)."""
    batch_name = hash_name(tmp_path.resolve())
    yield
    try:
        result = condor_schedd.act(htcondor2.JobAction.Remove, f'JobBatchName == "{batch_name}"')
        logging.getLogger(__name__).info("cleanup_condor_jobs: %s -> %s", batch_name, result)
    except Exception as e:
        # Not fatal to the test itself, but silently swallowing this would
        # hide a broken cleanup path -- log it loudly instead.
        logging.getLogger(__name__).warning("cleanup_condor_jobs FAILED for %s: %s", batch_name, e)


@pytest.fixture(autouse=True)
def reset_logging():
    """Remove all root logger handlers after each test to prevent handler
    accumulation across tests (setup_logging adds a new FileHandler each call)."""
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
# Single node
# ---------------------------------------------------------------------------

class TestMonitorSingle:
    def test_success(self, make_condor_jdl, htflow_log):
        a = make_condor_jdl("a", task_id=1)
        assert run_monitor("--jdl", str(a), log_file=htflow_log) == 0

    def test_failure(self, make_condor_jdl, htflow_log):
        a = make_condor_jdl("a", task_id=1, exit_code=1)
        assert run_monitor("--jdl", str(a), log_file=htflow_log) == 1

    def test_private_log_not_clobbered(self, make_condor_jdl, htflow_log, tmp_path):
        """The JDL's own `log` line must still receive events -- proving the
        shared dagman_log (SUBMIT_KEY_DagmanLogFile) is additive, not a
        replacement. Regression test for the Execute() fix in monitor.py."""
        a = make_condor_jdl("a", task_id=1)
        assert run_monitor("--jdl", str(a), log_file=htflow_log) == 0
        private_log = tmp_path / "a.private.log"
        assert private_log.exists()
        assert private_log.stat().st_size > 0

    def test_shared_log_written(self, make_condor_jdl, htflow_log, tmp_path):
        """flowman/dataflow.shared.log is what MonitorEngine itself watches."""
        a = make_condor_jdl("a", task_id=1)
        assert run_monitor("--jdl", str(a), log_file=htflow_log) == 0
        shared_log = tmp_path / "flowman" / "dataflow.shared.log"
        assert shared_log.exists()
        assert shared_log.stat().st_size > 0


# ---------------------------------------------------------------------------
# Linear chain: A -> B -> C
# ---------------------------------------------------------------------------

class TestMonitorLinear:
    def test_all_success(self, make_condor_jdl, htflow_log):
        a = make_condor_jdl("a", task_id=1, outputs=["a.dne"])
        b = make_condor_jdl("b", task_id=2, inputs=["a.dne"], outputs=["b.dne"])
        c = make_condor_jdl("c", task_id=3, inputs=["b.dne"])
        assert run_monitor("--jdl", str(a), str(b), str(c), log_file=htflow_log) == 0

    def test_ordering(self, make_condor_jdl, htflow_log, exec_log):
        a = make_condor_jdl("a", task_id=1, outputs=["a.dne"])
        b = make_condor_jdl("b", task_id=2, inputs=["a.dne"], outputs=["b.dne"])
        c = make_condor_jdl("c", task_id=3, inputs=["b.dne"])
        run_monitor("--jdl", str(a), str(b), str(c), log_file=htflow_log)
        assert exec_order(exec_log) == [1, 2, 3]

    def test_middle_fails(self, make_condor_jdl, htflow_log, exec_log):
        """b fails -> c is orphaned and must never run."""
        a = make_condor_jdl("a", task_id=1, outputs=["a.dne"])
        b = make_condor_jdl("b", task_id=2, exit_code=1, inputs=["a.dne"], outputs=["b.dne"])
        c = make_condor_jdl("c", task_id=3, inputs=["b.dne"])
        assert run_monitor("--jdl", str(a), str(b), str(c), log_file=htflow_log) == 1
        assert exec_order(exec_log) == [1, 2]


# ---------------------------------------------------------------------------
# Factory / late-materialization nodes (max_materialize)
#
# A node backed by a real HTCondor job factory takes a different completion
# path in monitor.py than a single-proc node: CLUSTER_SUBMIT (not a plain
# SUBMIT) marks node.internal.factory = True, and _check_node_done() then
# refuses to consider the node done until CLUSTER_REMOVE arrives, regardless
# of how many procs have already individually terminated. None of that code
# was previously exercised by any test.
# ---------------------------------------------------------------------------

class TestMonitorFactory:
    def test_all_procs_success(self, make_condor_jdl, htflow_log, exec_log):
        a = make_condor_jdl("a", task_id=0, count=3)
        assert run_monitor("--jdl", str(a), log_file=htflow_log) == 0
        assert sorted(exec_order(exec_log)) == [0, 1, 2]

    def test_one_proc_fails(self, make_condor_jdl, htflow_log, exec_log):
        """proc 0 exits 0, procs 1 and 2 exit nonzero (exit_code == $(Process))
        -- the node, and the whole run, must FAIL only once every proc has
        reported in, not on the first failure."""
        a = make_condor_jdl("a", task_id=0, count=3, exit_code="$(Process)")
        assert run_monitor("--jdl", str(a), log_file=htflow_log) == 1
        assert sorted(exec_order(exec_log)) == [0, 1, 2]


# ---------------------------------------------------------------------------
# Recovery: resume from a real, prior shared job event log
#
# Unlike ManualEngine (a hand-rolled flowman/manual.state text format),
# MonitorEngine.Recover() replays flowman/dataflow.shared.log -- the actual
# HTCondor event log -- through the same event handling Update() uses. That
# in_recovery=True code path (handle set from event.cluster rather than a
# SubmitResult) had no coverage at all before this test.
# ---------------------------------------------------------------------------

class TestMonitorRecover:
    def test_completed_run_recovers_without_resubmitting(self, make_condor_jdl, tmp_path, exec_log):
        a = make_condor_jdl("a", task_id=1)

        first_log = tmp_path / "run1.log"
        assert run_monitor("--jdl", str(a), log_file=first_log) == 0
        first_order = exec_order(exec_log)
        assert first_order == [1]

        # Same cwd/flowman dir (same tmp_path -> same batch name -> same
        # flowman/dataflow.shared.log), same JDL -- a second invocation must
        # recover the node as already-SUCCESS from the real event log and
        # exit immediately, without submitting (or running) anything again.
        second_log = tmp_path / "run2.log"
        assert run_monitor("--jdl", str(a), log_file=second_log) == 0
        assert exec_order(exec_log) == first_order  # unchanged: no re-run

        recovery_output = second_log.read_text()
        assert "[RECOVERY]" in recovery_output
        assert "SUBMIT" in recovery_output or "JOB_TERMINATED" in recovery_output


# ---------------------------------------------------------------------------
# A job that goes on hold
#
# monitor.py's __process_log_events() does not handle JOB_HELD at all (see
# its "Known limitation" docs) -- a held job is otherwise invisible to it and
# would hang Terminate() forever. make_condor_jdl's periodic_remove =
# JobStatus == 5 is meant to convert a hold into a JOB_ABORTED event instead,
# which *is* handled. This test doesn't wait for HTCondor's own periodic
# evaluation cycle to fire (its interval isn't something this test controls,
# and waiting it out would make this one test disproportionately slow) --
# instead it forces the same real-world outcome (a held job getting removed)
# as soon as the hold is observed, from a background thread. run_monitor()
# itself still runs on the main thread, since its watchdog uses
# signal.alarm(), which only works on the interpreter's main thread.
# ---------------------------------------------------------------------------

class TestMonitorHeldJob:
    def test_held_job_resolves_as_failure(self, tmp_path, condor_schedd):
        jdl = tmp_path / "bad.sub"
        jdl.write_text(
            "universe = local\n"
            # Deliberately unexecutable -- HTCondor holds this almost
            # immediately (it can't even start the process), independent of
            # task.py's own logic.
            "executable = /nonexistent/not-a-real-executable\n"
            "log = bad.private.log\n"
            "output = bad.out\n"
            "error = bad.err\n"
            "periodic_remove = JobStatus == 5\n"
            "queue\n"
        )

        batch_name = hash_name(tmp_path.resolve())

        def _remove_once_held():
            deadline = time.time() + (WATCHDOG_SECONDS - 15)
            while time.time() < deadline:
                ads = condor_schedd.query(
                    constraint=f'JobBatchName == "{batch_name}"',
                    projection=["ClusterId", "JobStatus"],
                )
                held = [ad for ad in ads if ad.get("JobStatus") == 5]
                if held:
                    condor_schedd.act(htcondor2.JobAction.Remove, f'ClusterId == {held[0]["ClusterId"]}')
                    return
                time.sleep(1)

        watcher = threading.Thread(target=_remove_once_held, daemon=True)
        watcher.start()

        htflow_log = tmp_path / "htflow.log"
        assert run_monitor("--jdl", str(jdl), log_file=htflow_log) == 1

        watcher.join(timeout=5)
