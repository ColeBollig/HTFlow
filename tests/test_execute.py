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
import pytest
from pathlib import Path
from unittest.mock import patch

from htflow.__main__ import main
from htflow.config import ExecutionConfig
from htflow.dataflow import HTCondorDataFlow
from htflow.engines import manual as manual_engine_module
from htflow.engines.manual import ManualEngine
from htflow.utils.directory import ChangeDir
from htflow.utils.filelock import lock_exclusive_nonblocking, unlock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_execute(*args, log_file, cwd=None):
    """Invoke `htflow execute manual` with DEBUG logging to log_file.

    Argument order: htflow [global-flags] execute manual [execute-flags] [--jdl ...]
    The positional 'engine' arg ("manual") precedes --jdl to prevent nargs="+"
    from consuming it.
    """
    argv = [
        "htflow",
        "--log-level", "DEBUG",
        "--log-file", str(log_file),
        "execute", "manual",
        "--interval", "0.01",
        *args,
    ]
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
def make_jdl(tmp_path, task_script):
    def _make(name, task_id, *, exit_code=0, inputs=None, outputs=None, log="exec.log"):
        lines = [
            f"executable = {sys.executable}",
            f"arguments = {str(task_script)} --id {task_id} --log {log} --exit-code {exit_code}",
        ]
        if inputs:
            lines.append(f"transfer_input_files = {','.join(inputs)}")
        if outputs:
            lines.append(f"transfer_output_files = {','.join(outputs)}")
        lines.append("queue")
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
    """Each test runs from its own tmp_path so flowman/ is isolated per-test.

    Without this, all tests share ./flowman/manual.state in the project root and
    Recover() would try to match prior-test JDL paths against the current DAG,
    raising RuntimeError on every test after the first.
    """
    monkeypatch.chdir(tmp_path)


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

class TestExecuteSingle:
    def test_success(self, make_jdl, htflow_log):
        a = make_jdl("a", task_id=1)
        assert run_execute("--jdl", str(a), log_file=htflow_log) == 0

    def test_failure(self, make_jdl, htflow_log):
        a = make_jdl("a", task_id=1, exit_code=1)
        assert run_execute("--jdl", str(a), log_file=htflow_log) == 1

    def test_bad_executable(self, tmp_path, htflow_log):
        jdl = tmp_path / "bad.sub"
        jdl.write_text("executable = /nonexistent/exe\nqueue\n")
        assert run_execute("--jdl", str(jdl), log_file=htflow_log) == 1

    def test_htflow_log_written(self, make_jdl, htflow_log):
        a = make_jdl("a", task_id=1)
        run_execute("--jdl", str(a), log_file=htflow_log)
        assert htflow_log.exists()
        content = htflow_log.read_text()
        assert len(content) > 0
        assert "DEBUG" in content


# ---------------------------------------------------------------------------
# Linear chain: A → B → C
# ---------------------------------------------------------------------------

class TestExecuteLinear:
    def test_all_success(self, make_jdl, htflow_log):
        a = make_jdl("a", task_id=1, outputs=["a.dne"])
        b = make_jdl("b", task_id=2, inputs=["a.dne"], outputs=["b.dne"])
        c = make_jdl("c", task_id=3, inputs=["b.dne"])
        assert run_execute("--jdl", str(a), str(b), str(c), log_file=htflow_log) == 0

    def test_first_fails(self, make_jdl, htflow_log):
        a = make_jdl("a", task_id=1, exit_code=1, outputs=["a.dne"])
        b = make_jdl("b", task_id=2, inputs=["a.dne"], outputs=["b.dne"])
        c = make_jdl("c", task_id=3, inputs=["b.dne"])
        assert run_execute("--jdl", str(a), str(b), str(c), log_file=htflow_log) == 1

    def test_middle_fails(self, make_jdl, htflow_log):
        a = make_jdl("a", task_id=1, outputs=["a.dne"])
        b = make_jdl("b", task_id=2, exit_code=1, inputs=["a.dne"], outputs=["b.dne"])
        c = make_jdl("c", task_id=3, inputs=["b.dne"])
        assert run_execute("--jdl", str(a), str(b), str(c), log_file=htflow_log) == 1

    def test_ordering(self, make_jdl, htflow_log, exec_log):
        a = make_jdl("a", task_id=1, outputs=["a.dne"])
        b = make_jdl("b", task_id=2, inputs=["a.dne"], outputs=["b.dne"])
        c = make_jdl("c", task_id=3, inputs=["b.dne"])
        run_execute("--jdl", str(a), str(b), str(c), log_file=htflow_log)
        assert exec_order(exec_log) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Diamond: top → (left, right) → bottom
# ---------------------------------------------------------------------------

class TestExecuteDiamond:
    def test_all_success(self, make_jdl, htflow_log):
        top    = make_jdl("top",    task_id=1, outputs=["top.dne"])
        left   = make_jdl("left",   task_id=2, inputs=["top.dne"], outputs=["left.dne"])
        right  = make_jdl("right",  task_id=3, inputs=["top.dne"], outputs=["right.dne"])
        bottom = make_jdl("bottom", task_id=4, inputs=["left.dne", "right.dne"])
        assert run_execute("--jdl", str(top), str(left), str(right), str(bottom), log_file=htflow_log) == 0

    def test_left_fails(self, make_jdl, htflow_log):
        top    = make_jdl("top",    task_id=1, outputs=["top.dne"])
        left   = make_jdl("left",   task_id=2, exit_code=1, inputs=["top.dne"], outputs=["left.dne"])
        right  = make_jdl("right",  task_id=3, inputs=["top.dne"], outputs=["right.dne"])
        bottom = make_jdl("bottom", task_id=4, inputs=["left.dne", "right.dne"])
        assert run_execute("--jdl", str(top), str(left), str(right), str(bottom), log_file=htflow_log) == 1

    def test_both_branches_fail(self, make_jdl, htflow_log):
        top    = make_jdl("top",    task_id=1, outputs=["top.dne"])
        left   = make_jdl("left",   task_id=2, exit_code=1, inputs=["top.dne"], outputs=["left.dne"])
        right  = make_jdl("right",  task_id=3, exit_code=1, inputs=["top.dne"], outputs=["right.dne"])
        bottom = make_jdl("bottom", task_id=4, inputs=["left.dne", "right.dne"])
        assert run_execute("--jdl", str(top), str(left), str(right), str(bottom), log_file=htflow_log) == 1

    def test_ordering(self, make_jdl, htflow_log, exec_log):
        top    = make_jdl("top",    task_id=1, outputs=["top.dne"])
        left   = make_jdl("left",   task_id=2, inputs=["top.dne"], outputs=["left.dne"])
        right  = make_jdl("right",  task_id=3, inputs=["top.dne"], outputs=["right.dne"])
        bottom = make_jdl("bottom", task_id=4, inputs=["left.dne", "right.dne"])
        run_execute("--jdl", str(top), str(left), str(right), str(bottom), log_file=htflow_log)
        order = exec_order(exec_log)
        assert len(order) == 4
        assert order[0] == 1                  # top runs first
        assert set(order[1:3]) == {2, 3}      # left and right run in parallel (any order)
        assert order[3] == 4                  # bottom runs last


# ---------------------------------------------------------------------------
# Fan-out: root → (a, b, c)  — no shared convergence node
# ---------------------------------------------------------------------------

class TestExecuteFanOut:
    def test_all_success(self, make_jdl, htflow_log):
        root = make_jdl("root", task_id=1, outputs=["root.dne"])
        a    = make_jdl("a",    task_id=2, inputs=["root.dne"])
        b    = make_jdl("b",    task_id=3, inputs=["root.dne"])
        c    = make_jdl("c",    task_id=4, inputs=["root.dne"])
        assert run_execute("--jdl", str(root), str(a), str(b), str(c), log_file=htflow_log) == 0

    def test_one_leaf_fails(self, make_jdl, htflow_log, exec_log):
        root = make_jdl("root", task_id=1, outputs=["root.dne"])
        a    = make_jdl("a",    task_id=2, inputs=["root.dne"])
        b    = make_jdl("b",    task_id=3, exit_code=1, inputs=["root.dne"])
        c    = make_jdl("c",    task_id=4, inputs=["root.dne"])
        assert run_execute("--jdl", str(root), str(a), str(b), str(c), log_file=htflow_log) == 1
        # other leaves still ran (all three leaves are attempted regardless of b's failure)
        order = exec_order(exec_log)
        assert 1 in order            # root ran
        assert len(order) == 4       # all four tasks executed (root + 3 leaves)

    def test_root_fails(self, make_jdl, htflow_log, exec_log):
        root = make_jdl("root", task_id=1, exit_code=1, outputs=["root.dne"])
        a    = make_jdl("a",    task_id=2, inputs=["root.dne"])
        b    = make_jdl("b",    task_id=3, inputs=["root.dne"])
        c    = make_jdl("c",    task_id=4, inputs=["root.dne"])
        assert run_execute("--jdl", str(root), str(a), str(b), str(c), log_file=htflow_log) == 1
        # leaves were orphaned and never ran
        order = exec_order(exec_log)
        assert order == [1]


# ---------------------------------------------------------------------------
# Recovery: resume from a prior-run state file
# ---------------------------------------------------------------------------

class TestRecover:
    def _write_state(self, tmp_path, *jdl_paths):
        """Pre-populate flowman/manual.state as if the given JDLs already completed."""
        flowman = tmp_path / "flowman"
        flowman.mkdir(exist_ok=True)
        lines = "".join(
            f"*** FINISHED {1_000_000 + i}.0 {jdl}\n"
            for i, jdl in enumerate(jdl_paths)
        )
        (flowman / "manual.state").write_text(lines)

    def test_skips_completed_root(self, make_jdl, htflow_log, exec_log, tmp_path):
        """Root already in state file: only the downstream node executes."""
        a = make_jdl("a", task_id=1, outputs=["a.dne"])
        b = make_jdl("b", task_id=2, inputs=["a.dne"])
        self._write_state(tmp_path, a)

        assert run_execute("--jdl", str(a), str(b), log_file=htflow_log) == 0
        assert exec_order(exec_log) == [2]

    def test_skips_chain_prefix(self, make_jdl, htflow_log, exec_log, tmp_path):
        """First two nodes in state file: only the last node executes."""
        a = make_jdl("a", task_id=1, outputs=["a.dne"])
        b = make_jdl("b", task_id=2, inputs=["a.dne"], outputs=["b.dne"])
        c = make_jdl("c", task_id=3, inputs=["b.dne"])
        self._write_state(tmp_path, a, b)

        assert run_execute("--jdl", str(a), str(b), str(c), log_file=htflow_log) == 0
        assert exec_order(exec_log) == [3]

    def test_all_complete_exits_success(self, make_jdl, htflow_log, exec_log, tmp_path):
        """All nodes in state file: nothing executes and the run exits 0."""
        a = make_jdl("a", task_id=1, outputs=["a.dne"])
        b = make_jdl("b", task_id=2, inputs=["a.dne"])
        self._write_state(tmp_path, a, b)

        assert run_execute("--jdl", str(a), str(b), log_file=htflow_log) == 0
        assert exec_order(exec_log) == []


# ---------------------------------------------------------------------------
# Lock contention: another engine holds the lock
# ---------------------------------------------------------------------------

class TestExecuteLocked:
    def test_locked_exits_engine_active(self, make_jdl, htflow_log, tmp_path):
        """execute exits 75 when another engine already holds the flowman lock."""
        a = make_jdl("a", task_id=1)
        flowman = tmp_path / "flowman"
        flowman.mkdir()
        lock_file = flowman / "flowman.lock"
        lock_file.touch()
        fp = open(lock_file, "w")
        lock_exclusive_nonblocking(fp)
        try:
            assert run_execute("--jdl", str(a), log_file=htflow_log) == 75
        finally:
            unlock(fp)
            fp.close()


# ---------------------------------------------------------------------------
# --relative-to-source: task execution directory
# ---------------------------------------------------------------------------

class TestExecuteRelativeToSource:
    def _make_jdl_in_subdir(self, subdir, task_script):
        subdir.mkdir()
        jdl = subdir / "a.sub"
        jdl.write_text(
            f"executable = {sys.executable}\n"
            f"arguments = {task_script} --id 1 --log exec.log --exit-code 0\n"
            "queue\n"
        )
        return jdl

    def test_default_runs_from_htflow_cwd(self, tmp_path, task_script, htflow_log):
        subdir = tmp_path / "sub"
        jdl = self._make_jdl_in_subdir(subdir, task_script)

        assert run_execute("--jdl", str(jdl), log_file=htflow_log) == 0
        assert (tmp_path / "exec.log").exists()
        assert not (subdir / "exec.log").exists()

    def test_flag_runs_from_jdl_directory(self, tmp_path, task_script, htflow_log):
        subdir = tmp_path / "sub"
        jdl = self._make_jdl_in_subdir(subdir, task_script)

        assert run_execute("--relative-to-source", "--jdl", str(jdl), log_file=htflow_log) == 0
        assert (subdir / "exec.log").exists()
        assert not (tmp_path / "exec.log").exists()


# ---------------------------------------------------------------------------
# --resolve-from: never changes the task's execution directory
# ---------------------------------------------------------------------------

class TestExecuteResolveFrom:
    def _make_jdl_in_subdir(self, subdir, task_script):
        subdir.mkdir()
        jdl = subdir / "a.sub"
        jdl.write_text(
            f"executable = {sys.executable}\n"
            f"arguments = {task_script} --id 1 --log exec.log --exit-code 0\n"
            "queue\n"
        )
        return jdl

    def test_flag_still_runs_from_htflow_cwd(self, tmp_path, task_script, htflow_log):
        """--resolve-from only rewrites transfer file entries; it must never chdir,
        so the task still inherits HTFlow's own cwd exactly like the default case."""
        subdir = tmp_path / "sub"
        jdl = self._make_jdl_in_subdir(subdir, task_script)
        target = tmp_path / "target"
        target.mkdir()

        assert run_execute("--resolve-from", str(target), "--jdl", str(jdl), log_file=htflow_log) == 0
        assert (tmp_path / "exec.log").exists()
        assert not (subdir / "exec.log").exists()
        assert not (target / "exec.log").exists()


# ---------------------------------------------------------------------------
# --max-active-nodes: CLI-level behavior
# ---------------------------------------------------------------------------

class TestExecuteMaxActiveNodes:
    def _make_independent_roots(self, make_jdl, n):
        return [make_jdl(chr(ord("a") + i), task_id=i + 1) for i in range(n)]

    def test_default_runs_every_independent_root(self, make_jdl, htflow_log, exec_log):
        roots = self._make_independent_roots(make_jdl, 5)
        assert run_execute("--jdl", *[str(r) for r in roots], log_file=htflow_log) == 0
        assert sorted(exec_order(exec_log)) == [1, 2, 3, 4, 5]

    def test_limit_below_ready_count_still_completes(self, make_jdl, htflow_log, exec_log):
        """A cap smaller than the number of independent ready nodes just spreads
        their execution across more Execute()/Update() cycles -- everything
        still eventually runs and the workflow still succeeds."""
        roots = self._make_independent_roots(make_jdl, 5)
        assert run_execute("--jdl", *[str(r) for r in roots], "--max-active-nodes", "2", log_file=htflow_log) == 0
        assert sorted(exec_order(exec_log)) == [1, 2, 3, 4, 5]

    def test_limit_below_ready_count_logs_deferral(self, make_jdl, htflow_log):
        roots = self._make_independent_roots(make_jdl, 5)
        run_execute("--jdl", *[str(r) for r in roots], "--max-active-nodes", "2", log_file=htflow_log)
        assert "Max limit of active nodes 2 reached" in htflow_log.read_text()

    @pytest.mark.parametrize("value", ["-2", "-100"])
    def test_invalid_value_exits_2(self, make_jdl, htflow_log, value):
        a = make_jdl("a", task_id=1)
        assert run_execute("--jdl", str(a), "--max-active-nodes", value, log_file=htflow_log) == 2


# ---------------------------------------------------------------------------
# --max-active-nodes: precise per-cycle capping (ManualEngine, direct)
# ---------------------------------------------------------------------------
#
# These bypass the full Terminate()-driven poll loop and call Bootstrap() +
# a single Execute() directly -- run_execute() can't safely exercise
# max_active_nodes=0 (nothing would ever run, so Terminate() would never
# return and the CLI loop would hang forever). Checking active_nodes/
# ready_nodes after one Execute() call is also a far more precise way to
# confirm the cap itself than inferring it from eventual end-to-end success.

class TestManualEngineMaxActiveNodesCap:
    def _bootstrap_and_execute_once(self, make_jdl, limit):
        jdls = [str(make_jdl(chr(ord("a") + i), task_id=i + 1)) for i in range(5)]
        config = ExecutionConfig(max_active_nodes=limit)
        dag = HTCondorDataFlow(files=jdls, config=config).generate()
        engine = ManualEngine(dag, config=config)
        try:
            engine.Bootstrap()
            engine.Execute()
            return len(dag.internal.active_nodes), len(dag.internal.ready_nodes)
        finally:
            engine.Cleanup()

    def test_limit_caps_active_nodes_this_cycle(self, make_jdl):
        active, ready = self._bootstrap_and_execute_once(make_jdl, 2)
        assert active == 2
        assert ready == 3

    def test_unlimited_starts_every_ready_node(self, make_jdl):
        active, ready = self._bootstrap_and_execute_once(make_jdl, -1)
        assert active == 5
        assert ready == 0

    def test_zero_starts_nothing(self, make_jdl):
        active, ready = self._bootstrap_and_execute_once(make_jdl, 0)
        assert active == 0
        assert ready == 5


# ---------------------------------------------------------------------------
# _split_arguments(): shlex.split()'s posix mode must match the platform
# ---------------------------------------------------------------------------
#
# shlex.split()'s default posix=True treats backslash as an escape character
# -- wrong for a Windows-style path/argument, which should pass through
# unmangled. _split_arguments(windows=...) exercises both branches directly
# rather than requiring an actual Windows runner or monkeypatching os.name
# (which pathlib itself also consults, so patching it process-wide has
# unrelated side effects on any Path() construction in the same test).

class TestSplitArguments:
    def test_windows_path_argument_preserves_backslashes(self):
        cmd = manual_engine_module._split_arguments(r"C:\Users\foo\bar.txt", windows=True)
        assert cmd == [r"C:\Users\foo\bar.txt"]

    def test_posix_argument_still_processes_backslash_escapes(self):
        """Unchanged existing (POSIX) behavior: shlex's posix=True mode still
        treats a backslash as an escape character, e.g. joining an
        escaped space into a single token."""
        cmd = manual_engine_module._split_arguments(r"foo\ bar", windows=False)
        assert cmd == ["foo bar"]
