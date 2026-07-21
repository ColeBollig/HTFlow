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
import fcntl
import logging
import pytest
from pathlib import Path
from unittest.mock import patch

from htflow.__main__ import main
from htflow.utils.directory import ChangeDir


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
            "executable = python3",
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
        fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            assert run_execute("--jdl", str(a), log_file=htflow_log) == 75
        finally:
            fcntl.flock(fp, fcntl.LOCK_UN)
            fp.close()


# ---------------------------------------------------------------------------
# --relative-to-source: task execution directory
# ---------------------------------------------------------------------------

class TestExecuteRelativeToSource:
    def _make_jdl_in_subdir(self, subdir, task_script):
        subdir.mkdir()
        jdl = subdir / "a.sub"
        jdl.write_text(
            f"executable = python3\n"
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
            f"executable = python3\n"
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
