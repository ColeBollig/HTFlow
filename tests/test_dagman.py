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

# Live integration tests for a "flowman" node -- a real HTCondor job running
# an htflow engine, generated via 'htflow submit htcondor --dry-run' exactly
# as a user would -- driven by real DAGMan (condor_submit_dag). First file in
# this suite to actually invoke condor_submit_dag.
#
# Gated by the `condor_schedd` fixture (tests/conftest.py): skips (or, with
# HTFLOW_REQUIRE_CONDOR=1, fails) when no Schedd is reachable, and is
# auto-tagged `live` -- live-condor-tests.yml's `pytest -m live` picks these
# up on both Linux and Windows with no workflow changes.
#
# DAGMan's own manager job is 'universe = scheduler' (always on the AP) --
# the DAGMan cluster only leaves the queue once every node it manages,
# including a flowman wrapper and anything it in turn submits, has finished.

import re
import sys
import time
import logging
import subprocess
import pytest
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch

import htcondor2

from htflow.__main__ import main
from htflow.commands.submit import htcondor as submit_htcondor
from htflow.engines.monitor import ATTR_MANAGER_ID
from htflow.utils.directory import ChangeDir

# Forwarded as flowman's own inner '--interval' (same as test_submit.py's
# POLL_INTERVAL) -- no need to pay the 1.0s default for nothing.
POLL_INTERVAL = "0.2"

# Hard ceiling, not the expected time -- DAGMan's own poll cycle is ~5s on
# top of real matchmaking for any manual-mode flowman node, repeated per
# sequential node transition in a multi-node DAG.
DAG_WAIT_SECONDS = 200
WATCHDOG_SECONDS = 240

pytestmark = [pytest.mark.usefixtures("condor_schedd"), pytest.mark.timeout(WATCHDOG_SECONDS)]

CLUSTER_RE = re.compile(r"submitted to cluster (\d+)")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_flowman_sub(capsys, *, tmp_path, flow_dir, name, mode, extra_args=None):
    """Run the real 'htflow submit htcondor --dry-run' from inside the
    flow's own directory and write its output to tmp_path/<name>.sub, where
    DAGMan finds it -- same top-level-JDL / flows/<name>/jdl/ layout the
    project's own sample uses."""
    args = [
        "submit", "htcondor", "--mode", mode, "--dir", "jdl", "--interval", POLL_INTERVAL,
        "--submit-output", str(tmp_path / f"{name}.debug"),
        "--submit-error", str(tmp_path / f"{name}.debug"),
        "--submit-log", str(tmp_path / f"{name}.log"),
        "--dry-run",
    ]
    if extra_args:
        args += extra_args
    argv = ["htflow", "--no-log", *args]
    with ChangeDir(flow_dir):
        with patch.object(sys, "argv", argv):
            try:
                main()
                code = 0
            except SystemExit as e:
                code = e.code
    sub_text = capsys.readouterr().out
    assert code == 0, f"htflow submit htcondor --dry-run failed (exit {code}): {sub_text}"
    sub_path = tmp_path / f"{name}.sub"
    sub_path.write_text(sub_text)
    return sub_path


def _submit_dag(dag_path: Path, cwd: Path) -> int:
    """Run condor_submit_dag for real -- a native HTCondor CLI tool, not
    htflow's own -- and return the DAGMan manager job's own ClusterId."""
    result = subprocess.run(
        ["condor_submit_dag", str(dag_path)],
        cwd=str(cwd), capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, (
        f"condor_submit_dag failed (exit {result.returncode}):\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    m = CLUSTER_RE.search(result.stdout)
    assert m, f"expected 'submitted to cluster <id>' in condor_submit_dag output, got: {result.stdout!r}"
    return int(m.group(1))


def _wait_for_history(condor_schedd, constraint: str, timeout: float, expect: int = 1):
    """Same idiom as test_submit.py's helper of the same name -- poll
    history() until at least `expect` ads matching `constraint` show up."""
    deadline = time.time() + timeout
    ads = []
    while time.time() < deadline:
        ads = list(condor_schedd.history(constraint=constraint, match=-1))
        if len(ads) >= expect:
            return ads
        time.sleep(1)
    raise TimeoutError(f"expected >= {expect} history ad(s) for {constraint!r} within {timeout}s, got {len(ads)}")


def _wait_for_dag(condor_schedd, cluster_id: int, timeout: float):
    """The DAGMan manager job's history ad -- present once it's fully
    finished (every node done, or it gave up and wrote a rescue file)."""
    return _wait_for_history(condor_schedd, f"ClusterId == {cluster_id}", timeout)[0]


def _describe_dag_nodes(condor_schedd, cluster_id: int, tmp_path: Path = None) -> str:
    """Best-effort per-node diagnostic summary, so a CI failure is
    self-diagnosing from the pytest log alone instead of requiring a manual
    download of the failure-diagnostics artifact. When tmp_path is given,
    also tails any failed node's own --submit-output file
    (tmp_path/<node>.debug -- see _generate_flowman_sub) -- HTCondor
    redirects the job's stdout there, which is where htflow's own log
    output for a flowman-node-level failure actually shows up (a HELD job
    never gets this far at all)."""
    try:
        ads = list(condor_schedd.history(
            constraint=f"DAGManJobId == {cluster_id}", match=-1,
            projection=["ClusterId", "ProcId", "Cmd", "ExitCode", "HoldReason", "DAGNodeName"],
        ))
        if not ads:
            return f"  (no per-node history ads found for DAGManJobId == {cluster_id})"
        lines = []
        for ad in ads:
            node = ad.get("DAGNodeName")
            lines.append(
                f"  node={node!r} cluster={ad.get('ClusterId')}.{ad.get('ProcId')} "
                f"cmd={ad.get('Cmd')!r} exit={ad.get('ExitCode')} hold={ad.get('HoldReason')!r}"
            )
            if tmp_path is not None and ad.get("ExitCode") not in (0, None):
                debug_file = tmp_path / f"{node}.debug"
                if debug_file.exists():
                    tail = debug_file.read_text(errors="replace").strip().splitlines()[-40:]
                    lines.append(f"    --- {debug_file.name} (last {len(tail)} line(s)) ---")
                    lines.extend(f"    {line}" for line in tail)
        return "\n".join(lines)
    except Exception as e:
        return f"  (failed to fetch node diagnostics: {e})"


def _assert_dag_success(condor_schedd, cluster_id: int, ad, tmp_path: Path = None) -> None:
    """assert ad['ExitCode'] == 0, attaching a per-node breakdown (and, for
    any failed node, its own debug output) on failure."""
    assert ad["ExitCode"] == 0, (
        f"DAG (cluster {cluster_id}) failed with ExitCode={ad['ExitCode']}:\n"
        f"{_describe_dag_nodes(condor_schedd, cluster_id, tmp_path)}"
    )


def exec_order(exec_log_path):
    """Return list of task IDs in the order they were written to exec_log_path."""
    if not exec_log_path.exists():
        return []
    return [int(line) for line in exec_log_path.read_text().splitlines() if line.strip()]


@contextmanager
def _cleanup_dag(condor_schedd, cluster_id: int):
    """Best-effort removal of everything a DAG run touched, in case the test
    body raises before finishing naturally. Sweeps two layers deep: every
    node DAGMan submitted directly, plus -- for a monitor-mode flowman
    wrapper -- whatever real jobs it in turn submitted (a different
    ClusterId than this top DAGMan cluster)."""
    try:
        yield
    finally:
        try:
            wrapper_ids = [
                ad["ClusterId"] for ad in condor_schedd.query(
                    constraint=f"DAGManJobId == {cluster_id}",
                    projection=["ClusterId"],
                )
            ]
            constraint = f"ClusterId == {cluster_id} || DAGManJobId == {cluster_id}"
            if wrapper_ids:
                constraint += " || " + " || ".join(f"{ATTR_MANAGER_ID} == {wid}" for wid in wrapper_ids)
            result = condor_schedd.act(htcondor2.JobAction.Remove, constraint)
            logging.getLogger(__name__).info("cleanup_dag(cluster=%d): %s", cluster_id, result)
        except Exception as e:
            logging.getLogger(__name__).warning("cleanup_dag(cluster=%d) FAILED: %s", cluster_id, e)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def task_script():
    return Path(__file__).parent / "task.py"


@pytest.fixture(scope="session")
def flaky_script():
    return Path(__file__).parent / "flaky.py"


@pytest.fixture(scope="session")
def cat_script():
    return Path(__file__).parent / "cat_files.py"


@pytest.fixture
def exec_log(tmp_path):
    return tmp_path / "exec.log"


@pytest.fixture(autouse=True)
def reset_logging():
    """setup_logging() adds a fresh FileHandler per invocation -- drop them
    all between tests, same as every other live-Schedd test file."""
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


@pytest.fixture
def make_outer_jdl(tmp_path, task_script):
    """A plain DAG-level node -- a real vanilla-universe HTCondor job
    submitted by DAGMan itself, not by any flowman engine. Writes --id to
    the same shared exec.log every inner node uses, so ordering can be
    asserted across the DAG/flowman boundary."""
    def _make(name, task_id, *, exit_code=0, log="exec.log"):
        lines = [
            f"executable = {sys.executable}",
            f"arguments = {task_script} --id {task_id} --log {log} --exit-code {exit_code}",
            "should_transfer_files = NO",
            f"output = {name}.out",
            f"error = {name}.err",
            f"log = {name}.private.log",
            "queue",
        ]
        p = tmp_path / f"{name}.sub"
        p.write_text("\n".join(lines) + "\n")
        return p
    return _make


@pytest.fixture
def make_inner_manual_jdl(task_script):
    """An inner node for a flowman(--mode manual) sub-flow -- ManualEngine
    spawns this as a subprocess, so it needs nothing HTCondor-specific, just
    executable/arguments and the transfer_*_files dependency declarations.
    --log defaults to '../exec.log' since the wrapper's own initialdir is
    the flow's directory, one level below tmp_path."""
    def _make(flow_dir, name, task_id, *, exit_code=0, inputs=None, outputs=None, log="../exec.log"):
        lines = [
            f"executable = {sys.executable}",
            f"arguments = {task_script} --id {task_id} --log {log} --exit-code {exit_code}",
        ]
        if inputs:
            lines.append(f"transfer_input_files = {','.join(inputs)}")
        if outputs:
            lines.append(f"transfer_output_files = {','.join(outputs)}")
        lines.append("queue")
        jdl_dir = flow_dir / "jdl"
        jdl_dir.mkdir(parents=True, exist_ok=True)
        p = jdl_dir / f"{name}.sub"
        p.write_text("\n".join(lines) + "\n")
        return p
    return _make


@pytest.fixture
def make_inner_flaky_jdl(flaky_script):
    """Same shape as make_inner_manual_jdl, but backed by flaky.py -- fails
    its first invocation, succeeds every time after."""
    def _make(flow_dir, name, task_id, *, marker, inputs=None, outputs=None, log="../exec.log"):
        lines = [
            f"executable = {sys.executable}",
            f"arguments = {flaky_script} --id {task_id} --log {log} --marker {marker}",
        ]
        if inputs:
            lines.append(f"transfer_input_files = {','.join(inputs)}")
        if outputs:
            lines.append(f"transfer_output_files = {','.join(outputs)}")
        lines.append("queue")
        jdl_dir = flow_dir / "jdl"
        jdl_dir.mkdir(parents=True, exist_ok=True)
        p = jdl_dir / f"{name}.sub"
        p.write_text("\n".join(lines) + "\n")
        return p
    return _make


@pytest.fixture
def make_inner_monitor_jdl(task_script):
    """An inner node for a flowman(--mode monitor) sub-flow -- MonitorEngine
    really submits this, so it needs the same shape as test_monitor.py's
    make_condor_jdl (universe = local, a held-job safety net)."""
    def _make(flow_dir, name, task_id, *, exit_code=0, inputs=None, outputs=None, log="../exec.log"):
        lines = [
            "universe = local",
            f"executable = {sys.executable}",
            f"arguments = {task_script} --id {task_id} --log {log} --exit-code {exit_code}",
            f"log = {name}.private.log",
            f"output = {name}.out",
            f"error = {name}.err",
            "periodic_remove = JobStatus == 5",
        ]
        if inputs:
            lines.append(f"transfer_input_files = {','.join(inputs)}")
        if outputs:
            lines.append(f"transfer_output_files = {','.join(outputs)}")
        lines.append("queue")
        jdl_dir = flow_dir / "jdl"
        jdl_dir.mkdir(parents=True, exist_ok=True)
        p = jdl_dir / f"{name}.sub"
        p.write_text("\n".join(lines) + "\n")
        return p
    return _make


@pytest.fixture
def write_dag(tmp_path):
    def _write(name, lines):
        p = tmp_path / f"{name}.dag"
        p.write_text("\n".join(lines) + "\n")
        return p
    return _write


@pytest.fixture
def manual_mode_getenv(monkeypatch):
    """Test-only, same as test_submit.py's fixture of the same name:
    --no-shared-fs sets no getenv by design, but this dev pool's own job
    environment still needs PYTHONPATH/CONDOR_CONFIG to find htflow."""
    original = submit_htcondor._vanilla_universe_defaults

    def patched(args, df):
        desc = original(args, df)
        desc.setdefault("getenv", "PATH,PYTHONPATH,CONDOR_CONFIG")
        return desc

    monkeypatch.setitem(submit_htcondor.MODE_DEFAULTS, "manual", patched)


# Linear DAG, single flowman node, --mode manual

class TestDagFlowmanManual:
    """outer_a -> flow(manual, linear a->b->c) -> outer_b, ordered against
    plain DAG-level jobs on either side."""

    def test_all_success_and_ordering(
        self, capsys, tmp_path, condor_schedd,
        make_outer_jdl, make_inner_manual_jdl, write_dag, exec_log,
    ):
        make_outer_jdl("outer_a", task_id=1)
        make_outer_jdl("outer_b", task_id=5)

        flow_dir = tmp_path / "flow"
        make_inner_manual_jdl(flow_dir, "a", task_id=2, outputs=["a.dne"])
        make_inner_manual_jdl(flow_dir, "b", task_id=3, inputs=["a.dne"], outputs=["b.dne"])
        make_inner_manual_jdl(flow_dir, "c", task_id=4, inputs=["b.dne"])
        _generate_flowman_sub(capsys, tmp_path=tmp_path, flow_dir=flow_dir, name="flow", mode="manual")

        dag = write_dag("dag", [
            "JOB outer_a outer_a.sub",
            "JOB flow flow.sub",
            "JOB outer_b outer_b.sub",
            "PARENT outer_a CHILD flow",
            "PARENT flow CHILD outer_b",
        ])

        cluster_id = _submit_dag(dag, tmp_path)
        with _cleanup_dag(condor_schedd, cluster_id):
            ad = _wait_for_dag(condor_schedd, cluster_id, DAG_WAIT_SECONDS)

        _assert_dag_success(condor_schedd, cluster_id, ad, tmp_path)
        assert exec_order(exec_log) == [1, 2, 3, 4, 5]


# Linear DAG, single flowman node, --mode monitor

class TestDagFlowmanMonitor:
    """Same shape as TestDagFlowmanManual, but MonitorEngine really submits
    its inner node as a second real HTCondor job."""

    def test_all_success_and_ordering(
        self, capsys, tmp_path, condor_schedd,
        make_outer_jdl, make_inner_monitor_jdl, write_dag, exec_log,
    ):
        make_outer_jdl("outer_a", task_id=1)
        make_outer_jdl("outer_b", task_id=3)

        flow_dir = tmp_path / "flow"
        make_inner_monitor_jdl(flow_dir, "a", task_id=2)
        _generate_flowman_sub(capsys, tmp_path=tmp_path, flow_dir=flow_dir, name="flow", mode="monitor")

        dag = write_dag("dag", [
            "JOB outer_a outer_a.sub",
            "JOB flow flow.sub",
            "JOB outer_b outer_b.sub",
            "PARENT outer_a CHILD flow",
            "PARENT flow CHILD outer_b",
        ])

        cluster_id = _submit_dag(dag, tmp_path)
        with _cleanup_dag(condor_schedd, cluster_id):
            ad = _wait_for_dag(condor_schedd, cluster_id, DAG_WAIT_SECONDS)

        _assert_dag_success(condor_schedd, cluster_id, ad, tmp_path)
        assert exec_order(exec_log) == [1, 2, 3]

        shared_log = flow_dir / "flowman" / "dataflow.shared.log"
        assert shared_log.exists()
        assert shared_log.stat().st_size > 0


# A flowman node whose own inner flow is a diamond

class TestDagFlowmanNestedDiamond:
    """Proves a genuinely nested dependency graph (fan-out/fan-in inside a
    single flowman node) runs correctly end-to-end under DAGMan."""

    def test_all_success_and_partial_ordering(
        self, capsys, tmp_path, condor_schedd,
        make_inner_manual_jdl, write_dag, exec_log,
    ):
        flow_dir = tmp_path / "flow"
        make_inner_manual_jdl(flow_dir, "top", task_id=1, outputs=["top.dne"])
        make_inner_manual_jdl(flow_dir, "left", task_id=2, inputs=["top.dne"], outputs=["left.dne"])
        make_inner_manual_jdl(flow_dir, "right", task_id=3, inputs=["top.dne"], outputs=["right.dne"])
        make_inner_manual_jdl(flow_dir, "bottom", task_id=4, inputs=["left.dne", "right.dne"])
        _generate_flowman_sub(capsys, tmp_path=tmp_path, flow_dir=flow_dir, name="flow", mode="manual")

        dag = write_dag("dag", ["JOB flow flow.sub"])

        cluster_id = _submit_dag(dag, tmp_path)
        with _cleanup_dag(condor_schedd, cluster_id):
            ad = _wait_for_dag(condor_schedd, cluster_id, DAG_WAIT_SECONDS)

        _assert_dag_success(condor_schedd, cluster_id, ad, tmp_path)
        order = exec_order(exec_log)
        assert order[0] == 1  # top ran first
        assert order[-1] == 4  # bottom ran last
        assert set(order[1:3]) == {2, 3}  # left/right ran in between, either order


# Two independent flowman nodes as DAG siblings (mirrors the alpha/beta sample)

class TestDagFlowmanIndependentSiblings:
    """Two flowman(manual) nodes as DAG siblings -- proves two separate
    flowman/ locks never interfere when DAGMan runs them concurrently.

    Each sibling gets its OWN log file (not the usual shared exec_log) --
    "independent" means no shared resource between them at all, including
    the one both would otherwise contend for. This also sidesteps a real
    Windows-only failure: alpha and beta are two separate HTCondor jobs,
    likely each running under vanilla universe's own per-job low-privilege
    execute account -- a shared file one of them creates can end up with an
    ACL the other's account can't write to. TestDagFlowmanNestedDiamond's
    concurrent writers, by contrast, are subprocesses of one wrapper job's
    own process (same account), and passes reliably."""

    def test_both_succeed_independently(
        self, capsys, tmp_path, condor_schedd,
        make_inner_manual_jdl, write_dag,
    ):
        alpha_dir = tmp_path / "alpha"
        make_inner_manual_jdl(alpha_dir, "a", task_id=1, log="../alpha.exec.log")
        _generate_flowman_sub(capsys, tmp_path=tmp_path, flow_dir=alpha_dir, name="alpha", mode="manual")

        beta_dir = tmp_path / "beta"
        make_inner_manual_jdl(beta_dir, "b", task_id=2, log="../beta.exec.log")
        _generate_flowman_sub(capsys, tmp_path=tmp_path, flow_dir=beta_dir, name="beta", mode="manual")

        dag = write_dag("dag", ["JOB alpha alpha.sub", "JOB beta beta.sub"])

        cluster_id = _submit_dag(dag, tmp_path)
        with _cleanup_dag(condor_schedd, cluster_id):
            ad = _wait_for_dag(condor_schedd, cluster_id, DAG_WAIT_SECONDS)

        _assert_dag_success(condor_schedd, cluster_id, ad, tmp_path)
        assert exec_order(tmp_path / "alpha.exec.log") == [1]
        assert exec_order(tmp_path / "beta.exec.log") == [2]
        assert (alpha_dir / "flowman" / "manual.state").exists()
        assert (beta_dir / "flowman" / "manual.state").exists()


# Failure propagation: inner failure -> flowman fails -> DAGMan withholds
# downstream nodes and writes a rescue file

class TestDagFlowmanFailurePropagation:
    """An inner node fails -> the flowman wrapper exits nonzero -> DAGMan
    marks the node Failed and never runs its children."""

    def test_failure_blocks_downstream_and_writes_rescue(
        self, capsys, tmp_path, condor_schedd,
        make_outer_jdl, make_inner_manual_jdl, write_dag, exec_log,
    ):
        flow_dir = tmp_path / "flow"
        make_inner_manual_jdl(flow_dir, "a", task_id=1, exit_code=1)
        _generate_flowman_sub(capsys, tmp_path=tmp_path, flow_dir=flow_dir, name="flow", mode="manual")

        make_outer_jdl("after", task_id=2)

        dag = write_dag("dag", [
            "JOB flow flow.sub",
            "JOB after after.sub",
            "PARENT flow CHILD after",
        ])

        cluster_id = _submit_dag(dag, tmp_path)
        with _cleanup_dag(condor_schedd, cluster_id):
            ad = _wait_for_dag(condor_schedd, cluster_id, DAG_WAIT_SECONDS)

        assert ad["ExitCode"] != 0
        assert exec_order(exec_log) == [1]  # 'after' (id 2) never ran
        assert list(tmp_path.glob("dag.dag.rescue*"))  # write_dag("dag", ...) -> dag.dag


# RETRY-driven recovery: manual.state skips an already-finished inner node

class TestDagFlowmanRetryRecovery:
    """DAGMan RETRY resubmits the flowman node fresh -- manual.state must
    let it skip an inner node that already finished."""

    def test_retry_recovers_without_rerunning_finished_node(
        self, capsys, tmp_path, condor_schedd,
        make_inner_manual_jdl, make_inner_flaky_jdl, write_dag, exec_log,
    ):
        flow_dir = tmp_path / "flow"
        marker = tmp_path / "b.marker"
        make_inner_manual_jdl(flow_dir, "a", task_id=1, outputs=["a.dne"])
        make_inner_flaky_jdl(flow_dir, "b", task_id=2, marker=str(marker), inputs=["a.dne"])
        _generate_flowman_sub(capsys, tmp_path=tmp_path, flow_dir=flow_dir, name="flow", mode="manual")

        dag = write_dag("dag", ["JOB flow flow.sub", "RETRY flow 1"])

        cluster_id = _submit_dag(dag, tmp_path)
        with _cleanup_dag(condor_schedd, cluster_id):
            ad = _wait_for_dag(condor_schedd, cluster_id, DAG_WAIT_SECONDS)

        _assert_dag_success(condor_schedd, cluster_id, ad, tmp_path)
        order = exec_order(exec_log)
        # 'a' (id 1) must appear once -- manual.state skipped it on retry.
        # 'b' (id 2) appears once too, from its succeeding 2nd attempt.
        assert order.count(1) == 1
        assert order.count(2) == 1


# --no-shared-fs flowman node, driven by DAGMan

@pytest.mark.usefixtures("manual_mode_getenv")
class TestDagFlowmanNoSharedFs:
    """The real file-transfer round trip, driven by DAGMan rather than
    'htflow submit htcondor' directly (test_submit.py's case)."""

    def test_root_in_leaf_out_round_trip(self, capsys, tmp_path, condor_schedd, cat_script, write_dag):
        flow_dir = tmp_path / "flow"
        jdl_dir = flow_dir / "jdl"
        jdl_dir.mkdir(parents=True)
        # htflow resolves relative paths against its own cwd at generation
        # time (flow_dir here), so the root file must live there too.
        (flow_dir / "ext.txt").write_text("hello from a DAG\n")
        (jdl_dir / "a.sub").write_text(
            f"executable = {sys.executable}\n"
            f"arguments = {cat_script} out.txt 0 ext.txt\n"
            "transfer_input_files = ext.txt\n"
            "transfer_output_files = out.txt\n"
            "queue\n"
        )
        _generate_flowman_sub(
            capsys, tmp_path=tmp_path, flow_dir=flow_dir, name="flow",
            mode="manual", extra_args=["--no-shared-fs"],
        )

        dag = write_dag("dag", ["JOB flow flow.sub"])

        cluster_id = _submit_dag(dag, tmp_path)
        with _cleanup_dag(condor_schedd, cluster_id):
            ad = _wait_for_dag(condor_schedd, cluster_id, DAG_WAIT_SECONDS)

        _assert_dag_success(condor_schedd, cluster_id, ad, tmp_path)
        # --no-shared-fs sets no initialdir -- output lands wherever the job
        # was actually submitted from (tmp_path), not the flow's own dir.
        assert (tmp_path / "out.txt").read_text() == "hello from a DAG\n"


# Two-layer orchestration: DAGMan -> flowman(monitor) -> further real jobs

class TestDagFlowmanTwoLayerOrchestration:
    """Three chained job identities. TestDagFlowmanMonitor is the
    single-inner-node smoke test; this asserts the full three-tier chain."""

    def test_three_tier_job_chain(
        self, capsys, tmp_path, condor_schedd,
        make_inner_monitor_jdl, write_dag, exec_log,
    ):
        flow_dir = tmp_path / "flow"
        make_inner_monitor_jdl(flow_dir, "a", task_id=1, outputs=["a.dne"])
        make_inner_monitor_jdl(flow_dir, "b", task_id=2, inputs=["a.dne"])
        _generate_flowman_sub(capsys, tmp_path=tmp_path, flow_dir=flow_dir, name="flow", mode="monitor")

        dag = write_dag("dag", ["JOB flow flow.sub"])

        cluster_id = _submit_dag(dag, tmp_path)
        with _cleanup_dag(condor_schedd, cluster_id):
            dag_ad = _wait_for_dag(condor_schedd, cluster_id, DAG_WAIT_SECONDS)

            wrapper_ads = _wait_for_history(condor_schedd, f"DAGManJobId == {cluster_id}", 30)
            assert len(wrapper_ads) == 1
            wrapper_id = wrapper_ads[0]["ClusterId"]

            inner_ads = _wait_for_history(condor_schedd, f"{ATTR_MANAGER_ID} == {wrapper_id}", 30, expect=2)

        _assert_dag_success(condor_schedd, cluster_id, dag_ad, tmp_path)
        assert wrapper_ads[0]["ExitCode"] == 0
        assert len(inner_ads) == 2
        assert all(ad["ExitCode"] == 0 for ad in inner_ads)
        assert exec_order(exec_log) == [1, 2]
