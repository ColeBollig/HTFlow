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

import os
import re
import sys
import shutil
import pytest
from pathlib import Path

from htflow.utils.naming import hash_name

# htcondor only publishes Linux wheels. On platforms where it isn't installed
# (e.g. macOS CI), inject a minimal stub so that test collection and the
# dataflow logic work correctly without the real package.
try:
    import htcondor2
    _HTCONDOR2_SOURCE = f"REAL package ({htcondor2.__file__})"
    _HTCONDOR2_IS_REAL = True
except ImportError:
    from unittest.mock import MagicMock
    _HTCONDOR2_IS_REAL = False

    class _Submit:
        """Minimal stand-in for htcondor2.Submit that parses key=value pairs."""

        def __init__(self, source):
            self._data = {}
            if isinstance(source, dict):
                for key, value in source.items():
                    self._data[key.lower()] = str(value)
            else:
                for line in source.splitlines():
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        key, _, value = line.partition("=")
                        self._data[key.strip().lower()] = value.strip()

        def get(self, key: str):
            return self._data.get(key.lower())

        def expand(self, key: str):
            return self._data.get(key.lower(), "")

        def __setitem__(self, key: str, value: str):
            self._data[key.lower()] = value

        def __str__(self):
            return "\n".join(f"{k} = {v}" for k, v in self._data.items())

    _mock = MagicMock()
    _mock.Submit = _Submit
    sys.modules["htcondor2"] = _mock
    _HTCONDOR2_SOURCE = "MOCK stub (real htcondor2 package not installed)"

# Bind the module-level name in both branches above (the mock is only
# registered into sys.modules, not this module's namespace).
import htcondor2


def pytest_report_header(config):
    """Print whether tests are running against the real htcondor2 bindings
    or the fallback mock, so this is obvious in any pytest run's output."""
    return f"htcondor2 bindings: {_HTCONDOR2_SOURCE}"


# ---------------------------------------------------------------------------
# HTCondor Schedd availability -- gates tests/test_monitor.py
# ---------------------------------------------------------------------------
#
# Set HTFLOW_REQUIRE_CONDOR=1 (or true/yes/on) to turn "no Schedd found" into
# a hard FAILURE instead of a SKIP. Local development defaults to skipping so
# the suite still runs without HTCondor installed; CI sets the env var so a
# missing Schedd can never silently skip these tests instead of running them.
HTFLOW_REQUIRE_CONDOR = os.environ.get("HTFLOW_REQUIRE_CONDOR", "").strip().lower() in ("1", "true", "yes", "on")

# Fixed, grep-able marker embedded in every Schedd-related skip reason, so it's
# unambiguous (in output, and to the pytest_terminal_summary hook below) which
# skips are "no HTCondor here" versus anything else.
CONDOR_SKIP_MARKER = "HTCONDOR-SKIP"

_condor_schedd_probed = False
_condor_schedd_result = (False, "not probed yet")


def _probe_condor_schedd():
    """Check once per test session whether a live, reachable HTCondor Schedd
    exists on localhost. Cached after the first call. Returns (available, detail)."""
    global _condor_schedd_probed, _condor_schedd_result
    if _condor_schedd_probed:
        return _condor_schedd_result

    _condor_schedd_probed = True

    if not _HTCONDOR2_IS_REAL:
        _condor_schedd_result = (False, "htcondor2 is not installed (using the mock stub)")
        return _condor_schedd_result

    try:
        schedd = htcondor2.Schedd()
        # constraint="false" matches nothing, so this is cheap regardless of
        # queue size -- but it still forces a real round trip to the daemon,
        # which is exactly what proves it's alive and reachable.
        schedd.query(constraint="false", limit=0)
    except Exception as e:
        _condor_schedd_result = (False, f"{type(e).__name__}: {e}")
    else:
        _condor_schedd_result = (True, None)

    return _condor_schedd_result


@pytest.fixture
def condor_schedd():
    """Require a live, reachable HTCondor Schedd on localhost; returns the
    connected htcondor2.Schedd().

    By default, a test using this fixture is SKIPPED when no Schedd is found
    (so the suite still runs on a machine without HTCondor). Set
    HTFLOW_REQUIRE_CONDOR=1 to make a missing Schedd a hard FAILURE instead --
    this is what CI uses to assert these tests actually ran.
    """
    available, detail = _probe_condor_schedd()
    if not available:
        reason = f"{CONDOR_SKIP_MARKER}: no reachable HTCondor Schedd found ({detail})"
        if HTFLOW_REQUIRE_CONDOR:
            pytest.fail(f"{reason} -- HTFLOW_REQUIRE_CONDOR is set, treating this as a failure")
        pytest.skip(reason)

    return htcondor2.Schedd()


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Print a loud, hard-to-miss banner if any HTCondor-Schedd-gated test was
    skipped -- visible in every plain `pytest` run, not just -v/-ra ones."""
    skipped = terminalreporter.stats.get("skipped", [])
    condor_skips = [r for r in skipped if CONDOR_SKIP_MARKER in str(getattr(r, "longrepr", ""))]

    if not condor_skips:
        return

    terminalreporter.write_sep("=", "HTCondor Schedd tests SKIPPED", red=True, bold=True)
    terminalreporter.write_line(
        f"{len(condor_skips)} test(s) requiring a live HTCondor Schedd were SKIPPED "
        f"-- no reachable Schedd was found on this host.",
        red=True, bold=True,
    )
    terminalreporter.write_line(
        "Set HTFLOW_REQUIRE_CONDOR=1 to make this a hard FAILURE instead of a skip "
        "(this is what CI uses to guarantee these tests actually run)."
    )
    terminalreporter.write_sep("=", red=True, bold=True)


@pytest.fixture
def tmp_path(request):
    """Override pytest's built-in tmp_path: use tests/execution/<Class>__<test>/ so
    per-test logs are easy to inspect after a run. The directory is wiped at the
    START of each test (not at teardown) so artifacts are preserved on failure."""
    tests_dir = Path(__file__).parent
    cls  = (request.cls.__name__ + "__") if request.cls else ""
    name = re.sub(r"[^\w]", "_", cls + request.node.name)
    path = tests_dir / "execution" / name
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


@pytest.fixture
def node_name():
    """HTCondorDataFlow.generate() names each node via htflow.utils.naming.hash_name —
    the sha256 hex digest of the exact JDL path it was given, truncated to
    ExecutionConfig.node_name_length hex characters (default: the full 64).
    Tests use this fixture directly instead of hardcoding hashes."""
    return hash_name


@pytest.fixture
def make_sub(tmp_path):
    def _make(name, *, inputs=None, outputs=None, extra=""):
        lines = ["executable = example.sh"]
        if inputs:
            lines.append(f"transfer_input_files = {','.join(inputs)}")
        if outputs:
            lines.append(f"transfer_output_files = {','.join(outputs)}")
        if extra:
            lines.append(extra)
        lines.append("queue")
        p = tmp_path / f"{name}.sub"
        p.write_text("\n".join(lines) + "\n")
        return p
    return _make
