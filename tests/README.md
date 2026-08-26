# HTFlow Test Suite

## Setup

Create and activate a virtual environment from the project root:

```sh
python3 -m venv .venv
source .venv/bin/activate
```

Install the package in editable mode along with `pytest`:

```sh
pip install -e .
pip install pytest
```

> **Note:** Some tests exercise the real `htcondor` Python package. It only publishes Linux wheels (no arm64 macOS build), so install it where available:
> ```sh
> pip install -e ".[htcondor]"
> ```
> If it isn't importable, `conftest.py` transparently injects a minimal mock so the suite still runs. Every `pytest` run prints which one was used via a report header line (`htcondor2 bindings: REAL package (...)` or `MOCK stub (...)`).

---

## Running Tests

### CTest

From the `tests/` directory:

```sh
mkdir build && cd build
cmake ..
make
ctest
```

Run with verbose output:

```sh
ctest -V
```

Run a specific test by name:

```sh
ctest -R test_change_directory
```

CTest test names match the file stems: `test_dag`, `test_dataflow`, `test_change_directory`, `test_cli`, `test_execute`, `test_sources`, `test_naming`, `test_monitor`. `test_monitor` skips gracefully under `ctest` if no HTCondor Schedd is reachable — see below for making that a hard failure instead.

---

### pytest (manual)

From the `tests/` directory:

```sh
pytest
```

Run a single test file:

```sh
pytest test_change_directory.py -v
```

---

### `test_monitor.py` (requires a live HTCondor Schedd)

`test_monitor.py` exercises `MonitorEngine` end-to-end against a real, reachable `htcondor2.Schedd()` (e.g. a local `minicondor`) -- unlike the rest of the suite, it submits and watches actual HTCondor jobs. If no Schedd is found it **skips** by default so the rest of the suite still runs; set `HTFLOW_REQUIRE_CONDOR=1` to make a missing Schedd a hard failure instead (this is what CI does, to guarantee the tests actually ran rather than silently skipping):

```sh
HTFLOW_REQUIRE_CONDOR=1 pytest tests/test_monitor.py -q
```

Because each test is mostly waiting on real daemon round trips (submit, schedule, run, report back), it's I/O-bound rather than CPU-bound -- a good fit for parallelizing across processes even without extra cores. Install `pytest-xdist` and run with `-n`:

```sh
pip install pytest-xdist
pytest -n auto tests/test_monitor.py -q
```

Each test uses its own isolated `tmp_path` (own `flowman/` lock directory, own batch name), so they don't collide when run concurrently against the same Schedd.

If individual runs still feel slow, `pytest -v --durations=0 tests/test_monitor.py` prints a per-test timing breakdown, which is the fastest way to see whether the cost is spread evenly or concentrated in specific tests.
