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

CTest test names match the file stems: `test_dag`, `test_dataflow`, `test_change_directory`, `test_cli`, `test_execute`, `test_sources`, `test_naming`, `test_monitor`, `test_submit`. `test_monitor` and `test_submit` both skip gracefully under `ctest` if no HTCondor Schedd is reachable — see below for making that a hard failure instead.

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

### `test_monitor.py` / `test_submit.py` (require a live HTCondor Schedd)

`test_monitor.py` exercises `MonitorEngine` end-to-end against a real, reachable `htcondor2.Schedd()` (e.g. a local `minicondor`) -- unlike the rest of the suite, it submits and watches actual HTCondor jobs. `test_submit.py` does the same for `htflow submit htcondor`: it submits the real wrapper job (`--mode manual` as vanilla universe, `--mode monitor` as local universe) and, for `--mode monitor`, the real inner job the wrapper itself submits, then reads both jobs' actual `ExitCode` back from `condor_schedd.history()` rather than from `htflow submit`'s own process exit code (which only reflects the submission succeeding, not the job's eventual result). If no Schedd is found either file **skips** by default so the rest of the suite still runs; set `HTFLOW_REQUIRE_CONDOR=1` to make a missing Schedd a hard failure instead. `.github/workflows/live-condor-tests.yml` does exactly this: it installs and starts a real `minicondor` on AlmaLinux 10, then runs with the env var set so a broken/missing Schedd fails CI instead of silently skipping the tests:

```sh
HTFLOW_REQUIRE_CONDOR=1 pytest tests/test_monitor.py tests/test_submit.py -q
```

Because each test is mostly waiting on real daemon round trips (submit, schedule, run, report back), it's I/O-bound rather than CPU-bound -- a good fit for parallelizing across processes even without extra cores. Install `pytest-xdist` and run with `-n`:

```sh
pip install pytest-xdist
pytest -n auto tests/test_monitor.py tests/test_submit.py -q
```

`CMakeLists.txt` detects `pytest-xdist` at configure time and adds `-n auto` to these two `ctest` targets automatically when it's installed (falling back to sequential, with a `message(STATUS ...)` note, when it isn't) -- so a plain `ctest` run gets this speedup for free, no flag needed. This parallelizes *within* each file's own `pytest` invocation (worker processes pulling from the same item queue), not by splitting either file into more `ctest` targets.

Each test uses its own isolated `tmp_path` (own `flowman/` lock directory, own batch name), so they don't collide when run concurrently against the same Schedd.

If individual runs still feel slow, `pytest -v --durations=0 tests/test_monitor.py` prints a per-test timing breakdown, which is the fastest way to see whether the cost is spread evenly or concentrated in specific tests.

#### The `live_condor` marker

Any test that uses the `condor_schedd` fixture (directly, or transitively through another fixture that depends on it) is automatically tagged `live_condor` by a `pytest_collection_modifyitems` hook in `conftest.py` -- no need to remember to mark a new test by hand, just use the fixture. This is what a future test file gated on a live Schedd needs to do to be picked up the same way `test_monitor.py` is, including by CI (`.github/workflows/live-condor-tests.yml` selects tests by this marker, not by filename).

Select or exclude by marker directly:

```sh
pytest -m live_condor            # only the live-Schedd tests
pytest -m "not live_condor"      # everything except them (works with no HTCondor installed at all)
```
