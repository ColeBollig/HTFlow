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

Each `ctest` target also carries a `LABELS` property mirroring the pytest markers below, so you can filter by category instead of by name. Since `ctest` targets are whole files, labels are per-file: `test_monitor` and `test_submit` carry `integration;live;condor;slow`, and the other seven carry `unit;fast`:

```sh
ctest -L unit               # the 7 non-Schedd files
ctest -L integration        # only test_monitor and test_submit
ctest -L live               # same set as -L integration here (only these two files touch a live Schedd)
ctest -L condor             # same set again at file granularity (see below for test_cli.py's finer-grained condor tests)
ctest -L slow               # same set again -- real daemon round trips are never sub-10s
ctest -LE live              # everything except the live-Schedd tests
```

`regression` has no file-level presence (it tags specific test *functions* inside otherwise-`unit` files, e.g. `test_dataflow.py`'s `TestEndToEndTopologies`), so `ctest -L regression` always returns nothing -- select it with `pytest -m regression` instead, which has per-test granularity. Same story for `condor` on `test_cli.py`: its four `TestSubmitHtcondor*` classes (dry-run only, never touch a Schedd) are marked `condor` at the pytest level but the file as a whole stays labeled `unit;fast` in CTest, since most of `test_cli.py` isn't HTCondor-specific.

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

---

### Markers

Every test is auto-tagged along four independent axes by a `pytest_collection_modifyitems` hook in `conftest.py` -- no need to remember to mark a new test by hand:

| Axis | Values | Default | Auto-applied when |
|---|---|---|---|
| kind | `unit`, `regression`, `integration` | `unit` | `integration`: uses the `condor_schedd` fixture (directly or transitively). `regression`: never auto-applied -- add `@pytest.mark.regression` by hand to a test written to catch a specific bug from recurring (e.g. `test_dataflow.py`'s `TestEndToEndTopologies`, `test_submit.py::test_inner_job_tagged_with_manager_id`). |
| liveness | `live` | (unset) | uses the `condor_schedd` fixture -- requires a real, reachable HTCondor Schedd. |
| backend | `condor` | (unset) | uses the `condor_schedd` fixture. Also applied by hand to backend tests that don't need a live Schedd -- `test_cli.py`'s four `TestSubmitHtcondor*` classes (dry-run submit-description generation). |
| speed | `fast`, `slow` | `fast` | `slow`: uses the `condor_schedd` fixture -- real daemon round trips are never sub-10s. |

`kind` and `speed` aren't strictly exclusive: a test can be both `regression` and `integration`/`live`/`condor`/`slow` at once (both regression examples above are). `condor` pairs with `live`: every `live` test is `condor` (you can't touch a real Schedd without exercising the backend), but not every `condor` test is `live` -- the dry-run htcondor-submit tests never touch a Schedd.

Select or exclude by marker directly:

```sh
pytest -m live                   # only the live-Schedd tests
pytest -m "not live"             # everything except them (works with no HTCondor installed at all)
pytest -m condor                 # the HTCondor backend -- live Schedd tests plus dry-run submit tests
pytest -m "condor and not live"  # just the dry-run htcondor-submit tests (no Schedd needed)
pytest -m unit                   # fast, isolated tests only
pytest -m regression             # tests written to catch a specific bug from recurring
pytest -m "not slow"             # skip anything that takes 10+ seconds
```

This is what a future test file gated on a live Schedd needs to do to be picked up the same way `test_monitor.py` is, including by CI (`.github/workflows/live-condor-tests.yml` selects tests by the `live` marker, not by filename).
