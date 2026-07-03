# Contributing to HTFlow

## Getting Started

```bash
git clone <this repo>
cd HTFlow
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
pip install pytest
```

Some tests exercise the real `htcondor` Python package. It only publishes Linux wheels (no arm64 macOS build), so install it where available:

```bash
pip install -e ".[htcondor]"
```

## Running Tests

From `tests/`:

```bash
mkdir build && cd build
cmake .. && make
ctest
```

Or run `pytest` directly from `tests/` (optionally targeting a single file, e.g. `pytest test_dataflow.py -v`).

When the real `htcondor` package isn't importable, `tests/conftest.py` transparently injects a minimal mock so the suite still runs. Every `pytest` run prints which one was used via a report header line (`htcondor2 bindings: REAL package (...)` or `MOCK stub (...)`) — check this if a test's behavior seems off on a platform without real bindings (e.g. macOS).

See [`tests/README.md`](tests/README.md) for more detail.

## Making Changes

- Keep documentation in `docs/` in sync with any behavior change — each module under `htflow/` has a corresponding doc.
- Add or update tests for anything you change; all suites listed in `tests/CMakeLists.txt` must pass.
- Follow the existing commit style: a short, present-tense summary line, with a body explaining *why* when the change isn't self-evident.
