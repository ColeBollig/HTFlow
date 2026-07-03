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

CTest test names match the file stems: `test_dag`, `test_dataflow`, `test_change_directory`, `test_cli`, `test_execute`, `test_sources`.

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
