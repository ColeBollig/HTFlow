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

> **Note:** Some tests require the `htcondor` Python package. Install it if it is available for your platform:
> ```sh
> pip install -e ".[htcondor]"
> ```

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

CTest test names match the file stems: `test_dag`, `test_dataflow`, `test_change_directory`.

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
