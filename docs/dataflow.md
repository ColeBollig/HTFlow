# `htflow.dataflow` — HTCondor DataFlow

Analyses a collection of HTCondor submit (JDL) files, infers file-transfer dependencies between them, and produces an HTCondor DAGMan-compatible DAG file.

The module works by reading `transfer_input_files` and `transfer_output_files` from each JDL file and connecting nodes whose output files are consumed as inputs by other nodes.

---

## Assumptions

The dataflow analysis enforces six assumptions about the submit files it processes. Violating any of them raises an `AssumptionError`.

| # | `Assumption` member  | Constraint                                                                 |
|---|----------------------|----------------------------------------------------------------------------|
| 1 | `SINGLE_FILE_SRC`    | Each output file is produced by exactly one node                           |
| 3 | `NO_MACROS`          | No unresolved `$(...)` macro substitutions in file transfer lists          |
| 4 | `NO_DIRECTORIES`     | `output_directory` is not set                                              |
| 5 | `NO_URL`             | No `://` URLs in file lists; `output_destination` is not set               |
| 6 | `NO_REMAPS`          | `transfer_output_remaps` is not set                                        |

> **Note:** Assumption 2 (complete file lists) is enforced implicitly — only the files explicitly listed are considered when building the dependency graph.

---

## `AssumptionError`

Raised when a JDL file violates one of the enforced assumptions.

```python
class AssumptionError(Exception):
    assumption: Assumption   # which assumption was violated
    source: Path             # path to the offending JDL file
```

The exception message follows the format:

```
Assumption <N> Violated: <description> in <path>
```

---

## `HTCondorDataFlow`

### Constructor

```python
HTCondorDataFlow(
    files: List[Union[Path, str]] = [],
    filename: str = "dataflow.dag"
)
```

| Parameter  | Type                     | Description                                          |
|------------|--------------------------|------------------------------------------------------|
| `files`    | `list` of `Path`/`str`   | HTCondor submit files to analyse                     |
| `filename` | `str`                    | Output path for the generated DAGMan file            |

### Properties

| Property    | Type                                                              | Description                                                    |
|-------------|-------------------------------------------------------------------|----------------------------------------------------------------|
| `files`     | `List[Path]`                                                      | Current list of JDL files (also settable)                      |
| `filename`  | `str`                                                             | Output DAG filename (also settable)                            |
| `dag`       | `Optional[dag.Dag]`                                               | The internal DAG, populated after calling `generate()`         |
| `mapping`   | `Dict[Path, Tuple[Optional[int], Optional[List[int]]]]`          | Maps each file to `(source_node_id, [dependent_node_ids])`     |
| `groupings` | `Tuple[List[Path], List[Path], List[Path]]`                       | Files grouped as `(roots, intermediate, leafs)` — see below    |

#### File groupings

After `generate()` is called, `groupings` classifies every tracked file:

- **Roots** — consumed by a node but not produced by any node in the set (external inputs)
- **Intermediate** — produced by one node and consumed by at least one other node
- **Leafs** — produced by a node and not consumed by any other node in the set (terminal outputs)

### Methods

#### `generate() → dag.Dag`

Parses all JDL files, builds the dependency graph, checks for cycles, and returns a deep copy of the resulting `Dag`. Raises `RuntimeError` if a cycle is detected. Calling `generate()` multiple times is safe — internal state is reset each time.

```python
dag = HTCondorDataFlow(files=["a.sub", "b.sub"]).generate()
```

#### `write() → Path`

Runs `generate()` internally and writes an HTCondor DAGMan file to `filename`. Returns the `Path` of the written file.

The generated file contains a `JOB` entry for every node followed by `PARENT … CHILD …` lines for every dependency edge. When a JDL file's parent directory differs from the current working directory, a `DIR <directory>` clause is appended to its `JOB` line so DAGMan submits the job from the correct location. Children that share an identical set of parents are collapsed onto a single `PARENT … CHILD …` line.

```python
path = HTCondorDataFlow(files=["a.sub", "b.sub"], filename="out.dag").write()
```

#### `add(jdl: Union[Path, str])`

Appends a single JDL file to the internal file list.

```python
df = HTCondorDataFlow()
df.add("step1.sub")
df.add("step2.sub")
```

#### `+= jdl`

Operator shorthand for `add()`.

```python
df = HTCondorDataFlow()
df += "step1.sub"
df += "step2.sub"
```

---

## Example

Given three submit files with these transfer declarations:

```
# fetch.sub
transfer_output_files = data.csv

# process.sub
transfer_input_files  = data.csv
transfer_output_files = result.json

# report.sub
transfer_input_files  = result.json
```

The following code produces a DAGMan file wiring them together:

```python
from htflow.dataflow import HTCondorDataFlow

df = HTCondorDataFlow(
    files=["fetch.sub", "process.sub", "report.sub"],
    filename="pipeline.dag",
)
df.write()
```

`pipeline.dag` output:

```
# Automatically written HTCondor DAG file from Dataflow
# Generated: ...
JOB NODE-0 fetch.sub
JOB NODE-1 process.sub
JOB NODE-2 report.sub

# Node relationships determined by dataflow:
PARENT NODE-0 CHILD NODE-1
PARENT NODE-1 CHILD NODE-2
```

---

## Error Handling

```python
from htflow.dataflow import HTCondorDataFlow, AssumptionError, Assumption

try:
    HTCondorDataFlow(files=["job.sub"]).generate()
except AssumptionError as e:
    print(f"Violated assumption: {e.assumption}")
    print(f"Offending file: {e.source}")
except RuntimeError as e:
    print(f"Cycle detected: {e}")
```
