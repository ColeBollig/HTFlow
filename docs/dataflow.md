# `htflow.dataflow` — HTCondor DataFlow

Analyses a collection of HTCondor submit (JDL) files, infers file-transfer dependencies between them, and produces an HTCondor DAGMan-compatible DAG file.

The module works by reading `transfer_input_files` and `transfer_output_files` from each JDL file and connecting nodes whose output files are consumed as inputs by other nodes.

---

## Assumptions

The dataflow analysis enforces seven assumptions about the submit files it processes. Violating any of them raises an `AssumptionError`.

| # | `Assumption` member  | Constraint                                                                                        |
|---|----------------------|---------------------------------------------------------------------------------------------------|
| 1 | `SINGLE_FILE_SRC`    | Each output file is produced by exactly one node                                                  |
| 2 | `COMPLETE_LIST`      | When a job declares a `JobType`, a matching entry must exist in the `job_shapes` mapping          |
| 3 | `NO_MACROS`          | No unresolved `$(...)` macro substitutions in file transfer lists                                 |
| 4 | `NO_DIRECTORIES`     | `output_directory` is not set                                                                     |
| 5 | `NO_URL`             | Only `osdf://` and `pelican://` URL-scheme files are permitted; all other protocols and `output_destination` are rejected |
| 6 | `NO_REMAPS`          | `transfer_output_remaps` is not set                                                               |
| 7 | `NO_PARENT_TRAVERSAL` | No `transfer_input_files`/`transfer_output_files` entry's first path component is `..` — only enforced when neither `--relative-to-source` nor `--resolve-from` is active |

> **Allowed protocols:** `osdf` and `pelican` URL-scheme files pass assumption 5 and are tracked in the dataflow mapping like any other file. Their keys in `mapping` are stored as plain strings (not `Path` objects) so that the original URL — including any triple-slash prefix such as `osdf:///federation/file.txt` — is preserved exactly as written in the submit file.

> **Assumption 7 scope:** only a *leading* `..` component is rejected (`../file.txt`, `../../file.txt`), not `..` appearing later in a path (`sub/../file.txt` is allowed) or a filename that merely starts with two dots (`..hidden.txt` is allowed). The check is skipped entirely when `--relative-to-source` or `--resolve-from` is set, since both flags mean the user has taken explicit control of path anchoring and may legitimately need to reach outside a JDL's own directory.

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
    filename: str = "dataflow.dag",
    job_shapes: Dict[str, Dict[str, str]] = None,
    config: Optional[ExecutionConfig] = None
)
```

| Parameter    | Type                          | Description                                                        |
|--------------|-------------------------------|--------------------------------------------------------------------|
| `files`      | `list` of `Path`/`str`        | HTCondor submit files to analyse                                   |
| `filename`   | `str`                         | Output path for the generated DAGMan file                          |
| `job_shapes` | `dict[str, dict[str, str]]`   | Job type shape definitions; see [Job Type Shapes](#job-type-shapes). Defaults to `None`, normalized internally to `{}`. |
| `config`     | `Optional[ExecutionConfig]`   | Shared static configuration controlling dataflow/execution behavior; see [`htflow.config`](config.md). Defaults to `ExecutionConfig()` when omitted. |

### Properties

| Property     | Type                                                             | Description                                                    |
|--------------|------------------------------------------------------------------|----------------------------------------------------------------|
| `files`      | `List[Path]`                                                     | Current list of JDL files (also settable)                      |
| `filename`   | `str`                                                            | Output DAG filename (also settable)                            |
| `shapes`     | `Dict[str, Dict[str, str]]`                                      | Job type shape definitions (also settable)                     |
| `config`     | `ExecutionConfig`                                                 | Shared static configuration controlling dataflow/execution behavior (read-only) |
| `types`      | `Set[str]`                                                       | Set of distinct `JobType` values found across all JDL files    |
| `dag`        | `Optional[dag.Dag]`                                              | The internal DAG, populated after calling `generate()`. Each node's `name` is a content-addressed hash (16 hex characters by default) — see [Node naming](#node-naming) — and `dag.internal` holds the resulting `Dict[str, int]` of hashed name → node `id`. |
| `mapping`    | `Dict[Union[Path, str], Tuple[Optional[int], Optional[List[int]]]]` | Maps each file to `(source_node_id, [dependent_node_ids])`. Local files are keyed by `Path`; URL-scheme files (`osdf://`, `pelican://`) are keyed by their original string. |
| `groupings`  | `Tuple[List[Path], List[Path], List[Path]]`                      | Files grouped as `(roots, intermediate, leafs)` — see below    |

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

##### Node naming

Each node's `name` is produced by [`htflow.utils.naming.node_name()`](utils/naming.md): the SHA-256 hex digest of the exact JDL path passed to `files=[...]` (as `str(Path(...))`, before any `--relative-to-source`/`--resolve-from` rewriting), truncated to `config.node_name_length` hex characters — not a sequential `NODE-<i>` label. This makes node names content-addressed and deterministic: the same path always hashes to the same node name across separate `generate()`/`write()` calls, which is what appears in the `JOB`/`PARENT`/`CHILD` lines of a written DAG file (see [Example](#example) below). A node's *name* depends only on its own path, never on the order of `files=[...]` — see [Ordering](utils/naming.md#ordering) for how that interacts with node *ids*, which are order-dependent.

`node_name_length` defaults to `16` (of a possible `4`–`64`; see [`docs/config.md`](config.md#executionconfig)) — long enough that a collision between distinct JDL paths is vanishingly unlikely, while keeping `JOB`/`PARENT`/`CHILD` lines in a written `.dag` file readable. If two distinct paths ever did truncate to the same name, `Dag.AddNode()` raises `RuntimeError` rather than silently merging the nodes.

After `generate()`/`write()` runs, `dag.internal` holds a `Dict[str, int]` mapping each node's hashed name back to its integer node `id`.

```python
>>> df = HTCondorDataFlow(files=["/abs/path/to/fetch.sub"])
>>> d = df.generate()
>>> d[0].name
'5b12f19236ac40e2'
>>> d.internal
{'5b12f19236ac40e2': 0}
```

#### `write() → Path`

Runs the same internal parsing/graph-building logic as `generate()` and writes an HTCondor DAGMan file to `filename`. Returns the `Path` of the written file.

The generated file contains a `JOB` entry for every node followed by `PARENT … CHILD …` lines for every dependency edge. Children that share an identical set of parents are collapsed onto a single `PARENT … CHILD …` line.

The `JOB` line's form depends only on `config.relative_to_source` — `config.resolve_from` never affects it:

- **`relative_to_source=False` (default, including when `resolve_from` is set)** — each `JOB` line references its submit file by absolute path, so JDL files may live anywhere on disk. No `DIR` clause is emitted, so relative paths inside a submit file (e.g. `executable`, `transfer_input_files`) resolve against wherever `condor_submit_dag` is invoked from, not against the JDL's own directory. If `resolve_from` was set, any relative `transfer_input_files`/`transfer_output_files` entries were already rewritten to absolute paths during `__resolve()` — see [Job Type Shapes](#job-type-shapes) below — so this concern doesn't apply to those entries regardless.
- **`relative_to_source=True`** — each `JOB` line references its submit file by bare filename. When the JDL's parent directory differs from the current working directory, a `DIR <directory>` clause is appended so DAGMan submits (and resolves relative paths for) that job from the JDL's own directory.

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

`pipeline.dag` output (with the default `relative_to_source=False`, `JOB` lines always show each submit file's absolute path — see [`write()`](#write--path)). Each `JOB` name is the (default 16-character) SHA-256 hex digest of its JDL path — see [Node naming](#node-naming) above:

```
# Automatically written HTCondor DAG file from Dataflow
# Generated: ...
JOB 5b12f19236ac40e2 /abs/path/to/fetch.sub
JOB 61df75f00a41410c /abs/path/to/process.sub
JOB e3b64db3089fe61d /abs/path/to/report.sub

# Node relationships determined by dataflow:
PARENT 5b12f19236ac40e2 CHILD 61df75f00a41410c
PARENT 61df75f00a41410c CHILD e3b64db3089fe61d
```

---

## Job Type Shapes

Some jobs share a fixed set of input or output files that are implicit to their job type rather than listed in every submit file. The `job_shapes` parameter lets you declare these per-type file lists so they are merged into the dataflow analysis automatically.

### Shape dictionary format

```python
job_shapes = {
    "<JobType value>": {
        "InputFiles":  "<comma-separated file list>",  # optional
        "OutputFiles": "<comma-separated file list>",  # optional
    },
    ...
}
```

Both `InputFiles` and `OutputFiles` are optional within a type entry. Any files listed are merged (deduplicated) with the files already declared in the submit file before dependency edges are built.

### Behaviour during `generate()` / `write()`

1. If a JDL file contains `JobType = <name>`, `<name>` must appear as a key in `job_shapes` — otherwise `AssumptionError(COMPLETE_LIST)` is raised.
2. Files from the matching shape entry are merged into the node's transfer lists.
3. The same URL, macro, and parent-traversal assumptions (NO_URL, NO_MACROS, NO_PARENT_TRAVERSAL) apply to shape file lists.
4. If `config.resolve_from` is set, every entry in the node's (possibly shape-merged) `transfer_input_files`/`transfer_output_files` is then rewritten in place: a URL entry or an already-absolute entry is left untouched, while a relative entry `foo/bar.txt` becomes `str(resolve_from / "foo/bar.txt")`. This runs independently of job type shapes — it applies even to a node with no `JobType` at all.
5. When either step above actually changes a node's transfer lists, the resolved submit description is written to a new `<original-jdl-filename>.resolved` file (e.g. `fetch.sub` → `fetch.sub.resolved`), and the DAG node's internal path is updated to point at it. If neither step changed anything, no `.resolved` file is written and the node keeps its original JDL path. Where the file lands depends on `config.relative_to_source` only — `config.resolve_from` has no effect here:
   - **`relative_to_source=False` (default, including when `resolve_from` is set)** — all resolved files are centralized under `Engine.work_dir() / "produced" / "resolved"` (i.e. `flowman/produced/resolved/`), regardless of where their source JDLs came from. If two or more JDLs needing resolution share the same filename but came from different source directories, each distinct source directory is assigned its own numbered subdirectory (`1/`, `2/`, …, in order of first appearance) so their resolved outputs don't collide; JDLs whose filename doesn't collide with anything are written flat at the top of `resolved/`. Directories are only created as needed — nothing is written if no shape changes any transfer list.
   - **`relative_to_source=True`** — the resolved file is written directly alongside the original JDL, as in earlier versions.

### Example

```python
# fetch.sub declares:  JobType = downloader
# process.sub declares: transfer_input_files = raw.csv
#                        transfer_output_files = result.json
# report.sub declares:  transfer_input_files = result.json

from htflow.dataflow import HTCondorDataFlow

shapes = {
    "downloader": {
        # every downloader job implicitly produces raw.csv
        "OutputFiles": "raw.csv",
    }
}

df = HTCondorDataFlow(
    files=["fetch.sub", "process.sub", "report.sub"],
    filename="pipeline.dag",
    job_shapes=shapes,
)
df.write()
```

This produces the same `PARENT <fetch-hash> CHILD <process-hash>` / `PARENT <process-hash> CHILD <report-hash>` DAG (node names are the SHA-256 hex digests described in [Node naming](#node-naming)) as if `fetch.sub` had `transfer_output_files = raw.csv` written explicitly.

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
