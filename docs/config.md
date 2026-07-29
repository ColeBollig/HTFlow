# `htflow.config` — ExecutionConfig

`ExecutionConfig` is shared, static configuration that controls the behavior of a dataflow and its execution — not a container specifically for path handling. A single instance is built once (typically from CLI arguments) and passed to `HTCondorDataFlow`, `Engine` subclasses (e.g. `ManualEngine`), and their nodes, so new behavior-controlling options can be added here instead of threading new parameters through every constructor.

---

## `ExecutionConfig`

```python
from htflow.config import ExecutionConfig

ExecutionConfig(
    relative_to_source: bool = False,
    resolve_from: Optional[Path] = None,
    node_name_length: int = 16,
)
```

| Field                | Type              | Description                                                                 |
|----------------------|-------------------|-------------------------------------------------------------------------------|
| `relative_to_source` | `bool`            | When `True`, relative paths in a submit file resolve against that submit file's own directory instead of the current working directory. See below for what this means per consumer. Defaults to `False`. |
| `resolve_from`        | `Optional[Path]` | When set, relative entries in a submit file's `transfer_input_files`/`transfer_output_files` are rewritten to absolute paths anchored at this directory. Mutually exclusive with `relative_to_source`. Defaults to `None`. |
| `node_name_length`    | `int`            | Length (in hex characters) of the SHA-256-derived node names `HTCondorDataFlow` assigns — see [`docs/dataflow.md`](dataflow.md#node-naming). Must be between `4` and `64` inclusive (`htflow.utils.naming.MIN_NODE_NAME_LENGTH`/`MAX_NODE_NAME_LENGTH`); the constructor raises `ValueError` otherwise. Defaults to `16`. Exposed on the CLI as `--node-name-length`, an intentionally undocumented/hidden flag (suppressed from `--help`) — see [`docs/cli.md`](cli.md). |

`ExecutionConfig` is a frozen `dataclass` — instances are immutable once created. Its `__post_init__` validates `node_name_length` at construction time; the other fields are unvalidated here (the CLI enforces their constraints itself — e.g. `--resolve-from` must be an absolute, existing directory).

`relative_to_source` and `resolve_from` take fundamentally different approaches to the same underlying concern (where a submit file's relative file paths point): `relative_to_source` changes *where the job runs from* (a chdir for `execute`, a DAGMan `DIR` clause for `convert`), while `resolve_from` changes *the submit file's own content* — it never changes any process's or job's working directory. Only one should be set on a given instance — nothing in `ExecutionConfig` itself enforces this (the CLI enforces it via a mutually exclusive argument group; see [`docs/cli.md`](cli.md)).

`resolve_from` only touches `transfer_input_files`/`transfer_output_files`. It does not touch `executable` or `arguments`, does not affect `--jdl`/`--dir`/`--job-shapes` input discovery, and does not change where `Engine.work_dir()` (`flowman/`) itself lives.

### Effect of `resolve_from`

`resolve_from` is applied during `HTCondorDataFlow.__resolve()`, after any job-type-shape merge (see [`docs/dataflow.md`](dataflow.md#job-type-shapes)): each entry in the node's (possibly shape-merged) `transfer_input_files`/`transfer_output_files` is rewritten in place —

- a URL entry (`osdf://…`, `pelican://…`) is left untouched
- an already-absolute path entry is left untouched
- a relative entry `foo/bar.txt` becomes `str(resolve_from / "foo/bar.txt")`

If anything actually changed (from the shape merge, the `resolve_from` rewrite, or both), a `.resolved` submit file is written — through the exact same pipeline `HTCondorDataFlow.__write_resolved()` uses for job-type shapes, and centralized under `Engine.work_dir() / "produced" / "resolved"` (siloed the same way on filename collisions) exactly as in the default case; `resolve_from` does not change resolved-file placement, and `relative_to_source`'s "write beside the original JDL" behavior is unaffected since the two flags are mutually exclusive. If nothing needed rewriting, no `.resolved` file is produced at all and the node's original JDL path is used as-is.

Because rewriting happens once, up front, at the submit-file-content level, `HTCondorDataFlow.write()`'s `JOB` line and `ManualEngine`/`ManualNode.Execute()`'s spawn behavior are completely unaffected by `resolve_from` — both behave exactly as in the default (`relative_to_source=False`) case: absolute `JOB` line with no `DIR` clause, and the task inherits HTFlow's own current working directory, with no `chdir` performed under any circumstances.

### Usage

```python
from pathlib import Path
from htflow.config import ExecutionConfig
from htflow.dataflow import HTCondorDataFlow
from htflow.engines.manual import ManualEngine

config = ExecutionConfig(resolve_from=Path("/data/run1"))
df = HTCondorDataFlow(files=["a.sub", "b.sub"], config=config)
dag = df.generate()
engine = ManualEngine(dag, config=config)
```

Every consumer defaults to `ExecutionConfig()` when `config` is omitted, so existing code that doesn't pass one keeps the default (`relative_to_source=False`, `resolve_from=None`) behavior.
