# `htflow.config` — ExecutionConfig

`ExecutionConfig` is shared, static configuration that controls the behavior of a dataflow and its execution — not a container specifically for path handling. A single instance is built once (typically from CLI arguments) and passed to `HTCondorDataFlow`, `Engine` subclasses (e.g. `ManualEngine`), and their nodes, so new behavior-controlling options can be added here instead of threading new parameters through every constructor. It currently only happens to have one field, `relative_to_source`.

---

## `ExecutionConfig`

```python
from htflow.config import ExecutionConfig

ExecutionConfig(relative_to_source: bool = False)
```

| Field                | Type   | Description                                                                 |
|----------------------|--------|-------------------------------------------------------------------------------|
| `relative_to_source` | `bool` | When `True`, relative paths in a submit file resolve against that submit file's own directory instead of the current working directory. See below for what this means per consumer. Defaults to `False`. |

`ExecutionConfig` is a frozen `dataclass` — instances are immutable once created.

### Effect of `relative_to_source`

| Consumer                              | `False` (default)                                                                 | `True`                                                                                     |
|----------------------------------------|-------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `HTCondorDataFlow.write()`             | `JOB` lines reference the submit file by absolute path; no `DIR` clause is emitted | `JOB` lines reference the submit file by name; a `DIR <directory>` clause is added when the JDL's directory differs from cwd |
| `ManualEngine` / `ManualNode.Execute()` | The spawned task inherits HTFlow's own current working directory                   | The spawned task is run with its JDL's parent directory as its working directory             |

### Usage

```python
from htflow.config import ExecutionConfig
from htflow.dataflow import HTCondorDataFlow
from htflow.engines.manual import ManualEngine

config = ExecutionConfig(relative_to_source=True)
df = HTCondorDataFlow(files=["a.sub", "b.sub"], config=config)
dag = df.generate()
engine = ManualEngine(dag, config=config)
```

Every consumer defaults to `ExecutionConfig()` when `config` is omitted, so existing code that doesn't pass one keeps the default (`relative_to_source=False`) behavior.
