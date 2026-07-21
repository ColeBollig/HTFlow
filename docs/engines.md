# `htflow.engines` — Execution Engines

Engines drive the execution of a dataflow DAG produced by `HTCondorDataFlow`. Each engine implements a common lifecycle interface defined by the abstract `Engine` base class.

---

## `EngineExecutionError`

Raised for known, user-actionable engine failures — specifically when the engine cannot acquire the working-directory lock because another instance is already running. This is distinct from unexpected exceptions, which propagate as-is for debugging.

```python
from htflow.engines.engine import EngineExecutionError
```

The CLI exits with code **75** when this error is raised during engine startup.

---

## `Engine` (Abstract Base Class)

`htflow.engines.engine.Engine` defines the interface that all engines must implement.

### Class Methods

| Method        | Returns  | Description                                                        |
|---------------|----------|--------------------------------------------------------------------|
| `work_dir()`  | `Path`   | Returns the engine working directory path (`flowman/` by default) |
| `lock_file()` | `Path`   | Returns the lock file path (`work_dir() / "flowman.lock"`)        |

These are class methods so tooling (such as `htflow cleanup`) can locate engine paths without instantiating an engine.

### Constructor

```python
Engine(config: Optional[ExecutionConfig] = None)
```

Every engine accepts a shared `ExecutionConfig` (see [`htflow.config`](config.md)) — static configuration controlling dataflow/execution behavior. It is exposed as `self.config` and defaults to `ExecutionConfig()` when omitted.

### Locking

`Engine.__init__()` creates the working directory and stores `self.config`, but does **not** itself acquire the lock. Each concrete engine is responsible for calling `AcquireLock()` explicitly — `ManualEngine.__init__()`, for example, calls `super().__init__(config)` and then `self.AcquireLock()` before its own state setup runs.

`AcquireLock()` uses a **non-blocking** exclusive file lock (`LOCK_EX | LOCK_NB`). If the lock is already held by another process, it raises `EngineExecutionError` immediately rather than blocking. This means attempting to run two engines against the same working directory is an immediate, clean failure rather than a silent wait.

`ReleaseLock()` is called automatically when execution completes normally (inside `Terminate()`) or when `Cleanup()` is invoked from a signal handler.

### Lifecycle Methods

An engine's run loop calls these methods in order:

| Method        | Description                                                               |
|---------------|---------------------------------------------------------------------------|
| `Recover()`   | Restore state from a previous interrupted run before execution begins     |
| `Bootstrap()` | One-time initialisation — enqueue the first batch of ready nodes          |
| `Execute()`   | Launch the next batch of ready nodes                                      |
| `Update()`    | Poll running nodes and transition their state based on exit status        |
| `Terminate()` | Check whether execution is complete; returns an exit code or `None`      |
| `Cleanup()`   | Emergency teardown — kill remaining processes, release resources          |

A minimal run loop:

```python
engine.Recover()
engine.Bootstrap()
while (code := engine.Terminate()) is None:
    engine.Execute()
    engine.Update()
# Terminate() handles lock release on normal exit
# Cleanup() is called only from signal handlers on interruption
```

---

## `ManualEngine`

`htflow.engines.manual.ManualEngine` executes DAG nodes locally as subprocesses, one batch at a time. It reads the `executable` and `arguments` fields from each node's HTCondor submit file and spawns a `subprocess.Popen` process.

### Constructor

```python
ManualEngine(dag: dag.Dag, config: Optional[ExecutionConfig] = None)
```

The `dag` argument must be a `Dag` produced by `HTCondorDataFlow.generate()`, with each node's `internal` field set to the path of its JDL file. The constructor acquires the working-directory lock, then attaches a `ManualDag` tracker to `dag.internal` and wraps every node in a `ManualNode` (passing along `config`). If any post-lock initialisation raises, the lock is released before the exception propagates.

**Raises** `EngineExecutionError` if the lock cannot be acquired (another engine is running).

### Exit Codes

| Constant        | Value | Meaning                              |
|-----------------|-------|--------------------------------------|
| `EXIT_SUCCESS`  | `0`   | All nodes completed successfully     |
| `EXIT_FAILURE`  | `1`   | At least one node failed or was orphaned |

### Method Details

#### `Recover()`

Reads `flowman/manual.state` (if it exists) and marks any previously completed nodes as `SUCCESS` before `Bootstrap()` runs. This allows a workflow interrupted mid-run to resume from where it left off rather than re-executing completed nodes.

The state file contains one line per completed node in the format:

```
*** FINISHED <timestamp> <jdl_path>
```

Paths that contain spaces are handled correctly. If a line references a JDL path not found in the current DAG, `Recover()` raises `RuntimeError`.

#### `Bootstrap()`

Transitions all root nodes (nodes with no parents, and not already marked `SUCCESS` by `Recover()`) from `BLOCKED` to `READY` and enqueues them for execution.

#### `Execute()`

Launches a subprocess for every node currently in the `READY` state. Each task's `executable`/`arguments` are read from its JDL and spawned via `subprocess.Popen`. Directory behavior depends only on `config.relative_to_source` — `config.resolve_from` never changes a task's working directory:

- **`relative_to_source=False` (default, including when `resolve_from` is set)** — no directory change occurs; the task inherits HTFlow's own current working directory, so relative paths in the submit file resolve against wherever HTFlow was invoked from, not against the JDL's own directory. If `resolve_from` was set, any relative `transfer_input_files`/`transfer_output_files` entries were already rewritten to absolute paths during `__resolve()` (see [`docs/dataflow.md`](dataflow.md#job-type-shapes) / [`docs/config.md`](config.md)), so this JDL is whatever `HTCondorDataFlow.generate()` produced — the original file, or a `.resolved` copy.
- **`relative_to_source=True`** — the task is run with the JDL's own parent directory as its working directory (via `ChangeDir`), matching the JDL-colocated behavior.

If a node's process fails to start, it is immediately transitioned to `FAILURE` and its children are `ORPHAN`ed.

#### `Update()`

Polls all `ACTIVE` nodes. For each process that has exited:
- Exit code `0` → `Done()`: node transitions to `SUCCESS`, its completion is appended to `flowman/manual.state`, and children that are now unblocked move to `READY`
- Any other exit code → `Fail()`: node transitions to `FAILURE`, children are `ORPHAN`ed

#### `Terminate() → Optional[int]`

Returns `None` while any nodes are still `READY` or `ACTIVE`. Once all nodes have reached a terminal state, releases the lock and returns:
- `0` (`EXIT_SUCCESS`) — every node succeeded
- `1` (`EXIT_FAILURE`) — at least one node failed or was orphaned

#### `Cleanup()`

Kills and waits on any still-running subprocesses, then releases the lock — but only if at least one node was `ACTIVE` when `Cleanup()` was invoked; if no nodes are active, it returns immediately without releasing the lock. Called only from signal handlers (`SIGINT`/`SIGTERM`) on interruption — not during normal termination.

### Usage

```python
from htflow.dataflow import HTCondorDataFlow
from htflow.engines.manual import ManualEngine

dag = HTCondorDataFlow(files=["a.sub", "b.sub", "c.sub"]).generate()
engine = ManualEngine(dag)

engine.Recover()
engine.Bootstrap()
while (code := engine.Terminate()) is None:
    engine.Execute()
    engine.Update()

exit(code)
```

---

## `ManualNode`

Wraps a `dag.Node` with execution state and process tracking for use by `ManualEngine`. Nodes are not created directly — `ManualEngine.__init__()` creates one per DAG node.

### State Machine

```
BLOCKED ──► READY ──► ACTIVE ──► SUCCESS
   │           │          │
   └───────────┴──────────┴──► FAILURE
                               ORPHAN
```

`Recover()` can also transition `BLOCKED` or `READY` nodes directly to `SUCCESS` when restoring a prior run's state.

| State     | Description                                                           |
|-----------|-----------------------------------------------------------------------|
| `BLOCKED` | Waiting for one or more parent nodes to complete                      |
| `READY`   | All parents have succeeded; queued for execution                      |
| `ACTIVE`  | Subprocess is running                                                 |
| `SUCCESS` | Subprocess exited with code `0` (terminal)                           |
| `FAILURE` | Subprocess exited non-zero, or failed to launch (terminal)           |
| `ORPHAN`  | A parent or ancestor failed; this node will never run (terminal)     |

State transitions are validated — setting an illegal transition raises `RuntimeError`.

### Key Methods

| Method                          | Description                                                                              |
|---------------------------------|------------------------------------------------------------------------------------------|
| `IsBlocked/Ready/Active()`      | State predicates                                                                         |
| `IsFailed/IsSuccess/IsOrphan()` | Terminal state predicates                                                                |
| `IsTerminal()`                  | `True` when in `SUCCESS`, `FAILURE`, or `ORPHAN`                                        |
| `Notify(parent_id)` → `bool`   | Called when a parent succeeds; returns `True` when all parents have reported in          |
| `Execute()`                     | Reads the JDL file, builds the command, and spawns the subprocess                       |
| `Done(dag)`                     | Transitions to `SUCCESS` and notifies children — does not itself write the state file; the caller (`ManualEngine.Update()`) appends the completion line after invoking `Done()` |
| `Fail(dag)`                     | Transitions to `FAILURE` and orphans all children                                       |
| `Orphaned(dag)`                 | Recursively marks this node and all descendants as `ORPHAN`                             |

---

## `ManualDag`

Internal bookkeeping structure attached to `dag.internal` by `ManualEngine`. Tracks which node IDs are currently `READY` and which are `ACTIVE`.

| Property       | Type        | Description                        |
|----------------|-------------|------------------------------------|
| `ready_nodes`  | `Set[int]`  | Node IDs queued for execution      |
| `active_nodes` | `Set[int]`  | Node IDs with a running subprocess |

---

## `MonitorEngine` *(not yet implemented)*

`htflow.engines.engine.MonitorEngine` is a placeholder for a future engine that will submit jobs to HTCondor and monitor them via the `htcondor2` API. It inherits from `Engine` but provides no implementation beyond the abstract interface.
