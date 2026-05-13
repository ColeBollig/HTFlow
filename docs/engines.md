# `htflow.engines` — Execution Engines

Engines drive the execution of a dataflow DAG produced by `HTCondorDataFlow`. Each engine implements a common lifecycle interface defined by the abstract `Engine` base class.

---

## `Engine` (Abstract Base Class)

`htflow.engines.engine.Engine` defines the interface that all engines must implement.

### Lifecycle Methods

An engine's run loop is expected to call these methods in order:

| Method        | Description                                                               |
|---------------|---------------------------------------------------------------------------|
| `Bootstrap()` | One-time initialisation before execution starts                           |
| `Execute()`   | Launch the next batch of ready nodes                                      |
| `Update()`    | Poll running nodes and transition their state based on exit status        |
| `Terminate()` | Check whether execution is complete; returns an exit code or `None`      |
| `Recover()`   | Restore state after an unexpected interruption                            |
| `Cleanup()`   | Final teardown — kill remaining processes, release resources              |

A minimal run loop looks like:

```python
engine.Bootstrap()
while (code := engine.Terminate()) is None:
    engine.Execute()
    engine.Update()
engine.Cleanup()
```

---

## `ManualEngine`

`htflow.engines.manual.ManualEngine` executes DAG nodes locally as subprocesses, one batch at a time. It reads the `executable` and `arguments` fields from each node's HTCondor submit file and spawns a `subprocess.Popen` process.

### Constructor

```python
ManualEngine(dag: dag.Dag)
```

The `dag` argument must be a `Dag` produced by `HTCondorDataFlow.generate()`, with each node's `internal` field set to the path of its JDL file. The constructor wraps every node in a `ManualNode` and attaches a `ManualDag` tracker to `dag.internal`.

### Method Details

#### `Bootstrap()`

Transitions all root nodes (nodes with no parents) from `BLOCKED` to `READY` and enqueues them for execution.

#### `Execute()`

Launches a subprocess for every node currently in the `READY` state. If a node's process fails to start, it is immediately transitioned to `FAILURE` and its children are `ORPHAN`ed.

#### `Update()`

Polls all `ACTIVE` nodes. For each process that has exited:
- Exit code `0` → `Done()`: node transitions to `SUCCESS`, children that are now unblocked move to `READY`
- Any other exit code → `Fail()`: node transitions to `FAILURE`, children are `ORPHAN`ed

#### `Terminate() → Optional[int]`

Returns `None` while any nodes are still `READY` or `ACTIVE`. Once all nodes have reached a terminal state, returns:
- `0` (`EXIT_SUCCESS`) — every node succeeded
- `1` (`EXIT_FAILURE`) — at least one node failed or was orphaned

#### `Recover()`

Not yet implemented (no-op).

#### `Cleanup()`

Kills and waits on any still-running subprocesses.

### Usage

```python
from htflow.dataflow import HTCondorDataFlow
from htflow.engines.manual import ManualEngine

dag = HTCondorDataFlow(files=["a.sub", "b.sub", "c.sub"]).generate()
engine = ManualEngine(dag)

engine.Bootstrap()
while (code := engine.Terminate()) is None:
    engine.Execute()
    engine.Update()
engine.Cleanup()

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

| Method                       | Description                                                                                  |
|------------------------------|----------------------------------------------------------------------------------------------|
| `IsBlocked/Ready/Active()`   | State predicates                                                                             |
| `IsFailed/IsSuccess/IsOrphan()` | Terminal state predicates                                                                 |
| `IsTerminal()`               | `True` when in `SUCCESS`, `FAILURE`, or `ORPHAN`                                            |
| `Notify(parent_id)` → `bool` | Called when a parent succeeds; returns `True` when all parents have reported in             |
| `Execute()`                  | Reads the JDL file, builds the command, and spawns the subprocess                           |
| `Done(dag)`                  | Transitions to `SUCCESS` and notifies children                                              |
| `Fail(dag)`                  | Transitions to `FAILURE` and orphans all children                                           |
| `Orphaned(dag)`              | Recursively marks this node and all descendants as `ORPHAN`                                 |

---

## `ManualDag`

Internal bookkeeping structure attached to `dag.internal` by `ManualEngine`. Tracks which node IDs are currently `READY` and which are `ACTIVE`.

| Property       | Type        | Description                        |
|----------------|-------------|------------------------------------|
| `ready_nodes`  | `Set[int]`  | Node IDs queued for execution      |
| `active_nodes` | `Set[int]`  | Node IDs with a running subprocess |

`ManualDag += node` enqueues a `dag.Node` into `ready_nodes`.

---

## `MonitorEngine` *(not yet implemented)*

`htflow.engines.engine.MonitorEngine` is a placeholder for a future engine that will submit jobs to HTCondor and monitor them via the `htcondor2` API. It inherits from `Engine` but provides no implementation beyond the abstract interface.
