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

## `NodeState`

`htflow.engines._internal.NodeState` is an enum of the node execution states shared across every engine's node wrapper:

| State     | Description                                                           |
|-----------|-----------------------------------------------------------------------|
| `BLOCKED` | Waiting for one or more parent nodes to complete                      |
| `READY`   | All parents have succeeded; queued for execution                      |
| `ACTIVE`  | Task is running                                                       |
| `SUCCESS` | Task completed successfully (terminal)                                |
| `FAILURE` | Task failed, or failed to launch (terminal)                           |
| `ORPHAN`  | A parent or ancestor failed; this node will never run (terminal)      |

```
BLOCKED ──► READY ──► ACTIVE ──► SUCCESS
   │           │          │
   └───────────┴──────────┴──► FAILURE
                               ORPHAN
```

State transitions are validated by the `state` setter — setting an illegal transition raises `RuntimeError`.

---

## `NodeInternal` (Abstract Base Class)

`htflow.engines._internal.NodeInternal` wraps a `dag.Node` with the execution-state bookkeeping common to every engine — state transitions, parent/child notification, and failure/orphan propagation. Concrete engines subclass it and implement `Execute()` for their own task-launching mechanism; `ManualNode` and `MonitorNode` (below) are the current subclasses.

### Constructor

```python
NodeInternal(node: dag.Node, config: Optional[ExecutionConfig] = None)
```

Validates that `node` is a `dag.Node`, starts the node in `NodeState.BLOCKED`, and seeds `_waiting_on` from `node.parents`.

### Properties

| Property  | Type              | Description                             |
|-----------|-------------------|------------------------------------------|
| `failure` | `Optional[str]`   | The reason passed to `Fail()`, if any     |
| `jdl`     | `Path`            | This node's original JDL path             |
| `state`   | `NodeState`       | Current state; the setter validates transitions (see [`NodeState`](#nodestate)) |

### Key Methods

| Method                          | Description                                                                              |
|---------------------------------|--------------------------------------------------------------------------------------------|
| `IsBlocked/Ready/Active()`      | State predicates                                                                         |
| `IsFailed/IsSuccess/IsOrphan()` | Terminal state predicates                                                                |
| `IsTerminal()`                  | `True` when in `SUCCESS`, `FAILURE`, or `ORPHAN`                                        |
| `Notify(parent_id)` → `bool`   | Called when a parent succeeds; returns `True` when all parents have reported in          |
| `Done(dag)`                     | Transitions to `SUCCESS` and notifies children, readying any that are now unblocked      |
| `Fail(dag, reason)`             | Transitions to `FAILURE` and orphans all children                                        |
| `Orphaned(dag)`                 | Recursively marks this node and all descendants as `ORPHAN`                             |
| `Execute(**kwargs)`             | **Abstract.** Subclasses implement how the node's task is actually launched; `**kwargs` lets different engines accept different launch-time arguments (`ManualNode.Execute()` takes none, `MonitorNode.Execute()` requires `schedd` plus the engine's submission options — see below) |

---

## `DagInternal` (Abstract Base Class)

`htflow.engines._internal.DagInternal` is the counterpart to `NodeInternal` at the DAG level: the engine-agnostic bookkeeping every engine attaches to `dag.internal` (see [`docs/dataflow.md`](dataflow.md#node-naming) for why `generate()` itself never sets this). It tracks which node ids are ready to run and which are currently active, and defines the one hook concrete engines must implement to say what "ready" means for their own node type.

### Constructor

```python
DagInternal()
```

Initializes `ready_nodes`/`active_nodes` as empty sets.

### Properties

| Property       | Type       | Description                                                  |
|-----------------|------------|----------------------------------------------------------------|
| `ready_nodes`  | `Set[int]` | Node ids queued for execution                                 |
| `active_nodes` | `Set[int]` | Node ids currently running (added by `Execute()`, pruned by `Update()` once a node reaches a terminal state) |

### Methods

| Method               | Description                                                                                       |
|-----------------------|-----------------------------------------------------------------------------------------------------|
| `prepare(node)`      | Adds `node.id` to `ready_nodes`, logs it, then calls the abstract `_prepare(node)` hook             |
| `_prepare(node)`     | **Abstract.** Engine-specific per-node preparation — both current engines just set `node.internal.state = NodeState.READY` |

`Bootstrap()` (on both `ManualEngine` and `MonitorEngine`) calls `dag.internal.prepare(node)` for each blocked root node; `NodeInternal.Done()` calls it again for any child that becomes newly unblocked.

---

## `ManualEngine`

`htflow.engines.manual.ManualEngine` executes DAG nodes locally as subprocesses, one batch at a time. It reads the `executable` and `arguments` fields from each node's HTCondor submit file and spawns a `subprocess.Popen` process.

### Constructor

```python
ManualEngine(dag: dag.Dag, config: Optional[ExecutionConfig] = None)
```

The `dag` argument must be a `Dag` produced by `HTCondorDataFlow.generate()`, with each node's `internal` field set to the path of its JDL file. The constructor acquires the working-directory lock, then attaches a `ManualDag` tracker (see [`DagInternal`](#daginternal-abstract-base-class)) to `dag.internal` and wraps every node in a `ManualNode` (passing along `config`). If any post-lock initialisation raises, the lock is released before the exception propagates.

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

`htflow.engines.manual.ManualNode` subclasses [`NodeInternal`](#nodeinternal-abstract-base-class), adding subprocess tracking for use by `ManualEngine`. Nodes are not created directly — `ManualEngine.__init__()` creates one per DAG node. All state-machine behavior (states, transitions, `Notify`/`Done`/`Fail`/`Orphaned`) is inherited unchanged from `NodeInternal`; `Recover()` can also transition `BLOCKED` or `READY` nodes directly to `SUCCESS` when restoring a prior run's state.

### Properties

| Property  | Type                          | Description                                              |
|-----------|-------------------------------|-----------------------------------------------------------|
| `process` | `Optional[subprocess.Popen]`  | The node's spawned subprocess, once `Execute()` has run   |

### `Execute()`

Reads the JDL file, builds the command from its `executable`/`arguments` fields, spawns the subprocess, and transitions to `NodeState.ACTIVE`. `Done()` does not itself write the state file; the caller (`ManualEngine.Update()`) appends the completion line after invoking `Done()`.

---

## `ManualDag`

`htflow.engines.manual.ManualDag` subclasses [`DagInternal`](#daginternal-abstract-base-class), attached to `dag.internal` by `ManualEngine`. It adds nothing beyond the base class — `_prepare(node)` just sets `node.internal.state = NodeState.READY`; `ready_nodes`/`active_nodes` are inherited unchanged.

---

## `MonitorEngine`

`htflow.engines.monitor.MonitorEngine` executes DAG nodes by submitting their JDLs to a live, local `htcondor2.Schedd()` and watching a shared HTCondor job event log for `SUBMIT`/`JOB_TERMINATED`/`JOB_ABORTED`/`CLUSTER_REMOVE` events, rather than running anything as a local subprocess. It requires a reachable HTCondor Schedd — see `tests/test_monitor.py` for the gating pattern used to skip (or, with `HTFLOW_REQUIRE_CONDOR=1`, fail) when one isn't available.

### Constructor

```python
MonitorEngine(dag: dag.Dag, config: Optional[ExecutionConfig] = None)
```

Same contract as `ManualEngine`: acquires the working-directory lock, attaches a `MonitorDag` tracker to `dag.internal`, wraps every node in a `MonitorNode`, and releases the lock if anything in the `try` block raises. In addition it:

- Creates (`touch`s) and opens `flowman/dataflow.shared.log` as an `htcondor2.JobEventLog` — this is the one file every submitted job's events get written into, regardless of what `log` (if any) each JDL sets for itself (see [Job submission](#job-submission) below). `htcondor2.JobEventLog` requires the file to already exist; nothing else creates it.
- Picks a default HTCondor batch name, `"flowman+" + hash_name(cwd)` (see [`docs/utils/naming.md`](utils/naming.md)) — content-addressed on the current working directory, so re-running from the same directory reuses the same batch name.
- If running itself as an HTCondor job (`_CONDOR_JOB_AD` env var set — e.g. launched by DAGMan), reads its own job ad and switches to `batch_name = f"flowman+{ClusterId}"`, and tags every job it submits with `My.ManagerId = <that ClusterId>` — this is what lets `Cleanup()` target exactly this run's jobs via a `ManagerId` constraint instead of scanning `active_nodes`.

**Raises** `EngineExecutionError` if the lock cannot be acquired (another engine is running).

### Exit Codes

Same as `ManualEngine`: `EXIT_SUCCESS = 0`, `EXIT_FAILURE = 1`.

### Job submission

`MonitorNode.Execute(schedd, **kwargs)` reads the node's JDL, then sets, on top of whatever the JDL itself already specifies:

| Submit key                       | Value                                  | Purpose                                                                 |
|-----------------------------------|-----------------------------------------|---------------------------------------------------------------------------|
| `node_name` (macro) / `My.NodeName` | the node's content-addressed name      | Lets `__process_log_events` map a `SUBMIT`/`CLUSTER_SUBMIT` event back to the node that submitted it |
| `submit_event_notes_attrs`       | `NodeName`                              | Tells the schedd to include `NodeName` in the submit event's `StructuredNotes` — only `DAGNodeName`/`JobBatchName` are included by default |
| `dagman_log`                      | `flowman/dataflow.shared.log`           | HTCondor's own `SUBMIT_KEY_DagmanLogFile` mechanism (the same one DAGMan itself uses) — every submitted job *additionally* writes its events here, on top of whatever `log` the JDL itself sets. This is what lets one `JobEventLog` watch every node regardless of per-JDL logging. |
| `My.ManagerId`                    | this run's own `ClusterId` (if any)     | Only set when `MonitorEngine` is itself running as a DAGMan-launched job — see [Constructor](#constructor-2) above |

After submission, `schedd.reschedule()` is called once per `Execute()` batch (not per node) — `schedd.submit()` does not itself kick the schedd the way the `condor_submit` CLI does, so without this, newly-submitted jobs would sit unprocessed until the schedd's own periodic scheduling cycle (`SCHEDD_INTERVAL`, defaults to 300s) comes around on its own.

### Method Details

#### `Recover()`

Replays every event already in `flowman/dataflow.shared.log` through the same handling `Update()` uses (see [Event processing](#event-processing)), with log messages prefixed `[RECOVERY]`. Unlike `ManualEngine.Recover()` (a custom state-file format), this reuses the real HTCondor event log directly — a job's handle is set from the event itself (`event.cluster`) rather than from a previously-submitted `htcondor2.SubmitResult`.

#### `Bootstrap()`

Same as `ManualEngine.Bootstrap()`: transitions blocked root nodes to `READY` via `dag.internal.prepare()`.

#### `Execute()`

Submits every node in `ready_nodes`, moving each to `active_nodes` on success. A submission failure (raised exception, not a subsequent job-level failure) immediately fails that node and orphans its children, matching `ManualEngine`. See [Job submission](#job-submission) above for what happens per node, and the `reschedule()` note for why it's called once per batch.

#### `Update()`

Reads any new events from the shared job event log and updates node/job state accordingly (see [Event processing](#event-processing)).

#### `Terminate() → Optional[int]`

Same contract as `ManualEngine.Terminate()`: `None` while `ready_nodes`/`active_nodes` are non-empty or any node is non-terminal; otherwise releases the lock and returns `EXIT_SUCCESS`/`EXIT_FAILURE`.

#### `Cleanup()`

Removes this run's jobs from the schedd (`htcondor2.JobAction.Remove`), then releases the lock. The removal constraint is `ManagerId == <ClusterId>` when running as a DAGMan-launched job, or otherwise `member(ClusterId, {...})` built from the cluster ids of every node currently in `active_nodes`. Called only from signal handlers on interruption, like `ManualEngine.Cleanup()`.

### Event processing

`__process_log_events()` (used by both `Recover()` and `Update()`) handles:

| Event                          | Effect                                                                                          |
|----------------------------------|----------------------------------------------------------------------------------------------------|
| `SUBMIT` / `CLUSTER_SUBMIT`     | Maps the event back to its node via `StructuredNotes["NodeName"]`, records the cluster id ↔ node id mapping, marks the node a `factory` node on `CLUSTER_SUBMIT` (late materialization — the event describes the whole cluster, not one proc, and carries `proc == -1`), otherwise increments the node's queued-job count and seeds that proc's exit state as unknown |
| `JOB_TERMINATED`                | Records the proc's exit code (or `-1 * signal number` if it terminated abnormally), decrements the queued count, and checks whether the node is now fully done |
| `JOB_ABORTED`                    | Same as `JOB_TERMINATED`, but records a sentinel `JOB_EXIT_ABORTED` exit code                     |
| `CLUSTER_REMOVE`                 | Only meaningful for factory nodes — signals that late materialization is completely finished; the node's completion check runs even though its per-proc queued count may not have naturally reached zero on its own |

A node is considered done once its queued-job count reaches zero (and, for factory nodes, only once `CLUSTER_REMOVE` has been seen): all-zero exit codes call `Done()`; any nonzero exit code calls `Fail()` with a summary of how many of the node's jobs didn't exit cleanly. Either way, the node id is dropped from `active_nodes`.

**Known limitation:** a job going on hold (`JOB_HELD`) is not one of the handled event types — a held job leaves its node stuck non-terminal indefinitely, since nothing decrements its queued count or marks it done/failed. This is deliberate: the engine itself doesn't assume a held job should be treated as failed or auto-removed (a real workflow's held job should stay held for inspection). `tests/test_monitor.py` works around this for its own safety by adding `periodic_remove = JobStatus == 5` to its test JDLs only — not something `MonitorEngine` does for real submissions.

### Usage

```python
from htflow.dataflow import HTCondorDataFlow
from htflow.engines.monitor import MonitorEngine

dag = HTCondorDataFlow(files=["a.sub", "b.sub", "c.sub"]).generate()
engine = MonitorEngine(dag)

engine.Recover()
engine.Bootstrap()
while (code := engine.Terminate()) is None:
    engine.Execute()
    engine.Update()

exit(code)
```

---

## `MonitorNode`

`htflow.engines.monitor.MonitorNode` subclasses [`NodeInternal`](#nodeinternal-abstract-base-class), adding HTCondor submission-handle and per-proc job-state tracking for use by `MonitorEngine`. Nodes are not created directly — `MonitorEngine.__init__()` creates one per DAG node.

### Properties

| Property   | Type                                     | Description                                                                 |
|-------------|--------------------------------------------|---------------------------------------------------------------------------|
| `handle`   | `Union[int, htcondor2.SubmitResult]`      | The `SubmitResult` from this node's own `Execute()`, or a bare cluster id when set during `Recover()` |
| `factory`  | `bool`                                     | Whether this node materializes more than one job (seen a `CLUSTER_SUBMIT` event) — gates the `CLUSTER_REMOVE`-only completion check |
| `jobs`     | `List[int]`                                | Per-proc exit codes, indexed by HTCondor proc id (`JOB_EXIT_UNKNOWN`/`JOB_EXIT_ABORTED` sentinels for unresolved/aborted procs) |
| `queued`   | `int`                                       | Count of this node's procs that have submitted but not yet exited          |
| `jobid`    | `Optional[int]`                            | This node's HTCondor cluster id, or `None` before submission                |

Also supports `node.internal[proc]`/`node.internal[proc] = exit_code` (`__getitem__`/`__setitem__`) for per-proc exit-code lookup/assignment, `len(node.internal)` for the number of tracked procs, and `job_queued()`/`job_exited()` to adjust `queued`.

### `Execute(schedd, **kwargs)`

Reads the JDL, tags and submits it as described in [Job submission](#job-submission) above, and transitions to `NodeState.ACTIVE`.

---

## `MonitorDag`

`htflow.engines.monitor.MonitorDag` subclasses [`DagInternal`](#daginternal-abstract-base-class), attached to `dag.internal` by `MonitorEngine`. On top of the inherited `ready_nodes`/`active_nodes`, it maintains a HTCondor cluster id ↔ node id mapping (`node_jid_map`), exposed through the same `__getitem__`/`__contains__`/`__setitem__` interface used elsewhere in `htflow`:

| Access                      | Behavior                                                                 |
|-------------------------------|-----------------------------------------------------------------------|
| `dag.internal[node_name: str]` | Looks the name up in the underlying `dag.Dag` and returns its node id (`KeyError` if not found) |
| `dag.internal[cluster_id: int]` | Returns the node id previously recorded for that HTCondor cluster id |
| `dag.internal[cluster_id] = node_id` | Records a new cluster id → node id mapping (set once, on the first `SUBMIT`/`CLUSTER_SUBMIT` event for that cluster) |
| `cluster_id in dag.internal` / `node_name in dag.internal` | Existence check for either key type                        |
