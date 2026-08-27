# `htflow` — Command Line Interface

## Synopsis

```
htflow [--log-level LEVEL | --no-log] [--log-file PATH] <command> [options]
```

---

## Global Flags

These flags apply to every command and must appear **before** the command name.

| Flag | Default | Description |
|---|---|---|
| `--log-level LEVEL` | `INFO` | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `--no-log` | off | Disable all logging output entirely (mutually exclusive with `--log-level`) |
| `--log-file PATH` | stdout | Write log output to a file instead of stdout |

---

## Input Source Flags

Most commands require at least one input source. At least one of the following flags must be provided, and together they must resolve to at least one JDL file. Failure to satisfy either condition exits with code **2**.

| Flag | Description |
|---|---|
| `--jdl PATH [PATH ...]` | One or more explicit HTCondor submit files. May be repeated (`--jdl a.sub --jdl b.sub`); values across all occurrences are combined. |
| `--dir DIR [DIR ...]` / `--directory` / `-d` | One or more directories to scan for dataflow input sources (top-level only). May be repeated, same as `--jdl`. |

If the same JDL file is discovered through more than one source, it is included once and a warning is logged.

`--dir` treats every file in the scanned directory as a JDL submit file by default (regardless of extension, including files with no extension), except for extensions with a specific parser override registered — see [`htflow.sources`](sources.md). Files that fail to parse as an HTCondor submit description are skipped with a printed message rather than aborting the scan.

---

## Path Resolution Flags

`--relative-to-source` and `--resolve-from` are mutually exclusive — passing both exits with code **2**. They take fundamentally different approaches: `--relative-to-source` changes *where a job runs from*; `--resolve-from` rewrites *the submit file's own content* and never changes any working directory.

| Flag | Default | Description |
|---|---|---|
| `--relative-to-source` | off | Resolve relative paths in a submit file against that submit file's own directory instead of HTFlow's current working directory. |
| `--resolve-from PATH` | none | Rewrite relative `transfer_input_files`/`transfer_output_files` entries in a submit file to absolute paths anchored at `PATH`. `PATH` must be an absolute path to an existing directory, or the command exits with code **2**. |

By default (neither flag set), relative paths inside a submit file (`executable`, `transfer_input_files`, etc.) resolve against HTFlow's own cwd — `execute` spawns tasks without changing directories, and `convert` writes `JOB` lines with each submit file's absolute path and no DAGMan `DIR` clause.

Passing `--relative-to-source` restores the opposite: each JDL's own directory becomes the base for its relative paths (a per-task `chdir` for `execute`, a DAGMan `DIR <directory>` clause for `convert`). It also governs where job-type-shape resolved submit files are written — see the `convert` section below.

Passing `--resolve-from PATH` instead rewrites each node's `transfer_input_files`/`transfer_output_files` entries in place: relative, non-URL entries become absolute paths under `PATH`; URLs and already-absolute entries are left untouched. This produces a `.resolved` submit file exactly like job-type-shape resolution does (only when something actually changed), reusing the same centralized placement under `flowman/produced/resolved/` — `--resolve-from` does not change that placement. It never touches `executable` or `arguments`, never changes any process's working directory, and has no effect on `--jdl`/`--dir`/`--job-shapes` input discovery. Once the rewrite is done, `execute` and `convert` behave exactly as in the default case (no chdir, no `DIR` clause).

Full details: [`docs/config.md`](config.md).

---

## Job Type Shapes Flag

| Flag | Default | Description |
|---|---|---|
| `--job-shapes PATH` | none | JSON file containing job type shape definitions (see [`docs/dataflow.md`](dataflow.md#job-type-shapes)). |

This is a shared flag accepted by `convert`, `execute`, and `show` (both the `files` and `types` views) — every command that builds a dataflow. It is not accepted by `cleanup`, which never builds a dataflow.

---

## Undocumented Flags

| Flag | Default | Description |
|---|---|---|
| `--node-name-length LENGTH` | `16` | Length (hex characters) of the content-addressed node names `HTCondorDataFlow` assigns — see [`docs/dataflow.md`](dataflow.md#node-naming). Must be between `4` and `64`, or the command exits with code **2**. Deliberately suppressed from `--help` output (`argparse.SUPPRESS`) since it's an internal tuning knob, not something most users need to touch. |

Shared by the same commands as `--job-shapes` above.

---

## Commands

### `htflow convert [FILE]`

Convert the dataflow into an HTCondor DAGMan file.

```
htflow convert [FILE] --jdl a.sub b.sub [--job-shapes shapes.json]
htflow convert [FILE] --dir ./jobs/
```

| Argument | Description |
|---|---|
| `FILE` | Output DAG filename (default: `dataflow.dag`) |

Also accepts the shared [`--job-shapes PATH`](#job-type-shapes-flag) flag.

Prints the path of the written DAG file on success.

If a job-type shape or `--resolve-from` changes a node's transfer lists, `convert` writes a resolved submit file — by default under `flowman/produced/resolved/` (created only if needed), or beside the original JDL under `--relative-to-source`. This is the one case where `convert` touches `flowman/` even though it never acquires the engine lock or writes state.

---

### `htflow execute ENGINE`

Execute the dataflow using the specified engine.

```
htflow execute manual --jdl a.sub b.sub [--interval SECONDS]
htflow execute manual --dir ./jobs/
htflow execute monitor --jdl a.sub b.sub [--interval SECONDS]
```

| Argument | Description |
|---|---|
| `ENGINE` | Engine to use — `manual` (local subprocesses) or `monitor` (submits to a local HTCondor Schedd and watches it) — see [`docs/engines.md`](engines.md) |
| `--interval SECONDS` | Polling interval in seconds (default: `1.0`) |

Also accepts the shared [`--job-shapes PATH`](#job-type-shapes-flag) flag.

By default, each task runs with HTFlow's own current working directory; pass `--relative-to-source` to run each task from its own JDL's directory instead. `--resolve-from PATH` never changes the task's working directory — it rewrites relative `transfer_input_files`/`transfer_output_files` entries to absolute paths under `PATH` ahead of time instead (see [Path Resolution Flags](#path-resolution-flags) above).

The engine acquires an exclusive lock on `flowman/flowman.lock` before starting. If another engine is already running (lock held), the command exits immediately with code **75**.

On `SIGINT` or `SIGTERM`, the lock is released before exit — `manual` kills its running subprocesses first; `monitor` removes its submitted jobs from the schedd first.

`manual` writes state to `flowman/manual.state` as nodes complete, allowing a future run to resume from where the interrupted run left off. `monitor` instead watches (and, on restart, replays) the real HTCondor job event log at `flowman/dataflow.shared.log` — every job it submits writes there in addition to whatever `log` its own JDL sets.

---

### `htflow submit BACKEND`

Submit the workflow as a managed job to the given backend, rather than executing it directly in the current process. Each backend has its own subparser with its own flags.

#### `htflow submit htcondor --mode {manual,monitor}`

Submits an HTCondor job that itself runs `htflow execute <mode>` against the same dataflow, instead of running the engine in the current process:

```
htflow submit htcondor --mode manual --jdl a.sub b.sub [--interval SECONDS] [--dry-run]
htflow submit htcondor --mode monitor --dir ./jobs/ [--interval SECONDS] [--dry-run]
```

| Argument | Description |
|---|---|
| `--mode {manual,monitor}` | **Required.** Which engine to run inside the submitted job — `manual` runs as a **vanilla** universe job; `monitor` runs as a **local** universe job (so it runs on the submit machine itself and, via `_CONDOR_JOB_AD`, tags every job it in turn submits with `My.ManagerId` set to its own `ClusterId` — see [`docs/engines.md`](engines.md#monitorengine)). |
| `--interval SECONDS` | Polling interval passed through to the inner `htflow execute` (default: `1.0`) |
| `--dry-run` | Print the generated HTCondor submit description instead of submitting it |

Also accepts the shared [`--job-shapes PATH`](#job-type-shapes-flag) and [Path Resolution Flags](#path-resolution-flags), forwarded to the inner `htflow execute <mode>` invocation exactly as given (all `--jdl`/`--dir`-resolved files are passed through as absolute paths, regardless of `--relative-to-source`/`--resolve-from`, so the submitted job doesn't depend on its own working directory to find them).

The generated submit description resolves the `htflow` executable via `PATH` (`shutil.which`). Everything else about the job's execution environment is assembled per mode (`htflow/commands/submit/htcondor.py`'s `MODE_DEFAULTS`), not uniformly, because the two universes give different guarantees:

- **`monitor`** (`_local_universe_defaults()`): local universe runs on the AP by construction — always the same host as `htflow submit` itself. `should_transfer_files = NO`, `transfer_executable = false`, and `initialdir` set to the submitting directory are simply facts about local universe, not assumptions. `getenv` also includes `CONDOR_CONFIG`, since `MonitorEngine` talks to a live `htcondor2.Schedd()`.
- **`manual`** (`_vanilla_universe_defaults()`): vanilla universe is matched to whatever execute node satisfies `requirements`, with no guarantee it resembles the submit machine. The same `should_transfer_files`/`transfer_executable`/`initialdir` settings are used, but here they're an *assumption* that the pool shares a filesystem and environment with the submit host (true on CHTC's own pools, not guaranteed in general — a non-shared-filesystem pool would need real `transfer_input_files`/container-based delivery instead, which this command doesn't currently do). `ManualEngine` never talks to a live Schedd, so `CONDOR_CONFIG` is left out of `getenv`.

Both modes' `getenv` is a scoped list (not `getenv = true`), loosely mirroring DAGMan's own manager-job `getenv` filter (`src/condor_utils/dagman_utils.cpp`). Prints the submitted cluster id on success.

If `htflow` cannot be found on `PATH`, or the dataflow itself is invalid (same checks as `execute`/`convert`), the command exits with code **125** before ever touching the schedd.

---

### `htflow show files`

Display all files tracked in the dataflow, grouped by storage protocol.

```
htflow show files --jdl a.sub b.sub
htflow show files --dir ./jobs/
```

Output columns:

| Column | Description |
|---|---|
| `Gen` | `T` if the file is produced by a node in this dataflow; `-` if it is an external input |
| `Consumers` | Number of nodes that consume this file as input |
| `File` | File path or URL |

Files are grouped by protocol header (e.g. `CEDAR files in dataflow`, `OSDF files in dataflow` — only the protocol name is uppercased). Local files are grouped under `CEDAR`.

Also accepts the shared [`--job-shapes PATH`](#job-type-shapes-flag) flag.

---

### `htflow show types`

List all distinct `JobType` values declared across the provided JDL files.

```
htflow show types --jdl a.sub b.sub
htflow show types --dir ./jobs/ --job-shapes shapes.json
```

Also accepts the shared [`--job-shapes PATH`](#job-type-shapes-flag) flag.

---

### `htflow cleanup`

Remove the engine working directory (`flowman/`), including any `produced/resolved/` files written by `execute` or `convert`.

```
htflow cleanup
```

No `--jdl` or `--dir` flags are needed or accepted.

If another engine is currently running (lock held), cleanup refuses and exits with code **75**. If `flowman/` does not exist, a message is printed and the command exits successfully.

---

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | One or more workflow tasks failed during `execute`, or `execute` was interrupted via `SIGINT`/`SIGTERM` |
| `2` | Invalid command-line arguments (bad flag values, unknown flags) or no JDL files found |
| `75` | Engine already running — lock is held by another process |
| `125` | Setup or configuration failure — bad file, assumption violation, or `htflow` invoked with no subcommand at all |
