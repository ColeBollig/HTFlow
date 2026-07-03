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

## Path Resolution Flag

| Flag | Default | Description |
|---|---|---|
| `--relative-to-source` | off | Resolve relative paths in a submit file against that submit file's own directory instead of HTFlow's current working directory. |

By default (flag off), relative paths inside a submit file (`executable`, `transfer_input_files`, etc.) resolve against HTFlow's own cwd — `execute` spawns tasks without changing directories, and `convert` writes `JOB` lines with each submit file's absolute path and no DAGMan `DIR` clause. Passing `--relative-to-source` restores the opposite: each JDL's own directory becomes the base for its relative paths (a per-task `chdir` for `execute`, a DAGMan `DIR <directory>` clause for `convert`). It also governs where job-type-shape resolved submit files are written — see the `convert` section below. Full details: [`docs/config.md`](config.md).

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
| `--job-shapes PATH` | JSON file containing job type shape definitions |

Prints the path of the written DAG file on success.

If a job-type shape changes a node's transfer lists, `convert` writes a resolved submit file — by default under `flowman/produced/resolved/` (created only if needed), or beside the original JDL under `--relative-to-source`. This is the one case where `convert` touches `flowman/` even though it never acquires the engine lock or writes state.

---

### `htflow execute ENGINE`

Execute the dataflow using the specified engine.

```
htflow execute manual --jdl a.sub b.sub [--interval SECONDS]
htflow execute manual --dir ./jobs/
```

| Argument | Description |
|---|---|
| `ENGINE` | Engine to use — currently only `manual` |
| `--interval SECONDS` | Polling interval in seconds (default: `1.0`) |

By default, each task runs with HTFlow's own current working directory; pass `--relative-to-source` to run each task from its own JDL's directory instead (see [Path Resolution Flag](#path-resolution-flag) above).

The engine acquires an exclusive lock on `flowman/flowman.lock` before starting. If another engine is already running (lock held), the command exits immediately with code **75**.

On `SIGINT` or `SIGTERM`, running subprocesses are killed and the lock is released before exit.

State is written to `flowman/manual.state` as nodes complete, allowing a future run to resume from where the interrupted run left off.

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

Files are grouped by protocol header (e.g. `CEDAR FILES IN DATAFLOW`, `OSDF FILES IN DATAFLOW`). Local files are grouped under `CEDAR`.

---

### `htflow show types`

List all distinct `JobType` values declared across the provided JDL files.

```
htflow show types --jdl a.sub b.sub
htflow show types --dir ./jobs/ --job-shapes shapes.json
```

| Argument | Description |
|---|---|
| `--job-shapes PATH` | JSON file of job type shapes to load alongside (optional) |

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
| `1` | One or more workflow tasks failed during `execute` |
| `2` | Invalid command-line arguments or no JDL files found |
| `75` | Engine already running — lock is held by another process |
| `125` | Setup or configuration failure (bad file, assumption violation, etc.) |
