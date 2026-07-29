# HTFlow

Experimental dataflow runner built specifically to integrate with HTCondor. HTFlow analyses a set of HTCondor submit (JDL) files, infers file-transfer dependencies between them, and either converts the result into an HTCondor DAGMan file or executes the workflow directly.

---

## Quick Start

```bash
pip install -e .

# Convert a set of submit files into a DAGMan file
htflow convert pipeline.dag --jdl fetch.sub process.sub report.sub

# Or scan a directory for submit files
htflow convert pipeline.dag --dir ./jobs/

# Execute the workflow locally (manual engine)
htflow execute manual --jdl fetch.sub process.sub report.sub

# Inspect the dataflow
htflow show files --jdl fetch.sub process.sub report.sub
htflow show types --jdl fetch.sub process.sub report.sub

# Clean up the engine working directory
htflow cleanup
```

---

## Commands

| Command | Description |
|---|---|
| `convert [FILE]` | Write an HTCondor DAGMan file |
| `execute manual` | Run the workflow locally as subprocesses |
| `show files` | Display all tracked files grouped by storage protocol |
| `show types` | List all `JobType` values declared in the submit files |
| `cleanup` | Remove the engine working directory (`flowman/`) |

See [`docs/cli.md`](docs/cli.md) for full flag and exit-code reference.

---

## Input Sources

At least one input source must be supplied to any command that processes JDL files:

| Flag | Description |
|---|---|
| `--jdl PATH [...]` | Explicit submit file paths. May be repeated (`--jdl a.sub --jdl b.sub`). |
| `--dir DIR [...]` / `-d` | Directory to scan for dataflow input sources (top-level only). May be repeated, same as `--jdl`. |

Both flags may be combined; duplicates are deduplicated with a warning.

`--dir` treats **every file** in the scanned directory as a JDL submit file by default — regardless of extension, including files with no extension at all — except for extensions with a specific parser override registered (see [`docs/sources.md`](docs/sources.md)). Each candidate is opened and parsed as an HTCondor submit description; files that fail to parse are skipped with a printed message rather than aborting the scan or being included as bogus nodes.

---

## Execution & Path Resolution

By default, relative paths inside a submit file (`executable`, `transfer_input_files`, etc.) resolve against **HTFlow's own current working directory** — not the directory the JDL file happens to live in:

- `htflow execute manual` spawns each task without changing directories.
- `htflow convert` writes `JOB` lines with each submit file's absolute path and no DAGMan `DIR` clause.

Pass `--relative-to-source` to restore the opposite behavior — each JDL's own directory becomes the base for its relative paths (a per-task `chdir` for `execute`, a DAGMan `DIR <directory>` clause for `convert`). Or pass `--resolve-from PATH` for a fundamentally different approach: instead of changing where anything runs, it rewrites each node's relative `transfer_input_files`/`transfer_output_files` entries in place to absolute paths under `PATH` (URLs and already-absolute entries are left alone). It never touches `executable`/`arguments` and never changes any working directory — `execute`/`convert` behave exactly as the default case once the rewrite is done. The two flags are mutually exclusive. All three behaviors are driven by a single shared `ExecutionConfig` object; see [`docs/config.md`](docs/config.md).

`--relative-to-source` also governs where job-type-shape resolved submit files land — see [Engine Working Directory](#engine-working-directory) below. `--resolve-from` reuses that same resolved-file mechanism (writing a `.resolved` file only when a rewrite actually happened) but does not change its placement.

---

## Documentation

| Document | Contents |
|---|---|
| [`docs/cli.md`](docs/cli.md) | All commands, flags, and exit codes |
| [`docs/commands.md`](docs/commands.md) | CLI command dispatch architecture and how to add a new command |
| [`docs/dataflow.md`](docs/dataflow.md) | `HTCondorDataFlow` API and enforced assumptions |
| [`docs/engines.md`](docs/engines.md) | Engine lifecycle, locking, recovery, and `ManualEngine` |
| [`docs/sources.md`](docs/sources.md) | Input file collection architecture and extension guide |
| [`docs/config.md`](docs/config.md) | `ExecutionConfig` — shared dataflow/execution behavior settings |
| [`docs/dag.md`](docs/dag.md) | DAG data structure |
| [`docs/utils/directory.md`](docs/utils/directory.md) | `ChangeDir` context manager |
| [`docs/utils/naming.md`](docs/utils/naming.md) | `node_name()` — content-addressed DAG node naming |

---

## Engine Working Directory

When `execute` runs, it creates a `flowman/` directory in the current working directory containing:

- `flowman.lock` — exclusive file lock preventing concurrent engine runs
- `manual.state` — completion log used by `Recover()` to resume interrupted runs
- `produced/resolved/` — resolved submit files, created by `execute` **or** `convert` whenever a node's transfer lists change — from a `JobType` shape (see [Job Type Shapes](docs/dataflow.md#job-type-shapes)), from `--resolve-from` rewriting a relative entry to absolute, or both — and `--relative-to-source` isn't set. Only created when there's actually something to resolve.

Run `htflow cleanup` to remove this directory once a workflow is complete.

---

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development setup and running tests.
