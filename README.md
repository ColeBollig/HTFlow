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
| `--jdl PATH [...]` | Explicit submit file paths |
| `--dir DIR [...]` / `-d` | Directory to scan for `*.sub` files (top-level) |

Both flags may be combined; duplicates are deduplicated with a warning.

---

## Documentation

| Document | Contents |
|---|---|
| [`docs/cli.md`](docs/cli.md) | All commands, flags, and exit codes |
| [`docs/dataflow.md`](docs/dataflow.md) | `HTCondorDataFlow` API and enforced assumptions |
| [`docs/engines.md`](docs/engines.md) | Engine lifecycle, locking, recovery, and `ManualEngine` |
| [`docs/sources.md`](docs/sources.md) | JDL collection architecture and extension guide |
| [`docs/dag.md`](docs/dag.md) | DAG data structure |
| [`docs/utils/directory.md`](docs/utils/directory.md) | `ChangeDir` context manager |

---

## Engine Working Directory

When `execute` runs, it creates a `flowman/` directory in the current working directory containing:

- `flowman.lock` — exclusive file lock preventing concurrent engine runs
- `manual.state` — completion log used by `Recover()` to resume interrupted runs

Run `htflow cleanup` to remove this directory once a workflow is complete.
