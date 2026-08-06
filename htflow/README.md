# `htflow` Source Layout

This document describes how the `htflow` package itself is organized. For usage docs (CLI flags, commands, examples) see the top-level [`README.md`](../README.md) and [`docs/`](../docs/). This file is for anyone modifying or extending the package.

## Directory Tree

```
htflow/
├── __init__.py         # empty, marks the package
├── __main__.py         # CLI entry point (argparse, dispatch)
├── config.py           # ExecutionConfig — shared static settings
├── dag.py              # generic Node/Dag data structure (no HTCondor knowledge)
├── dataflow.py         # HTCondorDataFlow — JDL parsing → Dag translation
├── exit_codes.py       # named process exit codes
├── commands/           # one subpackage per `htflow <command>`
│   ├── __init__.py      # auto-discovers subcommands, exposes COMMANDS
│   ├── _discovery.py    # generic "plugin" discovery helper (reused by show/)
│   ├── cleanup/         # `htflow cleanup`
│   ├── convert/         # `htflow convert`
│   ├── execute/         # `htflow execute <engine>`
│   └── show/            # `htflow show <view>` (itself a mini plugin system)
│       ├── files.py      # view: files grouped by storage protocol
│       └── types.py      # view: distinct JobType values
├── engines/             # execution backends for `htflow execute`
│   ├── __init__.py
│   ├── engine.py         # abstract Engine base class + locking
│   ├── _internal.py      # shared NodeState/NodeInternal/DagInternal machinery
│   └── manual.py         # ManualEngine — runs nodes as local subprocesses
├── sources/             # resolves --jdl/--dir into a list of JDL file paths
│   ├── __init__.py       # public API: collect_jdl_files, InputError
│   ├── _errors.py
│   ├── _registry.py      # per-extension handler registry (used by from_dir)
│   ├── core.py           # collect_jdl_files() — runs each resolver
│   ├── from_dir.py       # --dir resolver
│   └── from_jdl.py       # --jdl resolver + default extension handler
└── utils/               # small standalone helpers, no cross-package deps
    ├── directory.py      # ChangeDir context manager
    └── naming.py         # node_name() — content-addressed node naming
```

## Top-Level Modules

| Module | Responsibility |
|---|---|
| `__main__.py` | CLI entry point. Builds the top-level `argparse` parser, registers one subparser per command discovered in `commands.COMMANDS`, resolves input files via `sources.collect_jdl_files`, builds `ExecutionConfig` + `HTCondorDataFlow`, and dispatches to the selected command's `run()`. Installed as the `htflow` console script (see `pyproject.toml`'s `[project.scripts]`), and also runnable as `python -m htflow`. |
| `config.py` | `ExecutionConfig` — a frozen dataclass holding settings (`relative_to_source`, `resolve_from`, `node_name_length`) that are threaded through `HTCondorDataFlow`, engines, and nodes, so new behavior flags don't require touching every constructor. |
| `dag.py` | Generic, engine-agnostic DAG: `Node` (id, parent/child sets, an opaque `internal` slot for engine-specific payloads) and `Dag` (add/connect nodes, BFS/DFS `Walk`, `Cycle()` detection). Has no HTCondor-specific logic. |
| `dataflow.py` | The core translation layer. `HTCondorDataFlow` reads a set of HTCondor submit (JDL) files, infers producer→consumer relationships from `transfer_input_files`/`transfer_output_files`, builds a `dag.Dag`, and can `write()` a DAGMan `.dag` file or `generate()` an in-memory DAG for engines. Enforces a documented set of assumptions (see `docs/dataflow.md`) via `AssumptionError`. |
| `exit_codes.py` | Named process exit codes shared across commands/engines (`EXIT_SETUP_FAILURE`, `EXIT_ENGINE_ACTIVE`). |

## `commands/` — CLI Subcommands

Each `htflow <command>` is a subpackage exposing `add_parser(name, subparsers, common_parser)` and `run(...)`. `commands/__init__.py` auto-discovers them via `_discovery.discover()`, which walks the package directory and imports every submodule/subpackage **not** prefixed with `_` — so adding a new top-level command is just adding a new subpackage here with those two functions; no separate registration step.

| Package | Command | Purpose |
|---|---|---|
| `cleanup/` | `htflow cleanup` | Removes the engine working directory (`flowman/`); refuses while an engine holds the lock file. |
| `convert/` | `htflow convert [FILE]` | Calls `HTCondorDataFlow.write()` to emit a DAGMan `.dag` file. |
| `execute/` | `htflow execute <engine>` | Dynamically loads `htflow.engines.<name>` and runs its lifecycle (`Recover → Bootstrap → Execute/Update loop → Terminate`). Engine names are hardcoded in this package's `choices` list — adding an engine means updating both `engines/` and this list. |
| `show/` | `htflow show <view>` | A second-level plugin system: discovers "view" modules under `show/` the same way `commands/` discovers commands. Each view needs only a `run(df, args)` function. |

`show/`'s views:
- **`files.py`** — groups tracked files by storage protocol (`cedar`, `osdf://`, `pelican://`, ...) and prints generation/consumer counts.
- **`types.py`** — prints the distinct `JobType` values found across the given JDL files.

## `engines/` — Execution Backends

| Module | Purpose |
|---|---|
| `engine.py` | Abstract `Engine` base class: `work_dir()`/`lock_file()` (paths under `flowman/`), `AcquireLock()`/`ReleaseLock()` (non-blocking `fcntl` lock so two engines can't run against the same working directory), and the abstract lifecycle (`Bootstrap`, `Execute`, `Update`, `Terminate`, `Recover`, `Cleanup`). |
| `_internal.py` | Shared, engine-agnostic node/DAG execution-state machinery used by concrete engines: `NodeState` enum, abstract `NodeInternal` (validated state transitions, `Notify`/`Fail`/`Done` lifecycle), and abstract `DagInternal` (tracks ready/active node sets via `prepare()`). New engines should build on this rather than reinventing node state tracking. |
| `manual.py` | `ManualEngine` — the only currently working engine. Runs each ready node as a local subprocess (`Popen` on the JDL's `executable`/`arguments`), optionally `chdir`'d via `utils.directory.ChangeDir` when `--relative-to-source` is set, and persists an append-only state log (`flowman/manual.state`) so `Recover()` can resume an interrupted run. |

## `sources/` — Resolving CLI Input

Turns `--jdl`/`--dir` arguments into a flat, de-duplicated list of JDL file paths for `HTCondorDataFlow`.

| Module | Purpose |
|---|---|
| `core.py` | `collect_jdl_files(args)` — runs each active CLI resolver (`from_jdl`, `from_dir`), flattens/de-duplicates results, raises `InputError` if nothing resolves. |
| `from_jdl.py` | Resolver for `--jdl`: returns the explicitly listed paths. Also registers the default per-extension handler used by `from_dir.py`, which validates a scanned file parses as a valid `htcondor2.Submit`. |
| `from_dir.py` | Resolver for `--dir`: scans a directory's top-level files and looks up a handler per extension via `_registry`. |
| `_registry.py` | Pluggable handler registry keyed by file extension (`register`, `set_default_handler`, `handler_for`) — how parser overrides for specific extensions are added (see `docs/sources.md`). |
| `_errors.py` | Defines `InputError`, the one exception this subpackage raises. |

## `utils/` — Standalone Helpers

Small helpers with no dependencies on the rest of the package.

| Module | Purpose |
|---|---|
| `directory.py` | `ChangeDir` — context manager that temporarily `chdir`s (restoring on exit, even on exception), with an `enabled` flag so callers can no-op it conditionally. |
| `naming.py` | `node_name(path, length)` — content-addressed DAG node naming (truncated sha256 digest of the JDL path), plus length validation constants. Configurable via `--node-name-length`/`ExecutionConfig.node_name_length`. |

## Adding New Functionality

- **New CLI command** → new subpackage under `commands/` with `add_parser`/`run`. Auto-discovered, no registration needed.
- **New `show` view** → new module under `commands/show/` with `run(df, args)`. Auto-discovered.
- **New execution engine** → new module under `engines/`, building on `_internal.py`'s `NodeInternal`/`DagInternal`, subclassing `engine.Engine`, and adding its short name to `commands/execute/__init__.py`'s `choices` list (this step is *not* auto-discovered).
- **New input source handler** → register an extension handler via `sources._registry.register()`, following the pattern in `sources/from_jdl.py`.

## Further Reading

Detailed design docs live under [`../docs/`](../docs/), one file per module/subpackage (`docs/cli.md`, `docs/commands.md`, `docs/config.md`, `docs/dag.md`, `docs/dataflow.md`, `docs/engines.md`, `docs/sources.md`, `docs/utils/`).
