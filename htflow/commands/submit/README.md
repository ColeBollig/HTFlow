# `htflow/commands/submit` — Submitting an Engine as a Job

`htflow submit <backend>` wraps an engine invocation (`htflow execute <mode>`) as a job submitted to a backend, instead of running the engine directly in the current process. Backends are discovered the same way top-level commands are (see [`../../README.md`](../../README.md)), each with its own real subparser — currently there's one: `htcondor.py`.

This file covers the *conceptual* model and assumptions each mode makes. For the full flag reference see [`docs/cli.md`](../../../docs/cli.md); for `ManualEngine`/`MonitorEngine` themselves see [`docs/engines.md`](../../../docs/engines.md).

## `htflow submit htcondor --mode {manual,monitor}`

The submit description's payload is always `htflow execute <mode>` itself, run via HTCondor's `shell` submit command (not `executable`/`arguments`) — `htflow` is resolved via `PATH` wherever the job actually runs, never baked in as a submit-side path.

`--mode` picks between two fundamentally different deployment models, because the two HTCondor universes behind them give different guarantees.

### `--mode monitor` — local universe

`MonitorEngine` submitted as a **local** universe job, which by construction always runs on the AP — the same machine `htflow submit` itself ran on. That's not an optimization, it's what makes self-submission work at all: the job reads its own `_CONDOR_JOB_AD`, picks up its own `ClusterId`, and tags every node it in turn submits with `My.ManagerId` set to that id — letting `Cleanup()` target exactly this run's jobs instead of scanning state.

Because same-host is guaranteed, `should_transfer_files = NO`, `initialdir` set to the submitting directory, and forwarding `PATH`/`PYTHONPATH`/`CONDOR_CONFIG` (plus a few more, mirroring DAGMan's own manager-job `getenv` filter) are simply facts, not assumptions.

### `--mode manual` — vanilla universe

`ManualEngine` submitted as a **vanilla** universe job, matched to whatever execute node satisfies `requirements` — no guarantee it resembles the submit machine at all. Two ways to run it:

**Default (assumes a shared filesystem)** — `should_transfer_files = NO`, `initialdir` forced to the submitting directory, no `getenv` at all. Zero setup, correct on a shared-filesystem pool (CHTC's own pools included), wrong on a heterogeneous one. The empty `getenv` is a deliberate choice: the target pool is assumed to already put `htflow` (and whatever it needs to import) on the job's own default `PATH` — nothing is inherited from the submitting shell.

**`--no-shared-fs`** — assumes `htflow` is installed on the execute node instead of assuming shared storage, and does real HTCondor file transfer:

| Direction | What moves | Why |
|---|---|---|
| in (`transfer_input_files`) | every `--jdl` file, `--job-shapes` (if given), and the flow's **root** files (external inputs — local or a validated URL) | everything the wrapper job's own `htflow execute manual` needs before it can run anything |
| out (`transfer_output_files`) | the flow's **leaf** files (local ones only — a URL leaf isn't something `transfer_output_files` can publish to) and `flowman/` | final results, plus engine state (`manual.state`) for inspection/recovery |
| never transferred | **intermediate** files | every node runs as a subprocess of this one job, on this one host — an intermediate file only ever needs to exist locally |

Root/leaf/intermediate come from `HTCondorDataFlow.groupings` — the same file-dependency graph `convert`/`execute` already build. A fail-fast collision check (exit 125) catches two distinct source files that would flatten to the same basename once transferred, since HTCondor drops non-relative `transfer_input_files` entries flat into the job's scratch directory.

Every entry going into `transfer_input_files` — each `--jdl` file, `--job-shapes`, and each local root — is resolved to an absolute path before being written into the description, even if it started out relative (a bare `--jdl a.sub`, a `--dir`-discovered file, or a root declared as a plain relative filename in its own JDL). This matters because `transfer_input_files`' own relative-path resolution happens against whatever directory the job is actually *submitted* from, not necessarily the directory `htflow` itself ran in to build the description — those two can differ, e.g. a flowman node generated via `--dry-run` and later driven by DAGMan is submitted from wherever the `.dag` file lives, not from `--dir`'s own directory. Leaving an entry relative silently breaks the job (`Transfer input files failure ... No such file or directory`) the moment it's submitted from anywhere else.

### `--container IMAGE` (manual mode only)

Sets `container_image` — confirmed against HTCondor's own source (`submit_utils.cpp`) that setting this alongside `universe = vanilla` implicitly makes it a container job; there is no separate `universe = container` needed. Independent of `--no-shared-fs` — use either alone, or combine them.

### `--submit-output`/`--submit-error`/`--submit-log` and `--dry-run`

The submitted job's own `output`/`error`/`log` default to `flowman/submit.<mode>.debug`/`.debug`/`.log`, but `--submit-output`/`--submit-error`/`--submit-log` override any of the three independently — useful for pointing them somewhere other than `flowman/`, e.g. when scripting `--dry-run` to generate a `.sub` file ahead of time for something other than `htflow`'s own submission path (a DAGMan `JOB` line, for instance).

Building the submit description never touches disk — `flowman/` (needed as the default log location, and as where `--no-shared-fs`'s `manual.state` comes back via `transfer_output_files`) is only created right before the job is actually handed to the schedd, so `--dry-run` has zero filesystem side effects.

### `-a`/`--append` and `-p`/`--prepend` — raw submit commands

An escape hatch for anything `htflow` itself doesn't know how to set — pool-specific `requirements`, `+WantFlocking`, `request_gpus`, and so on. Both take a `KEY VALUE` pair, are repeatable, and are inserted into the description as plain dict entries (a value is used verbatim, not quoted — a string literal needs its own quotes, e.g. `-a requirements '(OpSys == "LINUX")'`).

The two differ only in *when* they're layered into the dict being built in `_build_submit()`, which is what decides who wins a key collision (later dict writes overwrite earlier ones for the same key):

1. `-p`/`--prepend` values seed the dict first.
2. Every key `htflow` computes itself is applied next — `executable`, `arguments`, `output`/`error`/`log`, then the mode's own `universe`/transfer/`getenv` settings and `container_image` (`MODE_DEFAULTS[mode](args, df)`).
3. `-a`/`--append` values are applied last.

So `--prepend` only takes effect for a key `htflow` doesn't otherwise set — it's the safe one for adding new commands without any risk of silently breaking htflow's own required setup. `--append` always wins, even over `universe`, `batch_name`, or transfer settings — the deliberate "I know what I'm doing" override.

## Testing this

`--dry-run` (never touches the schedd, and — as of the fix above — never touches the filesystem either) is covered by `tests/test_cli.py`'s `TestSubmitHtcondor*` classes, including `TestSubmitHtcondorDryRunNoSideEffects`, `TestSubmitHtcondorLogPaths`, `TestSubmitHtcondorAppendPrepend`, and (the `transfer_input_files` absolute-path fix) `test_dir_input_transfer_paths_are_absolute`. Submitting against a live Schedd is `tests/test_submit.py`, gated by the `condor_schedd` fixture the same way `tests/test_monitor.py` is. Its `manual_mode_getenv` fixture injects a `getenv` for manual-mode tests *only at the test level* — the shipped default stays empty; the fixture just makes the test pool's own job environment deterministic enough to find `htflow`.

A flowman node's submit description generated this way and then actually driven by real DAGMan -- via the htcondor2 API (`Submit.from_dag()` + `Schedd.submit()`), not the `condor_submit_dag` CLI tool -- in several topologies (linear, nested diamond, independent siblings, failure propagation, `RETRY` recovery, `--no-shared-fs`, two-layer `--mode monitor` orchestration) — the scenario that surfaced the `transfer_input_files` bug above — is `tests/test_dagman.py`, gated the same way.
