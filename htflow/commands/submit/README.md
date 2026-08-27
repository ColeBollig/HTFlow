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

### `--container IMAGE` (manual mode only)

Sets `container_image` — confirmed against HTCondor's own source (`submit_utils.cpp`) that setting this alongside `universe = vanilla` implicitly makes it a container job; there is no separate `universe = container` needed. Independent of `--no-shared-fs` — use either alone, or combine them.

## Testing this

`--dry-run` (never touches the schedd) is covered by `tests/test_cli.py`'s `TestSubmitHtcondor*` classes. Submitting against a live Schedd is `tests/test_submit.py`, gated by the `condor_schedd` fixture the same way `tests/test_monitor.py` is. Its `manual_mode_getenv` fixture injects a `getenv` for manual-mode tests *only at the test level* — the shipped default stays empty; the fixture just makes the test pool's own job environment deterministic enough to find `htflow`.
