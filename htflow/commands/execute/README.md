# `htflow execute <engine>`

Runs the dataflow directly in the current process, using one of two engines. (To run it as a *submitted* HTCondor job instead, see [`../submit/README.md`](../submit/README.md).)

```
htflow execute manual --jdl a.sub b.sub [--interval SECONDS]
htflow execute monitor --dir ./jobs/ [--interval SECONDS]
```

| Argument | Description |
|---|---|
| `engine` (positional, required) | `manual` — spawn each ready node as a local subprocess. `monitor` — submit each ready node to a local HTCondor Schedd and watch it. |
| `--interval SECONDS` | Polling interval between `Execute()`/`Update()` cycles (default: `1.0`) |

Also accepts every `common_parser` flag — `--jdl`/`--dir`, `--job-shapes`, `--relative-to-source`/`--resolve-from`, `--node-name-length`; see [`docs/cli.md`](../../../docs/cli.md) for the full reference.

## Behavior

`_load_engine(name)` dynamically imports `htflow.engines.<name>` and picks the first concrete `Engine` subclass it finds. Adding a third engine means dropping a new module under `engines/` *and* adding its name to this file's `choices` list — unlike `commands/` itself, engine names aren't auto-discovered. See [`docs/commands.md`](../../../docs/commands.md).

The run loop: acquire `flowman/flowman.lock` (exits **75** if another engine already holds it) → `engine.Recover()` → `engine.Bootstrap()` → repeat `Execute()` / `Update()` / `sleep(--interval)` until `Terminate()` returns an exit code. `SIGINT`/`SIGTERM` release the lock via the engine's own `Cleanup()` first. See [`docs/engines.md`](../../../docs/engines.md) for what `ManualEngine`/`MonitorEngine` actually do.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | every node succeeded |
| `1` | at least one node failed, or the run was interrupted |
| `75` (`EXIT_ENGINE_ACTIVE`) | another engine already holds `flowman/flowman.lock` |
| `125` (`EXIT_SETUP_FAILURE`) | the requested engine name failed to load |
