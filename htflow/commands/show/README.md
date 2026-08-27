# `htflow show <view>`

Inspects the dataflow without executing or converting it. A second-level plugin system — same idea as `commands/` itself, but discovering "view" modules under `show/` instead of top-level commands.

```
htflow show files --jdl a.sub b.sub
htflow show types --dir ./jobs/
```

| Argument | Description |
|---|---|
| `view` (positional, required) | Which view to run — see below. Both `choices` and each view's `--help` line are generated from the discovered modules, never hand-written. |

Also accepts every `common_parser` flag — `--jdl`/`--dir`, `--job-shapes`, `--relative-to-source`/`--resolve-from`, `--node-name-length`; see [`docs/cli.md`](../../../docs/cli.md) for the full reference.

## Views

| View | Module | Purpose |
|---|---|---|
| `files` | `files.py` | Lists every tracked file grouped by storage protocol (`cedar`, `osdf://`, `pelican://`, ...), with generation (`Gen`) and consumer counts. |
| `types` | `types.py` | Lists the distinct `JobType` values declared across the given JDL files. |

Unlike `submit/`'s backends, a view needs only a `run(df, args)` function — no `add_parser`, since a view is a value of the `view` positional's `choices`, not its own subparser (see [`../submit/README.md`](../submit/README.md) and [`docs/commands.md`](../../../docs/commands.md) for why `submit/` needed the heavier pattern instead). Adding a view is just dropping a new module under `show/` with a one-line docstring (used verbatim as its `--help` line) and a `run(df, args)` — no separate registration.
