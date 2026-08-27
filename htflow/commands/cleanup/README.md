# `htflow cleanup`

Removes the engine working directory (`flowman/`) created by `execute`/`convert`.

```
htflow cleanup
```

No flags beyond `-h`/`--help`. `cleanup` doesn't build a dataflow, so — unlike every other command here — it doesn't accept `--jdl`/`--dir`/`--job-shapes`/`--relative-to-source`/`--resolve-from`/`--node-name-length` at all (its `add_parser()` doesn't pass `parents=[common_parser]`). That's also why its `run(args)` takes only `args`, not `(df, args)` like the rest — `__main__.py` detects this generically by checking whether `args` even has a `jdl` attribute, not by special-casing `cleanup` by name. See [`docs/commands.md`](../../../docs/commands.md).

## Behavior

- If `flowman/` doesn't exist: prints a message, exits `0` — nothing to do.
- If another engine currently holds `flowman/flowman.lock` (a non-blocking `fcntl` lock): refuses and exits **75** (`EXIT_ENGINE_ACTIVE`).
- Otherwise: removes `flowman/` entirely, including `produced/resolved/` (job-type-shape-resolved submit files written by `execute`/`convert`).
