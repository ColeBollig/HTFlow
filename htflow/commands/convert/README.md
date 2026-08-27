# `htflow convert`

Writes the dataflow to an HTCondor DAGMan `.dag` file — the same DAG `execute` would run, but as a file DAGMan itself can run, without htflow in the loop at all.

```
htflow convert [FILE] --jdl a.sub b.sub [--job-shapes shapes.json]
htflow convert [FILE] --dir ./jobs/
```

| Argument | Description |
|---|---|
| `FILE` (optional, positional) | Output DAG filename. Default: `dataflow.dag` |

Also accepts every flag `common_parser` defines — `--jdl`/`--dir`, `--job-shapes`, `--relative-to-source`/`--resolve-from`, `--node-name-length` — identical across every command that builds a dataflow; see [`docs/cli.md`](../../../docs/cli.md) for the full reference rather than duplicating it per command.

## Behavior

Calls `HTCondorDataFlow.write()` and prints the path written.

If a `JobType` shape or `--resolve-from` changes a node's transfer lists, a resolved submit file is written alongside the `.dag` — under `flowman/produced/resolved/` by default, or beside the original JDL under `--relative-to-source`. This is the one case `convert` touches `flowman/` at all — it never acquires the engine lock or writes any other state there. See [`docs/dataflow.md`](../../../docs/dataflow.md#job-type-shapes).
