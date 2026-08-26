# `htflow.utils.naming` — Content-addressed naming

Shared, content-addressed naming helper: hashes a path into a short, deterministic name. Used by `HTCondorDataFlow` for DAG node names (see [`docs/dataflow.md`](../dataflow.md#node-naming)), by `htflow.engines.monitor.MonitorEngine` for its default HTCondor batch name, and by the test suite — all derive names from a single implementation instead of duplicating the hashing logic.

---

## Constants

| Name                  | Value | Description                                                        |
|------------------------|-------|----------------------------------------------------------------------|
| `MIN_HASH_LENGTH`      | `4`   | Shortest allowed hash length, in hex characters                     |
| `MAX_HASH_LENGTH`      | `64`  | Longest allowed hash length — the full SHA-256 hex digest            |
| `DEFAULT_HASH_LENGTH`  | `16`  | Default used by `hash_name()` and `ExecutionConfig.node_name_length` |

---

## `hash_name(path: Union[Path, str], length: int = DEFAULT_HASH_LENGTH) → str`

Returns the SHA-256 hex digest of `str(Path(path))`, truncated to `length` hex characters.

- **Content-addressed and deterministic** — the same `path` (as a string, once wrapped in `Path`) always produces the same name, whether hashed once or a thousand times, in this process or a future one. Different paths produce different names for all practical purposes; a truncated-name collision between two genuinely different paths is possible in principle (shorter `length` values make this more likely) but is not silently tolerated — see [Collisions](#collisions) below.
- **Exact-path sensitive** — `hash_name("a.sub")` and `hash_name("/abs/path/a.sub")` differ, even if both resolve to the same file on disk, because the path string is hashed as given, not resolved first. `HTCondorDataFlow` relies on this: it hashes each entry in `files=[...]` exactly as passed, before any `--relative-to-source`/`--resolve-from` rewriting.
- **`Path`/`str` equivalent** — `hash_name(Path("a.sub")) == hash_name("a.sub")`, since both are normalized through `Path(path)` before hashing.

```python
from htflow.utils.naming import hash_name

hash_name("/abs/path/to/fetch.sub")            # '5b12f19236ac40e2' (16 hex chars, the default)
hash_name("/abs/path/to/fetch.sub", length=64)  # full 64-character digest
hash_name("/abs/path/to/fetch.sub", length=4)   # '5b12'
```

Raises `ValueError` if `length` is outside `[MIN_HASH_LENGTH, MAX_HASH_LENGTH]` or is not a plain `int` (a `bool` is rejected even though it's technically an `int` subclass).

### Collisions

Truncating a hash trades collision-resistance for a shorter, more readable name. `Dag.AddNode()` (see [`docs/dag.md`](../dag.md)) already refuses to add two nodes with the same name, raising `RuntimeError`, so a name collision between two distinct JDL paths surfaces immediately as a hard error rather than silently merging two nodes into one. At the default length of 16 hex characters (64 bits of digest), this is astronomically unlikely for any realistic number of JDL files; it becomes a real practical concern only if `node_name_length` is pushed down toward the low end of its allowed range on a very large dataflow.

---

## `validate_hash_length(length: int) → None`

Standalone validation used by both `hash_name()` and `ExecutionConfig.__post_init__` (see [`docs/config.md`](../config.md#executionconfig)), so the two call sites can't drift out of sync on what counts as a valid length. Raises `ValueError` with the same message `hash_name()` would raise; returns `None` (no exception) for any valid length.

```python
from htflow.utils.naming import validate_hash_length

validate_hash_length(16)   # OK, returns None
validate_hash_length(3)    # raises ValueError — below MIN_HASH_LENGTH
validate_hash_length(65)   # raises ValueError — above MAX_HASH_LENGTH
```

---

## Ordering

Node *names* (in the DAG-node sense, via `HTCondorDataFlow`) depend only on the path being hashed — never on the position of that path within `HTCondorDataFlow(files=[...])` or on any other file in the list. Node *ids*, by contrast, are assigned by `Dag.AddNode()` in the order nodes are added — i.e. by each file's position in `files=[...]` — so reordering `files` reorders which id each name maps to, even though the set of names is unchanged:

```python
from htflow.dataflow import HTCondorDataFlow
from htflow.utils.naming import hash_name

forward  = HTCondorDataFlow(files=["a.sub", "b.sub"]).generate()
backward = HTCondorDataFlow(files=["b.sub", "a.sub"]).generate()

# Same name for the same path regardless of position in files=[...]...
forward[0].name == hash_name("a.sub") == backward[1].name  # True
forward[1].name == hash_name("b.sub") == backward[0].name  # True

# ...but which id each name lands on depends on files=[...] order
forward[0].name == backward[0].name  # False
```

`dag.internal` is not involved in any of this — it's an opaque slot each execution engine (`ManualEngine`, `MonitorEngine`, ...) attaches its own bookkeeping to; `HTCondorDataFlow.generate()` never touches it. See [`docs/engines.md`](../engines.md) for what each engine actually stores there.
