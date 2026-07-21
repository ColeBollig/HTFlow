# `htflow.commands` — CLI Command Dispatch

The `commands` package is where every CLI command (`execute`, `convert`, `cleanup`, `show`) lives. Each command gets its own subdirectory named after it, with an `__init__.py` that owns both that command's argparse subparser definition and its execution logic. `htflow/__main__.py` itself only builds the shared parser scaffolding and loops over registered command modules to let them attach themselves — it has no per-command knowledge at all.

---

## Design

A command's name is its file/directory name — nothing declares it explicitly. All discovery goes through one generic helper, `htflow/commands/_discovery.py`:

```python
def discover(path, package, required_attrs=()) -> Dict[str, ModuleType]:
    """Import every module/subpackage directly inside `path`, skipping names
    starting with '_', and return them keyed by name. Raises RuntimeError if
    a discovered module is missing any of `required_attrs` as a callable."""
```

This is generic over nesting level — the same function backs both:

- **Top-level commands**: `htflow/commands/__init__.py` calls `discover(__path__, __name__, required_attrs=("add_parser", "run"))` to find `execute/`, `convert/`, `cleanup/`, `show/`.
- **A command's own subcommands**: `htflow/commands/show/__init__.py` calls `discover(__path__, __name__, required_attrs=("run",))` to find `files.py`/`types.py` — no `add_parser` required, since sub-views aren't separate argparse subparsers, just values of the `view` positional.

A top-level command module only needs to implement:

```python
def add_parser(name, subparsers, common_parser) -> ArgumentParser   # registers this command's subparser
def run(df, args) -> None                                           # executes the command
```

`add_parser` receives its own registered `name` as a parameter (derived from the directory it lives in) rather than declaring a `NAME` constant itself — the directory *is* the source of truth. Discovery validates that both `add_parser` and `run` exist and are callable, raising `RuntimeError` immediately at import time if a command package is missing either (a malformed command fails loud, not silently at dispatch time). Names starting with `_` are skipped, so private helper modules (like `_discovery.py` itself) can live in `htflow/commands/` without being mistaken for a command.

The result is `COMMANDS`, a `{name: module}` dict, and `CMD_TO_FUNCTION`, a `{name: module.run}` dict derived from it. `__main__.py`'s `parse_args()` builds the shared `common_parser` (`--jdl`, `--dir`, `--job-shapes`, `--relative-to-source`, `--resolve-from` — cross-command flags not owned by any single command) and then does:

```python
subparsers = parser.add_subparsers(dest="command")
for name, cmd in commands.COMMANDS.items():
    cmd.add_parser(name, subparsers, common_parser)
```

Dispatch after parsing is a flat lookup: `commands.CMD_TO_FUNCTION[args.command]`. Because discovery sorts by directory name, commands are listed in `htflow --help` alphabetically, not in a hand-curated order.

### `cleanup`'s asymmetric signature

Every command's `run()` takes `(df, args)` — a `HTCondorDataFlow` already constructed from `args.jdl`/`args.job_shapes` by `main()` — **except `cleanup`, which takes `run(args)` only**. `cleanup` doesn't accept `--jdl`/`--dir` at all (its `add_parser()` doesn't pass `parents=[common_parser]`), so there's no dataflow to build. `main()` detects this generically, without knowing "cleanup" by name — any command whose parser omits `common_parser` gets this treatment:

```python
if not hasattr(args, "jdl"):
    action(args)
    return
```

### `show`'s internal sub-dispatch

`show` has two views (`files`, `types`), each a flat sibling module — `htflow/commands/show/files.py` and `htflow/commands/show/types.py` — mirroring how `htflow.sources` holds `from_jdl.py`/`from_dir.py` as flat modules rather than nested subpackages. `show/__init__.py` discovers them the same way `commands/__init__.py` discovers top-level commands:

```python
_VIEWS = discover(__path__, __name__, required_attrs=("run",))

def run(df, args):
    _VIEWS[args.subcmd].run(df, args)
```

`choices=list(_VIEWS)` on the `view` positional is built from the discovered names directly (so argparse itself rejects invalid values), and the multi-line help text for each view is built from that module's own docstring (`module.__doc__`) rather than being hand-written in `show/__init__.py` — `files.py`/`types.py` each start with a one-line docstring describing what they show.

Because this dispatch is internal to `show`, `commands.CMD_TO_FUNCTION` stays a flat `{name: callable}` mapping at every level — `__main__.py` never needs to know that `show` has subcommands at all.

---

## `htflow.exit_codes`

```python
from htflow.exit_codes import EXIT_SETUP_FAILURE, EXIT_ENGINE_ACTIVE
```

`EXIT_SETUP_FAILURE = 125` and `EXIT_ENGINE_ACTIVE = 75` live in their own top-level module rather than in `__main__.py`, since both `commands.execute` and `commands.cleanup` need them and `__main__.py` imports `htflow.commands` — importing the codes back from `htflow.__main__` would be circular.

---

## Extending

### Adding a new top-level command

Create `htflow/commands/<name>/__init__.py` implementing the standard contract — that's it, nothing to register anywhere:

```python
def add_parser(name, subparsers, common_parser):
    p = subparsers.add_parser(name, parents=[common_parser], help="...")
    p.add_argument(...)
    return p

def run(df, args):
    ...
```

`htflow/commands/__init__.py` discovers the new `<name>/` directory automatically the next time it's imported and derives the command's name from the directory itself. If `add_parser`/`run` are missing or not callable, discovery raises `RuntimeError` at import time rather than failing silently later.

### Adding a new sub-view under an existing command (e.g. `show`)

Create a flat sibling module, e.g. `htflow/commands/show/my_view.py`, with a one-line module docstring and `run(df, args)` — nothing to register. `show/__init__.py`'s own `discover()` call picks it up automatically, and both `choices=[...]` and its help text are derived from the discovered module and its docstring.
