# `htflow.sources` — JDL File Collection

The `sources` package is responsible for resolving CLI input flags into a concrete list of JDL (`*.sub`) files. It enforces that at least one input source flag was given and that at least one file was found, routing failures through argparse so the user sees a clean usage message.

---

## Design

The package uses two registries that separate concerns cleanly:

| Registry | Location | Purpose |
|---|---|---|
| `CLI_RESOLVERS` | `core.py` | Ordered list of source modules, one per CLI flag (`--jdl`, `--dir`, …) |
| `FILE_HANDLERS` | `_registry.py` | Maps file extensions to handler functions; used by directory scanning |

Adding a new **CLI source** (e.g. `--manifest`) requires only a new module and one entry in `CLI_RESOLVERS`. Adding a new **file type** for directory scanning (e.g. `.manifest`) requires registering a handler in `FILE_HANDLERS` — `from_dir` picks it up automatically.

---

## `InputError`

```python
from htflow.sources import InputError
```

Raised when user-supplied input arguments are invalid — for example, no source flag provided, a `--dir` path that is not a directory, or all sources resolving to zero files. The CLI catches `InputError` via `parser.error()`, which prints the usage summary and exits with code **2**.

---

## `collect_jdl_files(args)`

```python
from htflow.sources import collect_jdl_files

files: List[Path] = collect_jdl_files(args)
```

The main entry point. Given a parsed `argparse.Namespace`, it:

1. Finds all active resolvers (those whose flag was provided)
2. Raises `InputError` if none were active
3. Calls each active resolver and collects the resulting paths
4. Deduplicates — if the same file appears from multiple sources, it is included once and a `WARNING` is logged
5. Raises `InputError` if the final list is empty
6. Returns the deduplicated `List[Path]`

This function is called inside `parse_args()` immediately after `argparse` finishes, and its result is written back to `args.jdl` so the rest of the application is unaware of which source(s) provided the files.

---

## Sources

### `from_jdl` — `--jdl` flag

Handles the `--jdl PATH [PATH ...]` argument. Each value is converted to a `Path` and returned as-is. File existence is not checked here — that happens later when `HTCondorDataFlow` opens the files, preserving the existing exit-125 behaviour for missing files.

`from_jdl` also registers `.sub` as a supported file type in `FILE_HANDLERS`, so `from_dir` automatically picks up `*.sub` files when scanning directories.

### `from_dir` — `--dir`/`--directory`/`-d` flag

Handles the `--dir DIR [DIR ...]` argument. For each directory:

1. Raises `InputError` if the path is not a directory
2. Iterates the **top level only** (not recursive)
3. For each file whose extension is in `FILE_HANDLERS`, calls the registered handler to obtain JDL paths
4. Logs a `WARNING` if no supported files were found in that directory

Files within a directory are processed in sorted order for determinism.

---

## File Type Registry

### `FILE_HANDLERS`

```python
from htflow.sources._registry import FILE_HANDLERS
```

A `dict` mapping file extension strings (e.g. `".sub"`) to callables with the signature:

```python
def handler(path: Path) -> List[Path]: ...
```

A handler receives the path to a file found in a directory scan and returns the JDL paths it represents. For `.sub` files this is simply `[path]`. A future `.manifest` handler might return many paths by reading the manifest file's contents.

`FILE_HANDLERS` is populated at import time — importing `from_jdl` (which `core.py` does) registers `.sub` automatically.

### `register(ext, handler)`

```python
from htflow.sources._registry import register

register(".manifest", my_handler)
```

Adds or replaces a handler for the given extension. Call this at module level so the registration runs on import.

---

## Extending

### Adding a new CLI source

1. Create `htflow/sources/from_xyz.py` implementing:

```python
def active(args: argparse.Namespace) -> bool:
    return args.xyz is not None           # was the flag provided?

def resolve(args: argparse.Namespace) -> List[Path]:
    ...                                   # return discovered JDL paths
```

2. Add the flag to `common_parser` in `htflow/__main__.py`.

3. Add the module to `CLI_RESOLVERS` in `htflow/sources/core.py`:

```python
from . import from_jdl, from_dir, from_xyz

CLI_RESOLVERS = [
    from_jdl,
    from_dir,
    from_xyz,
]
```

Nothing else needs to change — `collect_jdl_files()` iterates `CLI_RESOLVERS` and the new source is included automatically.

### Adding a new file type for directory scanning

1. Create a handler function (can live in a new or existing module):

```python
def handle_manifest(path: Path) -> List[Path]:
    return [Path(line.strip()) for line in path.read_text().splitlines() if line.strip()]
```

2. Register it, and import the module from `core.py` so the registration runs:

```python
# in from_manifest.py
from ._registry import register
register(".manifest", handle_manifest)
```

```python
# in core.py
from . import from_jdl, from_dir, from_manifest
```

`from_dir` reads `FILE_HANDLERS` at call time, so it picks up `.manifest` files in any subsequent directory scan without any further changes.
