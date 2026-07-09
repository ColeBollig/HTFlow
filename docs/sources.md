# `htflow.sources` — Dataflow Input File Collection

The `sources` package is responsible for resolving CLI input flags into a concrete list of dataflow input files (JDL/HTCondor submit files by default). It enforces that at least one input source flag was given and that at least one file was found, routing failures through argparse so the user sees a clean usage message. `--jdl` and `--dir` may each be given multiple times.

---

## Design

The package uses two registries that separate concerns cleanly:

| Registry | Location | Purpose |
|---|---|---|
| `CLI_RESOLVERS` | `core.py` | Ordered list of source modules, one per CLI flag (`--jdl`, `--dir`, …) |
| `FILE_HANDLERS` / default handler | `_registry.py` | Maps specific file extensions to override handlers, with a single default handler (JDL) used for everything else |

Adding a new **CLI source** (e.g. `--manifest`) requires only a new module and one entry in `CLI_RESOLVERS`. Adding a new **file type override** for directory scanning (e.g. `.snakemake`) requires registering a handler for that extension in `FILE_HANDLERS` — `from_dir` picks it up automatically and uses it instead of the default for that one extension.

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

This function is called inside `parse_args()`, after `argparse` finishes and the `--jdl`/`--dir` values have been flattened and command validity checked, and its result is written back to `args.jdl` so the rest of the application is unaware of which source(s) provided the files.

---

## Sources

### `from_jdl` — `--jdl` flag

Handles the `--jdl PATH [PATH ...]` argument (repeatable). Each value is converted to a `Path` and returned as-is — `resolve()` does **not** parse or validate the file. File existence/validity is not checked here — that happens later when `HTCondorDataFlow` opens the files, preserving the existing exit-125 behaviour for missing or invalid files named explicitly via `--jdl`.

`from_jdl` also defines `handle_file()`, which registers itself as the **default handler** (see below) used by `from_dir` for any file whose extension has no more specific override registered.

### `from_dir` — `--dir`/`--directory`/`-d` flag

Handles the `--dir DIR [DIR ...]` argument (repeatable). For each directory:

1. Raises `InputError` if the path is not a directory
2. Iterates the **top level only** (not recursive)
3. For each file, looks up a handler via `handler_for(entry.suffix)` — an extension with a registered override uses that handler; everything else (including files with no extension) falls through to the default JDL handler
4. Logs a `WARNING` if no supported files were found in that directory

Files within a directory are processed in sorted order for determinism.

**All files in a scanned directory are assumed to be JDL submit files unless their extension has an explicit override registered.** For example, `foo`, `bar.sub`, and `bat.txt` are all treated as JDL candidates by default; a hypothetical `.snakemake` extension with its own registered parser would be the one exception routed elsewhere.

### The JDL default handler — content-based validation

`from_jdl.handle_file()` is the default handler. For each candidate file it opens the file and attempts to parse it as an HTCondor submit description:

```python
def handle_file(path: Path) -> List[Path]:
    with open(path, "r") as f:
        content = f.read()

    try:
        htcondor2.Submit(content)
    except ValueError as e:
        print(f"Skipping '{path}': not a valid HTCondor submit file ({e})")
        return []

    return [path]
```

If parsing raises `ValueError` (malformed submit syntax, binary/garbage content, etc.), the file is skipped — a message is printed and the file is silently excluded from the resolved list rather than aborting the whole scan. This is what lets a directory scan safely assume "everything is a JDL by default": files that aren't actually valid submit descriptions (a `README`, a stray output file, etc.) get filtered out automatically instead of crashing `--dir` or being folded into the dataflow as garbage nodes.

This validation only applies to the directory-scan default handler — `from_jdl.resolve()` (the `--jdl` flag path) does not run it, so explicitly-named files keep their existing error behavior.

---

## File Type Registry

### `FILE_HANDLERS` and the default handler

```python
from htflow.sources._registry import FILE_HANDLERS, register, set_default_handler, handler_for
```

`FILE_HANDLERS` is a `dict` mapping file extension strings (e.g. `".snakemake"`) to **override** handlers — used only for extensions that need something other than JDL parsing. A single default handler (set via `set_default_handler`) is used for every extension without an override, including no extension at all.

All handlers share the signature:

```python
def handler(path: Path) -> List[Path]: ...
```

A handler receives the path to a file found in a directory scan and returns the JDL paths it represents. The JDL default handler either returns `[path]` (valid submit file) or `[]` (failed to parse, skipped). A hypothetical `.manifest`/`.snakemake` handler might return many paths by reading and expanding that file's contents.

### `register(ext, handler)`

```python
from htflow.sources._registry import register

register(".snakemake", my_snakemake_handler)
```

Registers a handler that **overrides** the default for one specific extension. Call this at module level so the registration runs on import. Files with that extension use this handler instead of the JDL default; every other extension (and extensionless files) is unaffected and still falls through to the default.

### `set_default_handler(handler)`

```python
from htflow.sources._registry import set_default_handler

set_default_handler(my_handler)
```

Sets the handler used for any extension without a specific override. `from_jdl` calls this once at import time with `handle_file`, making JDL parsing the fallback for everything.

### `handler_for(ext)`

```python
from htflow.sources._registry import handler_for

handler_for(".sub")           # -> the default (JDL) handler, unless overridden
handler_for(".snakemake")     # -> the registered override, if one exists
```

Returns the extension-specific override if one is registered, otherwise the default handler. `from_dir` calls this for every file it encounters.

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

### Adding a new file type override for directory scanning

Only needed when a specific extension should be parsed differently than plain JDL — e.g. a `.snakemake` extension routed to a Snakemake-aware parser (not implemented here; this is illustrative):

1. Create a handler function (can live in a new module):

```python
def handle_snakemake(path: Path) -> List[Path]:
    ...  # parse the Snakefile and return the JDL paths it expands to
```

2. Register it as an override, and import the module from `core.py` so the registration runs:

```python
# in from_snakemake.py
from ._registry import register
register(".snakemake", handle_snakemake)
```

```python
# in core.py
from . import from_jdl, from_dir, from_snakemake
```

`from_dir` calls `handler_for()` at scan time, so it picks up the `.snakemake` override in any subsequent directory scan without any further changes — every other file in that directory still falls through to the JDL default handler.
