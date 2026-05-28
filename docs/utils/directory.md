# `htflow.utils.directory` — ChangeDir

A context manager that temporarily changes the current working directory, then restores it on exit — even if the block raises an exception.

---

## `ChangeDir`

### Constructor

```python
ChangeDir(dest: Union[Path, str])
```

| Parameter | Type            | Description                       |
|-----------|-----------------|-----------------------------------|
| `dest`    | `Path` or `str` | Directory to switch into on entry |

### Usage

```python
from htflow.utils.directory import ChangeDir

with ChangeDir("/tmp/workdir"):
    # cwd is now /tmp/workdir
    ...
# cwd is restored here
```

The context manager returns `self` on `__enter__`, so the bound name is available inside the block if needed:

```python
with ChangeDir("/tmp/workdir") as cd:
    print(cd.destination)   # Path("/tmp/workdir")
    print(cd.origin)  # original directory
```

### Path construction

`ChangeDir` supports the `/` operator to build paths relative to the destination:

```python
with ChangeDir("/tmp/workdir") as cd:
    path = cd / "subdir/file.txt"  # Path("/tmp/workdir/subdir/file.txt")
```

### Exception safety

The original directory is always restored, regardless of whether the block exits normally or raises:

```python
with ChangeDir("/tmp/workdir"):
    raise RuntimeError("something went wrong")
# cwd is still restored before the exception propagates
```

### Nesting

`ChangeDir` blocks can be nested; each level independently saves and restores its own previous directory:

```python
with ChangeDir("/tmp/a"):
    # cwd == /tmp/a
    with ChangeDir("/tmp/b"):
        # cwd == /tmp/b
    # cwd == /tmp/a
# cwd == original
```

### Same-directory optimization

If `dest` resolves to the current working directory, `ChangeDir` skips the `os.chdir` call entirely. The block behaves identically from the caller's perspective. `cd.origin` will be `None` in this case, since no directory switch occurred and there is nothing to restore.

This comparison uses `.resolve()` on both sides, so relative paths, `"."`, and symlinks pointing to the current directory are all treated as same-directory.

```python
# Assuming cwd is already /tmp/workdir
with ChangeDir("/tmp/workdir") as cd:
    assert cd.origin is None   # no chdir happened
    # cwd is still /tmp/workdir
```
