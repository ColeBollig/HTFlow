# `htflow.dag` — DAG Data Structure

Generic directed acyclic graph (DAG) implementation used as the backbone of HTFlow's dataflow representation. The module provides `Node` and `Dag` container classes along with supporting enumerations.

---

## Enumerations

### `Relationship`

Describes the directional relationship between two nodes.

| Member   | Value | Meaning                          |
|----------|-------|----------------------------------|
| `PARENT` | `0`   | The referenced node is a parent  |
| `CHILD`  | `1`   | The referenced node is a child   |

### `WalkOrder`

Controls the traversal strategy used by `Dag.Walk()`.

| Member | Value | Meaning              |
|--------|-------|----------------------|
| `DFS`  | `0`   | Depth-first search   |
| `BFS`  | `1`   | Breadth-first search |

---

## `Node`

A single vertex in the DAG.

### Constructor

```python
Node(name: str, id: int)
```

Nodes are not created directly — use `Dag.AddNode()` which assigns the `id` automatically.

### Properties

| Property   | Type                  | Description                                                         |
|------------|-----------------------|---------------------------------------------------------------------|
| `id`       | `int`                 | Unique integer identifier within the containing DAG                 |
| `name`     | `str`                 | Human-readable node name                                            |
| `parents`  | `Optional[Set[int]]`  | Set of parent node IDs, or `None` if this is a root node            |
| `children` | `Optional[Set[int]]`  | Set of child node IDs, or `None` if this is a leaf node             |
| `internal` | `Any`                 | Arbitrary payload attached to the node (readable and writable)      |
| `visited`  | `bool`                | Whether this node has been visited during the current DAG walk      |

### Methods

#### `AddDependencies(dependencies, relation=Relationship.CHILD)`

Registers one or more dependency edges on this node.

- `dependencies` — a single `int` node ID, or a `list`/`set` of `int` node IDs
- `relation` — `Relationship.CHILD` (default) adds the IDs as children; `Relationship.PARENT` adds them as parents

```python
node.AddDependencies(3)                            # node → child id 3
node.AddDependencies([1, 2], Relationship.PARENT)  # parents 1 and 2 → node
```

#### `RemoveDependency(dependency: int)`

Removes `dependency` from both the parent and child sets of this node. If the resulting set is empty it is set back to `None`.

#### `PrepareWalk()`

Resets `visited` to `False`. Called automatically by `Dag.Walk()` before traversal begins.

#### `WalkVisit()`

Marks this node as visited. Called automatically by `Dag.Walk()` during traversal.

### Operators

| Operator     | Behaviour                                                                 |
|--------------|---------------------------------------------------------------------------|
| `==`         | Two nodes are equal when both `name` and `id` match                      |
| `<` / `>`    | Compare by `id`; also accepts a plain `int` on the right-hand side       |
| `hash(node)` | Hashed on `(name, id)` — nodes are safe to use in sets and dict keys     |
| `str(node)`  | Returns `name`                                                            |
| `repr(node)` | Full debug string including `id`, `internal`, parent IDs, and child IDs  |

---

## `Dag`

A directed acyclic graph consisting of `Node` instances.

### Constructor

```python
Dag(name: str = "root")
```

### Properties

| Property   | Type             | Description                                                    |
|------------|------------------|----------------------------------------------------------------|
| `name`     | `str`            | Name of this DAG                                               |
| `size`     | `int`            | Number of live (non-removed) nodes                             |
| `roots`    | `List[Node]`     | Nodes with no parents                                          |
| `leafs`    | `List[Node]`     | Nodes with no children                                         |
| `internal` | `Any`            | Arbitrary payload attached to the DAG (readable and writable)  |

### Indexing

Nodes can be retrieved by name or by ID:

```python
dag["my-node"]  # lookup by name  → Node or None
dag[2]          # lookup by ID    → Node or None
```

Returns `None` if the node does not exist (or has been removed).

### Iteration and Length

```python
len(dag)        # number of live nodes
for node in dag:  # yields every live node in insertion order
    ...
```

Removed nodes (set to `None` internally) are skipped during iteration.

### Methods

#### `AddNode(name: str) → Node`

Creates a new node with the given name, assigns the next sequential ID, and appends it to the DAG. Raises `RuntimeError` if a node with that name already exists.

```python
node = dag.AddNode("step-1")
node.internal = "/path/to/job.sub"
```

#### `Remove(name: str) → bool`

Removes the named node and cleans up all parent/child references to it across the remaining nodes. Returns `True` on success, `False` if the name is not found.

#### `Connect(parents, children)`

Creates directed edges from every node in `parents` to every node in `children`. Both arguments can be any combination of:

- A node name (`str`)
- A node ID (`int`)
- A `Node` object
- A `list` or `set` of the above

```python
dag.Connect("a", "b")               # a → b
dag.Connect("a", ["b", "c"])        # a → b, a → c
dag.Connect(["a", "b"], "c")        # a → c, b → c
```

Raises `RuntimeError` if a referenced node does not exist in the DAG.

#### `Walk(action, order=WalkOrder.BFS)`

Traverses the DAG starting from all root nodes and calls `action(node)` on each node exactly once.

- `action` — any callable that accepts a single `Node`
- `order` — `WalkOrder.BFS` (default) or `WalkOrder.DFS`

```python
dag.Walk(lambda n: print(n.name))
dag.Walk(process_node, order=WalkOrder.DFS)
```

Multi-root DAGs are handled correctly. Nodes reachable via multiple paths are visited only once.

#### `Cycle() → bool`

Returns `True` if the graph contains any cycle, including cycles in subgraphs that are disjoint from the roots (which `Walk()` would never reach).

```python
if dag.Cycle():
    raise RuntimeError("DAG contains a cycle")
```

#### `Dump()`

Prints a debug summary of the DAG and all its nodes to stdout.

---

## Example

```python
from htflow.dag import Dag, WalkOrder

d = Dag("pipeline")

a = d.AddNode("fetch")
b = d.AddNode("process")
c = d.AddNode("upload")

d.Connect("fetch", "process")
d.Connect("process", "upload")

assert not d.Cycle()

d.Walk(lambda n: print(n.name))  # fetch → process → upload
```
