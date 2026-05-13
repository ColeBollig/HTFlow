# Copyright 2026 Center for High Throughput Computing (CHTC)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import deque
from typing import Callable, Any, Optional, Union, Set, List
import enum

class Relationship(enum.Enum):
    """Enumeration denoting the relationship between two sets of nodes."""
    PARENT = 0
    CHILD = 1

class WalkOrder(enum.Enum):
    """Enumeration denoting DAG walk order"""
    DFS = 0 # Depth first search
    BFS = 1 # Breadth first search

class Node():
    """
    Container class for a DAG node.
    """
    # Keyword Args are for the internal node data class creation
    def __init__(self, name: str, id: int):
        if not isinstance(name, str):
            raise ValueError("name must be a string")

        if not isinstance(id, int):
            raise ValueError("id must be an integer")

        # Node Name
        self._name = name

        # Unique node id within associated DAG
        self._id = id

        # Edge sets to parents and children (by id)
        self._parents = None
        self._children = None

        # Represent node visited for walking DAG
        self._walk_visited = False

        # Specific node
        self._internal = None

    def __hash__(self) -> int:
        """Hashing function for node class for use in map and set"""
        return hash((self._name, self._id))

    def __eq__(self, other: Node) -> bool:
        """Compare if this node is equal to another"""
        if not isinstance(other, Node):
            return False
        return self._name == other.name and self._id == other.id

    def __lt__(self, other: Union[int, Node]) -> bool:
        """Compare if this node is less than another"""
        if isinstance(other, int):
            return self._id < other
        elif isinstance(other, Node):
            return self._id < other.id

        return False

    def __gt__(self, other: Union[int, Node]) -> bool:
        """Compare if this node is greater than another"""
        if isinstance(other, int):
            return self._id > other
        elif isinstance(other, Node):
            return self._id > other.id

        return False

    def __str__(self) -> str:
        """Return node name when turned into string"""
        return self._name

    def __repr__(self) -> str:
        """Internal representation of node class"""
        parents = ",".join([f"{_id}" for _id in self._parents]) if self._parents is not None else None
        children = ",".join([f"{_id}" for _id in self._children]) if self._children is not None else None
        return f"Node({self._name}, {self._id}): Internal=[{self._internal!r}] Parents={parents} Children={children}"

    @property
    def id(self) -> int:
        """Get this nodes id"""
        return self._id

    @property
    def name(self) -> str:
        """Get this nodes name"""
        return self._name

    @property
    def parents(self) -> Optional[Set[int]]:
        """Get this nodes set of parent node ids"""
        return self._parents

    @property
    def children(self) -> Optional[Set[int]]:
        """Get this nodes set of children node ids"""
        return self._children

    @property
    def internal(self) -> Any:
        """Get internal node data structure"""
        return self._internal

    @internal.setter
    def internal(self, value: Any) -> None:
        """Set insternal node data structure"""
        self._internal = value

    @property
    def visited(self) -> bool:
        """Get whether this node has been visited during/since a DAG walk"""
        return self._walk_visited

    def PrepareWalk(self) -> None:
        """Prepare this node for a DAG walk"""
        self._walk_visited = False

    def WalkVisit(self) -> None:
        """Set this node has been visited this DAG walk"""
        self._walk_visited = True

    def __add_dependency(self, node_id: int, relation: Relationship) -> None:
        """Internal add dependency to a specific node id"""
        if relation == Relationship.CHILD:
            if self._children is None:
                self._children = set()
            self._children.add(node_id)
        elif relation == Relationship.PARENT:
            if self._parents is None:
                self._parents = set()
            self._parents.add(node_id)

    def AddDependencies(self, dependencies: Union[int, List[int], Set[int]], relation: Relationship = Relationship.CHILD) -> None:
        """Add parent/child denpendencies to node(s) specified by id"""
        if not isinstance(relation, Relationship):
            raise ValueError("relation must be a Relationship enumeration value")

        if isinstance(dependencies, int):
            self.__add_dependency(dependencies, relation)
        elif isinstance(dependencies, (list, set)):
            if not all([isinstance(i, int) for i in dependencies]):
                raise ValueError("dependencies list/set must only contain integers")

            for dep in dependencies:
                self.__add_dependency(dep, relation)
        else:
            raise ValueError("dependencies must be an integer or list/set of integers")

    def RemoveDependency(self, dependency: int) -> None:
        """Remove a node specified by id from this nodes dependencies"""
        if not isinstance(dependency, int):
            raise ValueError("dependency must be an integer")

        if self._parents is not None:
            self._parents.discard(dependency)
            if len(self._parents) == 0:
                self._parents = None

        if self._children is not None:
            self._children.discard(dependency)
            if len(self._children) == 0:
                self._children = None


class Dag():
    """
    Container class for a DAG consisting of Nodes.
    """
    def __init__(self, name: str = "root"):
        if not isinstance(name, str):
            raise ValueError("name must be a string")

        # Name of this DAG
        self._name = name

        # List of nodes (sorted by node-id)
        self._nodes = list()

        # node-id generator for this DAG
        self._id_generator = 0

        # Internal node name -> id mapping
        self._node_name_to_id = dict()

        # count of internal nodes (used for len/size)
        self._num_nodes = 0

        # List of sub-DAGs
        #self._sub_dags = dict()

        # Specific instance internal data
        self._internal = None

    @property
    def internal(self) -> Any:
        """Get internal DAG data structure"""
        return self._internal

    @internal.setter
    def internal(self, value: Any) -> None:
        """Set internal DAG data structure"""
        self._internal = value

    @property
    def name(self) -> str:
        """Get DAG name"""
        return self._name

    @property
    def size(self) -> int:
        return self._num_nodes

    def __getitem__(self, key) -> Optional[Node]:
        """Get a node in the DAG by name or unique id"""
        node = None

        if isinstance(key, int):
            if key >= 0 and key < len(self._nodes):
                node = self._nodes[key]
        elif isinstance(key, str):
            if key in self._node_name_to_id:
                node = self._nodes[self._node_name_to_id[key]]
        else:
            raise ValueError("key must be a string or int")

        return node

    def __str__(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"DAG({self._name}): Internal=[{self._internal!r}] NumNodes={self._num_nodes} NextNodeId={self._id_generator}"

    def __len__(self) -> int:
        return self._num_nodes

    def __iter__(self):
        for node in self._nodes:
            if node is not None:
                yield node

    def Dump(self) -> None:
        print(repr(self))
        for n in self._nodes:
            print(f"\t{n!r}")

    def AddNode(self, name: str) -> Node:
        """Add a node with specified name into DAG"""
        if not isinstance(name, str):
            raise ValueError("node name must be a string")

        if name in self._node_name_to_id:
            raise RuntimeError(f"node {name} already exists in DAG")

        assert self._id_generator == len(self._nodes)

        node = Node(name, self._id_generator)

        self._nodes.append(node)
        self._node_name_to_id[name] = self._id_generator

        self._id_generator += 1
        self._num_nodes += 1

        return node

    def Remove(self, name: str) -> bool:
        """Remove node with specified name from DAG"""
        if not isinstance(name, str):
            raise ValueError("node name must be a string")

        if name not in self._node_name_to_id:
            return False

        id = self._node_name_to_id[name]
        self._nodes[id] = None
        del self._node_name_to_id[name]

        self._num_nodes -= 1

        for node in self:
            node.RemoveDependency(id)

        return True

    @property
    def roots(self) -> List[Node]:
        """Get root nodes of the DAG"""
        return [node for node in self if node.parents is None]

    @property
    def leafs(self) -> List[Node]:
        """Get leaf nodes of the DAG"""
        return [node for node in self if node.children is None]

    def __resolve_node_connection_ids(
            self,
            destination: set,
            source: Union[str, int, Node, List[Union[str, int, Node]], Set[Union[str, int, Node]]]
            ) -> bool:
        """Internal make resolve node(s) source into node ids and insert into set"""

        def node_dne_error(src: Union[str, int]):
            if isinstance(src, str):
                return f"Node {src} does not exist in DAG"
            return f"Node with ID:{src} does not exist in DAG"

        if isinstance(source, (str, int)):
            node = self[source]
            if node is None:
                raise RuntimeError(node_dne_error(source))
            destination.add(node.id)
        elif isinstance(source, Node):
            destination.add(source.id)
        elif isinstance(source, (list, set)):
            for ref in source:
                if isinstance(ref, (str, int)):
                    node = self[ref]
                    if node is None:
                        raise RuntimeError(node_dne_error(ref))
                    destination.add(node.id)
                elif isinstance(ref, Node):
                    destination.add(ref.id)
                else:
                    return False
        else:
            return False
        return True

    def Connect(
            self,
            parents: Union[str, int, Node, List[Union[str, int, Node]], Set[Union[str, int, Node]]],
            children: Union[str, int, Node, List[Union[str, int, Node]], Set[Union[str, int, Node]]]
            ) -> None:
        """Make connection between parent and child nodes in the DAG"""

        parent_id_set = set()
        child_id_set = set()

        if not self.__resolve_node_connection_ids(parent_id_set, parents):
            raise ValueError("parents must be a string, integer, Node or a list/set of specified types")
        elif not self.__resolve_node_connection_ids(child_id_set, children):
            raise ValueError("children must be a string, integer, Node or a list/set of specified types")

        for node_id in parent_id_set:
            self[node_id].AddDependencies(child_id_set, Relationship.CHILD)

        for node_id in child_id_set:
            self[node_id].AddDependencies(parent_id_set, Relationship.PARENT)

    def __prepare_walk(self) -> None:
        """Prepare this DAG for a walk"""
        for node in self:
            node.PrepareWalk()

    def Walk(self, action: Callable[[Node], None], order: WalkOrder = WalkOrder.BFS) -> None:
        """Walk the DAG in specified search order (Breadth/Depth First) and execute action"""
        self.__prepare_walk()

        queue = deque(self.roots)

        while len(queue) > 0:
            node = queue.popleft()

            if node.visited:
                continue

            action(node)

            node.WalkVisit()

            children = node.children
            if children is not None:
                if order == WalkOrder.DFS:
                    queue.extendleft([self._nodes[n] for n in children if not self._nodes[n].visited])
                elif order == WalkOrder.BFS:
                    queue.extend([self._nodes[n] for n in children if not self._nodes[n].visited])

    def __cycle_walk(self, node: Node, ancestors: List[int]) -> bool:
        """Internal cycle check traversal depth first"""
        if node.visited:
            cycle = False
            if node.id in ancestors:
                ancestors.append(node.id)
                cycle = True
            return cycle

        node.WalkVisit()

        if node.children is not None:
            ancestors.append(node.id)

            for child in node.children:
                if self.__cycle_walk(self._nodes[child], ancestors):
                    return True

            ancestors.pop()

        return False

    def Cycle(self) -> bool:
        """Check the DAG for any cycles"""
        self.__prepare_walk()

        for node in self.roots:
            ancestors = list()
            if self.__cycle_walk(node, ancestors):
                return True

        # Verify all nodes visited in order to detect disjoint cycle in DAG
        return not all([node.visited for node in self])

