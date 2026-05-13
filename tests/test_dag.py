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

import pytest
from htflow.dag import Node, Dag, Relationship, WalkOrder


# ---------------------------------------------------------------------------
# Node
# ---------------------------------------------------------------------------

class TestNodeInit:
    def test_valid(self):
        n = Node("a", 0)
        assert n.name == "a"
        assert n.id == 0

    def test_invalid_name(self):
        with pytest.raises(ValueError):
            Node(123, 0)

    def test_invalid_id(self):
        with pytest.raises(ValueError):
            Node("a", "0")


class TestNodeComparisons:
    def test_eq_same(self):
        assert Node("a", 0) == Node("a", 0)

    def test_eq_different_name(self):
        assert Node("a", 0) != Node("b", 0)

    def test_eq_different_id(self):
        assert Node("a", 0) != Node("a", 1)

    def test_eq_non_node(self):
        assert Node("a", 0) != "a"

    def test_hash_equal_nodes(self):
        assert hash(Node("a", 0)) == hash(Node("a", 0))

    def test_hash_different_nodes(self):
        assert hash(Node("a", 0)) != hash(Node("b", 1))

    def test_lt_int(self):
        assert Node("a", 0) < 1
        assert not Node("a", 1) < 0

    def test_gt_int(self):
        assert Node("a", 1) > 0
        assert not Node("a", 0) > 1

    def test_lt_node(self):
        assert Node("a", 0) < Node("b", 1)

    def test_gt_node(self):
        assert Node("b", 1) > Node("a", 0)

    def test_lt_unrecognized_type(self):
        assert not Node("a", 0).__lt__("x")

    def test_gt_unrecognized_type(self):
        assert not Node("a", 0).__gt__("x")


class TestNodeStr:
    def test_str(self):
        assert str(Node("mynode", 5)) == "mynode"

    def test_repr_no_edges(self):
        r = repr(Node("n", 0))
        assert "n" in r
        assert "None" in r

    def test_repr_with_edges(self):
        n = Node("n", 1)
        n.AddDependencies(0, Relationship.PARENT)
        n.AddDependencies(2, Relationship.CHILD)
        r = repr(n)
        assert "0" in r
        assert "2" in r

    def test_repr_internal_is_repr_formatted(self):
        n = Node("a", 0)
        n.internal = "hello"
        assert "'hello'" in repr(n)


class TestNodeInternal:
    def test_default_none(self):
        assert Node("a", 0).internal is None

    def test_set_get(self):
        n = Node("a", 0)
        n.internal = {"key": "value"}
        assert n.internal == {"key": "value"}


class TestNodeWalk:
    def test_initial_not_visited(self):
        assert not Node("a", 0).visited

    def test_walk_visit(self):
        n = Node("a", 0)
        n.WalkVisit()
        assert n.visited

    def test_prepare_walk_resets(self):
        n = Node("a", 0)
        n.WalkVisit()
        n.PrepareWalk()
        assert not n.visited


class TestNodeAddDependencies:
    def test_single_child(self):
        n = Node("a", 0)
        n.AddDependencies(1, Relationship.CHILD)
        assert 1 in n.children

    def test_single_parent(self):
        n = Node("a", 0)
        n.AddDependencies(1, Relationship.PARENT)
        assert 1 in n.parents

    def test_list_of_children(self):
        n = Node("a", 0)
        n.AddDependencies([1, 2, 3], Relationship.CHILD)
        assert n.children == {1, 2, 3}

    def test_set_of_parents(self):
        n = Node("a", 0)
        n.AddDependencies({1, 2}, Relationship.PARENT)
        assert n.parents == {1, 2}

    def test_invalid_relation(self):
        with pytest.raises(ValueError):
            Node("a", 0).AddDependencies(1, "CHILD")

    def test_invalid_dependency_type(self):
        with pytest.raises(ValueError):
            Node("a", 0).AddDependencies("1", Relationship.CHILD)

    def test_invalid_list_elements(self):
        with pytest.raises(ValueError):
            Node("a", 0).AddDependencies(["x"], Relationship.CHILD)

    def test_default_relation_is_child(self):
        n = Node("a", 0)
        n.AddDependencies(1)
        assert 1 in n.children


class TestNodeRemoveDependency:
    def test_removes_from_parents(self):
        n = Node("a", 0)
        n.AddDependencies(1, Relationship.PARENT)
        n.RemoveDependency(1)
        assert n.parents is None

    def test_removes_from_children(self):
        n = Node("a", 0)
        n.AddDependencies(1, Relationship.CHILD)
        n.RemoveDependency(1)
        assert n.children is None

    def test_nulls_empty_parent_set(self):
        n = Node("a", 0)
        n.AddDependencies(1, Relationship.PARENT)
        n.RemoveDependency(1)
        assert n.parents is None

    def test_nulls_empty_child_set(self):
        n = Node("a", 0)
        n.AddDependencies(1, Relationship.CHILD)
        n.RemoveDependency(1)
        assert n.children is None

    def test_partial_removal(self):
        n = Node("a", 0)
        n.AddDependencies([1, 2], Relationship.CHILD)
        n.RemoveDependency(1)
        assert n.children == {2}

    def test_nonexistent_id_no_error(self):
        n = Node("a", 0)
        n.AddDependencies(1, Relationship.CHILD)
        n.RemoveDependency(99)
        assert n.children == {1}

    def test_no_deps_no_error(self):
        n = Node("a", 0)
        n.RemoveDependency(1)
        assert n.parents is None
        assert n.children is None

    def test_invalid_type(self):
        with pytest.raises(ValueError):
            Node("a", 0).RemoveDependency("1")

    def test_removes_from_both(self):
        n = Node("a", 1)
        n.AddDependencies(0, Relationship.PARENT)
        n.AddDependencies(2, Relationship.CHILD)
        n.RemoveDependency(0)
        n.RemoveDependency(2)
        assert n.parents is None
        assert n.children is None


# ---------------------------------------------------------------------------
# Dag
# ---------------------------------------------------------------------------

class TestDagInit:
    def test_default_name(self):
        d = Dag()
        assert d.name == "root"

    def test_custom_name(self):
        assert Dag("mygraph").name == "mygraph"

    def test_invalid_name(self):
        with pytest.raises(ValueError):
            Dag(123)


class TestDagStr:
    def test_str_is_string(self):
        assert isinstance(str(Dag()), str)

    def test_str_returns_name(self):
        assert str(Dag("foo")) == "foo"


class TestDagRepr:
    def test_repr_contains_name(self):
        assert "foo" in repr(Dag("foo"))

    def test_repr_num_nodes_zero(self):
        assert "0" in repr(Dag())

    def test_repr_num_nodes_after_add(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        assert "NumNodes=2" in repr(d)

    def test_repr_num_nodes_after_remove(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.Remove("a")
        assert "NumNodes=1" in repr(d)

    def test_repr_next_id_after_adds(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        assert "NextNodeId=2" in repr(d)

    def test_repr_no_internal(self):
        assert "None" in repr(Dag())


class TestDagDump:
    def test_dump_does_not_raise(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        d.Connect(a, b)
        d.Dump()

    def test_dump_empty_does_not_raise(self):
        Dag().Dump()


class TestDagInternal:
    def test_default_none(self):
        assert Dag().internal is None

    def test_set_get(self):
        d = Dag()
        d.internal = [1, 2, 3]
        assert d.internal == [1, 2, 3]


class TestDagAddNode:
    def test_adds_node(self):
        d = Dag()
        n = d.AddNode("a")
        assert n.name == "a"
        assert n.id == 0

    def test_sequential_ids(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        assert a.id == 0
        assert b.id == 1

    def test_duplicate_raises(self):
        d = Dag()
        d.AddNode("a")
        with pytest.raises(RuntimeError):
            d.AddNode("a")

    def test_invalid_name(self):
        with pytest.raises(ValueError):
            Dag().AddNode(42)


class TestDagGetItem:
    def test_by_name(self):
        d = Dag()
        d.AddNode("a")
        assert d["a"].name == "a"

    def test_by_id(self):
        d = Dag()
        d.AddNode("a")
        assert d[0].name == "a"

    def test_nonexistent_name_returns_none(self):
        assert Dag()["missing"] is None

    def test_nonexistent_id_returns_none(self):
        assert Dag()[99] is None

    def test_removed_node_by_id_returns_none(self):
        d = Dag()
        d.AddNode("a")
        d.Remove("a")
        assert d[0] is None

    def test_invalid_key_type(self):
        with pytest.raises(ValueError):
            Dag()[3.14]


class TestDagIter:
    def test_yields_all_nodes(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        names = {n.name for n in d}
        assert names == {"a", "b"}

    def test_skips_removed_nodes(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.Remove("a")
        names = {n.name for n in d}
        assert names == {"b"}

    def test_empty_dag(self):
        assert list(Dag()) == []

    def test_len_empty(self):
        assert len(Dag()) == 0

    def test_len_after_add(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        assert len(d) == 2

    def test_len_after_remove(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.Remove("a")
        assert len(d) == 1


class TestDagSize:
    def test_size_empty(self):
        assert Dag().size == 0

    def test_size_after_single_add(self):
        d = Dag()
        d.AddNode("a")
        assert d.size == 1

    def test_size_after_multiple_adds(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.AddNode("c")
        assert d.size == 3

    def test_size_after_remove(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.Remove("a")
        assert d.size == 1

    def test_size_equals_len(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.AddNode("c")
        d.Remove("b")
        assert d.size == len(d)


class TestDagRootsAndLeafs:
    def test_all_roots_when_disconnected(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        assert len(d.roots) == 2

    def test_all_leafs_when_disconnected(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        assert len(d.leafs) == 2

    def test_empty_dag_no_roots(self):
        assert Dag().roots == []

    def test_empty_dag_no_leafs(self):
        assert Dag().leafs == []

    def test_middle_node_not_root_or_leaf(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        d.Connect(a, b)
        d.Connect(b, c)
        root_names = {n.name for n in d.roots}
        leaf_names = {n.name for n in d.leafs}
        assert "b" not in root_names
        assert "b" not in leaf_names

    def test_roots_after_connect(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        d.Connect(a, b)
        root_names = {n.name for n in d.roots}
        assert root_names == {"a"}

    def test_leafs_after_connect(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        d.Connect(a, b)
        leaf_names = {n.name for n in d.leafs}
        assert leaf_names == {"b"}

    def test_roots_restored_after_remove(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        d.Connect(a, b)
        d.Remove("a")
        root_names = {n.name for n in d.roots}
        assert "b" in root_names

    def test_leafs_restored_after_remove(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        d.Connect(a, b)
        d.Remove("b")
        leaf_names = {n.name for n in d.leafs}
        assert "a" in leaf_names


class TestDagConnect:
    def test_by_node_objects(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        d.Connect(a, b)
        assert b.id in a.children
        assert a.id in b.parents

    def test_by_name(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.Connect("a", "b")
        assert d["b"].id in d["a"].children

    def test_by_id(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.Connect(0, 1)
        assert 1 in d[0].children

    def test_one_to_many(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        d.Connect(a, [b, c])
        assert b.id in a.children
        assert c.id in a.children

    def test_many_to_one(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        d.Connect([a, b], c)
        assert c.id in a.children
        assert c.id in b.children
        assert a.id in c.parents
        assert b.id in c.parents

    def test_by_name_in_list(self):
        d = Dag()
        d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        d.Connect(["a", "b"], c)
        assert c.id in d["a"].children
        assert c.id in b.children

    def test_by_id_in_list(self):
        d = Dag()
        a = d.AddNode("a")
        d.AddNode("b")
        c = d.AddNode("c")
        d.Connect([0, 1], c)
        assert c.id in a.children
        assert c.id in d[1].children

    def test_idempotent_connect(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        d.Connect(a, b)
        d.Connect(a, b)
        assert len(a.children) == 1
        assert len(b.parents) == 1

    def test_connect_to_removed_node_by_id(self):
        d = Dag()
        d.AddNode("a")
        b = d.AddNode("b")
        d.Remove("a")
        with pytest.raises(RuntimeError):
            d.Connect(0, b)

    def test_missing_error_message_contains_name(self):
        d = Dag()
        b = d.AddNode("b")
        with pytest.raises(RuntimeError, match="ghost"):
            d.Connect("ghost", b)

    def test_missing_error_message_contains_id(self):
        d = Dag()
        b = d.AddNode("b")
        with pytest.raises(RuntimeError, match="99"):
            d.Connect(99, b)

    def test_invalid_parent(self):
        d = Dag()
        d.AddNode("a")
        with pytest.raises(RuntimeError):
            d.Connect("missing", "a")

    def test_invalid_child(self):
        d = Dag()
        d.AddNode("a")
        with pytest.raises(RuntimeError):
            d.Connect("a", "missing")

    def test_missing_parent_by_id(self):
        d = Dag()
        b = d.AddNode("b")
        with pytest.raises(RuntimeError):
            d.Connect(99, b)

    def test_missing_child_by_id(self):
        d = Dag()
        a = d.AddNode("a")
        with pytest.raises(RuntimeError):
            d.Connect(a, 99)

    def test_missing_parent_name_in_list(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        with pytest.raises(RuntimeError):
            d.Connect([a, "missing"], b)

    def test_missing_child_name_in_list(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        with pytest.raises(RuntimeError):
            d.Connect(a, [b, "missing"])

    def test_missing_parent_id_in_list(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        with pytest.raises(RuntimeError):
            d.Connect([a, 99], b)

    def test_missing_child_id_in_list(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        with pytest.raises(RuntimeError):
            d.Connect(a, [b, 99])

    def test_missing_parent_name_in_set(self):
        d = Dag()
        b = d.AddNode("b")
        with pytest.raises(RuntimeError):
            d.Connect({"missing"}, b)

    def test_missing_child_name_in_set(self):
        d = Dag()
        a = d.AddNode("a")
        with pytest.raises(RuntimeError):
            d.Connect(a, {"missing"})

    def test_empty_parent_list_does_not_raise(self):
        d = Dag()
        b = d.AddNode("b")
        d.Connect([], b)
        assert b.parents is None

    def test_empty_child_list_does_not_raise(self):
        d = Dag()
        a = d.AddNode("a")
        d.Connect(a, [])
        assert a.children is None

    def test_empty_parent_set_does_not_raise(self):
        d = Dag()
        b = d.AddNode("b")
        d.Connect(set(), b)
        assert b.parents is None

    def test_empty_child_set_does_not_raise(self):
        d = Dag()
        a = d.AddNode("a")
        d.Connect(a, set())
        assert a.children is None

    def test_both_empty_lists_does_not_raise(self):
        d = Dag()
        a = d.AddNode("a")
        d.Connect([], [])
        assert a.children is None
        assert a.parents is None

    def test_invalid_parent_type(self):
        d = Dag()
        b = d.AddNode("b")
        with pytest.raises(ValueError):
            d.Connect(3.14, b)

    def test_invalid_child_type(self):
        d = Dag()
        a = d.AddNode("a")
        with pytest.raises(ValueError):
            d.Connect(a, 3.14)

    def test_invalid_parent_type_none(self):
        d = Dag()
        b = d.AddNode("b")
        with pytest.raises(ValueError):
            d.Connect(None, b)

    def test_invalid_child_type_none(self):
        d = Dag()
        a = d.AddNode("a")
        with pytest.raises(ValueError):
            d.Connect(a, None)

    def test_invalid_type_in_parent_list(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        with pytest.raises(ValueError):
            d.Connect([a, 3.14], b)

    def test_invalid_type_in_child_list(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        with pytest.raises(ValueError):
            d.Connect(a, [b, 3.14])

    def test_invalid_type_in_parent_set(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        with pytest.raises(ValueError):
            d.Connect({a, 3.14}, b)

    def test_invalid_type_in_child_set(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        with pytest.raises(ValueError):
            d.Connect(a, {b, 3.14})


class TestDagRemove:
    def test_returns_true_on_success(self):
        d = Dag()
        d.AddNode("a")
        assert d.Remove("a") is True

    def test_returns_false_on_missing(self):
        assert Dag().Remove("missing") is False

    def test_invalid_type(self):
        with pytest.raises(ValueError):
            Dag().Remove(0)

    def test_node_no_longer_accessible(self):
        d = Dag()
        d.AddNode("a")
        d.Remove("a")
        assert d["a"] is None

    def test_cleans_child_reference(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        d.Connect(a, b)
        d.Remove("b")
        assert a.children is None

    def test_cleans_parent_reference(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        d.Connect(a, b)
        d.Remove("a")
        assert b.parents is None

    def test_remove_middle_node_cleans_both_sides(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        d.Connect(a, b)
        d.Connect(b, c)
        d.Remove("b")
        assert a.children is None
        assert c.parents is None

    def test_remove_decrements_size(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.Remove("a")
        assert d.size == 1
        assert len(d) == 1

    def test_remove_does_not_affect_other_node_by_id(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.AddNode("c")
        d.Remove("b")
        assert d[2] is not None
        assert d[2].name == "c"

    def test_remove_first_node_leaves_rest_accessible(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.Remove("a")
        assert d["b"] is not None
        assert d["b"].id == 1

    def test_remove_last_node_leaves_rest_accessible(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        d.Remove("b")
        assert d[0] is not None
        assert d[0].name == "a"
        assert d.size == 1

    def test_add_node_after_remove(self):
        d = Dag()
        d.AddNode("a")
        d.Remove("a")
        c = d.AddNode("c")
        assert c.name == "c"
        assert d["c"] is not None


class TestDagWalk:
    def _linear_dag(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        d.Connect(a, b)
        d.Connect(b, c)
        return d

    def test_bfs_visits_all(self):
        d = self._linear_dag()
        visited = []
        d.Walk(lambda n: visited.append(n.name), WalkOrder.BFS)
        assert set(visited) == {"a", "b", "c"}

    def test_dfs_visits_all(self):
        d = self._linear_dag()
        visited = []
        d.Walk(lambda n: visited.append(n.name), WalkOrder.DFS)
        assert set(visited) == {"a", "b", "c"}

    def test_bfs_order_linear(self):
        d = self._linear_dag()
        visited = []
        d.Walk(lambda n: visited.append(n.name), WalkOrder.BFS)
        assert visited == ["a", "b", "c"]

    def test_dfs_order_linear(self):
        d = self._linear_dag()
        visited = []
        d.Walk(lambda n: visited.append(n.name), WalkOrder.DFS)
        assert visited == ["a", "b", "c"]

    def test_each_node_visited_once(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        d.Connect(a, b)
        d.Connect(a, c)
        d.Connect(b, c)
        visited = []
        d.Walk(lambda n: visited.append(n.name))
        assert visited.count("c") == 1

    def test_walk_after_remove(self):
        d = self._linear_dag()
        d.Remove("b")
        visited = []
        d.Walk(lambda n: visited.append(n.name))
        assert "b" not in visited

    def test_empty_dag(self):
        visited = []
        Dag().Walk(lambda n: visited.append(n.name))
        assert visited == []

    def test_single_node(self):
        d = Dag()
        d.AddNode("a")
        visited = []
        d.Walk(lambda n: visited.append(n.name))
        assert visited == ["a"]

    def test_walk_resets_between_calls(self):
        d = self._linear_dag()
        first = []
        d.Walk(lambda n: first.append(n.name))
        second = []
        d.Walk(lambda n: second.append(n.name))
        assert first == second

    def test_multiple_roots_visits_all(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        e = d.AddNode("e")
        d.Connect(a, b)
        d.Connect(c, e)
        visited = []
        d.Walk(lambda n: visited.append(n.name))
        assert set(visited) == {"a", "b", "c", "e"}

    def test_default_order_is_bfs(self):
        d = self._linear_dag()
        visited = []
        d.Walk(lambda n: visited.append(n.name))
        assert visited == ["a", "b", "c"]

    def _diamond_dag(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        e = d.AddNode("e")
        d.Connect(a, b)
        d.Connect(a, c)
        d.Connect(b, e)
        d.Connect(c, e)
        return d

    def test_bfs_order_diamond(self):
        d = self._diamond_dag()
        visited = []
        d.Walk(lambda n: visited.append(n.name), WalkOrder.BFS)
        assert visited[0] == "a"
        assert set(visited[1:3]) == {"b", "c"}
        assert visited[-1] == "e"

    def test_dfs_order_diamond(self):
        d = self._diamond_dag()
        visited = []
        d.Walk(lambda n: visited.append(n.name), WalkOrder.DFS)
        assert visited[0] == "a"
        assert visited[1] in {"b", "c"}
        assert visited[2] == "e"
        assert visited[3] in {"b", "c"}
        assert visited[1] != visited[3]


class TestDagCycle:
    def test_no_cycle_linear(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        d.Connect(a, b)
        d.Connect(b, c)
        assert d.Cycle() is False

    def test_no_cycle_diamond(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        e = d.AddNode("e")
        d.Connect(a, b)
        d.Connect(a, c)
        d.Connect(b, e)
        d.Connect(c, e)
        assert d.Cycle() is False

    def test_no_cycle_disconnected(self):
        d = Dag()
        d.AddNode("a")
        d.AddNode("b")
        assert d.Cycle() is False

    def test_reachable_cycle(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        d.Connect(a, b)
        d.Connect(b, c)
        d.Connect(c, b)
        assert d.Cycle() is True

    def test_disjoint_cycle(self):
        d = Dag()
        d.AddNode("root")
        b = d.AddNode("b")
        c = d.AddNode("c")
        d.Connect(b, c)
        d.Connect(c, b)
        assert d.Cycle() is True

    def test_cycle_removed(self):
        d = Dag()
        a = d.AddNode("a")
        b = d.AddNode("b")
        c = d.AddNode("c")
        d.Connect(a, b)
        d.Connect(b, c)
        d.Connect(c, b)
        d.Remove("c")
        assert d.Cycle() is False

    def test_single_node_no_cycle(self):
        d = Dag()
        d.AddNode("a")
        assert d.Cycle() is False

    def test_self_cycle(self):
        d = Dag()
        a = d.AddNode("a")
        d.Connect(a, a)
        assert d.Cycle() is True
