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
from pathlib import Path
from unittest.mock import patch, MagicMock
from htflow.dataflow import HTCondorDataFlow, AssumptionError, Assumption
from htflow.utils.directory import ChangeDir


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

class TestInit:
    def test_defaults(self):
        df = HTCondorDataFlow()
        assert df.files == []
        assert df.filename == "dataflow.dag"
        assert df.shapes == {}

    def test_custom_job_shapes_stored(self):
        shapes = {"worker": {"InputFiles": "a.txt"}}
        assert HTCondorDataFlow(job_shapes=shapes).shapes == shapes

    def test_invalid_job_shapes_not_dict(self):
        with pytest.raises(ValueError):
            HTCondorDataFlow(job_shapes="bad")

    def test_invalid_job_shapes_key_not_string(self):
        with pytest.raises(ValueError):
            HTCondorDataFlow(job_shapes={1: {"InputFiles": "a.txt"}})

    def test_invalid_job_shapes_value_not_dict(self):
        with pytest.raises(ValueError):
            HTCondorDataFlow(job_shapes={"worker": "flat"})

    def test_invalid_job_shapes_nested_value_not_string(self):
        with pytest.raises(ValueError):
            HTCondorDataFlow(job_shapes={"worker": {"InputFiles": 42}})

    def test_file_list_strings_and_paths(self, tmp_path):
        f = tmp_path / "a.sub"
        f.touch()
        df = HTCondorDataFlow(files=[str(f), f])
        assert df.files == [f, f]

    def test_custom_filename(self):
        assert HTCondorDataFlow(filename="out.dag").filename == "out.dag"

    def test_invalid_files_not_list(self):
        with pytest.raises(ValueError):
            HTCondorDataFlow(files="a.sub")

    def test_invalid_files_list_contains_int(self):
        with pytest.raises(ValueError):
            HTCondorDataFlow(files=[1])

    def test_invalid_filename_type(self):
        with pytest.raises(ValueError):
            HTCondorDataFlow(filename=42)


# ---------------------------------------------------------------------------
# shapes property
# ---------------------------------------------------------------------------

class TestShapesProperty:
    def test_getter_returns_stored_shapes(self):
        shapes = {"mytype": {"OutputFiles": "out.txt"}}
        assert HTCondorDataFlow(job_shapes=shapes).shapes == shapes

    def test_setter_replaces_shapes(self):
        df = HTCondorDataFlow()
        shapes = {"mytype": {"InputFiles": "in.txt"}}
        df.shapes = shapes
        assert df.shapes == shapes

    def test_setter_invalid_raises(self):
        with pytest.raises(ValueError):
            HTCondorDataFlow().shapes = "not a dict"


# ---------------------------------------------------------------------------
# types property
# ---------------------------------------------------------------------------

class TestTypesProperty:
    def test_empty_when_no_files(self):
        assert HTCondorDataFlow().types == set()

    def test_empty_when_no_job_type_key(self, make_sub):
        f = make_sub("a", outputs=["x.txt"])
        assert HTCondorDataFlow(files=[f]).types == set()

    def test_single_type(self, make_sub):
        f = make_sub("a", extra="JobType = worker")
        assert HTCondorDataFlow(files=[f]).types == {"worker"}

    def test_multiple_distinct_types(self, make_sub):
        a = make_sub("a", extra="JobType = alpha")
        b = make_sub("b", extra="JobType = beta")
        assert HTCondorDataFlow(files=[a, b]).types == {"alpha", "beta"}

    def test_deduplicates_same_type(self, make_sub):
        a = make_sub("a", extra="JobType = worker")
        b = make_sub("b", extra="JobType = worker")
        assert HTCondorDataFlow(files=[a, b]).types == {"worker"}


# ---------------------------------------------------------------------------
# files property
# ---------------------------------------------------------------------------

class TestFilesProperty:
    def test_getter_returns_paths(self, tmp_path):
        f = tmp_path / "a.sub"
        f.touch()
        df = HTCondorDataFlow(files=[str(f)])
        assert all(isinstance(x, Path) for x in df.files)

    def test_setter_replaces_list(self, tmp_path):
        f1 = tmp_path / "a.sub"; f1.touch()
        f2 = tmp_path / "b.sub"; f2.touch()
        df = HTCondorDataFlow(files=[str(f1)])
        df.files = [str(f2)]
        assert df.files == [f2]

    def test_setter_converts_strings_to_paths(self, tmp_path):
        f = tmp_path / "a.sub"; f.touch()
        df = HTCondorDataFlow()
        df.files = [str(f)]
        assert df.files == [f]

    def test_setter_invalid_type(self):
        with pytest.raises(ValueError):
            df = HTCondorDataFlow()
            df.files = "bad"

    def test_iadd_string(self, tmp_path):
        f = tmp_path / "a.sub"; f.touch()
        df = HTCondorDataFlow()
        df += str(f)
        assert df.files == [f]

    def test_iadd_path(self, tmp_path):
        f = tmp_path / "a.sub"; f.touch()
        df = HTCondorDataFlow()
        df += f
        assert df.files == [f]

    def test_iadd_invalid_returns_not_implemented(self):
        result = HTCondorDataFlow().__iadd__(42)
        assert result is NotImplemented

    def test_add_appends(self, tmp_path):
        f = tmp_path / "a.sub"; f.touch()
        df = HTCondorDataFlow()
        df.add(f)
        assert df.files == [f]

    def test_add_invalid_type(self):
        with pytest.raises(ValueError):
            HTCondorDataFlow().add(99)


# ---------------------------------------------------------------------------
# filename property
# ---------------------------------------------------------------------------

class TestFilenameProperty:
    def test_getter_setter_roundtrip(self):
        df = HTCondorDataFlow()
        df.filename = "custom.dag"
        assert df.filename == "custom.dag"

    def test_setter_invalid_type(self):
        with pytest.raises(ValueError):
            df = HTCondorDataFlow()
            df.filename = 123


# ---------------------------------------------------------------------------
# generate()
# ---------------------------------------------------------------------------

class TestGenerate:
    def test_empty_file_list(self):
        d = HTCondorDataFlow().generate()
        assert len(d) == 0

    def test_single_file_no_transfers(self, make_sub):
        f = make_sub("a")
        d = HTCondorDataFlow(files=[f]).generate()
        assert len(d) == 1
        assert d[0].children is None
        assert d[0].parents is None

    def test_two_independent_files(self, make_sub):
        a = make_sub("a", outputs=["x.txt"])
        b = make_sub("b", outputs=["y.txt"])
        d = HTCondorDataFlow(files=[a, b]).generate()
        assert len(d) == 2
        assert d[0].children is None
        assert d[1].children is None

    def test_linear_chain(self, make_sub):
        a = make_sub("a", outputs=["link.txt"])
        b = make_sub("b", inputs=["link.txt"])
        d = HTCondorDataFlow(files=[a, b]).generate()
        assert d[1].id in d[0].children
        assert d[0].id in d[1].parents

    def test_diamond(self, make_sub):
        a = make_sub("a", outputs=["shared.txt"])
        b = make_sub("b", inputs=["shared.txt"], outputs=["b_out.txt"])
        c = make_sub("c", inputs=["shared.txt"], outputs=["c_out.txt"])
        e = make_sub("e", inputs=["b_out.txt", "c_out.txt"])
        dag = HTCondorDataFlow(files=[a, b, c, e]).generate()
        assert dag[1].id in dag[0].children
        assert dag[2].id in dag[0].children
        assert dag[3].id in dag[1].children
        assert dag[3].id in dag[2].children

    def test_comma_separated_list_with_spaces(self, tmp_path):
        # HTCondor format allows spaces around commas
        f = tmp_path / "a.sub"
        f.write_text("executable = x\ntransfer_output_files = x.txt , y.txt\nqueue\n")
        g = tmp_path / "b.sub"
        g.write_text("executable = x\ntransfer_input_files = x.txt\nqueue\n")
        dag = HTCondorDataFlow(files=[f, g]).generate()
        assert dag[1].id in dag[0].children

    def test_node_internal_is_jdl_path(self, make_sub):
        f = make_sub("a", outputs=["x.txt"])
        d = HTCondorDataFlow(files=[f]).generate()
        assert d[0].internal == f

    def test_generate_is_idempotent(self, make_sub):
        a = make_sub("a", outputs=["x.txt"])
        b = make_sub("b", inputs=["x.txt"])
        df = HTCondorDataFlow(files=[a, b])
        d1 = df.generate()
        d2 = df.generate()
        assert len(d1) == len(d2) == 2

    def test_node_names_are_sequential(self, make_sub):
        a = make_sub("a")
        b = make_sub("b")
        d = HTCondorDataFlow(files=[a, b]).generate()
        assert d[0].name == "NODE-0"
        assert d[1].name == "NODE-1"


# ---------------------------------------------------------------------------
# Assumption violations
# ---------------------------------------------------------------------------

class TestAssumptions:
    def test_single_file_src_raises(self, make_sub):
        a = make_sub("a", outputs=["dup.txt"])
        b = make_sub("b", outputs=["dup.txt"])
        with pytest.raises(AssumptionError) as exc:
            HTCondorDataFlow(files=[a, b]).generate()
        assert exc.value.assumption == Assumption.SINGLE_FILE_SRC

    def test_url_in_input_files(self, tmp_path):
        f = tmp_path / "url_in.sub"
        f.write_text("executable = x\ntransfer_input_files = http://host/file.txt\nqueue\n")
        with pytest.raises(AssumptionError) as exc:
            HTCondorDataFlow(files=[f]).generate()
        assert exc.value.assumption == Assumption.NO_URL

    def test_url_in_output_files(self, tmp_path):
        f = tmp_path / "url_out.sub"
        f.write_text("executable = x\ntransfer_output_files = s3://bucket/file.txt\nqueue\n")
        with pytest.raises(AssumptionError) as exc:
            HTCondorDataFlow(files=[f]).generate()
        assert exc.value.assumption == Assumption.NO_URL

    def test_output_destination(self, tmp_path):
        f = tmp_path / "dst.sub"
        f.write_text("executable = x\noutput_destination = s3://bucket/\nqueue\n")
        with pytest.raises(AssumptionError) as exc:
            HTCondorDataFlow(files=[f]).generate()
        assert exc.value.assumption == Assumption.NO_URL

    def test_output_directory(self, tmp_path):
        f = tmp_path / "dir.sub"
        f.write_text("executable = x\noutput_directory = /tmp/out\nqueue\n")
        with pytest.raises(AssumptionError) as exc:
            HTCondorDataFlow(files=[f]).generate()
        assert exc.value.assumption == Assumption.NO_DIRECTORIES

    def test_transfer_output_remaps(self, tmp_path):
        f = tmp_path / "remap.sub"
        f.write_text(
            "executable = x\n"
            "transfer_output_files = a.txt\n"
            "transfer_output_remaps = a.txt=/new/a.txt\n"
            "queue\n"
        )
        with pytest.raises(AssumptionError) as exc:
            HTCondorDataFlow(files=[f]).generate()
        assert exc.value.assumption == Assumption.NO_REMAPS

    def test_macro_in_input_files(self, tmp_path):
        # htcondor2.Submit.expand() resolves macros defined in the file; it may
        # silently drop undefined ones. Patch expand() to simulate a macro that
        # survives expansion (e.g. a late-binding job attribute like $(Process)).
        f = tmp_path / "macro.sub"
        f.write_text("executable = x\ntransfer_input_files = $(Process).txt\nqueue\n")
        mock_desc = MagicMock()
        mock_desc.get.side_effect = (
            lambda key: "$(Process).txt" if key == "transfer_input_files" else None
        )
        mock_desc.expand.side_effect = (
            lambda key: "$(Process).txt" if key == "transfer_input_files" else ""
        )
        with patch("htflow.dataflow.htcondor2.Submit", return_value=mock_desc):
            with pytest.raises(AssumptionError) as exc:
                HTCondorDataFlow(files=[f]).generate()
        assert exc.value.assumption == Assumption.NO_MACROS

    def test_assumption_error_source_is_jdl(self, tmp_path):
        f = tmp_path / "a.sub"
        g = tmp_path / "b.sub"
        f.write_text("executable = x\ntransfer_output_files = same.txt\nqueue\n")
        g.write_text("executable = x\ntransfer_output_files = same.txt\nqueue\n")
        with pytest.raises(AssumptionError) as exc:
            HTCondorDataFlow(files=[f, g]).generate()
        assert exc.value.source in (f, g)

    def test_assumption_error_is_exception(self, make_sub):
        a = make_sub("a", outputs=["dup.txt"])
        b = make_sub("b", outputs=["dup.txt"])
        with pytest.raises(Exception):
            HTCondorDataFlow(files=[a, b]).generate()

    def test_osdf_url_in_input_files_allowed(self, tmp_path):
        f = tmp_path / "osdf_ok.sub"
        f.write_text("executable = x\ntransfer_input_files = osdf://host/file.txt\nqueue\n")
        HTCondorDataFlow(files=[f]).generate()

    def test_pelican_url_in_input_files_allowed(self, tmp_path):
        f = tmp_path / "pelican_ok.sub"
        f.write_text("executable = x\ntransfer_input_files = pelican://host/file.txt\nqueue\n")
        HTCondorDataFlow(files=[f]).generate()

    def test_osdf_triple_slash_url_preserved(self, tmp_path):
        """osdf:///path (triple-slash) must not be normalized to osdf:/path by Path()."""
        url = "osdf:///my-federation/some-file.txt"
        f = tmp_path / "osdf_triple.sub"
        f.write_text(f"executable = x\ntransfer_input_files = {url}\nqueue\n")
        df = HTCondorDataFlow(files=[f])
        df.generate()
        assert url in df.mapping, f"URL key was normalized — got {list(df.mapping.keys())}"

    def test_disallowed_url_after_allowed_in_list_raises(self, tmp_path):
        """A disallowed URL later in the comma-separated list must still be caught."""
        f = tmp_path / "mixed.sub"
        f.write_text(
            "executable = x\n"
            "transfer_input_files = osdf://good.txt, http://evil.txt\n"
            "queue\n"
        )
        with pytest.raises(AssumptionError) as exc:
            HTCondorDataFlow(files=[f]).generate()
        assert exc.value.assumption == Assumption.NO_URL


# ---------------------------------------------------------------------------
# Cycle detection
# ---------------------------------------------------------------------------

class TestCycleDetection:
    def test_direct_cycle_raises(self, make_sub):
        # a produces x.txt and consumes y.txt; b produces y.txt and consumes x.txt
        a = make_sub("a", inputs=["y.txt"], outputs=["x.txt"])
        b = make_sub("b", inputs=["x.txt"], outputs=["y.txt"])
        with pytest.raises(RuntimeError, match="cycle"):
            HTCondorDataFlow(files=[a, b]).generate()


# ---------------------------------------------------------------------------
# groupings property
# ---------------------------------------------------------------------------

class TestGroupings:
    def test_empty(self):
        df = HTCondorDataFlow()
        df.generate()
        roots, intermediate, leafs = df.groupings
        assert roots == [] and intermediate == [] and leafs == []

    def test_unconsumed_output_is_leaf(self, make_sub):
        a = make_sub("a", outputs=["orphan.txt"])
        df = HTCondorDataFlow(files=[a])
        df.generate()
        _, _, leafs = df.groupings
        assert Path("orphan.txt") in leafs

    def test_external_input_is_root(self, make_sub):
        a = make_sub("a", inputs=["ext.txt"])
        df = HTCondorDataFlow(files=[a])
        df.generate()
        roots, _, _ = df.groupings
        assert Path("ext.txt") in roots

    def test_shared_file_is_intermediate(self, make_sub):
        a = make_sub("a", outputs=["mid.txt"])
        b = make_sub("b", inputs=["mid.txt"])
        df = HTCondorDataFlow(files=[a, b])
        df.generate()
        _, intermediate, _ = df.groupings
        assert Path("mid.txt") in intermediate

    def test_all_three_groups_in_combined_scenario(self, make_sub):
        a = make_sub("a", inputs=["ext.txt"], outputs=["mid.txt", "orphan.txt"])
        b = make_sub("b", inputs=["mid.txt"])
        df = HTCondorDataFlow(files=[a, b])
        df.generate()
        roots, intermediate, leafs = df.groupings
        assert Path("ext.txt") in roots
        assert Path("mid.txt") in intermediate
        assert Path("orphan.txt") in leafs


# ---------------------------------------------------------------------------
# mapping property
# ---------------------------------------------------------------------------

class TestMapping:
    def test_empty_before_generate(self):
        assert HTCondorDataFlow().mapping == {}

    def test_populated_after_generate(self, make_sub):
        a = make_sub("a", outputs=["x.txt"])
        b = make_sub("b", inputs=["x.txt"])
        df = HTCondorDataFlow(files=[a, b])
        df.generate()
        assert Path("x.txt") in df.mapping

    def test_source_node_id(self, make_sub):
        a = make_sub("a", outputs=["x.txt"])
        b = make_sub("b", inputs=["x.txt"])
        df = HTCondorDataFlow(files=[a, b])
        df.generate()
        src, _ = df.mapping[Path("x.txt")]
        assert src == 0

    def test_dependent_node_ids(self, make_sub):
        a = make_sub("a", outputs=["x.txt"])
        b = make_sub("b", inputs=["x.txt"])
        df = HTCondorDataFlow(files=[a, b])
        df.generate()
        _, deps = df.mapping[Path("x.txt")]
        assert 1 in deps

    def test_external_file_has_no_source(self, make_sub):
        a = make_sub("a", inputs=["ext.txt"])
        df = HTCondorDataFlow(files=[a])
        df.generate()
        src, _ = df.mapping[Path("ext.txt")]
        assert src is None

    def test_reset_between_generate_calls(self, make_sub):
        a = make_sub("a", outputs=["x.txt"])
        df = HTCondorDataFlow(files=[a])
        df.generate()
        df.generate()
        assert list(df.mapping.keys()) == [Path("x.txt")]


# ---------------------------------------------------------------------------
# write()
# ---------------------------------------------------------------------------

class TestWrite:
    def test_returns_path(self, make_sub, tmp_path):
        f = make_sub("a")
        df = HTCondorDataFlow(files=[f], filename=str(tmp_path / "out.dag"))
        assert isinstance(df.write(), Path)

    def test_file_exists_after_write(self, make_sub, tmp_path):
        f = make_sub("a")
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(files=[f], filename=str(dag_path)).write()
        assert dag_path.exists()

    def test_contains_job_entry_for_each_node(self, make_sub, tmp_path):
        a = make_sub("a")
        b = make_sub("b")
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(files=[a, b], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert "JOB NODE-0" in content
        assert "JOB NODE-1" in content

    def test_job_line_contains_jdl_path(self, make_sub, tmp_path):
        a = make_sub("a")
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(files=[a], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert a.name in content
        assert str(a.parent) in content

    def test_contains_parent_child_for_connected_nodes(self, make_sub, tmp_path):
        a = make_sub("a", outputs=["x.txt"])
        b = make_sub("b", inputs=["x.txt"])
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(files=[a, b], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert "PARENT NODE-0 CHILD NODE-1" in content

    def test_no_parent_child_for_independent_nodes(self, make_sub, tmp_path):
        a = make_sub("a", outputs=["x.txt"])
        b = make_sub("b", outputs=["y.txt"])
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(files=[a, b], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert "PARENT" not in content
        assert "# Node relationships determined by dataflow:" not in content

    def test_write_overwrites_not_appends(self, make_sub, tmp_path):
        a = make_sub("a")
        dag_path = tmp_path / "out.dag"
        df = HTCondorDataFlow(files=[a], filename=str(dag_path))
        df.write()
        df.write()
        content = dag_path.read_text()
        assert content.count("JOB NODE-0") == 1

    def test_job_line_dir_clause_added_for_absolute_path(self, make_sub, tmp_path):
        a = make_sub("a")
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(files=[a], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert f"DIR {a.parent}" in content
        assert f"JOB NODE-0 {a.name} DIR" in content

    def test_job_line_no_dir_when_jdl_in_cwd(self, tmp_path):
        sub = tmp_path / "a.sub"
        sub.write_text("executable = example.sh\nqueue\n")
        dag_path = tmp_path / "out.dag"
        with ChangeDir(tmp_path):
            HTCondorDataFlow(files=[sub], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert "DIR" not in content

    def test_job_line_dir_preserves_relative_path(self, tmp_path):
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        sub = subdir / "a.sub"
        sub.write_text("executable = example.sh\nqueue\n")
        dag_path = tmp_path / "out.dag"
        with ChangeDir(tmp_path):
            HTCondorDataFlow(files=[Path("subdir/a.sub")], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert "DIR subdir" in content
        assert "JOB NODE-0 a.sub DIR subdir" in content

    def test_shared_children_grouped_by_parent_set(self, make_sub, tmp_path):
        p1  = make_sub("p1",  outputs=["x.txt"])
        p2  = make_sub("p2",  outputs=["y.txt"])
        p3  = make_sub("p3",  outputs=["z.txt"])
        c1  = make_sub("c1",  inputs=["x.txt", "y.txt", "z.txt"])
        c2  = make_sub("c2",  inputs=["x.txt", "y.txt", "z.txt"])
        c10 = make_sub("c10", inputs=["x.txt"])
        c20 = make_sub("c20", inputs=["y.txt"])
        c30 = make_sub("c30", inputs=["z.txt"])
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(
            files=[p1, p2, p3, c1, c2, c10, c20, c30],
            filename=str(dag_path),
        ).write()
        content = dag_path.read_text()
        assert "PARENT NODE-0 NODE-1 NODE-2 CHILD NODE-3 NODE-4" in content
        assert "PARENT NODE-0 CHILD NODE-5" in content
        assert "PARENT NODE-1 CHILD NODE-6" in content
        assert "PARENT NODE-2 CHILD NODE-7" in content
        assert content.count("PARENT") == 4

    def test_distinct_children_written_separately(self, make_sub, tmp_path):
        a = make_sub("a", outputs=["x.txt"])
        b = make_sub("b", outputs=["y.txt"])
        c = make_sub("c", inputs=["x.txt"])
        d = make_sub("d", inputs=["y.txt"])
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(files=[a, b, c, d], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert content.count("PARENT") == 2
        assert "PARENT NODE-0 CHILD NODE-2" in content
        assert "PARENT NODE-1 CHILD NODE-3" in content

    def test_diamond_compresses_to_two_lines(self, make_sub, tmp_path):
        a = make_sub("a", outputs=["shared.txt"])
        b = make_sub("b", inputs=["shared.txt"], outputs=["b_out.txt"])
        c = make_sub("c", inputs=["shared.txt"], outputs=["c_out.txt"])
        e = make_sub("e", inputs=["b_out.txt", "c_out.txt"])
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(files=[a, b, c, e], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert content.count("PARENT") == 2
        assert "PARENT NODE-0 CHILD NODE-1 NODE-2" in content
        assert "PARENT NODE-1 NODE-2 CHILD NODE-3" in content

    def test_parent_child_lines_appear_after_all_job_lines(self, make_sub, tmp_path):
        a = make_sub("a", outputs=["x.txt"])
        b = make_sub("b", inputs=["x.txt"])
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(files=[a, b], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert content.rfind("JOB") < content.find("PARENT")

    def test_no_dataflow_relations_skips_relations_section(self, make_sub, tmp_path):
        a = make_sub("a")
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(files=[a], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert "PARENT" not in content
        assert "# Node relationships determined by dataflow:" not in content

    def test_relations_section_comment_present_when_relations_exist(self, make_sub, tmp_path):
        a = make_sub("a", outputs=["x.txt"])
        b = make_sub("b", inputs=["x.txt"])
        dag_path = tmp_path / "out.dag"
        HTCondorDataFlow(files=[a, b], filename=str(dag_path)).write()
        content = dag_path.read_text()
        assert "# Node relationships determined by dataflow:" in content


# ---------------------------------------------------------------------------
# Job type shapes
# ---------------------------------------------------------------------------

class TestJobTypeShapes:
    def test_unknown_job_type_raises_complete_list(self, make_sub):
        f = make_sub("a", extra="JobType = unknown")
        with pytest.raises(AssumptionError) as exc:
            HTCondorDataFlow(files=[f], job_shapes={}).generate()
        assert exc.value.assumption == Assumption.COMPLETE_LIST

    def test_known_job_type_no_error(self, make_sub):
        f = make_sub("a", extra="JobType = worker")
        HTCondorDataFlow(files=[f], job_shapes={"worker": {}}).generate()

    def test_no_job_type_key_unaffected_by_shapes(self, make_sub):
        f = make_sub("a", outputs=["x.txt"])
        HTCondorDataFlow(files=[f], job_shapes={"worker": {"OutputFiles": "extra.txt"}}).generate()

    def test_shape_output_creates_dag_edge(self, make_sub):
        a = make_sub("a", extra="JobType = worker")
        b = make_sub("b", inputs=["result.txt"])
        shapes = {"worker": {"OutputFiles": "result.txt"}}
        d = HTCondorDataFlow(files=[a, b], job_shapes=shapes).generate()
        assert d[1].id in d[0].children

    def test_shape_input_added_as_external_dependency(self, make_sub):
        a = make_sub("a", extra="JobType = worker")
        shapes = {"worker": {"InputFiles": "extra_in.txt"}}
        df = HTCondorDataFlow(files=[a], job_shapes=shapes)
        df.generate()
        roots, _, _ = df.groupings
        assert Path("extra_in.txt") in roots

    def test_shape_deduplicates_files_already_in_jdl(self, make_sub):
        a = make_sub("a", inputs=["shared.txt"], extra="JobType = worker")
        shapes = {"worker": {"InputFiles": "shared.txt"}}
        df = HTCondorDataFlow(files=[a], job_shapes=shapes)
        df.generate()
        _, deps = df.mapping[Path("shared.txt")]
        assert deps.count(0) == 1

    def test_resolved_jdl_written_when_shape_changes(self, make_sub):
        a = make_sub("a", extra="JobType = worker")
        shapes = {"worker": {"OutputFiles": "result.txt"}}
        d = HTCondorDataFlow(files=[a], job_shapes=shapes).generate()
        resolved = a.with_suffix(a.suffix + ".resolved")
        assert resolved.exists()
        assert d[0].internal == resolved

    def test_no_resolved_jdl_when_shape_has_no_file_keys(self, make_sub):
        a = make_sub("a", extra="JobType = worker")
        d = HTCondorDataFlow(files=[a], job_shapes={"worker": {}}).generate()
        assert not a.with_suffix(a.suffix + ".resolved").exists()
        assert d[0].internal == a

    def test_url_in_shape_input_raises(self, make_sub):
        a = make_sub("a", extra="JobType = worker")
        shapes = {"worker": {"InputFiles": "http://host/file.txt"}}
        with pytest.raises(AssumptionError) as exc:
            HTCondorDataFlow(files=[a], job_shapes=shapes).generate()
        assert exc.value.assumption == Assumption.NO_URL

    def test_url_in_shape_output_raises(self, make_sub):
        a = make_sub("a", extra="JobType = worker")
        shapes = {"worker": {"OutputFiles": "s3://bucket/file.txt"}}
        with pytest.raises(AssumptionError) as exc:
            HTCondorDataFlow(files=[a], job_shapes=shapes).generate()
        assert exc.value.assumption == Assumption.NO_URL

    def test_shape_both_input_and_output_files(self, make_sub):
        a = make_sub("a", extra="JobType = worker")
        b = make_sub("b", inputs=["result.txt"])
        shapes = {"worker": {"InputFiles": "external.txt", "OutputFiles": "result.txt"}}
        df = HTCondorDataFlow(files=[a, b], job_shapes=shapes)
        d = df.generate()
        assert d[1].id in d[0].children
        roots, _, _ = df.groupings
        assert Path("external.txt") in roots

    def test_generate_idempotent_with_shapes(self, make_sub):
        a = make_sub("a", extra="JobType = worker")
        b = make_sub("b", inputs=["result.txt"])
        shapes = {"worker": {"OutputFiles": "result.txt"}}
        df = HTCondorDataFlow(files=[a, b], job_shapes=shapes)
        d1 = df.generate()
        d2 = df.generate()
        assert d1[1].id in d1[0].children
        assert d2[1].id in d2[0].children
