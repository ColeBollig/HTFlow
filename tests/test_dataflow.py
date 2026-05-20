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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def make_sub(tmp_path):
    def _make(name, *, inputs=None, outputs=None, extra=""):
        lines = ["executable = example.sh"]
        if inputs:
            lines.append(f"transfer_input_files = {','.join(inputs)}")
        if outputs:
            lines.append(f"transfer_output_files = {','.join(outputs)}")
        if extra:
            lines.append(extra)
        lines.append("queue")
        p = tmp_path / f"{name}.sub"
        p.write_text("\n".join(lines) + "\n")
        return p
    return _make


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

class TestInit:
    def test_defaults(self):
        df = HTCondorDataFlow()
        assert df.files == []
        assert df.filename == "dataflow.dag"

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
        assert str(a) in content

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

    def test_write_overwrites_not_appends(self, make_sub, tmp_path):
        a = make_sub("a")
        dag_path = tmp_path / "out.dag"
        df = HTCondorDataFlow(files=[a], filename=str(dag_path))
        df.write()
        df.write()
        content = dag_path.read_text()
        assert content.count("JOB NODE-0") == 1
