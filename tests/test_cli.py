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

import sys
import json
import fcntl
import pytest
from pathlib import Path
from unittest.mock import patch

from htflow.__main__ import main
from htflow.utils.directory import ChangeDir


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def run_cli(*args, cwd=None):
    """Invoke main() with the given CLI args; returns the exit code (0 on success).

    Argument order follows argparse layout:
        [global-flags]  command  [command-flags]  [positional-args]
    Global flags (--no-log, --log-level, --log-file) go before the command name.
    Command flags (--jdl, --job-shapes) go after the command name.
    """
    argv = ["htflow", "--no-log", *args]
    with patch.object(sys, "argv", argv):
        try:
            if cwd is not None:
                with ChangeDir(cwd):
                    main()
            else:
                main()
            return 0
        except SystemExit as e:
            return e.code


# ---------------------------------------------------------------------------
# convert
# ---------------------------------------------------------------------------

class TestConvert:
    def test_exit_code_success(self, make_sub, tmp_path):
        a = make_sub("a")
        dag = tmp_path / "out.dag"
        assert run_cli("convert", str(dag), "--jdl", str(a)) == 0

    def test_creates_dag_file(self, make_sub, tmp_path):
        a = make_sub("a")
        dag = tmp_path / "out.dag"
        run_cli("convert", str(dag), "--jdl", str(a))
        assert dag.exists()

    def test_dag_contains_job_lines(self, make_sub, tmp_path):
        a = make_sub("a")
        b = make_sub("b")
        dag = tmp_path / "out.dag"
        run_cli("convert", str(dag), "--jdl", str(a), str(b))
        content = dag.read_text()
        assert "JOB NODE-0" in content
        assert "JOB NODE-1" in content

    def test_dag_contains_parent_child(self, make_sub, tmp_path):
        a = make_sub("a", outputs=["link.txt"])
        b = make_sub("b", inputs=["link.txt"])
        dag = tmp_path / "out.dag"
        run_cli("convert", str(dag), "--jdl", str(a), str(b))
        assert "PARENT NODE-0 CHILD NODE-1" in dag.read_text()

    def test_default_filename(self, make_sub, tmp_path):
        a = make_sub("a")
        run_cli("convert", "--jdl", str(a), cwd=tmp_path)
        assert (tmp_path / "dataflow.dag").exists()

    def test_with_job_shapes(self, make_sub, tmp_path):
        a = make_sub("a", extra="JobType = worker")
        b = make_sub("b", inputs=["out.txt"])
        shapes_file = tmp_path / "shapes.json"
        shapes_file.write_text(json.dumps({"worker": {"OutputFiles": "out.txt"}}))
        dag = tmp_path / "out.dag"
        run_cli("convert", str(dag), "--jdl", str(a), str(b), "--job-shapes", str(shapes_file), cwd=tmp_path)
        assert "PARENT NODE-0 CHILD NODE-1" in dag.read_text()

    def test_prints_output_path(self, make_sub, tmp_path, capsys):
        a = make_sub("a")
        dag = tmp_path / "out.dag"
        run_cli("convert", str(dag), "--jdl", str(a))
        assert str(dag) in capsys.readouterr().out


# ---------------------------------------------------------------------------
# show files
# ---------------------------------------------------------------------------

class TestShowFiles:
    def test_exit_code_success(self, make_sub):
        a = make_sub("a", outputs=["x.txt"])
        assert run_cli("show", "files", "--jdl", str(a)) == 0

    def test_prints_root_files(self, make_sub, capsys):
        a = make_sub("a", inputs=["ext.txt"])
        run_cli("show", "files", "--jdl", str(a))
        assert "ext.txt" in capsys.readouterr().out

    def test_prints_leaf_files(self, make_sub, capsys):
        a = make_sub("a", outputs=["out.txt"])
        run_cli("show", "files", "--jdl", str(a))
        assert "out.txt" in capsys.readouterr().out

    def test_prints_intermediate_files(self, make_sub, capsys):
        a = make_sub("a", outputs=["mid.txt"])
        b = make_sub("b", inputs=["mid.txt"])
        run_cli("show", "files", "--jdl", str(a), str(b))
        assert "mid.txt" in capsys.readouterr().out

    def test_shows_protocol_header(self, make_sub, capsys):
        a = make_sub("a", outputs=["x.txt"])
        run_cli("show", "files", "--jdl", str(a))
        assert "CEDAR files in dataflow" in capsys.readouterr().out

    def test_shows_table_header(self, make_sub, capsys):
        a = make_sub("a", outputs=["x.txt"])
        run_cli("show", "files", "--jdl", str(a))
        out = capsys.readouterr().out
        assert "Gen" in out
        assert "Consumers" in out

    def test_gen_dash_for_external_input(self, make_sub, capsys):
        a = make_sub("a", inputs=["ext.txt"])
        run_cli("show", "files", "--jdl", str(a))
        line = next(l for l in capsys.readouterr().out.splitlines() if "ext.txt" in l)
        assert "  -  " in line

    def test_gen_T_for_produced_file(self, make_sub, capsys):
        a = make_sub("a", outputs=["out.txt"])
        run_cli("show", "files", "--jdl", str(a))
        line = next(l for l in capsys.readouterr().out.splitlines() if "out.txt" in l)
        assert "  T  " in line

    def test_consumer_count(self, make_sub, capsys):
        a = make_sub("a", outputs=["x.txt"])
        b = make_sub("b", inputs=["x.txt"])
        c = make_sub("c", inputs=["x.txt"])
        run_cli("show", "files", "--jdl", str(a), str(b), str(c))
        line = next(l for l in capsys.readouterr().out.splitlines() if "x.txt" in l)
        assert "2" in line


# ---------------------------------------------------------------------------
# show types
# ---------------------------------------------------------------------------

class TestShowTypes:
    def test_no_job_type_prints_empty_message(self, make_sub, capsys):
        a = make_sub("a", outputs=["x.txt"])
        run_cli("show", "types", "--jdl", str(a))
        assert "No job types defined" in capsys.readouterr().out

    def test_prints_defined_types(self, make_sub, capsys):
        a = make_sub("a", extra="JobType = worker")
        run_cli("show", "types", "--jdl", str(a))
        assert "worker" in capsys.readouterr().out

    def test_with_job_shapes_loaded(self, make_sub, tmp_path):
        a = make_sub("a", extra="JobType = worker")
        shapes_file = tmp_path / "shapes.json"
        shapes_file.write_text(json.dumps({"worker": {}}))
        assert run_cli("show", "types", "--jdl", str(a), "--job-shapes", str(shapes_file)) == 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# --dir input source
# ---------------------------------------------------------------------------

class TestDirInput:
    def test_dir_flag_success(self, make_sub, tmp_path):
        make_sub("a", outputs=["x.txt"])
        dag = tmp_path / "out.dag"
        assert run_cli("convert", str(dag), "--dir", str(tmp_path)) == 0

    def test_dir_creates_dag(self, make_sub, tmp_path):
        make_sub("a", outputs=["x.txt"])
        dag = tmp_path / "out.dag"
        run_cli("convert", str(dag), "--dir", str(tmp_path))
        assert dag.exists()

    def test_no_input_source_exits_2(self, tmp_path):
        dag = tmp_path / "out.dag"
        assert run_cli("convert", str(dag)) == 2

    def test_empty_dir_exits_2(self, tmp_path):
        dag = tmp_path / "out.dag"
        empty = tmp_path / "empty"
        empty.mkdir()
        assert run_cli("convert", str(dag), "--dir", str(empty)) == 2

    def test_jdl_and_dir_combined(self, make_sub, tmp_path):
        a = make_sub("a", outputs=["link.txt"])
        d = tmp_path / "more"
        d.mkdir()
        b = make_sub("b", inputs=["link.txt"])
        b.rename(d / "b.sub")
        dag = tmp_path / "out.dag"
        assert run_cli("convert", str(dag), "--jdl", str(a), "--dir", str(d)) == 0

    def test_jdl_flag_repeated(self, make_sub, tmp_path):
        a = make_sub("a", outputs=["link.txt"])
        b = make_sub("b", inputs=["link.txt"])
        dag = tmp_path / "out.dag"
        code = run_cli("convert", str(dag), "--jdl", str(a), "--jdl", str(b))
        assert code == 0
        assert "JOB NODE-0" in dag.read_text()
        assert "JOB NODE-1" in dag.read_text()

    def test_dir_flag_repeated(self, make_sub, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        a = make_sub("a")
        a.rename(d1 / "a.sub")
        b = make_sub("b")
        b.rename(d2 / "b.sub")
        dag = tmp_path / "out.dag"
        code = run_cli("convert", str(dag), "--dir", str(d1), "--dir", str(d2))
        assert code == 0
        assert "JOB NODE-0" in dag.read_text()
        assert "JOB NODE-1" in dag.read_text()


class TestErrorHandling:
    def test_missing_jdl_file_exits_125(self, tmp_path):
        dag = tmp_path / "out.dag"
        code = run_cli("convert", str(dag), "--jdl", str(tmp_path / "nonexistent.sub"))
        assert code == 125

    def test_assumption_violation_exits_125(self, make_sub, tmp_path):
        a = make_sub("a", outputs=["dup.txt"])
        b = make_sub("b", outputs=["dup.txt"])
        dag = tmp_path / "out.dag"
        code = run_cli("convert", str(dag), "--jdl", str(a), str(b))
        assert code == 125

    def test_invalid_job_shapes_path_exits_125(self, make_sub, tmp_path):
        a = make_sub("a")
        dag = tmp_path / "out.dag"
        code = run_cli("convert", str(dag), "--jdl", str(a), "--job-shapes", str(tmp_path / "nope.json"))
        assert code == 125

    def test_invalid_job_shapes_json_exits_125(self, make_sub, tmp_path):
        a = make_sub("a")
        bad_json = tmp_path / "bad.json"
        bad_json.write_text("not valid json {{{")
        dag = tmp_path / "out.dag"
        code = run_cli("convert", str(dag), "--jdl", str(a), "--job-shapes", str(bad_json))
        assert code == 125


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------

class TestCleanup:
    def test_no_directory_exits_success(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert run_cli("cleanup") == 0

    def test_no_directory_prints_message(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        run_cli("cleanup")
        assert "Nothing to clean up" in capsys.readouterr().out

    def test_removes_directory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "flowman").mkdir()
        run_cli("cleanup")
        assert not (tmp_path / "flowman").exists()

    def test_removes_state_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        flowman = tmp_path / "flowman"
        flowman.mkdir()
        (flowman / "manual.state").write_text("*** FINISHED 1234.0 a.sub\n")
        run_cli("cleanup")
        assert not flowman.exists()

    def test_locked_exits_engine_active(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        flowman = tmp_path / "flowman"
        flowman.mkdir()
        lock_file = flowman / "flowman.lock"
        lock_file.touch()
        fp = open(lock_file, "w")
        fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            assert run_cli("cleanup") == 75
        finally:
            fcntl.flock(fp, fcntl.LOCK_UN)
            fp.close()
