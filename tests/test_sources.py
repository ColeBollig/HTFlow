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

import argparse
import logging
import pytest
from pathlib import Path

from htflow.sources import InputError, collect_jdl_files
from htflow.sources._registry import FILE_HANDLERS
from htflow.sources import from_jdl, from_dir


def make_args(**kwargs):
    defaults = {"jdl": None, "dir": None}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def make_sub(path: Path) -> Path:
    path.write_text("executable = example.sh\nqueue\n")
    return path


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_sub_registered(self):
        assert ".sub" in FILE_HANDLERS

    def test_sub_handler_returns_path(self, tmp_path):
        p = make_sub(tmp_path / "a.sub")
        assert FILE_HANDLERS[".sub"](p) == [p]


# ---------------------------------------------------------------------------
# from_jdl
# ---------------------------------------------------------------------------

class TestFromJdl:
    def test_active_when_jdl_given(self, tmp_path):
        assert from_jdl.active(make_args(jdl=[str(tmp_path / "a.sub")])) is True

    def test_inactive_when_jdl_none(self):
        assert from_jdl.active(make_args()) is False

    def test_resolve_returns_paths(self, tmp_path):
        p = make_sub(tmp_path / "a.sub")
        result = from_jdl.resolve(make_args(jdl=[str(p)]))
        assert result == [p]

    def test_resolve_multiple(self, tmp_path):
        a = make_sub(tmp_path / "a.sub")
        b = make_sub(tmp_path / "b.sub")
        result = from_jdl.resolve(make_args(jdl=[str(a), str(b)]))
        assert result == [a, b]


# ---------------------------------------------------------------------------
# from_dir
# ---------------------------------------------------------------------------

class TestFromDir:
    def test_active_when_dir_given(self, tmp_path):
        assert from_dir.active(make_args(dir=[str(tmp_path)])) is True

    def test_inactive_when_dir_none(self):
        assert from_dir.active(make_args()) is False

    def test_finds_sub_files(self, tmp_path):
        p = make_sub(tmp_path / "a.sub")
        result = from_dir.resolve(make_args(dir=[str(tmp_path)]))
        assert p in result

    def test_ignores_unsupported_extensions(self, tmp_path):
        make_sub(tmp_path / "a.sub")
        (tmp_path / "b.txt").write_text("ignored")
        result = from_dir.resolve(make_args(dir=[str(tmp_path)]))
        assert len(result) == 1
        assert (tmp_path / "b.txt") not in result

    def test_top_level_only(self, tmp_path):
        sub = tmp_path / "nested"
        sub.mkdir()
        make_sub(sub / "deep.sub")
        result = from_dir.resolve(make_args(dir=[str(tmp_path)]))
        assert result == []

    def test_warns_on_empty_dir(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="htflow.sources.from_dir"):
            result = from_dir.resolve(make_args(dir=[str(tmp_path)]))
        assert result == []
        assert "No supported files" in caplog.text

    def test_raises_input_error_for_nonexistent_dir(self, tmp_path):
        with pytest.raises(InputError, match="Not a directory"):
            from_dir.resolve(make_args(dir=[str(tmp_path / "nonexistent")]))

    def test_multiple_dirs(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        a = make_sub(d1 / "a.sub")
        b = make_sub(d2 / "b.sub")
        result = from_dir.resolve(make_args(dir=[str(d1), str(d2)]))
        assert set(result) == {a, b}


# ---------------------------------------------------------------------------
# collect_jdl_files
# ---------------------------------------------------------------------------

class TestCollectJdlFiles:
    def test_no_sources_raises(self):
        with pytest.raises(InputError, match="at least one input source"):
            collect_jdl_files(make_args())

    def test_jdl_only(self, tmp_path):
        p = make_sub(tmp_path / "a.sub")
        result = collect_jdl_files(make_args(jdl=[str(p)]))
        assert result == [p]

    def test_dir_only(self, tmp_path):
        p = make_sub(tmp_path / "a.sub")
        result = collect_jdl_files(make_args(dir=[str(tmp_path)]))
        assert p in result

    def test_dedup_warns_and_returns_once(self, tmp_path, caplog):
        p = make_sub(tmp_path / "a.sub")
        args = make_args(jdl=[str(p)], dir=[str(tmp_path)])
        with caplog.at_level(logging.WARNING, logger="htflow.sources.core"):
            result = collect_jdl_files(args)
        assert result.count(p) == 1
        assert "Duplicate" in caplog.text

    def test_no_files_found_raises(self, tmp_path):
        with pytest.raises(InputError, match="no JDL files found"):
            collect_jdl_files(make_args(dir=[str(tmp_path)]))

    def test_jdl_and_dir_combined(self, tmp_path):
        d = tmp_path / "sub"
        d.mkdir()
        a = make_sub(tmp_path / "a.sub")
        b = make_sub(d / "b.sub")
        result = collect_jdl_files(make_args(jdl=[str(a)], dir=[str(d)]))
        assert set(result) == {a, b}
