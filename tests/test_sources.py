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
from unittest.mock import patch

from htflow.sources import InputError, collect_jdl_files
from htflow.sources._registry import FILE_HANDLERS, handler_for, register
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
    def test_default_handler_used_for_sub(self, tmp_path):
        p = make_sub(tmp_path / "a.sub")
        assert handler_for(".sub")(p) == [p]

    def test_default_handler_used_for_arbitrary_extension(self, tmp_path):
        p = make_sub(tmp_path / "a.txt")
        assert handler_for(".txt")(p) == [p]

    def test_default_handler_used_for_no_extension(self, tmp_path):
        p = make_sub(tmp_path / "a")
        assert handler_for("")(p) == [p]

    def test_unregistered_extensions_share_the_default_handler(self):
        assert handler_for(".sub") is handler_for(".doesnotexist")

    def test_register_overrides_default_for_specific_extension(self, tmp_path):
        marker = object()
        register(".fake", lambda path: [marker])
        try:
            assert handler_for(".fake")(tmp_path / "x.fake") == [marker]
        finally:
            del FILE_HANDLERS[".fake"]


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

    def test_resolve_does_not_validate_content(self, tmp_path):
        """--jdl names files explicitly; resolve() does not parse/validate them
        (unlike the directory-scan default handler), preserving the existing
        exit-125-on-missing-file behavior handled later by HTCondorDataFlow."""
        bad = tmp_path / "bad.sub"
        bad.write_text("not a submit file")
        result = from_jdl.resolve(make_args(jdl=[str(bad)]))
        assert result == [bad]


# ---------------------------------------------------------------------------
# from_jdl.handle_file (the default directory-scan handler)
# ---------------------------------------------------------------------------

class TestFromJdlHandleFile:
    def test_valid_submit_file_returns_path(self, tmp_path):
        p = make_sub(tmp_path / "a.sub")
        assert from_jdl.handle_file(p) == [p]

    def test_invalid_submit_file_returns_empty_list(self, tmp_path):
        p = tmp_path / "a.sub"
        p.write_text("garbage")
        with patch("htflow.sources.from_jdl.htcondor2.Submit", side_effect=ValueError("boom")):
            assert from_jdl.handle_file(p) == []

    def test_invalid_submit_file_prints_skip_message(self, tmp_path, capsys):
        p = tmp_path / "a.sub"
        p.write_text("garbage")
        with patch("htflow.sources.from_jdl.htcondor2.Submit", side_effect=ValueError("boom")):
            from_jdl.handle_file(p)
        out = capsys.readouterr().out
        assert "Skipping" in out
        assert str(p) in out


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

    def test_extensionless_file_included_when_valid(self, tmp_path):
        p = make_sub(tmp_path / "foo")
        result = from_dir.resolve(make_args(dir=[str(tmp_path)]))
        assert p in result

    def test_arbitrary_extension_included_when_valid(self, tmp_path):
        p = make_sub(tmp_path / "bat.txt")
        result = from_dir.resolve(make_args(dir=[str(tmp_path)]))
        assert p in result

    def test_content_determines_inclusion_not_extension(self, tmp_path, capsys):
        good = make_sub(tmp_path / "a.txt")
        bad = tmp_path / "b.txt"
        bad.write_text("garbage")
        with patch("htflow.sources.from_jdl.htcondor2.Submit", side_effect=[None, ValueError("boom")]):
            result = from_dir.resolve(make_args(dir=[str(tmp_path)]))
        assert good in result
        assert bad not in result
        assert "Skipping" in capsys.readouterr().out

    def test_registered_extension_overrides_default_handler(self, tmp_path):
        handled = []

        def fake_handler(path):
            handled.append(path)
            return [path]

        register(".marker", fake_handler)
        try:
            marker = tmp_path / "sentinel.marker"
            marker.write_text("not valid JDL content at all")
            result = from_dir.resolve(make_args(dir=[str(tmp_path)]))
            assert handled == [marker]
            assert result == [marker]
        finally:
            del FILE_HANDLERS[".marker"]

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
