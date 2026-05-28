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

import os
from pathlib import Path

import pytest

from htflow.utils.directory import ChangeDir


def test_changes_directory(tmp_path):
    with ChangeDir(tmp_path):
        assert Path.cwd() == tmp_path


def test_restores_directory_on_exit(tmp_path):
    original = Path.cwd()
    with ChangeDir(tmp_path):
        pass
    assert Path.cwd() == original


def test_restores_directory_on_exception(tmp_path):
    original = Path.cwd()
    with pytest.raises(RuntimeError):
        with ChangeDir(tmp_path):
            raise RuntimeError("boom")
    assert Path.cwd() == original


def test_returns_self_on_enter(tmp_path):
    with ChangeDir(tmp_path) as cd:
        assert isinstance(cd, ChangeDir)


def test_accepts_string_path(tmp_path):
    original = Path.cwd()
    with ChangeDir(str(tmp_path)):
        assert Path.cwd() == tmp_path
    assert Path.cwd() == original


def test_nested_change_dirs(tmp_path):
    original = Path.cwd()
    inner = tmp_path / "inner"
    inner.mkdir()
    with ChangeDir(tmp_path):
        with ChangeDir(inner):
            assert Path.cwd() == inner
        assert Path.cwd() == tmp_path
    assert Path.cwd() == original


def test_origin_attribute_set_on_enter(tmp_path):
    original = Path.cwd()
    with ChangeDir(tmp_path) as cd:
        assert cd.origin == original
    assert cd.origin is None


def test_destination_attribute_set(tmp_path):
    cd = ChangeDir(tmp_path)
    assert cd.destination == tmp_path


def test_destination_attribute_string_converted(tmp_path):
    cd = ChangeDir(str(tmp_path))
    assert isinstance(cd.destination, Path)
    assert cd.destination == tmp_path


def test_invalid_type_raises_on_init():
    with pytest.raises(TypeError):
        ChangeDir(42)


def test_truediv_returns_path(tmp_path):
    cd = ChangeDir(tmp_path)
    result = cd / "subdir"
    assert result == tmp_path / "subdir"
    assert isinstance(result, Path)


def test_truediv_accepts_string(tmp_path):
    cd = ChangeDir(tmp_path)
    result = cd / "subdir"
    assert result == tmp_path / "subdir"


def test_truediv_accepts_path(tmp_path):
    cd = ChangeDir(tmp_path)
    result = cd / Path("subdir")
    assert result == tmp_path / "subdir"


def test_truediv_invalid_type_raises(tmp_path):
    cd = ChangeDir(tmp_path)
    with pytest.raises(TypeError):
        _ = cd / 99


def test_no_chdir_when_already_in_destination():
    original = Path.cwd()
    with ChangeDir(original) as cd:
        assert cd.origin is None
        assert Path.cwd() == original


def test_exit_noop_when_no_chdir_occurred():
    original = Path.cwd()
    with ChangeDir(Path.cwd()):
        pass
    assert Path.cwd() == original


def test_resolved_symlink_treated_as_same_dir(tmp_path):
    link = tmp_path / "link"
    link.symlink_to(tmp_path)
    os.chdir(tmp_path)
    with ChangeDir(link) as cd:
        assert cd.origin is None
    os.chdir(Path(__file__).parent)
