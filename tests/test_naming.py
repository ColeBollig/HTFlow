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

import hashlib
import pytest
from pathlib import Path

from htflow.utils.naming import (
    hash_name,
    validate_hash_length,
    MIN_HASH_LENGTH,
    MAX_HASH_LENGTH,
    DEFAULT_HASH_LENGTH,
)


def sha256_hex(path) -> str:
    """Reference implementation used only to check hash_name() against, independent
    of its internals."""
    return hashlib.sha256(str(Path(path)).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# hash_name()
# ---------------------------------------------------------------------------

class TestHashName:
    def test_deterministic_for_same_path(self):
        assert hash_name("a.sub") == hash_name("a.sub")

    def test_differs_for_different_paths(self):
        assert hash_name("a.sub") != hash_name("b.sub")

    def test_path_and_str_are_equivalent(self):
        assert hash_name(Path("a.sub")) == hash_name("a.sub")

    def test_relative_and_absolute_path_differ(self):
        """The exact path given is hashed as-is, not resolved first — a relative
        and absolute spelling of 'the same' file are different names."""
        assert hash_name("a.sub") != hash_name("/abs/path/a.sub")

    def test_default_length_is_16(self):
        assert DEFAULT_HASH_LENGTH == 16
        assert len(hash_name("a.sub")) == 16

    def test_matches_reference_sha256_prefix(self):
        assert hash_name("a.sub") == sha256_hex("a.sub")[:DEFAULT_HASH_LENGTH]

    @pytest.mark.parametrize("length", [4, 8, 16, 32, 63, 64])
    def test_length_controls_output_size(self, length):
        name = hash_name("a.sub", length=length)
        assert len(name) == length
        assert name == sha256_hex("a.sub")[:length]

    def test_min_length_boundary(self):
        assert hash_name("a.sub", length=MIN_HASH_LENGTH) == sha256_hex("a.sub")[:MIN_HASH_LENGTH]

    def test_max_length_is_full_digest(self):
        assert hash_name("a.sub", length=MAX_HASH_LENGTH) == sha256_hex("a.sub")

    def test_truncated_name_is_a_prefix_of_longer_name(self):
        """Shortening length must not change the earlier characters — a shorter
        name should always be a prefix of every longer name for the same path."""
        short = hash_name("a.sub", length=8)
        long = hash_name("a.sub", length=32)
        assert long.startswith(short)

    @pytest.mark.parametrize("length", [0, 1, 3, -1, 65, 100])
    def test_length_outside_range_raises(self, length):
        with pytest.raises(ValueError):
            hash_name("a.sub", length=length)

    @pytest.mark.parametrize("length", ["16", 16.0, None, True, False])
    def test_non_int_length_raises(self, length):
        with pytest.raises(ValueError):
            hash_name("a.sub", length=length)


# ---------------------------------------------------------------------------
# validate_hash_length()
# ---------------------------------------------------------------------------

class TestValidateHashLength:
    @pytest.mark.parametrize("length", [MIN_HASH_LENGTH, 16, MAX_HASH_LENGTH])
    def test_valid_lengths_return_none(self, length):
        assert validate_hash_length(length) is None

    @pytest.mark.parametrize("length", [MIN_HASH_LENGTH - 1, MAX_HASH_LENGTH + 1])
    def test_out_of_range_raises(self, length):
        with pytest.raises(ValueError):
            validate_hash_length(length)

    def test_bool_rejected_even_though_int_subclass(self):
        with pytest.raises(ValueError):
            validate_hash_length(True)
