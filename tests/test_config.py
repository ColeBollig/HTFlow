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

from htflow.config import (
    ExecutionConfig,
    DEFAULT_MAX_ACTIVE_NODES,
    validate_max_active_nodes,
)


# ---------------------------------------------------------------------------
# validate_max_active_nodes()
# ---------------------------------------------------------------------------

class TestValidateMaxActiveNodes:
    @pytest.mark.parametrize("value", [-1, 0, 1, 5, 100, 10_000])
    def test_valid_values_return_none(self, value):
        assert validate_max_active_nodes(value) is None

    @pytest.mark.parametrize("value", [-2, -5, -100])
    def test_below_unlimited_sentinel_raises(self, value):
        with pytest.raises(ValueError):
            validate_max_active_nodes(value)

    @pytest.mark.parametrize("value", ["5", 5.0, None])
    def test_non_int_raises(self, value):
        with pytest.raises(ValueError):
            validate_max_active_nodes(value)

    @pytest.mark.parametrize("value", [True, False])
    def test_bool_rejected_even_though_int_subclass(self, value):
        """False == 0 and True == 1 would otherwise both be in-range -- the
        explicit bool check is the only thing that rejects them."""
        with pytest.raises(ValueError):
            validate_max_active_nodes(value)


# ---------------------------------------------------------------------------
# ExecutionConfig.max_active_nodes
# ---------------------------------------------------------------------------

class TestExecutionConfigMaxActiveNodes:
    def test_default_is_100(self):
        assert DEFAULT_MAX_ACTIVE_NODES == 100
        assert ExecutionConfig().max_active_nodes == DEFAULT_MAX_ACTIVE_NODES

    @pytest.mark.parametrize("value", [-1, 0, 1, 100])
    def test_valid_values_accepted(self, value):
        assert ExecutionConfig(max_active_nodes=value).max_active_nodes == value

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError):
            ExecutionConfig(max_active_nodes=-2)
