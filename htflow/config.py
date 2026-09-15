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

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .utils.naming import DEFAULT_HASH_LENGTH, validate_hash_length

DEFAULT_MAX_ACTIVE_NODES = 100


def validate_max_active_nodes(value: int) -> None:
    """Raise ValueError unless value is -1 (unlimited), 0 (no execution), or a positive int (limit)"""
    if not isinstance(value, int) or isinstance(value, bool) or value < -1:
        raise ValueError(
            f"max active nodes must be -1 (unlimited), 0 (no execution), or a "
            f"positive integer (got {value!r})"
        )


@dataclass(frozen=True)
class ExecutionConfig:
    """Shared, static configuration controlling the behavior of a dataflow and its execution.

    Passed to HTCondorDataFlow, Engine subclasses, and their nodes so new
    behavior-controlling options can be added here instead of threading new
    parameters through every constructor.
    """
    relative_to_source: bool = False
    resolve_from: Optional[Path] = None
    node_name_length: int = DEFAULT_HASH_LENGTH
    max_active_nodes: int = DEFAULT_MAX_ACTIVE_NODES

    def __post_init__(self) -> None:
        validate_hash_length(self.node_name_length)
        validate_max_active_nodes(self.max_active_nodes)
