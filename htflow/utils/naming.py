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

import hashlib
from pathlib import Path
from typing import Union

MIN_HASH_LENGTH = 4
MAX_HASH_LENGTH = 64
DEFAULT_HASH_LENGTH = 16


def validate_hash_length(length: int) -> None:
    """Raise ValueError unless length is an int within [MIN_HASH_LENGTH, MAX_HASH_LENGTH]"""
    if not isinstance(length, int) or isinstance(length, bool) or not (MIN_HASH_LENGTH <= length <= MAX_HASH_LENGTH):
        raise ValueError(
            f"hash length must be an integer between {MIN_HASH_LENGTH} and "
            f"{MAX_HASH_LENGTH} (got {length!r})"
        )


def hash_name(path: Union[Path, str], length: int = DEFAULT_HASH_LENGTH) -> str:
    """Content-addressed name for a thing identified by `path`: the sha256 hex digest of `path`, truncated to `length` hex characters"""
    validate_hash_length(length)
    return hashlib.sha256(str(Path(path)).encode("utf-8")).hexdigest()[:length]
