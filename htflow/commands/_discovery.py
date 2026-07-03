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

import importlib
import pkgutil
from types import ModuleType
from typing import Dict, Sequence


def discover(path: Sequence[str], package: str, required_attrs: Sequence[str] = ()) -> Dict[str, ModuleType]:
    """Import every module and subpackage directly inside `path` and return
    them keyed by name (the file/directory name, extension stripped).

    Generic over nesting level: used both for top-level commands (where
    required_attrs is ("add_parser", "run")) and for a command's own
    subcommands, e.g. show's views (where required_attrs is just ("run",)).

    Names starting with '_' are skipped, so private helper code (like this
    module itself) can live alongside the discovered modules without being
    mistaken for one of them.

    Raises RuntimeError if a discovered module is missing any of
    `required_attrs` as a callable attribute.
    """
    discovered = {}
    for _, name, _ in sorted(pkgutil.iter_modules(path), key=lambda info: info.name):
        if name.startswith("_"):
            continue

        module = importlib.import_module(f"{package}.{name}")
        missing = [attr for attr in required_attrs if not callable(getattr(module, attr, None))]
        if missing:
            raise RuntimeError(f"'{package}.{name}' is missing required function(s): {', '.join(missing)}")

        discovered[name] = module

    return discovered
