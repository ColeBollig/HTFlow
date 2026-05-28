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

# htcondor only publishes Linux wheels. On platforms where it isn't installed
# (e.g. macOS CI), inject a minimal stub so that test collection and the
# dataflow logic work correctly without the real package.
try:
    import htcondor2
except ImportError:
    from unittest.mock import MagicMock

    class _Submit:
        """Minimal stand-in for htcondor2.Submit that parses key=value pairs."""

        def __init__(self, source):
            self._data = {}
            if isinstance(source, dict):
                for key, value in source.items():
                    self._data[key.lower()] = str(value)
            else:
                for line in source.splitlines():
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        key, _, value = line.partition("=")
                        self._data[key.strip().lower()] = value.strip()

        def get(self, key: str):
            return self._data.get(key.lower())

        def expand(self, key: str):
            return self._data.get(key.lower(), "")

        def __setitem__(self, key: str, value: str):
            self._data[key.lower()] = value

        def __str__(self):
            return "\n".join(f"{k} = {v}" for k, v in self._data.items())

    _mock = MagicMock()
    _mock.Submit = _Submit
    sys.modules["htcondor2"] = _mock
