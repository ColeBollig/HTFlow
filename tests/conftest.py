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
# (e.g. macOS CI), inject a mock so that test collection and import succeed.
try:
    import htcondor2
except ImportError:
    from unittest.mock import MagicMock
    sys.modules["htcondor2"] = MagicMock()
