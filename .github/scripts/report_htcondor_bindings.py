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

# CI-only diagnostic: prints whether the real htcondor2 bindings are
# installed or tests will fall back to tests/conftest.py's mock stub. Kept
# as a real .py file (invoked as `python .github/scripts/...`) rather than
# an inline `run: | python -c "..."` block, since GitHub Actions runs `run:`
# steps through bash on Linux/macOS but pwsh on Windows -- a multi-line
# quoted string handed to `-c` isn't guaranteed to survive both shells
# identically, and there's no Windows runner to verify that against here.

try:
    import htcondor2
    print("Using REAL htcondor2 bindings:", htcondor2.__file__)
except ImportError:
    print("htcondor2 not installed -- tests will fall back to the mock stub")
