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

# CI-only: block until `condor_who -quick` reports the local pool's
# condor_master as IsReady, or exit non-zero after a timeout. One script
# shared by both CI legs rather than a bash/PowerShell loop each.
#
# IsReady reflects whether condor_master considers every daemon done
# starting up -- more direct than inferring readiness from whether some
# client tool can connect. Mirrors HTCondor's own Ornithology test
# framework (condor.py's Condor._wait_for_ready()), simplified to just
# the IsReady check.

import subprocess
import sys
import time

TIMEOUT_SECONDS = 60
POLL_INTERVAL_SECONDS = 2


def _condor_who_quick_ad() -> dict:
    """Run `condor_who -quick` and parse its `KEY = value` lines into a dict."""
    result = subprocess.run(
        ["condor_who", "-quick"],
        capture_output=True,
        text=True,
    )
    ad = {}
    for line in result.stdout.splitlines():
        if " = " in line:
            key, _, value = line.partition(" = ")
            ad[key.strip()] = value.strip()
    return ad


def main() -> int:
    deadline = time.time() + TIMEOUT_SECONDS
    attempt = 0

    while time.time() < deadline:
        attempt += 1
        ad = _condor_who_quick_ad()

        if ad.get("IsReady") == "true":
            print(f"condor_master reports IsReady after {attempt} attempt(s)")
            subprocess.run(["condor_version"])
            subprocess.run(["condor_status"])
            return 0

        remaining = int(deadline - time.time())
        print(f"Waiting for IsReady (attempt {attempt}, giving up in {remaining}s)...")
        time.sleep(POLL_INTERVAL_SECONDS)

    print("::error::condor_who -quick never reported IsReady", file=sys.stderr)
    print("Last condor_who -quick output:", file=sys.stderr)
    subprocess.run(["condor_who", "-quick"])
    return 1


if __name__ == "__main__":
    sys.exit(main())
