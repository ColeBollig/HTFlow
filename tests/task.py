#!/usr/bin/env python3
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

# Minimal task executable for htflow execute integration tests.
# Appends its --id to --log, then exits with --exit-code.

import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--id",        type=int, required=True)
parser.add_argument("--log",       default="exec.log")
parser.add_argument("--exit-code", type=int, default=0, dest="exit_code")
args = parser.parse_args()

with open(args.log, "a") as f:
    f.write(f"{args.id}\n")

sys.exit(args.exit_code)
