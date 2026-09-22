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

# Fails on its first invocation (creates --marker and exits 1), succeeds on
# every one after that -- for exercising DAGMan RETRY together with an
# engine's own recovery/state file. On success, appends --id to --log
# exactly like task.py (same locking dance, duplicated not imported).

import argparse
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--id",     type=int, required=True)
parser.add_argument("--log",    default="exec.log")
parser.add_argument("--marker", required=True)
args = parser.parse_args()

if not os.path.exists(args.marker):
    open(args.marker, "w").close()
    sys.exit(1)

fd = os.open(args.log, os.O_CREAT | os.O_RDWR, 0o644)
try:
    if os.name == "nt":
        import msvcrt
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
    else:
        import fcntl
        fcntl.flock(fd, fcntl.LOCK_EX)
    os.lseek(fd, 0, os.SEEK_END)
    os.write(fd, f"{args.id}\n".encode())
finally:
    if os.name == "nt":
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)

sys.exit(0)
