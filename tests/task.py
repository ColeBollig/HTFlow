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
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--id",        type=int, required=True)
parser.add_argument("--log",       default="exec.log")
parser.add_argument("--exit-code", type=int, default=0, dest="exit_code")
args = parser.parse_args()

# Multiple task.py processes can append to the same --log concurrently.
# POSIX's O_APPEND makes that atomic; Windows has no equivalent, so lock a
# fixed sentinel byte at offset 0 (standalone, not importing filelock)
# before seeking to the real end and writing.
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

sys.exit(args.exit_code)
