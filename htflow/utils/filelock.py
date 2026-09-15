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

import os

# fcntl.flock() (POSIX) and msvcrt.locking() (Windows) are both exclusive,
# non-blocking file locks -- normalized here to the same interface and the
# same BlockingIOError-on-contention behavior so callers don't need their
# own os.name branch.

if os.name == "nt":
    import msvcrt

    def lock_exclusive_nonblocking(fp) -> None:
        """Acquire an exclusive, non-blocking lock on fp. Raises BlockingIOError if already locked."""
        # msvcrt.locking() can lock a byte range beyond EOF, so no need to
        # write a placeholder byte first -- doing that would itself hit
        # Windows' mandatory locking and raise PermissionError instead of
        # the intended BlockingIOError when another handle holds the lock.
        fp.seek(0)
        try:
            msvcrt.locking(fp.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as e:
            raise BlockingIOError(str(e)) from e

    def unlock(fp) -> None:
        """Release a lock previously acquired by lock_exclusive_nonblocking(fp)."""
        fp.seek(0)
        try:
            msvcrt.locking(fp.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass

else:
    import fcntl

    def lock_exclusive_nonblocking(fp) -> None:
        """Acquire an exclusive, non-blocking lock on fp. Raises BlockingIOError if already locked."""
        fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def unlock(fp) -> None:
        """Release a lock previously acquired by lock_exclusive_nonblocking(fp)."""
        fcntl.flock(fp, fcntl.LOCK_UN)
