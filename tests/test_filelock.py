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

# Exercises whichever branch (msvcrt/fcntl) is active on this platform --
# both share the same contract, and CI's matrix covers both platforms.

import pytest

from htflow.utils.filelock import lock_exclusive_nonblocking, unlock


def test_lock_then_unlock_does_not_raise(tmp_path):
    path = tmp_path / "flowman.lock"
    path.touch()
    fp = open(path, "w")
    try:
        lock_exclusive_nonblocking(fp)
        unlock(fp)
    finally:
        fp.close()


def test_lock_on_freshly_created_empty_file(tmp_path):
    """Regression test: an earlier version padded 0-byte files before
    locking, which hit PermissionError under contention on Windows."""
    path = tmp_path / "flowman.lock"
    fp = open(path, "w")
    try:
        assert path.stat().st_size == 0
        lock_exclusive_nonblocking(fp)
        unlock(fp)
    finally:
        fp.close()


def test_second_handle_blocked_while_locked(tmp_path):
    path = tmp_path / "flowman.lock"
    path.touch()

    holder = open(path, "w")
    contender = open(path, "w")
    try:
        lock_exclusive_nonblocking(holder)
        with pytest.raises(BlockingIOError):
            lock_exclusive_nonblocking(contender)
    finally:
        unlock(holder)
        holder.close()
        contender.close()


def test_unlock_releases_for_other_handle(tmp_path):
    """Proves unlock() actually releases the lock, not just that it
    doesn't raise -- a second handle can only acquire afterward."""
    path = tmp_path / "flowman.lock"
    path.touch()

    first = open(path, "w")
    second = open(path, "w")
    try:
        lock_exclusive_nonblocking(first)
        unlock(first)
        lock_exclusive_nonblocking(second)
        unlock(second)
    finally:
        first.close()
        second.close()


def test_unlock_without_prior_lock_does_not_raise(tmp_path):
    path = tmp_path / "flowman.lock"
    path.touch()
    fp = open(path, "w")
    try:
        unlock(fp)
    finally:
        fp.close()
