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

from abc import ABC, abstractmethod
from typing import Optional
from pathlib import Path

import fcntl

from ..config import ExecutionConfig


class EngineExecutionError(Exception):
    pass


class Engine(ABC):
    @classmethod
    def work_dir(cls) -> Path:
        return Path("flowman")

    @classmethod
    def lock_file(cls) -> Path:
        return cls.work_dir() / "flowman.lock"

    def __init__(self, config: Optional[ExecutionConfig] = None) -> None:
        """High level common engine initialization"""
        self.config = config or ExecutionConfig()
        self._work_dir = self.work_dir()
        self._work_dir.mkdir(exist_ok=True)
        self._lock_fp = None

    @property
    def lock(self) -> Path:
        return self.lock_file()

    @property
    def workdir(self) -> Path:
        return self._work_dir

    def AcquireLock(self) -> None:
        """Acquire execution file lock to lay claim to this directory"""
        if self._lock_fp is None:
            self._lock_fp = open(self.lock, "w")
            try:
                fcntl.flock(self._lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self._lock_fp.close()
                self._lock_fp = None
                raise EngineExecutionError("Another engine is already running (could not acquire lock).")

    def ReleaseLock(self) -> None:
        """Release execution file lock for another to execute"""
        if self._lock_fp:
            fcntl.flock(self._lock_fp, fcntl.LOCK_UN)
            self._lock_fp.close()
            self._lock_fp = None

    @abstractmethod
    def Bootstrap(self) -> None:
        """Engine specific intialization for execution"""
        pass

    @abstractmethod
    def Cleanup(self) -> None:
        """Engine specific cleanup code"""
        pass

    @abstractmethod
    def Execute(self) -> None:
        """Engine specific execution of one node in dataflow DAG"""
        pass

    @abstractmethod
    def Recover(self) -> None:
        """Engine specific state recovery"""
        pass

    @abstractmethod
    def Terminate(self) -> Optional[int]:
        """Engine specific termination check: returns exit code"""
        pass

    @abstractmethod
    def Update(self) -> None:
        """Engine specific state tracking of node execution"""
        pass
