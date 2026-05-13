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

class Engine(ABC):
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


class MonitorEngine(Engine):
    """Place jobs to condor and monitor"""
    pass
