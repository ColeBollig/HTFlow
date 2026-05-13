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
