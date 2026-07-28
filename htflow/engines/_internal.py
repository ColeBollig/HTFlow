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
from ..config import ExecutionConfig
from .. import dag
from typing import Set, Optional
from pathlib import Path
import enum
import logging

logger = logging.getLogger(__name__)

class NodeState(enum.Enum):
    """Enumeration of node execution states common across various engines"""
    BLOCKED = 0
    READY   = 1
    ACTIVE  = 2
    SUCCESS = 3
    FAILURE = 4
    ORPHAN  = 5

class NodeInternal(ABC):
    """Abstract class of node structure common across various engines"""

    def __init__(self, node: dag.Node, config: Optional[ExecutionConfig] = None) -> None:
        if not isinstance(node, dag.Node):
            raise ValueError("node must be dag.Node")

        self._node = node
        self._jdl = node.internal
        self._config = config or ExecutionConfig()
        self._state = NodeState.BLOCKED
        self._waiting_on = None if node.parents is None else set(node.parents)
        self._failure_reason = None

    def __repr__(self) -> str:
        """Get this classes string representation as JDL (Name): 'Failure'"""
        return f"{self._jdl} ({self._state.name}): '{self._failure_reason}'"

    @property
    def failure(self) -> Optional[str]:
        """Get this nodes failure reason"""
        return self._failure_reason

    @property
    def jdl(self) -> Path:
        """Get this nodes original JDL path"""
        return self._jdl

    @property
    def state(self) -> NodeState:
        """Get this nodes current state"""
        return self._state

    @state.setter
    def state(self, val) -> None:
        """Set this nodes current state"""
        if not isinstance(val, NodeState):
            raise ValueError(f"Invalid manual node state type provided ({type(val)})")

        STATE_TRANSITIONS = {
            NodeState.BLOCKED: [ NodeState.READY, NodeState.SUCCESS, NodeState.ORPHAN ],
            NodeState.READY:   [ NodeState.ACTIVE, NodeState.SUCCESS, NodeState.FAILURE, NodeState.ORPHAN ],
            NodeState.ACTIVE:  [ NodeState.SUCCESS, NodeState.FAILURE, NodeState.ORPHAN ],
            NodeState.SUCCESS: None,
            NodeState.FAILURE: None,
            NodeState.ORPHAN:  None,
        }

        if val not in STATE_TRANSITIONS:
            raise RuntimeError(f"Unknown state transition {val.name} specified")
        elif STATE_TRANSITIONS[self._state] is None:
            raise RuntimeError(f"Current state {self._state.name} does not allow transitions")
        elif val not in STATE_TRANSITIONS[self._state]:
            raise RuntimeError(f"Illegal state transition from {self._state.name} to {val.name}")

        logger.debug("Switching node %s state: %s -> %s", self._node.internal.jdl, self._state.name, val.name)

        self._state = val

    def IsBlocked(self) -> bool:
        """Return if node is currently in the BLOCKED state"""
        return self._state == NodeState.BLOCKED

    def IsReady(self) -> bool:
        """Return if node is currently in the READY state"""
        return self._state == NodeState.READY

    def IsActive(self) -> bool:
        """Return if node is currently in the ACTIVE state"""
        return self._state == NodeState.ACTIVE

    def IsOrphan(self) -> bool:
        """Return if node is currently in the ORPHAN state"""
        return self._state == NodeState.ORPHAN

    def IsFailed(self) -> bool:
        """Return if node is currently in the FAILED state"""
        return self._state == NodeState.FAILURE

    def IsSuccess(self) -> bool:
        """Return if node is currently in the SUCCESS state"""
        return self._state == NodeState.SUCCESS

    def IsTerminal(self) -> bool:
        """Return if this node is currently in a terminal state (i.e. no more work to be done)"""
        return self._state in [ NodeState.SUCCESS, NodeState.FAILURE, NodeState.ORPHAN ]

    def Notify(self, parent_id: int) -> bool:
        """
        Notify this node that it is no longer waiting on a specific parent node
        Returns if whether or not this node is ready for execution
        """
        assert self._waiting_on is not None
        assert parent_id in self._waiting_on
        self._waiting_on.remove(parent_id)
        return len(self._waiting_on) == 0

    def Orphaned(self, dag: dag.Dag) -> None:
        """Orphan this node and all of its ancestors"""
        # Already orphaned so return now to save time
        if self._state == NodeState.ORPHAN:
            return

        # Recursively orphan children
        if self._node.children is not None:
            for i in self._node.children:
                dag[i].internal.Orphaned(dag)

        # Don't attempt to change state from other terminal states
        # future proofing incase nodes are pre-done
        if not self.IsTerminal():
            self.state = NodeState.ORPHAN

    def Fail(self, dag: dag.Dag, reason: str = "Failure reason unkown") -> None:
        """Mark this node as failed"""
        if self._node.children is not None:
            for i in self._node.children:
                dag[i].internal.Orphaned(dag)
        self.state = NodeState.FAILURE
        self._failure_reason = reason

    def Done(self, dag: dag.Dag) -> None:
        """Mark this node as success"""
        if self._node.children is not None:
            for i in self._node.children:
                child = dag[i]
                if child.internal.Notify(self._node.id):
                    if not child.internal.IsTerminal():
                        dag.internal.prepare(child)
        self.state = NodeState.SUCCESS

    @abstractmethod
    def Execute(self) -> None:
        """Abstract method on how to execute this node"""
        pass
