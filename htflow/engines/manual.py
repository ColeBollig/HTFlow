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

from .engine import Engine
from ..config import ExecutionConfig
from ..utils.directory import ChangeDir
from .. import dag
from typing import Set, Optional
from pathlib import Path
from time import time as now
import enum
import subprocess
import shlex
import logging
import htcondor2

logger = logging.getLogger(__name__)

class ManualNodeState(enum.Enum):
    BLOCKED = 0
    READY   = 1
    ACTIVE  = 2
    SUCCESS = 3
    FAILURE = 4
    ORPHAN  = 5


class ManualNode():
    def __init__(self, node: dag.Node, config: Optional[ExecutionConfig] = None) -> None:
        if not isinstance(node, dag.Node):
            raise ValueError("node must be dag.Node")

        self._node = node
        self._jdl = node.internal
        self._config = config or ExecutionConfig()
        self._proc = None
        self._state = ManualNodeState.BLOCKED
        self._waiting_on = None if node.parents is None else set(node.parents)
        self._failure_reason = None

    def __repr__(self) -> str:
        return f"{self._jdl} ({self._state.name}): '{self._failure_reason}'"

    @property
    def failure(self) -> Optional[str]:
        return self._failure_reason

    @property
    def jdl(self) -> Path:
        return self._jdl

    @property
    def process(self) -> Optional[subprocess.Popen]:
        return self._proc

    @property
    def state(self) -> ManualNodeState:
        return self._state

    @state.setter
    def state(self, val) -> None:
        if not isinstance(val, ManualNodeState):
            raise ValueError(f"Invalid manual node state type provided ({type(val)})")

        STATE_TRANSITIONS = {
            ManualNodeState.BLOCKED: [ ManualNodeState.READY, ManualNodeState.SUCCESS, ManualNodeState.ORPHAN ],
            ManualNodeState.READY:   [ ManualNodeState.ACTIVE, ManualNodeState.SUCCESS, ManualNodeState.FAILURE, ManualNodeState.ORPHAN ],
            ManualNodeState.ACTIVE:  [ ManualNodeState.SUCCESS, ManualNodeState.FAILURE, ManualNodeState.ORPHAN ],
            ManualNodeState.SUCCESS: None,
            ManualNodeState.FAILURE: None,
            ManualNodeState.ORPHAN:  None,
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
        return self._state == ManualNodeState.BLOCKED

    def IsReady(self) -> bool:
        return self._state == ManualNodeState.READY

    def IsActive(self) -> bool:
        return self._state == ManualNodeState.ACTIVE

    def IsOrphan(self) -> bool:
        return self._state == ManualNodeState.ORPHAN

    def IsFailed(self) -> bool:
        return self._state == ManualNodeState.FAILURE

    def IsSuccess(self) -> bool:
        return self._state == ManualNodeState.SUCCESS

    def IsTerminal(self) -> bool:
        return self._state in [ ManualNodeState.SUCCESS, ManualNodeState.FAILURE, ManualNodeState.ORPHAN ]

    def Notify(self, parent_id: int) -> bool:
        assert self._waiting_on is not None
        assert parent_id in self._waiting_on
        self._waiting_on.remove(parent_id)
        return len(self._waiting_on) == 0

    def Execute(self) -> None:
        with open(self._jdl, "r") as f:
            desc = htcondor2.Submit(f.read())

        cmd = [ desc.expand("executable") ]
        args = desc.expand("arguments") or ""

        if args.startswith('"') and args.endswith('"') and len(args) >= 2:
            args = args[1:-1].replace('""', '"')

        cmd += shlex.split(args)

        logger.info("Executing: %s", " ".join(cmd))

        origin = Path(self._jdl).parent.resolve()
        with ChangeDir(origin, enabled=self._config.relative_to_source):
            self._proc = subprocess.Popen(
                cmd,
                stdout = subprocess.DEVNULL,
                stderr = subprocess.DEVNULL,
            )
        self.state = ManualNodeState.ACTIVE

    def Orphaned(self, dag: dag.Dag) -> None:
        # Already orphaned so return now to save time
        if self._state == ManualNodeState.ORPHAN:
            return

        # Recursively orphan children
        if self._node.children is not None:
            for i in self._node.children:
                dag[i].internal.Orphaned(dag)

        # Don't attempt to change state from other terminal states
        # future proofing incase nodes are pre-done
        if not self.IsTerminal():
            self.state = ManualNodeState.ORPHAN

    def Fail(self, dag: dag.Dag, reason: str = "Failure reason unkown") -> None:
        if self._node.children is not None:
            for i in self._node.children:
                dag[i].internal.Orphaned(dag)
        self.state = ManualNodeState.FAILURE
        self._failure_reason = reason

    def Done(self, dag: dag.Dag) -> None:
        if self._node.children is not None:
            for i in self._node.children:
                child = dag[i]
                if child.internal.Notify(self._node.id):
                    if not child.internal.IsTerminal():
                        dag.internal.prepare(child)
        self.state = ManualNodeState.SUCCESS

class ManualDag():
    def __init__(self) -> None:
        self._ready_nodes = set()
        self._active_nodes = set()

    def __repr__(self) -> str:
        return self.__class__.__name__

    @property
    def ready_nodes(self) -> Set[int]:
        return self._ready_nodes

    def prepare(self, node: dag.Node) -> None:
        if not isinstance(node, dag.Node):
            raise ValueError("Ready nodes can only be added to from a dag.Node")

        logger.info("Readying node %s for execution", node.internal.jdl)

        self._ready_nodes.add(node.id)
        node.internal.state = ManualNodeState.READY

    @property
    def active_nodes(self) -> Set[int]:
        return self._active_nodes


class ManualEngine(Engine):
    """Dataflow engine to manually execute tasks"""
    EXIT_SUCCESS = 0
    EXIT_FAILURE = 1

    def __init__(self, dag: dag.Dag, config: Optional[ExecutionConfig] = None) -> None:
        super().__init__(config)

        self.AcquireLock()

        try:
            self._dag = dag
            self._dag.internal = ManualDag()
            for node in self._dag:
                node.internal = ManualNode(node, config=self.config)

            self._had_failure = False

            self._state_file = self.workdir / "manual.state"
        except:
            self.ReleaseLock()
            raise

    def __exit(self) -> None:
        if self._had_failure:
            logger.error("####### Failed Nodes #######")

            for node in self._dag:
                if node.internal.IsFailed():
                    logger.error("    Node %s > %s", node.internal.jdl, node.internal.failure)

            logger.error("############################")

        self.ReleaseLock()

    def Bootstrap(self) -> None:
        """Manual dataflow bootstrap"""
        for node in self._dag.roots:
            if node.internal.IsBlocked():
                self._dag.internal.prepare(node)
            else:
                logger.debug("Root node %s skipping prepare due to intitial state %s", node.internal.jdl, node.internal.state.name)

    def Cleanup(self) -> None:
        """Manual dataflow final cleanup"""
        num_active = len(self._dag.internal.active_nodes)

        if num_active == 0:
            return

        logger.info("Cleaning up %d active nodes", num_active)

        # Kill all active node processes
        for i in self._dag.internal.active_nodes:
            process = self._dag[i].internal.process
            if process is not None:
                try:
                    process.kill()
                except Exception as e:
                    logging.warn("Failed to kill node %s: %s", self._dag[i].internal.jdl, e)

        # Wait for all processes to finish (unless we take to long and get killed)
        for i in self._dag.internal.active_nodes:
            process = self._dag[i].internal.process
            if process is not None:
                logger.info("Awaiting node %s task termination", self._dag[i].internal.jdl)
                process.wait()

        self.__exit()

    def Execute(self) -> None:
        """Manual dataflow execute of nodes"""
        attempted = list()

        for i in self._dag.internal.ready_nodes:
            attempted.append(i)

            node = self._dag[i]
            logger.info("Executing node %s", node.internal.jdl)
            try:
                node.internal.Execute()
                self._dag.internal.active_nodes.add(node.id)
            except Exception as e:
                self._had_failure = True
                node.internal.Fail(self._dag, str(e))
                logger.error("Failed to execute node %s task: %s", node.internal.jdl, e)

        self._dag.internal.ready_nodes.difference_update(attempted)


    def Recover(self) -> None:
        """Manual dataflow recovery"""
        if self._state_file.exists():
            logger.info("### Recovering state from %s", self._state_file)
            with open(self._state_file, "r") as f:
                for line in f:
                    _, _, _, jdl = line.strip().split(maxsplit=3)
                    located = False
                    for node in self._dag:
                        if node.internal.jdl == Path(jdl):
                            if node.internal.IsReady():
                                self._dag.internal.ready_nodes.remove(node.id)
                            node.internal.Done(self._dag)
                            located = True
                            break

                    if not located:
                        raise RuntimeError(f"State recovery failed to find node with {jdl}")

            logger.info("### Recovery finished")

    def Terminate(self) -> Optional[int]:
        """Manual dataflow termination check"""
        # Quick check for we are running things still so don't terminate
        if len(self._dag.internal.ready_nodes) > 0 or len(self._dag.internal.active_nodes) > 0:
            return None

        success = True

        for node in self._dag:
            if not node.internal.IsTerminal():
                return None
            elif not node.internal.IsSuccess():
                success = False

        self.__exit()

        logger.info("Dataflow execution finished: %s", "Success" if success else "Failed")

        return self.EXIT_SUCCESS if success else self.EXIT_FAILURE

    def Update(self) -> None:
        """Manual dataflow state update"""
        exited = list()

        for i in self._dag.internal.active_nodes:
            node = self._dag[i]
            if node.internal.process.poll() is not None:
                # NOTE: Current code will cause deadlock if subprocess.Popen is changed to use PIPEs
                exit_code = node.internal.process.returncode
                logger.info("Node %s exited with code %d", node.internal.jdl, exit_code)

                exited.append(node.id)

                if exit_code == 0:
                    node.internal.Done(self._dag)
                    with open(self._state_file, "a") as f:
                        f.write(f"*** FINISHED {now()} {node.internal.jdl}\n")
                else:
                    self._had_failure = True
                    node.internal.Fail(self._dag, f"Exited with code {exit_code}")

        self._dag.internal.active_nodes.difference_update(exited)

