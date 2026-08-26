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
from ._internal import NodeState, NodeInternal, DagInternal
from ..config import ExecutionConfig
from ..utils.directory import ChangeDir
from .. import dag
from typing import Optional
from pathlib import Path
from time import time as now
import subprocess
import shlex
import logging
import htcondor2

logger = logging.getLogger(__name__)

class ManualNode(NodeInternal):
    def __init__(self, node: dag.Node, config: Optional[ExecutionConfig] = None) -> None:
        super().__init__(node)

        self._config = config or ExecutionConfig()
        self._proc = None

    @property
    def process(self) -> Optional[subprocess.Popen]:
        return self._proc

    def Execute(self, **kwargs) -> None:
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

        self.state = NodeState.ACTIVE


class ManualDag(DagInternal):
    def _prepare(self, node: dag.Node) -> None:
        """Internal specific node preparations"""
        node.internal.state = NodeState.READY


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

