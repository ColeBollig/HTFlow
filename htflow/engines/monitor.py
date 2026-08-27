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
from ..utils.naming import hash_name
from .. import dag
from typing import Optional, Union, List, Any
from pathlib import Path
from time import time as now
from contextlib import contextmanager
import logging
import htcondor2
import classad2
import os
import enum

logger = logging.getLogger(__name__)

# Const keys to take from engine class and add as JDL commands
JDL_NODE_NAME = "node_name"
JDL_MANAGER_ID = "manager_id"
JDL_LOG_FILE = "logfile"


# Const submit key words to be used
SUBMIT_KEY_DAGMAN_LOG_FILE = "dagman_log"


# Custom ClassAd attribute names to set in jobs placed to local AP
ATTR_NODE_NAME = "NodeName"
ATTR_MANAGER_ID = "ManagerId"

# Special meaning exit codes for jobs
JOB_EXIT_UNKNOWN = -1000
JOB_EXIT_ABORTED = -1001

def _effective_handlers(logger):
    """Walk the logger hierarchy the way logging does when propagating,
    yielding every handler that would actually see a record from `logger`."""
    current = logger
    while current is not None:
        for handler in current.handlers:
            yield handler
        if not current.propagate:
            break
        current = current.parent


@contextmanager
def log_in_recovery_mode(logger):
    """Context manager to switch logger into recovery mode with prefix"""
    logger.debug("Entering recovery mode")

    # Keep track of old formatters. Handlers are typically attached to the
    # root logger (see __main__.py) rather than this module's logger, so we
    # must walk the propagation chain instead of only `logger.handlers`.
    old_formatters = []
    for handler in _effective_handlers(logger):
        old_formatters.append((handler, handler.formatter))
        # Get existing format string or default
        old_fmt = handler.formatter._fmt if handler.formatter else '%(message)s'
        handler.setFormatter(logging.Formatter(f"[RECOVERY] {old_fmt}"))

    try:
        yield logger
    finally:
        # Restore original formatters
        for handler, orig_formatter in old_formatters:
            handler.setFormatter(orig_formatter)

    logger.debug("Exiting recovery mode")


class MonitorNode(NodeInternal):
    def __init__(self, node: dag.Node, config: Optional[ExecutionConfig] = None) -> None:
        super().__init__(node)

        self._config = config or ExecutionConfig()
        self._handle = None
        self._is_factory = False
        self._jobs = list()
        self._num_queued = 0

    @property
    def handle(self) -> Union[int, htcondor2.SubmitResult]:
        return self._handle

    @handle.setter
    def handle(self, val: Union[int, htcondor2.SubmitResult]) -> None:
        if not isinstance(val, (int, htcondor2.SubmitResult)):
            raise ValueError("Monitor node handle value not int or htcondor2.SubmitResult")

        self._handle = val

    @property
    def factory(self) -> bool:
        return self._is_factory

    @factory.setter
    def factory(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise ValueError("factory value must be a bool")

        self._is_factory = value

    @property
    def jobs(self) -> List[int]:
        """Return list of associated job state"""
        return self._jobs

    def job_queued(self) -> None:
        assert self._num_queued >= 0
        self._num_queued += 1

    def job_exited(self) -> None:
        self._num_queued -= 1
        assert self._num_queued >= 0

    @property
    def queued(self) -> int:
        return self._num_queued

    def __getitem__(self, index: int) -> Optional[int]:
        """Get specific job state information if tracked"""
        if not isinstance(index, int):
            raise ValueError("MonitorNode job state lookup index is not an int")

        if index < 0:
            raise IndexError(f"MonitorNode job state lookup index out of range: {index}")

        return self._jobs[index] if index < len(self._jobs) else None

    def __setitem__(self, index: int, value: int) -> None:
        """Set specific job state information"""
        if not isinstance(index, int):
            raise ValueError("MonitorNode job state set index is not an int")

        if index < 0:
            raise IndexError(f"MonitorNode job state lookup index out of range: {index}")

        if not isinstance(value, int):
            raise ValueError("MonitorNode job state set value is not an int")

        if index >= len(self._jobs):
            self._jobs = self._jobs + [JOB_EXIT_UNKNOWN] * ((index + 1) - len(self._jobs))

        self._jobs[index] = value

    def __len__(self) -> int:
        """Get number of tracked job states"""
        return len(self._jobs)

    @property
    def jobid(self) -> Optional[int]:
        """Return this nodes job cluster id"""
        if not self._handle:
            return None

        return self._handle if isinstance(self._handle, int) else self._handle.cluster()

    def Execute(self, **kwargs) -> None:
        schedd = kwargs["schedd"]

        with open(self.jdl, "r") as f:
            JDL = htcondor2.Submit(f.read())

            JDL[JDL_NODE_NAME] = f"{self._node.name}"
            JDL[f"My.{ATTR_NODE_NAME}"] = f'"$({JDL_NODE_NAME})"'

            JDL["batch_name"] = kwargs["batchname"]
            JDL["submit_event_notes_attrs"] = ATTR_NODE_NAME
            JDL[SUBMIT_KEY_DAGMAN_LOG_FILE] = str(kwargs[JDL_LOG_FILE])

            if kwargs.get(JDL_MANAGER_ID) is not None:
                JDL[JDL_MANAGER_ID] = kwargs[JDL_MANAGER_ID]
                JDL[f"My.{ATTR_MANAGER_ID}"] = kwargs[JDL_MANAGER_ID]

            oauth = JDL.issue_credentials()
            if oauth is not None:
                raise RuntimeError(f"HTCondor job submission requires credential from {oauth}")

            origin = Path(self._jdl).parent.resolve()
            with ChangeDir(origin, enabled=self._config.relative_to_source):
                self.handle = schedd.submit(JDL)

        self.state = NodeState.ACTIVE

class MonitorDag(DagInternal):
    def __init__(self, external_dag: dag.Dag):
        super().__init__()
        self._external_dag = external_dag
        self._node_jid_map = dict()

    @property
    def node_jid_map(self) -> dict:
        return self._node_jid_map

    def __getitem__(self, key: Union[int, str]) -> int:
        """Get the node id from the internal mappings"""
        if isinstance(key, str):
            node = self._external_dag[key]
            if node is None:
                raise KeyError(key)
            return node.id
        elif isinstance(key, int):
            return self._node_jid_map[key]

        raise ValueError("Expected node id map key not int or str")

    def __contains__(self, key: Union[int, str]) -> bool:
        """Check if the internal mappings have the specified node id"""
        if isinstance(key, str):
            return self._external_dag[key] is not None
        elif isinstance(key, int):
            return key in self._node_jid_map

        raise ValueError("Contains key not int or str")

    def __setitem__(self, key: int, value: int) -> None:
        """Record a HTCondor cluster id -> node id mapping"""
        if not isinstance(key, int):
            raise ValueError("Expected node id map key not int")

        self._node_jid_map[key] = value

    def _prepare(self, node: dag.Node) -> None:
        """Internal specific node preparations"""
        node.internal.state = NodeState.READY

class MonitorEngine(Engine):
    """Place jobs to condor and monitor"""
    EXIT_SUCCESS = 0
    EXIT_FAILURE = 1

    def __init__(self, dag: dag.Dag, config: Optional[ExecutionConfig] = None) -> None:
        super().__init__(config)

        self.AcquireLock()

        try:
            self._dag = dag
            self._dag.internal = MonitorDag(dag)
            for node in self._dag:
                node.internal = MonitorNode(node, config=self.config)

            self._had_failure = False

            self._state_file = self.workdir / "dataflow.shared.log"
            # htcondor2.JobEventLog requires the file to already exist -- it
            # doesn't create it, and nothing else does either before the
            # first job gets submitted with this path as its dagman_log.
            self._state_file.touch(exist_ok=True)
            self._log_reader = htcondor2.JobEventLog(str(self._state_file))

            self._jid = None
            self._batch_name = "flowman+" + hash_name(Path(".").resolve())

            self._ad = None
            ad_file = os.getenv("_CONDOR_JOB_AD")
            if ad_file is not None:
                with open(ad_file, "r") as f:
                    self._ad = classad2.parseOne(f.read())

            if self._ad:
                self._jid = self._ad["ClusterId"]
                self._batch_name = f"flowman+{self._jid}"

            self._submit_options = {
                "batchname": "_batch_name",
                "manager_id": "_jid",
                JDL_LOG_FILE: "_state_file",
            }

        except:
            self.ReleaseLock()
            raise

    def keys(self) -> List[str]:
        return self._submit_options.keys()

    def __getitem__(self, key: str) -> Any:
        if not isinstance(key, str):
            raise ValueError("MonitorEngine get item key is not a str")

        return getattr(self, self._submit_options[key])

    def __exit(self) -> None:
        if self._had_failure:
            logger.error("####### Failed Nodes #######")

            for node in self._dag:
                if node.internal.IsFailed():
                    logger.error("    Node %s > %s", node.internal.jdl, node.internal.failure)

            logger.error("############################")

        self.ReleaseLock()

    def Bootstrap(self) -> None:
        """Monitor engine setup"""
        for node in self._dag.roots:
            if node.internal.IsBlocked():
                self._dag.internal.prepare(node)
            else:
                logger.debug("Root node %s skipping prepare due to intitial state %s", node.internal.jdl, node.internal.state.name)

    def Cleanup(self) -> None:
        """Monitor engine final cleanup"""
        const = f"{ATTR_MANAGER_ID}={self._jid}"
        if self._jid is None:
            const = "member(ClusterId, {" + ",".join([str(self._dag[nid].internal.jobid) for nid in self._dag.internal.active_nodes]) + "})"

        logger.info(f"Removing flow jobs with constraint: '{const}'")

        schedd = htcondor2.Schedd()
        ret = schedd.act(htcondor2.JobAction.Remove, const, "HTFlow being stopped")

        logger.debug(str(ret))
        self.__exit()

    def Execute(self) -> None:
        """Monitor engine place ready jobs to local HTCondor Schedd"""
        if len(self._dag.internal.ready_nodes) == 0:
            return

        schedd = htcondor2.Schedd()
        attempted = list()
        submitted_any = False

        for i in self._dag.internal.ready_nodes:
            attempted.append(i)

            node = self._dag[i]
            logger.info("Submitting %s", node.internal.jdl)
            try:
                node.internal.Execute(schedd=schedd, **self)
                self._dag.internal.active_nodes.add(node.id)
                submitted_any = True
            except Exception as e:
                self._had_failure = True
                node.internal.Fail(self._dag, str(e))
                logger.error("Failed to submit %s: %s", node.internal.jdl, e)

        self._dag.internal.ready_nodes.difference_update(attempted)

        if submitted_any:
            # schedd.submit() does NOT kick the schedd the way `condor_submit`
            # does -- without this, newly-submitted jobs sit unnoticed until
            # the schedd's own periodic scheduling cycle (SCHEDD_INTERVAL,
            # default 300s) comes around on its own.
            schedd.reschedule()

    def Recover(self) -> None:
        """Monitor engine recover state from shared job log"""
        with log_in_recovery_mode(logger):
            self.__process_log_events(True)

    def Terminate(self) -> Optional[int]:
        """Monitor engine terminal state check: returns exit code"""
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
        """Monitor engine job tracking """
        self.__process_log_events()

    def __process_log_events(self, in_recovery: bool = False):
        num_new_events = 0

        for event in self._log_reader.events(stop_after=0):
            num_new_events += 1

            logger.debug("%s for job %d.%d", event.type.name, event.cluster, event.proc)

            # Don't track events associated with common input transfer shadow
            if event.proc == -1000:
                logger.debug("Skipping event associated with common input transfer shadow")
                continue

            def _get_node() -> dag.Node:
                """Shared function code to get node from event cluster id"""
                nid = self._dag.internal[event.cluster]
                node = self._dag[nid]
                assert node.internal.jobid == event.cluster
                return node

            def _check_node_done(node: dag.Node, cluster_remove: bool = False) -> bool:
                if not cluster_remove and node.internal.factory:
                    return False

                if node.internal.queued != 0:
                    return False

                # Success is all exit codes == 0 -> any() returns false if all zero entries in list
                if not any(node.internal.jobs):
                    node.internal.Done(self._dag)
                else:
                    ec_counts = dict()
                    for v in node.internal.jobs:
                        if v not in ec_counts:
                            ec_counts[v] = 1
                        else:
                            ec_counts[v] += 1

                    logger.debug("Listing all exit codes for cluster %d:", node.internal.jobid)
                    for ec, n in ec_counts.items():
                        disp = str(ec)

                        if ec == JOB_EXIT_UNKNOWN:
                            disp = "UNKNOWN"
                        elif ec == JOB_EXIT_ABORTED:
                            disp = "ABORTED"

                        logger.debug("    Exit %s occurred %d times", disp, n)

                    failed = sum(n for ec, n in ec_counts.items() if ec != 0)
                    node.internal.Fail(self._dag, f"{failed}/{len(node.internal)} job(s) exited unsuccessfully")
                    self._had_failure = True

                self._dag.internal.active_nodes.discard(node.id)

                return True

            if event.type in [htcondor2.JobEventType.SUBMIT, htcondor2.JobEventType.CLUSTER_SUBMIT]:
                nid = self._dag.internal[event["StructuredNotes"][ATTR_NODE_NAME]]
                node = self._dag[nid]

                if in_recovery:
                    node.internal.handle = event.cluster
                else:
                    assert node.internal.handle.cluster() == event.cluster

                if event.type == htcondor2.JobEventType.CLUSTER_SUBMIT:
                    node.internal.factory = True
                else:
                    node.internal.job_queued()

                if event.cluster not in self._dag.internal:
                    self._dag.internal[event.cluster] = node.id

                # CLUSTER_SUBMIT events describe the whole cluster, not a single
                # proc, and carry proc == -1 -- there's no per-job state to seed yet.
                if event.proc >= 0:
                    node.internal[event.proc] = JOB_EXIT_UNKNOWN

            elif event.type == htcondor2.JobEventType.JOB_TERMINATED:
                node = _get_node()
                node.internal[event.proc] = event["ReturnValue"] if event["TerminatedNormally"] else event["TerminatedBySignal"] * -1
                node.internal.job_exited()
                _check_node_done(node)

            elif event.type == htcondor2.JobEventType.JOB_ABORTED:
                node = _get_node()
                node.internal[event.proc] = JOB_EXIT_ABORTED
                node.internal.job_exited()
                _check_node_done(node)

            elif event.type == htcondor2.JobEventType.CLUSTER_REMOVE:
                node = _get_node()
                assert node.internal.factory
                if not _check_node_done(node, True):
                    raise RuntimeError(f"Node {node.internal.jdl} not done after CLUSTER_REMOVE event")

        if not in_recovery:
            logger.debug("Processed %d new events", num_new_events)
