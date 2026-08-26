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

## Assumptions:
##    1. All files only have one source (external or a single node)
##    2. All input/output file lists are complete lists
##    3. There is no macro use requiring subsitution in input/output file lists
##    4. No directories are specified in input/output file lists
##    5. All files transfered are locally transferred (i.e. CEDAR)
##    6. No file remapping
##    7. No relative input/output file path begins with a parent directory reference (..),
##       unless --relative-to-source or --resolve-from is active

from __future__ import annotations

import htcondor2
import enum
from . import dag
from .config import ExecutionConfig
from .engines.engine import Engine
from .utils.naming import hash_name
import copy

from pathlib import Path
from time import ctime
from typing import List, Dict, Tuple, Set, Union, Optional, Final

ALLOWED_PROTOCOLS = [
    "osdf",
    "pelican",
]

class Assumption(enum.Enum):
    """Enumeration of enforcable assumptions"""
    SINGLE_FILE_SRC = 1
    COMPLETE_LIST = 2
    NO_MACROS = 3
    NO_DIRECTORIES = 4
    NO_URL = 5
    NO_REMAPS = 6
    NO_PARENT_TRAVERSAL = 7

class AssumptionError(Exception):
    def __init__(self, msg: str, assumption: Assumption, src: Path) -> None:
        super().__init__(f"Assumption {assumption.value} Violated: {msg} in {src}")
        self.assumption = assumption
        self.source = src

class HTCondorDataFlow():
    """
    Class to convert collection of Job Description files into a DAG
    based on the dependencies of the described output to input files.
    """

    KEY_JOB_TYPE = "JobType"
    KEY_INPUT_FILES = "InputFiles"
    KEY_OUTPUT_FILES = "OutputFiles"

    def __init__(self,
                 files: List[Union[Path, str]] = [],
                 filename: str = "dataflow.dag",
                 job_shapes: Dict[str, Dict[str, str]] = None,
                 config: Optional[ExecutionConfig] = None
    ) -> None:
        if not isinstance(files, list) or any([not isinstance(f, (Path, str)) for f in files]):
            raise ValueError("files must be a list of strings and/or pathlib.Path objects")
        if not isinstance(filename, str):
            raise ValueError("filename must be a string")
        if job_shapes is None:
            job_shapes = {}
        self.__verify_new_types(job_shapes)

        # List of JDL files to process. Note: list position is node id number
        self._files = [Path(f) for f in files]
        # Filename of DAG to write
        self._filename = filename

        # Mapping of job types -> shapes (input/output lists)
        self._job_type_shapes = job_shapes

        # Shared static configuration controlling dataflow/execution behavior
        self._config = config or ExecutionConfig()

        # Table: Output file -> (Source Node | Dependency Nodes)
        self._f2n_table = {}
        # Internal DAG structure
        self._dag = None

    @property
    def files(self) -> List[Path]:
        """Get list of JDL files"""
        return self._files

    @files.setter
    def files(self, value: List[Union[Path, str]]) -> None:
        """Add JDL file(s) to internal list"""
        if not isinstance(value, list) or any([not isinstance(f, (Path, str)) for f in value]):
            raise ValueError("files must be a list of strings and/or pathlib.Path objects")

        self._files = [Path(v) for v in value]

    def __iadd__(self, other: Union[Path, str]) -> HTCondorDataFlow:
        """Implementation of in-place addition (+=) to append JDL file to list of files to process"""
        if not isinstance(other, (Path, str)):
            return NotImplemented

        self._files.append(Path(other))
        return self

    def add(self, jdl: Union[Path, str]) -> None:
        """Append JDL file to list of files to process"""
        if not isinstance(jdl, (Path, str)):
            raise ValueError("file must be a string or pathlib.Path object")

        self._files.append(Path(jdl))

    @property
    def filename(self) -> str:
        """Get current DAG filename"""
        return self._filename

    @filename.setter
    def filename(self, value: str) -> None:
        """Set DAG filename"""
        if not isinstance(value, str):
            raise ValueError("filename must be a string")
        self._filename = value

    @property
    def shapes(self) -> Dict[str, Dict[str, str]]:
        """Get mapping of JDL job types job shape information pertinent to dataflow logic"""
        return self._job_type_shapes

    @shapes.setter
    def shapes(self, val: Dict[str, Dict[str, str]]) -> None:
        self.__verify_new_types(val)
        self._job_type_shapes = val

    @property
    def config(self) -> ExecutionConfig:
        """Get the shared static configuration controlling this dataflow's behavior"""
        return self._config

    @staticmethod
    def __verify_new_types(verify: Dict[str, Dict[str, str]]) -> None:
        """Verify incoming job type attribute information"""
        # Expect { Abitrary Name -> { JDL Key -> Value}} e.g. { foo -> { transfer_input_files -> "foo.txt,bar.txt" }}
        if not isinstance(verify, dict):
            raise ValueError("Dataflow types must be a dictionary")

        for key, val in verify.items():
            if not isinstance(key, str):
                raise ValueError("Dataflow types dictionary expects string keys")
            elif not isinstance(val, dict):
                raise ValueError("Dataflow types dictionary expects dictionary for values")

            for k, v in val.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    raise ValueError("Dataflow types per type dictionary expects only string values")

    @property
    def types(self) -> Set[str]:
        types = set()
        for jdl in self._files:
            with open(jdl, "r") as f:
                desc = htcondor2.Submit(f.read())
                t = desc.get(HTCondorDataFlow.KEY_JOB_TYPE)
                if t is not None:
                    types.add(t)

        return types

    @property
    def mapping(self) -> Dict[Union[Path, str], Tuple[Optional[int], Optional[List[int]]]]:
        """Get general mapping of output file to node information"""
        return self._f2n_table

    @property
    def groupings(self) -> Tuple[List[Path], List[Path], List[Path]]:
        """Get groupings of dataflow files (roots, intermediate, and leafs)"""
        roots = list()
        intermediate = list()
        leafs = list()

        for fname, details in self._f2n_table.items():
            src, dependencies = details
            if src is not None and len(dependencies) > 0:
                intermediate.append(fname)
            elif src is not None:
                leafs.append(fname)
            else:
                roots.append(fname)

        return (roots, intermediate, leafs)

    @property
    def dag(self) -> Final[Optional[dag.Dag]]:
        return self._dag

    def __reset(self) -> None:
        """Reset internal data structures"""
        self._f2n_table.clear()
        self._dag = None

    def __write_resolved(self, pending: List[Tuple[dag.Node, htcondor2.Submit]]) -> None:
        """Write resolved submit files for nodes whose transfer lists changed, whether from
        a job type shape merge or from --resolve-from's absolute-path rewriting.

        Under relative_to_source, each file is written beside its original JDL, as before.
        Otherwise all resolved files are centralized under the engine working directory;
        JDLs sharing a basename but originating from different source directories are
        siloed into numbered subdirectories (mapped 1:1 to their source directory) so
        they don't collide, while non-colliding files are written flat.
        """
        if self._config.relative_to_source:
            for node, desc in pending:
                new_jdl = node.internal.with_suffix(node.internal.suffix + ".resolved")
                with open(new_jdl, "w") as f:
                    f.write(str(desc))
                node.internal = new_jdl
            return

        dirs_by_name: Dict[str, Set[Path]] = {}
        for node, _ in pending:
            dirs_by_name.setdefault(node.internal.name, set()).add(node.internal.parent)

        colliding_dirs = {d for dirs in dirs_by_name.values() if len(dirs) > 1 for d in dirs}

        silo_of_dir: Dict[Path, int] = {}
        for node, _ in pending:
            src_dir = node.internal.parent
            if src_dir in colliding_dirs and src_dir not in silo_of_dir:
                silo_of_dir[src_dir] = len(silo_of_dir) + 1

        resolved_root = Engine.work_dir() / "produced" / "resolved"

        for node, desc in pending:
            src_dir = node.internal.parent
            target_dir = resolved_root / str(silo_of_dir[src_dir]) if src_dir in silo_of_dir else resolved_root
            target_dir.mkdir(parents=True, exist_ok=True)

            new_jdl = target_dir / (node.internal.name + ".resolved")
            with open(new_jdl, "w") as f:
                f.write(str(desc))
            node.internal = new_jdl

    def __resolve(self) -> None:
        """Produce node table from list of JDL files"""
        self.__reset()

        outfile_node = {}
        pending_resolutions = []

        self._dag = dag.Dag()
        for i, jdl in enumerate(self._files):
            name = hash_name(jdl, length=self._config.node_name_length)
            node = self._dag.AddNode(name)
            node.internal = jdl

        # Parent-directory traversal is only disallowed when no path resolution flag is
        # active; --relative-to-source/--resolve-from users have opted into their own
        # explicit path-anchoring behavior and may legitimately need to reach upward.
        enforce_no_parent_traversal = not self._config.relative_to_source and self._config.resolve_from is None

        def process_transfer_list(desc: htcondor2.Submit, key: str, jdl: Path, errkey: Optional[str] = None) -> list:
            if desc.get(key) is None:
                return []
            # Give a chance for simple macro expansion to resolve
            value = desc.expand(key) or ""
            if value is None or len(value) == 0:
                return []

            if not errkey:
                errkey = key

            entries = [v.strip() for v in value.split(",") if len(v.strip()) > 0]
            for entry in entries:
                if "://" in entry:
                    protocol = entry[:entry.find("://")].lower()
                    if protocol not in ALLOWED_PROTOCOLS:
                        raise AssumptionError(f"{errkey} contains URLs: {entry}", Assumption.NO_URL, jdl)
                elif "$(" in entry:
                    raise AssumptionError(f"{errkey} contains macro substitutions: {entry}", Assumption.NO_MACROS, jdl)
                elif enforce_no_parent_traversal and Path(entry).parts[:1] == ("..",):
                    raise AssumptionError(f"{errkey} contains a parent directory reference: {entry}", Assumption.NO_PARENT_TRAVERSAL, jdl)
            return entries

        def _file_key(f: str) -> Union[Path, str]:
            return f if "://" in f else Path(f)

        resolve_from = self._config.resolve_from

        def absolutize(entry: str) -> str:
            """Rewrite a relative, non-URL transfer-file-list entry to an absolute
            path anchored at --resolve-from; leave URLs/already-absolute entries as-is."""
            if "://" in entry:
                return entry
            p = Path(entry)
            return str(p) if p.is_absolute() else str(resolve_from / p)

        # Process all JDL files and associated with a node
        for node in self._dag:
            # Read JDL file for input/output file lists
            with open(node.internal, "r") as f:
                desc = htcondor2.Submit(f.read())

                if desc.get("output_destination") is not None:
                    raise AssumptionError(f"URL output destination specified", Assumption.NO_URL, node.internal)
                elif desc.get("output_directory") is not None:
                    raise AssumptionError(f"Output directory specified", Assumption.NO_DIRECTORIES, node.internal)
                elif desc.get("transfer_output_remaps") is not None:
                    raise AssumptionError(f"Output file re-mapping specified", Assumption.NO_REMAPS, node.internal)

                infiles = process_transfer_list(desc, "transfer_input_files", node.internal)
                outfiles = process_transfer_list(desc, "transfer_output_files", node.internal)

                # Extend transfer lists to include specific job type shape information
                job_type = desc.get(HTCondorDataFlow.KEY_JOB_TYPE)
                if job_type is not None:
                    if job_type not in self._job_type_shapes:
                        raise AssumptionError(f"No knowledge of job type '{job_type}'", Assumption.COMPLETE_LIST, node.internal)

                    SHAPE = htcondor2.Submit(self._job_type_shapes[job_type])
                    had_change = False

                    if HTCondorDataFlow.KEY_INPUT_FILES in self._job_type_shapes[job_type]:
                        had_change = True
                        inputs = process_transfer_list(SHAPE, HTCondorDataFlow.KEY_INPUT_FILES, node.internal, f"Job type '{job_type}' input file list")
                        infiles = list(dict.fromkeys(infiles + inputs))
                        desc["transfer_input_files"] = ",".join(infiles)

                    if HTCondorDataFlow.KEY_OUTPUT_FILES in self._job_type_shapes[job_type]:
                        had_change = True
                        outputs = process_transfer_list(SHAPE, HTCondorDataFlow.KEY_OUTPUT_FILES, node.internal, f"Job type '{job_type}' output file list")
                        outfiles = list(dict.fromkeys(outfiles + outputs))
                        desc["transfer_output_files"] = ",".join(outfiles)
                else:
                    had_change = False

                # Rewrite relative transfer_input_files/transfer_output_files entries to
                # absolute paths anchored at --resolve-from (independent of job type shapes).
                if resolve_from is not None:
                    resolved_infiles = [absolutize(f) for f in infiles]
                    resolved_outfiles = [absolutize(f) for f in outfiles]

                    if resolved_infiles != infiles or resolved_outfiles != outfiles:
                        had_change = True
                        infiles, outfiles = resolved_infiles, resolved_outfiles
                        desc["transfer_input_files"] = ",".join(infiles)
                        desc["transfer_output_files"] = ",".join(outfiles)

                if had_change:
                    pending_resolutions.append((node, desc))

            # Process output file list
            for f in outfiles:
                outfile = _file_key(f)

                # Ensure only one JDL file produces this output file
                if outfile in outfile_node:
                    curr = outfile_node[outfile]
                    raise AssumptionError(f"Output file '{outfile}' already defined in {self._dag[curr].internal}", Assumption.SINGLE_FILE_SRC, node.internal)
                outfile_node[outfile] = node.id

                dependencies = self._f2n_table[outfile][1] if outfile in self._f2n_table else []
                self._f2n_table[outfile] = (node.id, dependencies)

            # Process input file list
            for f in infiles:
                infile = _file_key(f)
                if infile in self._f2n_table:
                    self._f2n_table[infile][1].append(node.id)
                else:
                    self._f2n_table[infile] = (None, [node.id])

        if pending_resolutions:
            self.__write_resolved(pending_resolutions)

        for _, info in self._f2n_table.items():
            parent, children = info
            if parent is not None:
                self._dag.Connect(parent, children)

        if self._dag.Cycle():
            raise RuntimeError("Dataflow produces a cycle in DAG!")

    def generate(self) -> dag.Dag:
        """Generate a dataflow DAG based on JDL files"""
        self.__resolve()
        return copy.deepcopy(self._dag)

    def write(self) -> Path:
        """Procude a dataflow DAG file for HTCondor's DAGMan"""
        self.__resolve()

        DAG = Path(self._filename)
        with open(DAG, "w") as f:
            f.write("# Automatically written HTCondor DAG file from Dataflow\n")
            f.write(f"# Generated: {ctime()}\n")

            child_to_parents = {}
            for node in self._dag:
                jdl = Path(node.internal)
                if self._config.relative_to_source:
                    declaration = f"JOB {node.name} {jdl.name}" if jdl.parent.resolve() == Path.cwd() else f"JOB {node.name} {jdl.name} DIR {jdl.parent}"
                else:
                    declaration = f"JOB {node.name} {jdl.resolve()}"
                f.write(f"{declaration}\n")

                # Group children by their parent-set; each unique parent-set gets one PARENT…CHILD line
                if node.children is not None:
                    for child_id in node.children:
                        child_to_parents.setdefault(child_id, set()).add(node.id)

            # Only do secondary writing if we have parent/child relationships
            if child_to_parents:
                f.write("\n# Node relationships determined by dataflow:\n")

                parent_set_to_children = {}
                for child_id, parent_ids in child_to_parents.items():
                    parent_set_to_children.setdefault(frozenset(parent_ids), []).append(child_id)

                for parent_ids, child_ids in sorted(parent_set_to_children.items(), key=lambda kv: tuple(sorted(kv[0]))):
                    parents_str  = " ".join(self._dag[i].name for i in sorted(parent_ids))
                    children_str = " ".join(self._dag[i].name for i in sorted(child_ids))
                    f.write(f"PARENT {parents_str} CHILD {children_str}\n")

        return DAG
