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

from __future__ import annotations

import htcondor2
import enum
from . import dag
import copy

from pathlib import Path
from time import ctime
from typing import List, Dict, Tuple, Union, Optional, Final

class Assumption(enum.Enum):
    """Enumeration of enforcable assumptions"""
    SINGLE_FILE_SRC = 1
    NO_MACROS = 3
    NO_DIRECTORIES = 4
    NO_URL = 5
    NO_REMAPS = 6

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

    def __init__(self, files: List[Union[Path, str]] = [], filename: str = "dataflow.dag") -> None:
        if not isinstance(files, list) or any([not isinstance(f, (Path, str)) for f in files]):
            raise ValueError("files must be a list of strings and/or pathlib.Path objects")
        if not isinstance(filename, str):
            raise ValueError("filename must be a string")

        # List of JDL files to process. Note: list position is node id number
        self._files = [Path(f) for f in files]
        # Filename of DAG to write
        self._filename = filename

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
    def mapping(self) -> Dict[Path, Tuple[Optional[int], Optional[List[int]]]]:
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

    def __resolve(self) -> None:
        """Produce node table from list of JDL files"""
        self.__reset()

        outfile_node = {}

        self._dag = dag.Dag()
        for i, jdl in enumerate(self._files):
            node = self._dag.AddNode(f"NODE-{i}")
            node.internal = jdl

        def process_transfer_list(desc: htcondor2.Submit, key: str, jdl: Path) -> list:
            if desc.get(key) is None:
                return []
            # Give a chance for simple macro expansion to resolve
            value = desc.expand(key) or ""
            if value is None or len(value) == 0:
                return []

            if "://" in value:
                raise AssumptionError(f"{key} contains URLs", Assumption.NO_URL, jdl)
            elif "$(" in value:
                raise AssumptionError(f"{key} contains macro substitutions", Assumption.NO_MACROS, jdl)

            return [v.strip() for v in value.split(",") if len(v.strip()) > 0]

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

            # Process output file list
            for f in outfiles:
                outfile = Path(f)

                # Ensure only one JDL file produces this output file
                if outfile in outfile_node:
                    curr = outfile_node[outfile]
                    raise AssumptionError(f"Output file '{outfile}' already defined in {self._dag[curr].internal}", Assumption.SINGLE_FILE_SRC, node.internal)
                outfile_node[outfile] = node.id

                dependencies = self._f2n_table[outfile][1] if outfile in self._f2n_table else []
                self._f2n_table[outfile] = (node.id, dependencies)

            # Process input file list
            for f in infiles:
                infile = Path(f)
                if infile in self._f2n_table:
                    self._f2n_table[infile][1].append(node.id)
                else:
                    self._f2n_table[infile] = (None, [node.id])

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

            # TODO: Optimize writing multiple parents to shared children?
            for node in self._dag:
                f.write(f"JOB {node.name} {node.internal}\n")
                if node.children is not None:
                    relations = f"PARENT {node.name} CHILD"
                    for idx in node.children:
                        relations += f" {self._dag[idx].name}"
                    f.write(f"{relations}\n")

        return DAG
