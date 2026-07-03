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

"""Show list of root, intermediate, and leaf files in dataflow"""

from __future__ import annotations

import argparse
import logging

from htflow.dataflow import HTCondorDataFlow

logger = logging.getLogger(__name__)


def run(df: HTCondorDataFlow, args: argparse.Namespace) -> None:
    logger.debug("Displaying data flow files")

    df.generate()

    file_groups = dict()
    for f in df.mapping.keys():
        protocol = "cedar"
        f_str = str(f)

        if "://" in f_str:
            protocol = f_str[:f_str.find("://")].lower()

        if protocol in file_groups:
            file_groups[protocol].append(f)
        else:
            file_groups[protocol] = [ f ]

    first = True
    for protocol, files in file_groups.items():
        if not first:
            print("")

        print(f"{protocol.upper()} files in dataflow")
        print(" Gen | Consumers | File")
        print("-----+-----------+----->")
        for f in files:
            src, dependencies = df.mapping[f]
            gen = "-" if src is None else "T"
            consumers = len(dependencies)
            print(f"  {gen}  | {consumers:>9} | {f}")

        first = False
