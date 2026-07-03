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

from __future__ import annotations

import os
import pathlib
import logging
from typing import Union

logger = logging.getLogger(__name__)

class ChangeDir:
    """Context manager that temporarily changes the working directory.

    On exit — whether normal or via exception — the original directory is restored.
    """

    @staticmethod
    def __check_type(var: Union[pathlib.Path, str]) -> None:
        """Ensure incoming variable type is pathlib.Path/string or except"""
        if not isinstance(var, (pathlib.Path, str)):
            raise TypeError("ChangeDir only functions with pathlib.Path and string types")

    def __init__(self, dest: Union[pathlib.Path, str], enabled: bool = True) -> None:
        """Initialize a temporary directory change object to specified destination directory"""
        self.__check_type(dest)
        if not isinstance(enabled, bool):
            raise TypeError("ChangeDir enabled flag must be a bool")

        self.destination = pathlib.Path(dest)
        self.enabled = enabled
        self.origin = None

    def __enter__(self) -> ChangeDir:
        """Temporarily switch to destination directory"""
        if self.enabled and self.destination.resolve() != pathlib.Path.cwd().resolve():
            logger.debug("[ENTER] Switching directory to %s", self.destination)
            self.origin = pathlib.Path.cwd()
            os.chdir(self.destination)
        return self

    def __truediv__(self, other: Union[pathlib.Path, str]) -> pathlib.Path:
        """Create filesystem path based on the destination path"""
        self.__check_type(other)
        return self.destination / other

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Switch back to original directory"""
        if self.origin is not None:
            logger.debug("[EXIT]  Switching directory to %s", self.origin)
            os.chdir(self.origin)
            self.origin = None
