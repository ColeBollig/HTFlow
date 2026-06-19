from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, List

FILE_HANDLERS: Dict[str, Callable[[Path], List[Path]]] = {}


def register(ext: str, handler: Callable[[Path], List[Path]]) -> None:
    FILE_HANDLERS[ext] = handler
