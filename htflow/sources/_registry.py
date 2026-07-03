from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, List, Optional

Handler = Callable[[Path], List[Path]]

FILE_HANDLERS: Dict[str, Handler] = {}
_DEFAULT_HANDLER: Optional[Handler] = None


def register(ext: str, handler: Handler) -> None:
    """Register a handler that overrides the default for a specific extension (e.g. '.snakemake')."""
    FILE_HANDLERS[ext] = handler


def set_default_handler(handler: Handler) -> None:
    """Set the handler used for any extension without a specific override registered."""
    global _DEFAULT_HANDLER
    _DEFAULT_HANDLER = handler


def handler_for(ext: str) -> Optional[Handler]:
    """Get the handler for a given extension, falling back to the default handler."""
    return FILE_HANDLERS.get(ext, _DEFAULT_HANDLER)
