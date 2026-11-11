"""Structural interfaces shared by the domain, application and adapters.

This module holds no I/O and no implementation: the domain and the runner
only know the shapes declared here, so an in-memory test double and the
SQLite adapter are interchangeable.
"""

from typing import Any, ContextManager, Dict, List, Optional, Protocol, runtime_checkable


class HarvestError(Exception):
    """Business failure whose code is part of the replay contract."""


@runtime_checkable
class Portal(Protocol):
    """Remote delivery boundary. The domain never depends on it directly."""

    def request(self, request: Dict[str, Any]) -> Any:
        """Return a JSON value or raise HarvestError."""


@runtime_checkable
class Repository(Protocol):
    """Minimal persistence port required by the application runner."""

    def read_control(self) -> Dict[str, Any]:
        ...

    def read_record(self, identifier: str) -> Optional[Dict[str, Any]]:
        ...

    def read_receipt(self, page: str) -> Optional[Dict[str, Any]]:
        ...

    def write_control(self, control: Dict[str, Any]) -> None:
        ...

    def write_record(self, row: Dict[str, Any]) -> None:
        ...

    def write_receipt(self, receipt: Dict[str, Any]) -> None:
        ...

    def append_audit(self, event: Dict[str, Any]) -> None:
        ...

    def transaction(self) -> ContextManager[None]:
        ...

    def snapshot(self) -> Dict[str, Any]:
        ...
