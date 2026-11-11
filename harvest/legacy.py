"""Compatibility facade for the historical Collector entry points.

This module only wires the old constructor arguments to the real components:
a :class:`~harvest.adapters.sqlite.SqliteRepository` for persistence and a
:class:`~harvest.application.Runner` for execution. It contains no SQL, no
phase state machine and no alternate executor: every legacy call runs through
the same Runner, domain core and repository adapter as the rest of the system.
"""

from .adapters.sqlite import SqliteRepository
from .application import Runner
from .ports import HarvestError

__all__ = ["Collector", "HarvestError"]


class Collector:
    """Backward-compatible facade over Runner + SqliteRepository."""

    def __init__(self, connection, transport):
        self.repository = SqliteRepository(connection)
        self.runner = Runner(self.repository, transport)

    def run(self, steps=100, fault=None):
        return self.runner.run(steps=steps, fault=fault)

    def state(self):
        return self.runner.state()

    def snapshot(self):
        return self.runner.snapshot()

    def page(self, raw, cursor):
        return self.runner.page(raw, cursor)
