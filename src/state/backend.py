"""
State backend abstract interface.

All state persistence implementations must conform to this interface.
The orchestrator calls save_state() after every stage transition — this is
a hard contract, not a suggestion.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from src.models.project import ProjectState


class StateBackend(ABC):
    """Abstract base for state persistence backends.

    Implementations must guarantee that save_state() is durable — once it returns,
    the state is recoverable even after a process crash.
    """

    @abstractmethod
    async def save_state(self, state: ProjectState) -> None:
        """Persist the full project state.

        Called after every stage transition. Must be atomic — partial writes
        are not acceptable.
        """
        ...

    @abstractmethod
    async def load_state(self, run_id: str) -> Optional[ProjectState]:
        """Load a project state by run_id.

        Returns None if no state exists for the given run_id.
        """
        ...

    @abstractmethod
    async def get_latest_run_id(self, project_id: str) -> Optional[str]:
        """Get the latest successful run_id for a project.

        Returns None if the project has no successful runs.
        """
        ...

    @abstractmethod
    async def update_latest_run(self, project_id: str, run_id: str) -> None:
        """Update the latest successful run mapping for a project."""
        ...
