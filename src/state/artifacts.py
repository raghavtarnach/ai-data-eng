"""
Artifact versioning and storage.

Artifacts are stored under {storage_root}/{project_id}/{run_id}/{stage}/.
Run IDs are unique per execution — re-runs never overwrite prior artifacts.
This module provides a local filesystem implementation that can be swapped
for Azure Blob in production.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from src.config import settings


class ArtifactStore:
    """Immutable, versioned artifact storage.

    Directory layout:
        {storage_root}/
            {project_id}/
                {run_id}/
                    {stage}/
                        {filename}

    Writes are append-only. Once written, artifacts are never modified or deleted.
    """

    def __init__(self, storage_root: Optional[Path] = None):
        self._root = storage_root or settings.storage.root

    def _stage_dir(self, project_id: str, run_id: str, stage: str) -> Path:
        """Get the directory for a specific stage's artifacts."""
        return self._root / project_id / run_id / stage

    async def save_artifact(
        self,
        project_id: str,
        run_id: str,
        stage: str,
        filename: str,
        content: str | bytes | dict[str, Any],
    ) -> str:
        """Save an artifact to the versioned store.

        Args:
            project_id: Project identifier.
            run_id: Unique run identifier.
            stage: Pipeline stage name.
            filename: Artifact filename.
            content: Content to write — string, bytes, or dict (serialized as JSON).

        Returns:
            Absolute path to the saved artifact.
        """
        stage_dir = self._stage_dir(project_id, run_id, stage)
        stage_dir.mkdir(parents=True, exist_ok=True)

        filepath = stage_dir / filename

        if isinstance(content, dict):
            filepath.write_text(json.dumps(content, indent=2, default=str))
        elif isinstance(content, bytes):
            filepath.write_bytes(content)
        else:
            filepath.write_text(content)

        return str(filepath.resolve())

    async def load_artifact(
        self,
        project_id: str,
        run_id: str,
        stage: str,
        filename: str,
    ) -> Optional[str]:
        """Load an artifact's content by path.

        Returns None if the artifact doesn't exist.
        """
        filepath = self._stage_dir(project_id, run_id, stage) / filename
        if not filepath.exists():
            return None
        return filepath.read_text()

    async def list_artifacts(
        self, project_id: str, run_id: str, stage: str
    ) -> list[str]:
        """List all artifact filenames for a given stage."""
        stage_dir = self._stage_dir(project_id, run_id, stage)
        if not stage_dir.exists():
            return []
        return [f.name for f in stage_dir.iterdir() if f.is_file()]

    async def get_artifact_path(
        self, project_id: str, run_id: str, stage: str, filename: str
    ) -> Optional[Path]:
        """Get the absolute path to an artifact, or None if it doesn't exist."""
        filepath = self._stage_dir(project_id, run_id, stage) / filename
        return filepath if filepath.exists() else None
