"""
Unit tests for the artifact store.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.state.artifacts import ArtifactStore


@pytest.fixture
def artifact_store(tmp_path):
    return ArtifactStore(storage_root=tmp_path)


class TestArtifactStore:
    @pytest.mark.asyncio
    async def test_save_and_load_string(self, artifact_store):
        path = await artifact_store.save_artifact(
            "proj1", "run1", "IMPLEMENTATION", "extract.py", "print('hello')"
        )
        content = await artifact_store.load_artifact(
            "proj1", "run1", "IMPLEMENTATION", "extract.py"
        )
        assert content == "print('hello')"
        assert Path(path).exists()

    @pytest.mark.asyncio
    async def test_save_and_load_dict(self, artifact_store):
        data = {"key": "value", "nested": {"a": 1}}
        await artifact_store.save_artifact("proj1", "run1", "VALIDATION", "result.json", data)
        content = await artifact_store.load_artifact("proj1", "run1", "VALIDATION", "result.json")
        parsed = json.loads(content)
        assert parsed["key"] == "value"
        assert parsed["nested"]["a"] == 1

    @pytest.mark.asyncio
    async def test_save_and_load_bytes(self, artifact_store):
        await artifact_store.save_artifact(
            "proj1", "run1", "IMPLEMENTATION", "data.bin", b"\x00\x01\x02"
        )
        path = await artifact_store.get_artifact_path(
            "proj1", "run1", "IMPLEMENTATION", "data.bin"
        )
        assert path is not None
        assert path.read_bytes() == b"\x00\x01\x02"

    @pytest.mark.asyncio
    async def test_load_nonexistent_returns_none(self, artifact_store):
        content = await artifact_store.load_artifact("proj1", "run1", "STAGE", "nope.txt")
        assert content is None

    @pytest.mark.asyncio
    async def test_list_artifacts(self, artifact_store):
        await artifact_store.save_artifact("proj1", "run1", "STAGE", "a.py", "code a")
        await artifact_store.save_artifact("proj1", "run1", "STAGE", "b.py", "code b")
        files = await artifact_store.list_artifacts("proj1", "run1", "STAGE")
        assert set(files) == {"a.py", "b.py"}

    @pytest.mark.asyncio
    async def test_list_empty_stage(self, artifact_store):
        files = await artifact_store.list_artifacts("proj1", "run1", "EMPTY")
        assert files == []

    @pytest.mark.asyncio
    async def test_versioned_isolation(self, artifact_store):
        """Different run_ids should produce isolated artifacts."""
        await artifact_store.save_artifact("p1", "run1", "STAGE", "f.txt", "v1")
        await artifact_store.save_artifact("p1", "run2", "STAGE", "f.txt", "v2")

        c1 = await artifact_store.load_artifact("p1", "run1", "STAGE", "f.txt")
        c2 = await artifact_store.load_artifact("p1", "run2", "STAGE", "f.txt")
        assert c1 == "v1"
        assert c2 == "v2"
