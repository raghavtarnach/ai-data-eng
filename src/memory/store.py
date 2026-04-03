"""
Memory store — ChromaDB interface (v2 scope, stubbed for v1).

In v2, this module provides semantic retrieval of engineering patterns
from ChromaDB. For v1, all methods return empty results so agents
operate without memory context.

Memory retrieval triggers (per spec §9.3):
    1. Before architecture design — query architecture_pattern by tech tags
    2. Before code generation — query pipeline_template by transformation type

Memory is written only after successful VALIDATION — not after generation alone.
"""

from __future__ import annotations

from typing import Any


class MemoryStore:
    """Stub memory store for v1.

    All retrieval methods return empty results. The interface is
    finalized so agents can integrate without changes when ChromaDB
    is wired up in v2.
    """

    async def query_patterns(
        self,
        category: str,
        tags: list[str],
        top_k: int = 3,
    ) -> list[dict[str, Any]]:
        """Query engineering patterns by category and tags.

        Args:
            category: Pattern category (architecture_pattern | pipeline_template).
            tags: Filter tags matching the project's technology stack.
            top_k: Max results to return (default 3 per spec).

        Returns:
            List of pattern dicts with pattern_id, description, content, tags.
            Empty list in v1.
        """
        # v2: ChromaDB semantic search
        return []

    async def store_pattern(
        self,
        category: str,
        description: str,
        content: str,
        tags: list[str],
        source_run_id: str,
    ) -> str:
        """Store a validated engineering pattern in memory.

        Only called after successful VALIDATION stage.

        Args:
            category: Pattern category.
            description: Human-readable description.
            content: Code or config snippet.
            tags: Searchable tags.
            source_run_id: Run that produced this pattern.

        Returns:
            pattern_id of the stored pattern. Empty string in v1.
        """
        # v2: ChromaDB insert
        return ""


# Singleton
memory_store = MemoryStore()
