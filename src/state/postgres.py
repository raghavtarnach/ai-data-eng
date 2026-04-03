"""
PostgreSQL state persistence backend.

Uses SQLAlchemy async with asyncpg driver. State is stored as JSONB in a
single table for queryability. A separate table tracks the latest successful
run per project.

Tables:
    project_runs  (run_id UUID PK, project_id UUID, state JSONB, updated_at TIMESTAMPTZ)
    project_latest (project_id UUID PK, latest_run_id UUID FK)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Column, DateTime, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from src.config import settings
from src.models.project import ProjectState
from src.state.backend import StateBackend


# ─── SQLAlchemy ORM Models ─────────────────────────────────────────────────


class Base(DeclarativeBase):
    pass


class ProjectRunRow(Base):
    """Maps to the project_runs table."""

    __tablename__ = "project_runs"

    run_id = Column(String, primary_key=True)
    project_id = Column(String, nullable=False, index=True)
    state = Column(JSONB, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class ProjectLatestRow(Base):
    """Maps to the project_latest table."""

    __tablename__ = "project_latest"

    project_id = Column(String, primary_key=True)
    latest_run_id = Column(String, nullable=False)


# ─── Backend Implementation ────────────────────────────────────────────────


class PostgresStateBackend(StateBackend):
    """PostgreSQL-backed state persistence.

    Guarantees:
        - save_state() is atomic (single UPSERT)
        - State is queryable via standard SQL
        - Supports concurrent reads via connection pooling
    """

    def __init__(self, database_url: Optional[str] = None):
        url = database_url or settings.database.url
        self._engine = create_async_engine(
            url,
            pool_size=settings.database.pool_size,
            max_overflow=settings.database.max_overflow,
            echo=False,
        )
        self._session_factory = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )

    async def initialize(self) -> None:
        """Create tables if they don't exist."""
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        """Dispose of the connection pool."""
        await self._engine.dispose()

    async def save_state(self, state: ProjectState) -> None:
        """Persist project state via UPSERT.

        Uses PostgreSQL ON CONFLICT for atomicity — this is a single
        statement, not a read-then-write.
        """
        state_json = json.loads(state.model_dump_json())
        now = datetime.now(timezone.utc)

        async with self._session_factory() as session:
            async with session.begin():
                # Upsert into project_runs
                await session.execute(
                    text("""
                        INSERT INTO project_runs (run_id, project_id, state, updated_at)
                        VALUES (:run_id, :project_id, :state, :updated_at)
                        ON CONFLICT (run_id) DO UPDATE SET
                            state = EXCLUDED.state,
                            updated_at = EXCLUDED.updated_at
                    """),
                    {
                        "run_id": state.run_id,
                        "project_id": state.project_id,
                        "state": json.dumps(state_json),
                        "updated_at": now,
                    },
                )

    async def load_state(self, run_id: str) -> Optional[ProjectState]:
        """Load project state by run_id."""
        async with self._session_factory() as session:
            result = await session.execute(
                text("SELECT state FROM project_runs WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
            row = result.fetchone()
            if row is None:
                return None
            state_data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
            return ProjectState.model_validate(state_data)

    async def get_latest_run_id(self, project_id: str) -> Optional[str]:
        """Get the latest successful run_id for a project."""
        async with self._session_factory() as session:
            result = await session.execute(
                text(
                    "SELECT latest_run_id FROM project_latest WHERE project_id = :project_id"
                ),
                {"project_id": project_id},
            )
            row = result.fetchone()
            return row[0] if row else None

    async def update_latest_run(self, project_id: str, run_id: str) -> None:
        """Update the latest successful run mapping."""
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text("""
                        INSERT INTO project_latest (project_id, latest_run_id)
                        VALUES (:project_id, :run_id)
                        ON CONFLICT (project_id) DO UPDATE SET
                            latest_run_id = EXCLUDED.latest_run_id
                    """),
                    {"project_id": project_id, "run_id": run_id},
                )
