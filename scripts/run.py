"""
CLI entry point for the AI Data Engineering System.

Usage:
    python scripts/run.py --input project.json          # Start a new run
    python scripts/run.py --resume <run_id>             # Resume a halted run
    python scripts/run.py --input project.json --dry    # Validate input only
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import settings
from src.models.project import ProjectInput
from src.observability.logger import get_logger
from src.orchestrator.engine import Orchestrator
from src.state.artifacts import ArtifactStore
from src.state.postgres import PostgresStateBackend

logger = get_logger(__name__)


async def main(args: argparse.Namespace) -> int:
    """Main entry point."""

    # Initialize backends
    state_backend = PostgresStateBackend()
    await state_backend.initialize()

    artifact_store = ArtifactStore()
    orchestrator = Orchestrator(
        state_backend=state_backend,
        artifact_store=artifact_store,
    )

    try:
        if args.resume:
            # Resume from existing run
            state = await state_backend.load_state(args.resume)
            if not state:
                logger.error(f"No state found for run_id: {args.resume}")
                return 1

            logger.info(
                f"Resuming run {args.resume}",
                extra={
                    "current_stage": state.current_stage,
                    "completed": [s.value for s in state.completed_stages],
                },
            )

            # Reconstruct ProjectInput from state context
            project_input_data = state.context.get("project_input")
            if not project_input_data:
                # Fallback: reconstruct minimal input
                project_input = ProjectInput(
                    project_id=state.project_id,
                    run_id=state.run_id,
                    project_name="Resumed Run",
                    client_requirements="",
                )
            else:
                project_input = ProjectInput.model_validate(project_input_data)
                project_input.run_id = state.run_id

            final_state = await orchestrator.run(project_input)

        elif args.input:
            # Load project input from JSON file
            input_path = Path(args.input)
            if not input_path.exists():
                logger.error(f"Input file not found: {args.input}")
                return 1

            with open(input_path) as f:
                input_data = json.load(f)

            project_input = ProjectInput.model_validate(input_data)

            if args.dry:
                # Dry run — validate only
                print(json.dumps(project_input.model_dump(), indent=2, default=str))
                print("\n✅ Input validated successfully.")
                return 0

            logger.info(
                f"Starting new run",
                extra={
                    "project_id": project_input.project_id,
                    "run_id": project_input.run_id,
                    "project_name": project_input.project_name,
                },
            )

            final_state = await orchestrator.run(project_input)

        else:
            print("Error: Must specify --input or --resume")
            return 1

        # Print final state summary
        print("\n" + "=" * 60)
        print(f"Run ID:     {final_state.run_id}")
        print(f"Project ID: {final_state.project_id}")
        print(f"Status:     {final_state.status.value}")
        print(f"Completed:  {[s.value for s in final_state.completed_stages]}")

        if final_state.errors:
            print(f"\nErrors ({len(final_state.errors)}):")
            for err in final_state.errors:
                print(f"  [{err.error_type.value}] {err.stage.value}: {err.message}")

        return 0 if final_state.status.value in ("COMPLETED", "WAITING_FOR_CLARIFICATION") else 1

    finally:
        await state_backend.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="AI Data Engineering System — CLI Entry Point"
    )
    parser.add_argument(
        "--input",
        type=str,
        help="Path to ProjectInput JSON file",
    )
    parser.add_argument(
        "--resume",
        type=str,
        help="Run ID to resume from",
    )
    parser.add_argument(
        "--dry",
        action="store_true",
        help="Validate input only, do not execute",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    exit_code = asyncio.run(main(args))
    sys.exit(exit_code)
