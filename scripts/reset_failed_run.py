"""
Utility to reset a failed orchestration run back to IN_PROGRESS.

Clears retry counters for the current/failed stage so it can be retried.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.state.postgres import PostgresStateBackend
from src.models.project import RunStatus

async def main(run_id: str):
    backend = PostgresStateBackend()
    state = await backend.load_state(run_id)
    
    if not state:
        print(f"No state found for run_id: {run_id}")
        return

    print(f"Found run {run_id} in state: {state.status.value}")

    # Reset status
    state.status = RunStatus.IN_PROGRESS
    
    # Clear retry counts for the current stage if any
    if state.current_stage and state.current_stage.value in state.retry_counts:
        del state.retry_counts[state.current_stage.value]

    await backend.save_state(state)
    await backend.close()
    print(f"Run {run_id} reset to IN_PROGRESS. Ready to resume.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/reset_failed_run.py <run_id>")
        sys.exit(1)
        
    run_id = sys.argv[1]
    asyncio.run(main(run_id))
