"""
Utility to provide clarification answers to a halted run.

Resets REQUIREMENT_ANALYSIS stage and injects user answers + original project_input into state.context.
"""

import asyncio
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.state.postgres import PostgresStateBackend
from src.models.project import StageEnum, RunStatus

async def main(run_id: str, answers: dict, input_path: Path):
    backend = PostgresStateBackend()
    state = await backend.load_state(run_id)
    
    if not state:
        print(f"No state found for run_id: {run_id}")
        return

    print(f"Found run {run_id} in state: {state.status.value}")

    # Load original requirements
    with open(input_path) as f:
        project_input_data = json.load(f)

    # Inject answers and requirements into context
    # Merging with existing clarifications if any
    current_clarifications = state.context.get("user_clarifications", {})
    state.context["user_clarifications"] = {**current_clarifications, **answers}
    state.context["project_input"] = project_input_data
    
    # Reset REQUIREMENT_ANALYSIS so PM Agent re-runs with full context
    if StageEnum.REQUIREMENT_ANALYSIS in state.completed_stages:
        state.completed_stages.remove(StageEnum.REQUIREMENT_ANALYSIS)
    
    # Change status back to IN_PROGRESS
    state.status = RunStatus.IN_PROGRESS
    
    await backend.save_state(state)
    await backend.close()
    print(f"Run {run_id} updated successfully. All clarifications injected.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/provide_clarification.py <run_id>")
        sys.exit(1)
        
    run_id = sys.argv[1]
    input_file = Path("sample_project.json")
    
    # Combined answers from both sequences
    user_answers = {
        # Initial clarifications
        "file_naming": "<currentDate>.csv",
        "load_strategy": "incremental load",
        "error_handling": "retry for 2 times and log",
        "destination_structure": "<year>/<month>/<day>",
        # Latest technical clarifications
        "schema_evolution": "No",
        "date_format": "yyyymmdd",
        "file_handling": "only latest",
        "resources_available": "NA"
    }
    
    asyncio.run(main(run_id, user_answers, input_file))
