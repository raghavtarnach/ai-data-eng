# AI Data Engineering Platform

A production-grade multi-agent AI data engineering platform capable of interpreting client requirements, generating and validating data pipelines in a sandboxed environment, and ensuring production readiness through structured error handling, observability, and state persistence.

## Features

- **Multi-Agent Orchestration**: Coordinate complex AI tasks utilizing LangChain and Anthropic models.
- **Sandboxed Execution**: Safely execute and validate generated code and data pipelines in isolated Docker environments.
- **State Persistence**: Resume halted runs and track state stages reliably using a PostgreSQL backend.
- **Observability**: Built-in OpenTelemetry integration for monitoring, logging, and tracing system performance.

## Prerequisites

- **Python 3.9+**
- **Docker** (for executing the sandbox tasks)
- **PostgreSQL** (for storing application state)

## Setup

1. **Virtual Environment**
   Set up a Python virtual environment to manage dependencies locally.
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. **Install Dependencies**
   Install the core package and its development dependencies.
   ```bash
   pip install -e ".[dev]"
   ```

3. **Configure Environment**
   A template configuration is provided in `.env.example`. Copy it over and supply necessary credentials (e.g., `ANTHROPIC_API_KEY`, DB URI).
   ```bash
   cp .env.example .env
   ```

4. **Initialize Database**
   Ensure your Postgres database server is up and running. 
   ```bash
   python scripts/setup_db.py
   ```

## Usage

The system is commanded using the main script located at `scripts/run.py`.

### Start a New Pipeline Run
You pass your project requirements via a JSON template. Check `sample_project.json` for an example.
```bash
python scripts/run.py --input sample_project.json
```

### Dry Run (Validation Only)
Validate that the input JSON follows the proper schema without kicking off the execution.
```bash
python scripts/run.py --input sample_project.json --dry
```

### Resume a Halted Run
Using the state management capabilities, you can resume any pipeline from where it left off, using the `run_id`.
```bash
python scripts/run.py --resume <run_id>
```

## Project Directory Structure

- `src/agents/` — Individual agent definitions and prompts.
- `src/orchestrator/` — The orchestration engine that runs the workflow stages.
- `src/sandbox/` — Logic to configure and run the Docker execution environments.
- `src/state/` — State backend and local artifact storage mechanisms.
- `src/memory/`, `src/models/`, `src/observability/` — Additional modular components.
- `scripts/` — Useful CLI entry points (`run.py`, `setup_db.py`).
- `tests/` — Testing suite using Pytest.
