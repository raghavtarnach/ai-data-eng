import asyncio
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.state.postgres import PostgresStateBackend
from src.config import settings

async def setup():
    print(f"Initializing database at {settings.database.url}...")
    backend = PostgresStateBackend()
    try:
        await backend.initialize()
        print("✅ Database schema initialized successfully.")
    except Exception as e:
        print(f"❌ Failed to initialize database: {e}")
    finally:
        await backend.close()

if __name__ == "__main__":
    asyncio.run(setup())
