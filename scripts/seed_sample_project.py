"""
Seed a sample ProjectInput JSON for testing.

Generates a realistic data engineering project request that exercises
all parts of the system.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models.project import DataSource, ProjectConstraints, ProjectInput


def create_sample_project() -> ProjectInput:
    """Create a sample project input for testing."""
    return ProjectInput(
        project_id=str(uuid4()),
        run_id=str(uuid4()),
        project_name="E-Commerce Sales Analytics Pipeline",
        client_requirements="""
Build an ETL pipeline for an e-commerce company that:

1. EXTRACT: Read daily sales transaction data from a CSV file stored in blob storage.
   The CSV contains columns: transaction_id, customer_id, product_id, product_name,
   category, quantity, unit_price, discount_percent, transaction_date, store_location.

2. TRANSFORM:
   - Calculate total_amount = quantity * unit_price * (1 - discount_percent/100)
   - Add a `transaction_month` column extracted from transaction_date
   - Categorize transactions by amount: 'small' (<$50), 'medium' ($50-$200), 'large' (>$200)
   - Remove any duplicate transaction_ids (keep first occurrence)
   - Filter out transactions with quantity <= 0

3. LOAD: Write the transformed data to a Parquet file partitioned by transaction_month.

4. AGGREGATE: Create a summary table with:
   - Total revenue per category per month
   - Average transaction size per store_location
   - Top 10 products by revenue

The pipeline should handle up to 10 million rows efficiently.
Data quality checks should validate: no nulls in transaction_id and customer_id,
all amounts are positive, and dates are within the last 2 years.
        """.strip(),
        data_sources=[
            DataSource(
                name="daily_sales",
                type="blob",
                connection_ref="BLOB_STORAGE_CONNECTION_STRING",
            ),
        ],
        target_system="Azure Data Lake Storage Gen2",
        expected_output="Partitioned Parquet files with transformed sales data and summary aggregations",
        constraints=ProjectConstraints(
            performance="Process 10M rows in under 5 minutes",
            cost="$100/month maximum",
            tools="pandas,pyarrow,sqlalchemy",
        ),
    )


if __name__ == "__main__":
    project = create_sample_project()
    output_path = Path(__file__).parent.parent / "sample_project.json"

    with open(output_path, "w") as f:
        json.dump(project.model_dump(), f, indent=2, default=str)

    print(f"✅ Sample project written to: {output_path}")
    print(f"   Project ID: {project.project_id}")
    print(f"   Run ID:     {project.run_id}")
    print(f"\nRun with:")
    print(f"   python scripts/run.py --input {output_path}")
