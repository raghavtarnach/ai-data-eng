"""
Data sample provisioner with PII masking.

This module fills the spec gap around data_sample_ref: it creates sanitized
data samples for QA validation by extracting a limited subset of source data,
detecting and masking PII columns, and writing the result to a sandbox-accessible
path.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from src.observability.logger import get_logger

logger = get_logger(__name__)


# ─── PII Detection Patterns ────────────────────────────────────────────────

PII_PATTERNS: dict[str, re.Pattern] = {
    "email": re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),
    "phone_us": re.compile(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b"),
    "phone_intl": re.compile(r"\+\d{1,3}[-.\s]?\d{4,14}"),
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "credit_card": re.compile(r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"),
    "ip_address": re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"),
}

# Masking replacement values
MASK_VALUES: dict[str, str] = {
    "email": "***@masked.com",
    "phone_us": "***-***-****",
    "phone_intl": "+*-****-****",
    "ssn": "***-**-****",
    "credit_card": "****-****-****-****",
    "ip_address": "***.***.***.***",
}


@dataclass
class PIIDetectionResult:
    """Result of PII detection on a data column."""

    column_name: str
    pii_type: str
    match_count: int
    sample_size: int

    @property
    def match_rate(self) -> float:
        return self.match_count / max(self.sample_size, 1)


@dataclass
class DataSampleResult:
    """Result of data sample provisioning."""

    sample_ref: str  # Path to the sanitized sample file
    row_count: int
    columns: list[str]
    pii_detections: list[PIIDetectionResult] = field(default_factory=list)
    masked_columns: list[str] = field(default_factory=list)


class DataSampleProvisioner:
    """Creates sanitized data samples for QA validation.

    Workflow:
        1. Extract a limited sample (max 10k rows) from the source
        2. Detect PII columns via regex pattern matching
        3. Mask detected PII columns
        4. Write sanitized sample to a sandbox-accessible path
        5. Return a data_sample_ref pointer
    """

    MAX_SAMPLE_ROWS = 10_000
    PII_THRESHOLD = 0.1  # 10% match rate triggers masking

    def __init__(self, output_dir: Path):
        self._output_dir = output_dir
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def detect_pii_in_column(
        self, values: list[str], column_name: str
    ) -> list[PIIDetectionResult]:
        """Detect PII patterns in a list of string values.

        Args:
            values: Column values to scan.
            column_name: Name of the column being scanned.

        Returns:
            List of PII detection results for each matched pattern type.
        """
        detections = []
        sample_size = len(values)

        for pii_type, pattern in PII_PATTERNS.items():
            match_count = sum(1 for v in values if v and pattern.search(str(v)))
            if match_count / max(sample_size, 1) >= self.PII_THRESHOLD:
                detections.append(
                    PIIDetectionResult(
                        column_name=column_name,
                        pii_type=pii_type,
                        match_count=match_count,
                        sample_size=sample_size,
                    )
                )

        return detections

    def mask_value(self, value: str, pii_type: str) -> str:
        """Mask a single value based on its PII type."""
        if not value:
            return value
        return PII_PATTERNS[pii_type].sub(MASK_VALUES.get(pii_type, "***"), str(value))

    def provision_sample(
        self,
        data: list[dict[str, Any]],
        source_name: str,
        run_id: str,
    ) -> DataSampleResult:
        """Create a sanitized data sample.

        Args:
            data: Raw data rows (list of dicts).
            source_name: Name of the data source.
            run_id: Current run ID for namespacing.

        Returns:
            DataSampleResult with path to sanitized sample and metadata.
        """
        # Limit to max sample size
        sample = data[: self.MAX_SAMPLE_ROWS]

        if not sample:
            output_path = self._output_dir / f"{run_id}_{source_name}_empty.json"
            output_path.write_text("[]")
            return DataSampleResult(
                sample_ref=str(output_path),
                row_count=0,
                columns=[],
            )

        columns = list(sample[0].keys())
        all_detections: list[PIIDetectionResult] = []
        masked_columns: list[str] = []

        # Detect PII in each column
        for col in columns:
            col_values = [str(row.get(col, "")) for row in sample]
            detections = self.detect_pii_in_column(col_values, col)
            all_detections.extend(detections)

            # Mask if PII detected
            if detections:
                masked_columns.append(col)
                primary_pii_type = detections[0].pii_type
                for row in sample:
                    if col in row and row[col]:
                        row[col] = self.mask_value(str(row[col]), primary_pii_type)

        # Write sanitized sample
        import json

        output_path = self._output_dir / f"{run_id}_{source_name}_sample.json"
        output_path.write_text(json.dumps(sample, indent=2, default=str))

        logger.info(
            "Data sample provisioned",
            extra={
                "source": source_name,
                "row_count": len(sample),
                "pii_detections": len(all_detections),
                "masked_columns": masked_columns,
            },
        )

        return DataSampleResult(
            sample_ref=str(output_path),
            row_count=len(sample),
            columns=columns,
            pii_detections=all_detections,
            masked_columns=masked_columns,
        )
