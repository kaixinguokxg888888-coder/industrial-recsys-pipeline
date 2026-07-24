"""Configuration and immutable input contracts for the data audit."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

DataScope = Literal["full", "smoke_non_evidentiary"]
FingerprintMode = Literal["metadata", "sha256"]

EXPECTED_SCHEMAS: Final[dict[str, tuple[str, ...]]] = {
    "events": ("timestamp", "visitorid", "event", "itemid", "transactionid"),
    "item_properties_part1": ("timestamp", "itemid", "property", "value"),
    "item_properties_part2": ("timestamp", "itemid", "property", "value"),
    "category_tree": ("categoryid", "parentid"),
}

INPUT_FILENAMES: Final[dict[str, str]] = {
    "events": "events.csv",
    "item_properties_part1": "item_properties_part1.csv",
    "item_properties_part2": "item_properties_part2.csv",
    "category_tree": "category_tree.csv",
}


@dataclass(frozen=True)
class AuditConfig:
    """Runtime configuration with explicit smoke/full-data semantics."""

    data_dir: Path
    output_dir: Path
    chunk_size: int = 100_000
    max_rows_per_file: int | None = None
    fingerprint_mode: FingerprintMode = "sha256"

    @property
    def data_scope(self) -> DataScope:
        """Return the evidence scope attached to every generated artifact."""

        if self.max_rows_per_file is None:
            return "full"
        return "smoke_non_evidentiary"

    @property
    def is_smoke(self) -> bool:
        """Whether row limits make this run non-evidentiary."""

        return self.max_rows_per_file is not None

    def validate(self) -> None:
        """Reject unsafe or internally inconsistent configuration."""

        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self.max_rows_per_file is not None and self.max_rows_per_file <= 0:
            raise ValueError("max_rows_per_file must be positive when provided")

        raw_dir = self.data_dir.resolve()
        output_dir = self.output_dir.resolve()
        if output_dir == raw_dir or raw_dir in output_dir.parents:
            raise ValueError("output_dir must not be data/raw or a child of it")
