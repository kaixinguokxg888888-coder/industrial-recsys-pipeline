"""Chunk-aware source profiling and row-accounting utilities."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .config import AuditConfig, EXPECTED_SCHEMAS
from .io import iter_raw_chunks, normalize_chunk


@dataclass
class ProfiledSource:
    """A smoke/full frame and its reconciled source profile."""

    frame: pd.DataFrame
    profile: dict[str, Any]


def load_and_profile_source(
    path: Path,
    source_name: str,
    config: AuditConfig,
) -> ProfiledSource:
    """Load a bounded source while collecting deterministic quality counters."""

    frames: list[pd.DataFrame] = []
    missing_counts: Counter[str] = Counter()
    invalid_counts: Counter[str] = Counter()
    row_hash_counts: Counter[int] = Counter()
    row_count = 0
    timestamp_min: int | None = None
    timestamp_max: int | None = None

    for raw in iter_raw_chunks(path, source_name, config):
        row_count += len(raw)
        missing_counts.update(
            {column: int(raw[column].isna().sum()) for column in raw.columns}
        )
        parsed = normalize_chunk(raw, source_name)
        invalid_counts.update(parsed.invalid_parse_counts)
        frames.append(parsed.frame)

        hashes = pd.util.hash_pandas_object(raw, index=False)
        row_hash_counts.update(int(value) for value in hashes.to_numpy())

        if "timestamp" in parsed.frame:
            timestamps = parsed.frame["timestamp"].dropna()
            if not timestamps.empty:
                chunk_min = int(timestamps.min())
                chunk_max = int(timestamps.max())
                timestamp_min = (
                    chunk_min if timestamp_min is None else min(timestamp_min, chunk_min)
                )
                timestamp_max = (
                    chunk_max if timestamp_max is None else max(timestamp_max, chunk_max)
                )

    columns = list(EXPECTED_SCHEMAS[source_name])
    combined = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=columns)
    )
    duplicate_count = sum(count - 1 for count in row_hash_counts.values() if count > 1)
    profile = {
        "source": source_name,
        "rows_read": row_count,
        "columns": len(columns),
        "column_names": "|".join(columns),
        "missing_counts": dict(missing_counts),
        "missing_rates": {
            column: (missing_counts[column] / row_count if row_count else 0.0)
            for column in columns
        },
        "invalid_parse_counts": dict(invalid_counts),
        "duplicate_rows_hash_based": duplicate_count,
        "duplicate_method": "pandas_uint64_row_hash",
        "timestamp_min_ms": timestamp_min,
        "timestamp_max_ms": timestamp_max,
    }
    return ProfiledSource(frame=combined, profile=profile)


def profiles_to_frame(profiles: list[dict[str, Any]]) -> pd.DataFrame:
    """Flatten profiles for a traceable evidence table."""

    rows: list[dict[str, Any]] = []
    for profile in profiles:
        row = {
            key: value
            for key, value in profile.items()
            if key not in {"missing_counts", "missing_rates", "invalid_parse_counts"}
        }
        for column, value in profile["missing_counts"].items():
            row[f"missing_count__{column}"] = value
        for column, value in profile["missing_rates"].items():
            row[f"missing_rate__{column}"] = value
        for column, value in profile["invalid_parse_counts"].items():
            row[f"invalid_parse_count__{column}"] = value
        rows.append(row)
    return pd.DataFrame(rows)
