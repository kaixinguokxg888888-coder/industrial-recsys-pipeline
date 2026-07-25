"""Stable disk partitioning and run-scoped temporary workspace management."""

from __future__ import annotations

import json
import shutil
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

import pandas as pd

from .config import AuditConfig

STABLE_HASH_KEY = "auditpartition01"
TEMP_SPACE_FACTOR = 4.0


class ResourceValidationError(RuntimeError):
    """Raised when disk, memory, or path safety checks fail."""


def stable_partition_ids(
    frame: pd.DataFrame,
    keys: Sequence[str],
    partition_count: int,
) -> pd.Series:
    """Return reproducible partitions; hashes never determine row equality."""

    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    missing = [key for key in keys if key not in frame.columns]
    if missing:
        raise KeyError(f"partition keys are missing: {missing}")
    key_frame = frame.loc[:, list(keys)].astype("string").fillna("<AUDIT_NULL>")
    hashes = pd.util.hash_pandas_object(
        key_frame,
        index=False,
        hash_key=STABLE_HASH_KEY,
        categorize=True,
    )
    return (hashes % partition_count).astype("int64")


@dataclass
class DatasetStats:
    """Manifest counters for one partitioned dataset."""

    rows: int = 0
    files: int = 0
    nonempty_partitions: set[int] = field(default_factory=set)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "files": self.files,
            "nonempty_partition_count": len(self.nonempty_partitions),
            "nonempty_partitions": sorted(self.nonempty_partitions),
        }


class PartitionStore:
    """Write chunk fragments to deterministic partition directories."""

    def __init__(self, run_dir: Path, partition_count: int) -> None:
        self.run_dir = run_dir
        self.partition_count = partition_count
        self.datasets_dir = run_dir / "partitions"
        self.datasets_dir.mkdir(parents=True, exist_ok=True)
        self._sequence: defaultdict[tuple[str, int], int] = defaultdict(int)
        self.stats: defaultdict[str, DatasetStats] = defaultdict(DatasetStats)

    def write(
        self,
        dataset: str,
        frame: pd.DataFrame,
        keys: Sequence[str],
    ) -> None:
        """Partition and persist a frame with no equality inference from hashes."""

        if frame.empty:
            return
        partition_ids = stable_partition_ids(frame, keys, self.partition_count)
        for partition_id in sorted(int(value) for value in partition_ids.unique()):
            subset = frame.loc[partition_ids.eq(partition_id)].copy()
            sequence_key = (dataset, partition_id)
            sequence = self._sequence[sequence_key]
            self._sequence[sequence_key] += 1
            partition_dir = (
                self.datasets_dir / dataset / f"partition={partition_id:04d}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)
            path = partition_dir / f"chunk-{sequence:06d}.parquet"
            subset.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
            stats = self.stats[dataset]
            stats.rows += len(subset)
            stats.files += 1
            stats.nonempty_partitions.add(partition_id)

    def partition_files(self, dataset: str, partition_id: int) -> list[Path]:
        directory = self.datasets_dir / dataset / f"partition={partition_id:04d}"
        if not directory.exists():
            return []
        return sorted(directory.glob("chunk-*.parquet"))

    def read_partition(self, dataset: str, partition_id: int) -> pd.DataFrame:
        files = self.partition_files(dataset, partition_id)
        if not files:
            return pd.DataFrame()
        return pd.concat(
            [pd.read_parquet(path, engine="pyarrow") for path in files],
            ignore_index=True,
        )

    def iter_partitions(self, dataset: str) -> Iterator[tuple[int, pd.DataFrame]]:
        for partition_id in range(self.partition_count):
            frame = self.read_partition(dataset, partition_id)
            if not frame.empty:
                yield partition_id, frame

    def manifest(self) -> dict[str, Any]:
        return {
            "partition_count": self.partition_count,
            "datasets": {
                name: stats.to_dict() for name, stats in sorted(self.stats.items())
            },
        }


@dataclass
class RunWorkspace:
    """A unique workspace that cleans on success and preserves failure diagnostics."""

    config: AuditConfig
    input_size_bytes: int
    run_id: str = field(init=False)
    run_dir: Path = field(init=False)
    estimated_temp_bytes: int = field(init=False)
    available_disk_bytes_at_start: int = field(init=False)

    def __post_init__(self) -> None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.run_id = f"{timestamp}-{uuid.uuid4().hex[:8]}"
        self.run_dir = self.config.temp_dir.resolve() / self.run_id
        self.estimated_temp_bytes = int(self.input_size_bytes * TEMP_SPACE_FACTOR)
        self.available_disk_bytes_at_start = 0

    def prepare(self) -> None:
        self.config.validate()
        temp_root = self.config.temp_dir.resolve()
        temp_root.mkdir(parents=True, exist_ok=True)
        probe = temp_root / f".write-probe-{uuid.uuid4().hex}"
        try:
            probe.write_text("audit-write-probe", encoding="utf-8")
            probe.unlink()
        except OSError as exc:
            raise ResourceValidationError(
                f"temporary directory is not writable: {temp_root}"
            ) from exc

        disk = shutil.disk_usage(temp_root)
        self.available_disk_bytes_at_start = disk.free
        if disk.free < self.estimated_temp_bytes:
            raise ResourceValidationError(
                "insufficient temporary disk space: "
                f"required_estimate={self.estimated_temp_bytes}, available={disk.free}"
            )
        self.run_dir.mkdir(parents=False, exist_ok=False)
        self.write_status("prepared")

    def write_status(self, status: str, **details: Any) -> None:
        payload = {
            "run_id": self.run_id,
            "status": status,
            "run_dir": str(self.run_dir),
            "estimated_temp_bytes": self.estimated_temp_bytes,
            "available_disk_bytes_at_start": self.available_disk_bytes_at_start,
            **details,
        }
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "run_status.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def finish_success(self) -> None:
        self.write_status("completed")
        if not self.config.keep_temp:
            shutil.rmtree(self.run_dir)

    def finish_failure(self, exc: BaseException) -> None:
        self.write_status(
            "failed",
            error_type=type(exc).__name__,
            error_message=str(exc),
            retained_for_diagnostics=True,
        )


def enforce_memory_limit(config: AuditConfig, rss_bytes: int) -> None:
    """Fail explicitly after a checkpoint when the configured RSS limit is crossed."""

    if config.max_memory_mb is None:
        return
    limit_bytes = config.max_memory_mb * 1024 * 1024
    if rss_bytes > limit_bytes:
        raise ResourceValidationError(
            f"RSS memory limit exceeded: observed={rss_bytes}, limit={limit_bytes}"
        )
