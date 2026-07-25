"""Command-line orchestration for the RetailRocket data feasibility audit."""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Sequence

import psutil

from .config import AuditConfig
from .events import (
    behavior_combinations,
    event_distribution,
    event_time_trend,
    item_long_tail_tables,
    time_split_audit,
    user_sequence_tables,
)
from .evidence import EvidenceTable
from .external import (
    exact_duplicate_audit,
    item_coverage_from_sets,
    partitioned_prior_behavior_funnel,
    partitioned_property_audit,
    scan_and_partition_inputs,
)
from .io import InputFingerprint, collect_fingerprints, resolve_and_validate_inputs
from .partitioning import (
    PartitionStore,
    RunWorkspace,
    enforce_memory_limit,
)
from .properties import category_tree_audit
from .quality import profiles_to_frame
from .reporting import write_artifacts

LOGGER = logging.getLogger("data_audit")


def build_parser() -> argparse.ArgumentParser:
    """Build the stable command-line contract."""

    parser = argparse.ArgumentParser(
        description="Run the RetailRocket data feasibility audit."
    )
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument("--partition-count", type=int, default=64)
    parser.add_argument("--temp-dir", type=Path, default=Path("data/tmp/data_audit"))
    parser.add_argument("--keep-temp", action="store_true")
    parser.add_argument("--max-memory-mb", type=int, default=None)
    parser.add_argument(
        "--max-rows-per-file",
        type=int,
        default=None,
        help=(
            "Smoke-test limit for events and each item-property file. "
            "Any value makes outputs explicitly non-evidentiary."
        ),
    )
    parser.add_argument(
        "--fingerprint-mode",
        choices=("metadata", "sha256"),
        default="sha256",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def _fingerprints_to_dict(
    fingerprints: dict[str, InputFingerprint],
) -> dict[str, object]:
    return {
        name: fingerprint.to_dict()
        for name, fingerprint in fingerprints.items()
    }


def run(config: AuditConfig) -> list[Path]:
    """Run an audit and return written artifact paths."""

    config.validate()
    input_paths = resolve_and_validate_inputs(config.data_dir)
    include_sha256 = config.fingerprint_mode == "sha256"
    before = collect_fingerprints(input_paths, include_sha256)
    input_size_bytes = sum(path.stat().st_size for path in input_paths.values())
    workspace = RunWorkspace(config=config, input_size_bytes=input_size_bytes)
    workspace.prepare()
    started = time.perf_counter()
    process = psutil.Process(os.getpid())
    peak_rss = process.memory_info().rss
    store = PartitionStore(workspace.run_dir, config.partition_count)

    def memory_checkpoint() -> None:
        nonlocal peak_rss
        rss = process.memory_info().rss
        peak_rss = max(peak_rss, rss)
        enforce_memory_limit(config, rss)

    try:
        workspace.write_status("partitioning")
        scan = scan_and_partition_inputs(
            input_paths,
            config,
            store,
            memory_checkpoint=memory_checkpoint,
        )
        workspace.write_status("aggregating", partition_manifest=store.manifest())
        duplicate_table, duplicate_counts = exact_duplicate_audit(
            store, scan.category_tree
        )
        profiles = [
            profile.to_profile(duplicate_counts.get(source_name, 0))
            for source_name, profile in scan.profiles.items()
        ]
        tables: list[EvidenceTable] = [
            EvidenceTable(
                evidence_id="A01_FILE_PROFILES",
                title="Input file profiles and exact row accounting",
                population="input rows read for this run",
                denominator="rows read from each named source",
                frame=profiles_to_frame(profiles),
            ),
            duplicate_table,
            event_distribution(scan.events),
            event_time_trend(scan.events),
            behavior_combinations(scan.events),
        ]
        tables.extend(partitioned_prior_behavior_funnel(store))
        tables.extend(user_sequence_tables(scan.events))
        tables.extend(item_long_tail_tables(scan.events))
        tables.append(time_split_audit(scan.events))
        tables.extend(
            partitioned_property_audit(store, len(scan.property_items))
        )
        tables.extend(
            item_coverage_from_sets(
                scan.events, scan.property_items, scan.category_items
            )
        )
        tables.extend(category_tree_audit(scan.category_tree))
        memory_checkpoint()

        after = collect_fingerprints(input_paths, include_sha256)
        if before != after:
            raise RuntimeError("raw input fingerprint changed during the audit")

        elapsed_seconds = time.perf_counter() - started
        partition_manifest = store.manifest()
        aggregate_files = list(
            (workspace.run_dir / "aggregates").rglob("*.parquet")
        )
        manifest = {
            "run_id": workspace.run_id,
            "command_contract": (
                "python -m src.data_audit.run_audit "
                "--data-dir data/raw --output-dir reports"
            ),
            "configuration": {
                "data_dir": str(config.data_dir.resolve()),
                "output_dir": str(config.output_dir.resolve()),
                "chunk_size": config.chunk_size,
                "partition_count": config.partition_count,
                "temp_dir": str(config.temp_dir.resolve()),
                "keep_temp": config.keep_temp,
                "max_memory_mb": config.max_memory_mb,
                "max_rows_per_file": config.max_rows_per_file,
                "fingerprint_mode": config.fingerprint_mode,
            },
            "input_fingerprints_before": _fingerprints_to_dict(before),
            "input_fingerprints_after": _fingerprints_to_dict(after),
            "elapsed_seconds": elapsed_seconds,
            "peak_rss_bytes_observed": peak_rss,
            "temporary_workspace": {
                "run_dir": str(workspace.run_dir),
                "estimated_temp_bytes": workspace.estimated_temp_bytes,
                "available_disk_bytes_at_start": (
                    workspace.available_disk_bytes_at_start
                ),
                "keep_temp": config.keep_temp,
                "cleanup_on_success": not config.keep_temp,
                "retain_on_failure": True,
            },
            "partition_processing": {
                **partition_manifest,
                "aggregate_parquet_file_count": len(aggregate_files),
                "status": "completed",
            },
            "limitations": [
                (
                    "Smoke row limits select file prefixes and are non-representative."
                    if config.is_smoke
                    else "No row limit was configured."
                ),
                (
                    "Stable hashes select partitions only; duplicate equality uses "
                    "the complete original row fields."
                ),
                (
                    "The event table remains bounded in memory for non-funnel "
                    "aggregations and is guarded by optional max_memory_mb."
                ),
            ],
        }
        workspace.write_status(
            "writing_outputs", partition_manifest=partition_manifest
        )
        written = write_artifacts(config, tables, manifest)
        workspace.finish_success()
        LOGGER.info(
            "completed scope=%s elapsed=%.3fs peak_rss=%d artifacts=%d",
            config.data_scope,
            elapsed_seconds,
            peak_rss,
            len(written),
        )
        return written
    except Exception as exc:
        workspace.finish_failure(exc)
        raise


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point."""

    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = AuditConfig(
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size,
        partition_count=args.partition_count,
        temp_dir=args.temp_dir,
        keep_temp=args.keep_temp,
        max_memory_mb=args.max_memory_mb,
        max_rows_per_file=args.max_rows_per_file,
        fingerprint_mode=args.fingerprint_mode,
    )
    try:
        written = run(config)
    except Exception:
        LOGGER.exception("audit failed")
        return 1
    print(
        json.dumps(
            {
                "data_scope": config.data_scope,
                "artifacts_written": [str(path) for path in written],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
