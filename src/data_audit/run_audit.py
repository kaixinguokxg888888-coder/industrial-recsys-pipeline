"""Command-line orchestration for the RetailRocket data feasibility audit."""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Sequence

import pandas as pd
import psutil

from .config import AuditConfig
from .events import (
    behavior_combinations,
    event_distribution,
    event_time_trend,
    item_long_tail_tables,
    prior_behavior_funnel,
    time_split_audit,
    user_sequence_tables,
)
from .evidence import EvidenceTable
from .io import InputFingerprint, collect_fingerprints, resolve_and_validate_inputs
from .properties import (
    category_item_coverage,
    category_tree_audit,
    cross_source_item_coverage,
    property_tables,
)
from .quality import load_and_profile_source, profiles_to_frame
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
    started = time.perf_counter()
    process = psutil.Process(os.getpid())
    peak_rss = process.memory_info().rss

    sources = {}
    profiles = []
    for source_name, path in input_paths.items():
        LOGGER.info("reading %s from %s", source_name, path)
        profiled = load_and_profile_source(path, source_name, config)
        sources[source_name] = profiled.frame
        profiles.append(profiled.profile)
        peak_rss = max(peak_rss, process.memory_info().rss)

    events = sources["events"]
    properties = pd.concat(
        [
            sources["item_properties_part1"],
            sources["item_properties_part2"],
        ],
        ignore_index=True,
    )
    category_tree = sources["category_tree"]

    tables: list[EvidenceTable] = [
        EvidenceTable(
            evidence_id="A01_FILE_PROFILES",
            title="Input file profiles and row accounting",
            population="input rows read for this run",
            denominator="rows read from each named source",
            frame=profiles_to_frame(profiles),
        ),
        event_distribution(events),
        event_time_trend(events),
        behavior_combinations(events),
    ]
    tables.extend(prior_behavior_funnel(events))
    tables.extend(user_sequence_tables(events))
    tables.extend(item_long_tail_tables(events))
    tables.append(time_split_audit(events))
    tables.extend(property_tables(properties))
    tables.append(cross_source_item_coverage(events, properties))
    tables.append(category_item_coverage(events, properties))
    tables.extend(category_tree_audit(category_tree))

    after = collect_fingerprints(input_paths, include_sha256)
    if before != after:
        raise RuntimeError("raw input fingerprint changed during the audit")

    elapsed_seconds = time.perf_counter() - started
    manifest = {
        "command_contract": (
            "python -m src.data_audit.run_audit "
            "--data-dir data/raw --output-dir reports"
        ),
        "configuration": {
            "data_dir": str(config.data_dir.resolve()),
            "output_dir": str(config.output_dir.resolve()),
            "chunk_size": config.chunk_size,
            "max_rows_per_file": config.max_rows_per_file,
            "fingerprint_mode": config.fingerprint_mode,
        },
        "input_fingerprints_before": _fingerprints_to_dict(before),
        "input_fingerprints_after": _fingerprints_to_dict(after),
        "elapsed_seconds": elapsed_seconds,
        "peak_rss_bytes_observed": peak_rss,
        "limitations": [
            (
                "Smoke row limits select file prefixes and are non-representative."
                if config.is_smoke
                else "No row limit was configured."
            ),
            (
                "Duplicate counts currently use 64-bit row hashes and require "
                "phase-4 collision-risk review."
            ),
        ],
    }
    written = write_artifacts(config, tables, manifest)
    LOGGER.info(
        "completed scope=%s elapsed=%.3fs peak_rss=%d artifacts=%d",
        config.data_scope,
        elapsed_seconds,
        peak_rss,
        len(written),
    )
    return written


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
