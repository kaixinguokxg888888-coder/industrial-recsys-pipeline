"""Disk-partitioned exact aggregations for full-data-safe audit operations."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .config import AuditConfig, EXPECTED_SCHEMAS
from .events import distribution_summary
from .evidence import EvidenceTable
from .io import iter_raw_chunks, normalize_chunk
from .partitioning import PartitionStore


@dataclass
class StreamingProfile:
    """Source profile counters that never retain source rows."""

    source_name: str
    row_count: int = 0
    missing_counts: Counter[str] = field(default_factory=Counter)
    invalid_counts: Counter[str] = field(default_factory=Counter)
    timestamp_min: int | None = None
    timestamp_max: int | None = None

    def update(self, raw: pd.DataFrame, parsed: pd.DataFrame, invalid: dict[str, int]) -> None:
        self.row_count += len(raw)
        self.missing_counts.update(
            {column: int(raw[column].isna().sum()) for column in raw.columns}
        )
        self.invalid_counts.update(invalid)
        if "timestamp" in parsed:
            timestamps = parsed["timestamp"].dropna()
            if not timestamps.empty:
                low = int(timestamps.min())
                high = int(timestamps.max())
                self.timestamp_min = (
                    low if self.timestamp_min is None else min(self.timestamp_min, low)
                )
                self.timestamp_max = (
                    high if self.timestamp_max is None else max(self.timestamp_max, high)
                )

    def to_profile(self, exact_duplicate_rows: int) -> dict[str, Any]:
        columns = EXPECTED_SCHEMAS[self.source_name]
        return {
            "source": self.source_name,
            "rows_read": self.row_count,
            "columns": len(columns),
            "column_names": "|".join(columns),
            "missing_counts": dict(self.missing_counts),
            "missing_rates": {
                column: (
                    self.missing_counts[column] / self.row_count
                    if self.row_count
                    else 0.0
                )
                for column in columns
            },
            "invalid_parse_counts": dict(self.invalid_counts),
            "exact_duplicate_rows": exact_duplicate_rows,
            "duplicate_method": "stable_partition_then_exact_original_field_comparison",
            "timestamp_min_ms": self.timestamp_min,
            "timestamp_max_ms": self.timestamp_max,
        }


@dataclass
class PartitionedScanResult:
    """Bounded in-memory state plus disk partitions from the source scan."""

    events: pd.DataFrame
    category_tree: pd.DataFrame
    profiles: dict[str, StreamingProfile]
    property_items: set[int]
    category_items: set[int]
    special_property_record_counts: Counter[str]


def scan_and_partition_inputs(
    paths: dict[str, Path],
    config: AuditConfig,
    store: PartitionStore,
    memory_checkpoint: Callable[[], None] | None = None,
) -> PartitionedScanResult:
    """Scan sources once; properties are never concatenated in memory."""

    profiles: dict[str, StreamingProfile] = {}
    event_frames: list[pd.DataFrame] = []
    category_tree = pd.DataFrame()
    property_items: set[int] = set()
    category_items: set[int] = set()
    special_counts: Counter[str] = Counter()

    for source_name, path in paths.items():
        profile = StreamingProfile(source_name)
        profiles[source_name] = profile
        source_row_offset = 0
        for raw in iter_raw_chunks(path, source_name, config):
            parsed_chunk = normalize_chunk(raw, source_name)
            parsed = parsed_chunk.frame
            profile.update(raw, parsed, parsed_chunk.invalid_parse_counts)

            duplicate_frame = raw.copy()
            duplicate_frame["_source_name"] = source_name
            duplicate_frame["_source_row"] = range(
                source_row_offset, source_row_offset + len(raw)
            )
            source_row_offset += len(raw)
            store.write(
                f"duplicates_{'properties' if source_name.startswith('item_properties') else source_name}",
                duplicate_frame,
                list(EXPECTED_SCHEMAS[source_name]),
            )

            if source_name == "events":
                event_frames.append(parsed)
                store.write(
                    "events_user_item",
                    parsed,
                    ("visitorid", "itemid"),
                )
            elif source_name.startswith("item_properties"):
                store.write(
                    "properties_item_property",
                    parsed.assign(_source_name=source_name),
                    ("itemid", "property"),
                )
                property_value_candidates = (
                    parsed.loc[:, ["property", "value"]]
                    .drop_duplicates()
                    .assign(_source_name=source_name)
                )
                store.write(
                    "properties_property_value_candidates",
                    property_value_candidates,
                    ("property", "value"),
                )
                valid_items = parsed["itemid"].dropna().astype("int64")
                property_items.update(int(value) for value in valid_items)
                category_rows = parsed.loc[
                    parsed["property"].eq("categoryid"), "itemid"
                ].dropna()
                category_items.update(int(value) for value in category_rows)
                special = parsed.loc[
                    parsed["property"].isin(["categoryid", "available"]), "property"
                ]
                special_counts.update(str(value) for value in special)
            else:
                category_tree = parsed

            if memory_checkpoint is not None:
                memory_checkpoint()

    events = (
        pd.concat(event_frames, ignore_index=True)
        if event_frames
        else pd.DataFrame(columns=EXPECTED_SCHEMAS["events"])
    )
    return PartitionedScanResult(
        events=events,
        category_tree=category_tree,
        profiles=profiles,
        property_items=property_items,
        category_items=category_items,
        special_property_record_counts=special_counts,
    )


def exact_duplicate_audit(
    store: PartitionStore,
    category_tree: pd.DataFrame,
) -> tuple[EvidenceTable, dict[str, int]]:
    """Compare original fields exactly after stable hash partitioning."""

    source_duplicates: Counter[str] = Counter()
    event_columns = list(EXPECTED_SCHEMAS["events"])
    for _, frame in store.iter_partitions("duplicates_events"):
        source_duplicates["events"] += int(frame.duplicated(event_columns).sum())

    property_columns = list(EXPECTED_SCHEMAS["item_properties_part1"])
    combined_duplicates = 0
    cross_distinct_rows = 0
    cross_matching_occurrences = 0
    for _, frame in store.iter_partitions("duplicates_properties"):
        for source_name in ("item_properties_part1", "item_properties_part2"):
            source = frame.loc[frame["_source_name"].eq(source_name)]
            source_duplicates[source_name] += int(
                source.duplicated(property_columns).sum()
            )
        combined_duplicates += int(frame.duplicated(property_columns).sum())
        grouped = (
            frame.groupby(property_columns + ["_source_name"], dropna=False)
            .size()
            .unstack("_source_name", fill_value=0)
        )
        for source_name in ("item_properties_part1", "item_properties_part2"):
            if source_name not in grouped:
                grouped[source_name] = 0
        shared = grouped.loc[
            grouped["item_properties_part1"].gt(0)
            & grouped["item_properties_part2"].gt(0)
        ]
        cross_distinct_rows += len(shared)
        cross_matching_occurrences += int(
            shared[["item_properties_part1", "item_properties_part2"]]
            .min(axis=1)
            .sum()
        )

    tree_columns = list(EXPECTED_SCHEMAS["category_tree"])
    source_duplicates["category_tree"] = int(
        category_tree.duplicated(tree_columns).sum()
    )
    rows = [
        {
            "duplicate_scope": "events_file_internal",
            "exact_duplicate_row_count": source_duplicates["events"],
            "exact_distinct_shared_row_count": pd.NA,
        },
        {
            "duplicate_scope": "item_properties_part1_file_internal",
            "exact_duplicate_row_count": source_duplicates[
                "item_properties_part1"
            ],
            "exact_distinct_shared_row_count": pd.NA,
        },
        {
            "duplicate_scope": "item_properties_part2_file_internal",
            "exact_duplicate_row_count": source_duplicates[
                "item_properties_part2"
            ],
            "exact_distinct_shared_row_count": pd.NA,
        },
        {
            "duplicate_scope": "item_properties_cross_file",
            "exact_duplicate_row_count": cross_matching_occurrences,
            "exact_distinct_shared_row_count": cross_distinct_rows,
        },
        {
            "duplicate_scope": "item_properties_combined",
            "exact_duplicate_row_count": combined_duplicates,
            "exact_distinct_shared_row_count": pd.NA,
        },
        {
            "duplicate_scope": "category_tree_file_internal",
            "exact_duplicate_row_count": source_duplicates["category_tree"],
            "exact_distinct_shared_row_count": pd.NA,
        },
    ]
    source_duplicates["item_properties_combined"] = combined_duplicates
    source_duplicates["item_properties_cross_file"] = cross_matching_occurrences
    return (
        EvidenceTable(
            evidence_id="A02_EXACT_DUPLICATES",
            title="Exact duplicate rows after stable partitioning",
            population="original source rows read for this run",
            denominator="rows within the named file or combined property population",
            frame=pd.DataFrame(rows),
        ),
        dict(source_duplicates),
    )


def partitioned_prior_behavior_funnel(
    store: PartitionStore,
) -> tuple[EvidenceTable, EvidenceTable]:
    """Evaluate transactions per user-item partition with strict time cutoffs."""

    transaction_count = 0
    prior_view_count = 0
    prior_cart_count = 0
    view_intervals: list[int] = []
    cart_intervals: list[int] = []

    for _, frame in store.iter_partitions("events_user_item"):
        valid = frame.dropna(
            subset=["visitorid", "itemid", "timestamp", "event"]
        ).sort_values(
            ["visitorid", "itemid", "timestamp"], kind="mergesort"
        )
        for (_, _), group in valid.groupby(["visitorid", "itemid"], sort=False):
            last_view: int | None = None
            last_cart: int | None = None
            for timestamp, same_time in group.groupby("timestamp", sort=True):
                timestamp_value = int(timestamp)
                events = same_time["event"].astype(str).tolist()
                transactions_at_time = sum(
                    event == "transaction" for event in events
                )
                if transactions_at_time:
                    transaction_count += transactions_at_time
                    if last_view is not None:
                        prior_view_count += transactions_at_time
                        view_intervals.extend(
                            [timestamp_value - last_view] * transactions_at_time
                        )
                    if last_cart is not None:
                        prior_cart_count += transactions_at_time
                        cart_intervals.extend(
                            [timestamp_value - last_cart] * transactions_at_time
                        )
                if "view" in events:
                    last_view = timestamp_value
                if "addtocart" in events:
                    last_cart = timestamp_value

    summary = pd.DataFrame(
        [
            {
                "transaction_event_count": transaction_count,
                "with_strictly_prior_view_count": prior_view_count,
                "with_strictly_prior_addtocart_count": prior_cart_count,
                "with_strictly_prior_view_share": (
                    prior_view_count / transaction_count
                    if transaction_count
                    else 0.0
                ),
                "with_strictly_prior_addtocart_share": (
                    prior_cart_count / transaction_count
                    if transaction_count
                    else 0.0
                ),
            }
        ]
    )
    intervals = pd.concat(
        [
            distribution_summary(
                pd.Series(view_intervals, dtype="Int64"),
                "most_recent_prior_view_interval_ms",
            ),
            distribution_summary(
                pd.Series(cart_intervals, dtype="Int64"),
                "most_recent_prior_addtocart_interval_ms",
            ),
        ],
        ignore_index=True,
    )
    return (
        EvidenceTable(
            evidence_id="B04_TRANSACTION_PRIOR_BEHAVIOR",
            title="Strictly prior behavior for transaction events",
            population="transaction events with valid user, item, and timestamp",
            denominator="transaction events with valid user, item, and timestamp",
            frame=summary,
        ),
        EvidenceTable(
            evidence_id="B05_PRIOR_BEHAVIOR_INTERVALS",
            title="Most recent strictly prior behavior intervals",
            population="transactions with the named strictly prior behavior",
            denominator="transactions with the named strictly prior behavior",
            frame=intervals,
        ),
    )


def _value_set_signature(values: pd.Series) -> tuple[str, ...]:
    return tuple(sorted("<AUDIT_NULL>" if pd.isna(value) else str(value) for value in values.unique()))


def partitioned_property_audit(
    store: PartitionStore,
    total_property_items: int,
) -> tuple[EvidenceTable, EvidenceTable, EvidenceTable]:
    """Aggregate exact item-property histories one disk partition at a time."""

    aggregate_dir = store.run_dir / "aggregates" / "property_pairs"
    aggregate_dir.mkdir(parents=True, exist_ok=True)
    property_totals: defaultdict[str, Counter[str]] = defaultdict(Counter)
    property_first: dict[str, int] = {}
    property_last: dict[str, int] = {}

    for partition_id, frame in store.iter_partitions("properties_item_property"):
        valid = frame.dropna(subset=["itemid", "property"]).copy()
        if valid.empty:
            continue
        base = (
            valid.groupby(["itemid", "property"], dropna=False)
            .agg(
                record_count=("timestamp", "size"),
                unique_value_count=("value", "nunique"),
                first_timestamp_ms=("timestamp", "min"),
                last_timestamp_ms=("timestamp", "max"),
                distinct_timestamp_count=("timestamp", "nunique"),
                missing_value_count=("value", lambda values: int(values.isna().sum())),
            )
            .reset_index()
        )
        timestamp_sets = (
            valid.groupby(["itemid", "property", "timestamp"], dropna=False)["value"]
            .agg(_value_set_signature)
            .rename("value_set")
            .reset_index()
        )
        pair_flags = (
            timestamp_sets.groupby(["itemid", "property"], dropna=False)
            .agg(
                distinct_timestamp_value_sets=("value_set", "nunique"),
                same_timestamp_multiple_values=(
                    "value_set",
                    lambda values: any(len(value) > 1 for value in values),
                ),
            )
            .reset_index()
        )
        pair_flags["value_changes_over_time"] = (
            pair_flags["distinct_timestamp_value_sets"] > 1
        )
        pairs = base.merge(pair_flags, on=["itemid", "property"], how="left")
        pairs.to_parquet(
            aggregate_dir / f"partition-{partition_id:04d}.parquet",
            index=False,
            engine="pyarrow",
            compression="snappy",
        )
        for property_name, group in pairs.groupby("property", dropna=False):
            name = "<missing>" if pd.isna(property_name) else str(property_name)
            totals = property_totals[name]
            totals["record_count"] += int(group["record_count"].sum())
            totals["unique_item_count"] += len(group)
            totals["missing_value_count"] += int(group["missing_value_count"].sum())
            totals["item_property_pair_count"] += len(group)
            totals["pairs_with_multiple_timestamps"] += int(
                group["distinct_timestamp_count"].gt(1).sum()
            )
            totals["pairs_with_multiple_values"] += int(
                group["unique_value_count"].gt(1).sum()
            )
            totals["pairs_changing_over_time"] += int(
                group["value_changes_over_time"].sum()
            )
            totals["pairs_with_same_timestamp_multiple_values"] += int(
                group["same_timestamp_multiple_values"].sum()
            )
            timestamps = group["first_timestamp_ms"].dropna()
            if not timestamps.empty:
                property_first[name] = min(
                    property_first.get(name, int(timestamps.min())),
                    int(timestamps.min()),
                )
            timestamps = group["last_timestamp_ms"].dropna()
            if not timestamps.empty:
                property_last[name] = max(
                    property_last.get(name, int(timestamps.max())),
                    int(timestamps.max()),
                )

    global_unique_values: dict[str, int] = {}
    for _, frame in store.iter_partitions(
        "properties_property_value_candidates"
    ):
        exact_values = frame.drop_duplicates(["property", "value"])
        for property_name, group in exact_values.groupby("property", dropna=False):
            name = "<missing>" if pd.isna(property_name) else str(property_name)
            # An exact property-value pair always hashes to one partition.
            # Summing per-partition distinct pairs therefore remains globally exact.
            global_unique_values[name] = global_unique_values.get(name, 0) + int(
                group["value"].nunique(dropna=True)
            )

    profile_rows: list[dict[str, Any]] = []
    change_rows: list[dict[str, Any]] = []
    for name in sorted(property_totals):
        totals = property_totals[name]
        record_count = totals["record_count"]
        profile_rows.append(
            {
                "property": name,
                "record_count": record_count,
                "unique_item_count": totals["unique_item_count"],
                "unique_value_count": global_unique_values.get(name, 0),
                "missing_value_count": totals["missing_value_count"],
                "missing_value_rate": (
                    totals["missing_value_count"] / record_count
                    if record_count
                    else 0.0
                ),
                "item_coverage": (
                    totals["unique_item_count"] / total_property_items
                    if total_property_items
                    else 0.0
                ),
                "first_timestamp_ms": property_first.get(name),
                "last_timestamp_ms": property_last.get(name),
                "semantic_status": (
                    "explicit_dataset_property"
                    if name in {"categoryid", "available"}
                    else "unverified_hashed_property"
                ),
            }
        )
        change_rows.append(
            {
                "property": name,
                "item_property_pair_count": totals["item_property_pair_count"],
                "pairs_with_multiple_timestamps": totals[
                    "pairs_with_multiple_timestamps"
                ],
                "pairs_with_multiple_values": totals[
                    "pairs_with_multiple_values"
                ],
                "pairs_changing_over_time": totals[
                    "pairs_changing_over_time"
                ],
                "pairs_with_same_timestamp_multiple_values": totals[
                    "pairs_with_same_timestamp_multiple_values"
                ],
            }
        )

    profile = pd.DataFrame(profile_rows)
    changes = pd.DataFrame(change_rows)
    special = profile.loc[profile["property"].isin(["categoryid", "available"])].copy()
    return (
        EvidenceTable(
            evidence_id="E01_PROPERTY_PROFILE",
            title="Exact partitioned item-property profile",
            population="property records read for this run",
            denominator="distinct items observed in property records",
            frame=profile,
        ),
        EvidenceTable(
            evidence_id="E02_PROPERTY_CHANGE",
            title="Exact partitioned item-property temporal and multi-value profile",
            population="non-missing item-property pairs",
            denominator="item-property pairs for each property",
            frame=changes,
        ),
        EvidenceTable(
            evidence_id="E03_SPECIAL_PROPERTIES",
            title="Explicit categoryid and available property profile",
            population="records whose property is explicitly categoryid or available",
            denominator="records/items for each explicit property",
            frame=special,
        ),
    )


def item_coverage_from_sets(
    events: pd.DataFrame,
    property_items: set[int],
    category_items: set[int],
) -> tuple[EvidenceTable, EvidenceTable]:
    """Compute item/property/category coverage without materializing properties."""

    event_items = set(int(value) for value in events["itemid"].dropna())
    overlap = event_items & property_items
    union_items = event_items | property_items
    cross = pd.DataFrame(
        [
            {
                "event_item_count": len(event_items),
                "property_item_count": len(property_items),
                "overlap_item_count": len(overlap),
                "event_items_with_properties_share": (
                    len(overlap) / len(event_items) if event_items else 0.0
                ),
                "property_items_with_events_share": (
                    len(overlap) / len(property_items) if property_items else 0.0
                ),
            }
        ]
    )
    category = pd.DataFrame(
        [
            {
                "event_item_count": len(event_items),
                "property_item_count": len(property_items),
                "union_item_count": len(union_items),
                "items_with_categoryid_count": len(category_items),
                "event_item_category_coverage": (
                    len(event_items & category_items) / len(event_items)
                    if event_items
                    else 0.0
                ),
                "property_item_category_coverage": (
                    len(property_items & category_items) / len(property_items)
                    if property_items
                    else 0.0
                ),
                "union_item_category_coverage": (
                    len(union_items & category_items) / len(union_items)
                    if union_items
                    else 0.0
                ),
            }
        ]
    )
    return (
        EvidenceTable(
            evidence_id="A03_CROSS_SOURCE_ITEM_COVERAGE",
            title="Directional item coverage across events and properties",
            population="items observed in event/property rows read for this run",
            denominator="event items or property items as named by each share",
            frame=cross,
        ),
        EvidenceTable(
            evidence_id="A04_ITEM_CATEGORY_COVERAGE",
            title="Explicit categoryid item coverage",
            population="items observed in events or property rows for this run",
            denominator="event, property, or union items as named by each metric",
            frame=category,
        ),
    )
