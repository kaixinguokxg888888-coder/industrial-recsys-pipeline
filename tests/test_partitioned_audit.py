"""Tests for stable partitioning and exact disk-backed aggregations."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.data_audit.config import AuditConfig
from src.data_audit.external import (
    exact_duplicate_audit,
    partitioned_prior_behavior_funnel,
    partitioned_property_audit,
)
from src.data_audit.partitioning import (
    PartitionStore,
    RunWorkspace,
    stable_partition_ids,
)


def test_stable_partition_is_repeatable_and_key_complete() -> None:
    frame = pd.DataFrame(
        {
            "visitorid": pd.Series([1, 1, 2, None], dtype="Int64"),
            "itemid": pd.Series([10, 10, 20, 30], dtype="Int64"),
        }
    )
    first = stable_partition_ids(frame, ("visitorid", "itemid"), 17)
    second = stable_partition_ids(frame.copy(), ("visitorid", "itemid"), 17)
    assert first.tolist() == second.tolist()
    assert first.tolist() == [10, 10, 8, 6]
    assert first.iloc[0] == first.iloc[1]
    assert first.between(0, 16).all()


def test_exact_duplicates_across_chunks_and_property_files(tmp_path: Path) -> None:
    store = PartitionStore(tmp_path / "run", partition_count=1)
    columns = ["timestamp", "itemid", "property", "value"]
    duplicate = pd.DataFrame(
        [["1000", "1", "x", "A"]],
        columns=columns,
        dtype="string",
    )
    different = pd.DataFrame(
        [["1000", "1", "x", "B"]],
        columns=columns,
        dtype="string",
    )
    for source, frame in (
        ("item_properties_part1", duplicate),
        ("item_properties_part1", duplicate.copy()),
        ("item_properties_part1", different),
        ("item_properties_part2", duplicate.copy()),
    ):
        payload = frame.assign(_source_name=source, _source_row=[0])
        store.write("duplicates_properties", payload, columns)

    events = pd.DataFrame(
        [["1000", "1", "view", "10", pd.NA]],
        columns=["timestamp", "visitorid", "event", "itemid", "transactionid"],
        dtype="string",
    ).assign(_source_name="events", _source_row=[0])
    store.write(
        "duplicates_events",
        events,
        ["timestamp", "visitorid", "event", "itemid", "transactionid"],
    )
    tree = pd.DataFrame(columns=["categoryid", "parentid"], dtype="string")
    evidence, counts = exact_duplicate_audit(store, tree)
    rows = evidence.frame.set_index("duplicate_scope")
    assert counts["item_properties_part1"] == 1
    assert rows.loc[
        "item_properties_cross_file", "exact_duplicate_row_count"
    ] == 1
    assert rows.loc[
        "item_properties_cross_file", "exact_distinct_shared_row_count"
    ] == 1
    assert rows.loc[
        "item_properties_combined", "exact_duplicate_row_count"
    ] == 2
    # Partition count one deliberately creates hash-partition co-location;
    # original value B remains distinct because equality uses all raw fields.


def test_partitioned_funnel_cross_chunk_and_same_timestamp(tmp_path: Path) -> None:
    store = PartitionStore(tmp_path / "run", partition_count=3)
    columns = ["timestamp", "visitorid", "event", "itemid", "transactionid"]
    chunks = [
        [[1000, 1, "view", 10, None]],
        [[2000, 1, "addtocart", 10, None]],
        [[3000, 1, "transaction", 10, "t1"]],
        [[4000, 2, "view", 20, None], [4000, 2, "transaction", 20, "t2"]],
    ]
    for rows in chunks:
        frame = pd.DataFrame(rows, columns=columns)
        frame["timestamp"] = frame["timestamp"].astype("Int64")
        frame["visitorid"] = frame["visitorid"].astype("Int64")
        frame["itemid"] = frame["itemid"].astype("Int64")
        frame["event"] = frame["event"].astype("string")
        frame["transactionid"] = frame["transactionid"].astype("string")
        store.write("events_user_item", frame, ("visitorid", "itemid"))

    summary, intervals = partitioned_prior_behavior_funnel(store)
    row = summary.frame.iloc[0]
    assert row["transaction_event_count"] == 2
    assert row["with_strictly_prior_view_count"] == 1
    assert row["with_strictly_prior_addtocart_count"] == 1
    by_metric = intervals.frame.set_index("metric")
    assert by_metric.loc["most_recent_prior_view_interval_ms", "mean"] == 2000
    assert by_metric.loc["most_recent_prior_addtocart_interval_ms", "mean"] == 1000


def test_partitioned_property_change_and_same_time_multivalue(
    tmp_path: Path,
) -> None:
    store = PartitionStore(tmp_path / "run", partition_count=5)
    columns = ["timestamp", "itemid", "property", "value"]
    chunks = [
        [[1000, 1, "x", "A"], [1000, 2, "y", "A"]],
        [[2000, 1, "x", "B"], [1000, 2, "y", "B"]],
    ]
    for rows in chunks:
        frame = pd.DataFrame(rows, columns=columns)
        frame["timestamp"] = frame["timestamp"].astype("Int64")
        frame["itemid"] = frame["itemid"].astype("Int64")
        frame["property"] = frame["property"].astype("string")
        frame["value"] = frame["value"].astype("string")
        payload = frame.assign(_source_name="item_properties_part1")
        store.write(
            "properties_item_property", payload, ("itemid", "property")
        )
        store.write(
            "properties_property_value_candidates",
            payload.loc[:, ["property", "value", "_source_name"]].drop_duplicates(),
            ("property", "value"),
        )

    _, changes, _ = partitioned_property_audit(store, total_property_items=2)
    rows = changes.frame.set_index("property")
    assert rows.loc["x", "pairs_changing_over_time"] == 1
    assert rows.loc["x", "pairs_with_same_timestamp_multiple_values"] == 0
    assert rows.loc["y", "pairs_changing_over_time"] == 0
    assert rows.loc["y", "pairs_with_same_timestamp_multiple_values"] == 1


def test_temp_path_protection_and_failure_retention(tmp_path: Path) -> None:
    raw = tmp_path / "data" / "raw"
    config = AuditConfig(
        data_dir=raw,
        output_dir=tmp_path / "out",
        temp_dir=raw / "forbidden",
    )
    try:
        config.validate()
    except ValueError as exc:
        assert "temp_dir" in str(exc)
    else:
        raise AssertionError("temp_dir inside raw must be rejected")

    safe_config = AuditConfig(
        data_dir=raw,
        output_dir=tmp_path / "out",
        temp_dir=tmp_path / "tmp",
        keep_temp=False,
    )
    workspace = RunWorkspace(safe_config, input_size_bytes=1)
    workspace.prepare()
    workspace.finish_failure(RuntimeError("synthetic failure"))
    assert workspace.run_dir.exists()
    status = json.loads(
        (workspace.run_dir / "run_status.json").read_text(encoding="utf-8")
    )
    assert status["status"] == "failed"
    assert status["retained_for_diagnostics"] is True


def test_success_cleanup_and_keep_temp(tmp_path: Path) -> None:
    raw = tmp_path / "data" / "raw"
    cleanup_config = AuditConfig(
        data_dir=raw,
        output_dir=tmp_path / "out",
        temp_dir=tmp_path / "tmp-clean",
    )
    cleanup = RunWorkspace(cleanup_config, input_size_bytes=1)
    cleanup.prepare()
    cleanup.finish_success()
    assert not cleanup.run_dir.exists()

    keep_config = AuditConfig(
        data_dir=raw,
        output_dir=tmp_path / "out",
        temp_dir=tmp_path / "tmp-keep",
        keep_temp=True,
    )
    keep = RunWorkspace(keep_config, input_size_bytes=1)
    keep.prepare()
    keep.finish_success()
    assert keep.run_dir.exists()
