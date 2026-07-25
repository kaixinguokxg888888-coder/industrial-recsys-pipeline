"""Unit and integration tests for bounded, non-evidentiary audit behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.data_audit.config import AuditConfig
from src.data_audit.events import (
    gini,
    user_sequence_tables,
)
from src.data_audit.io import (
    InputValidationError,
    normalize_chunk,
    resolve_and_validate_inputs,
)
from src.data_audit.properties import category_tree_audit
from src.data_audit.run_audit import run


def _write_fixture(data_dir: Path) -> None:
    data_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            [1000, 1, "view", 10, None],
            [1000, 1, "transaction", 10, "txn-same-time"],
            [2000, 1, "addtocart", 10, None],
            [3000, 1, "transaction", 10, "txn-prior"],
            [4000, 2, "view", 20, None],
        ],
        columns=["timestamp", "visitorid", "event", "itemid", "transactionid"],
    ).to_csv(data_dir / "events.csv", index=False)
    pd.DataFrame(
        [
            [900, 10, "categoryid", "100"],
            [2500, 10, "available", "1"],
            [3500, 10, "available", "0"],
        ],
        columns=["timestamp", "itemid", "property", "value"],
    ).to_csv(data_dir / "item_properties_part1.csv", index=False)
    pd.DataFrame(
        [[800, 20, "categoryid", "200"]],
        columns=["timestamp", "itemid", "property", "value"],
    ).to_csv(data_dir / "item_properties_part2.csv", index=False)
    pd.DataFrame(
        [[100, None], [200, 100]],
        columns=["categoryid", "parentid"],
    ).to_csv(data_dir / "category_tree.csv", index=False)


def test_input_header_validation(tmp_path: Path) -> None:
    data_dir = tmp_path / "raw"
    _write_fixture(data_dir)
    (data_dir / "events.csv").write_text("wrong,header\n1,2\n", encoding="utf-8")
    with pytest.raises(InputValidationError, match="unexpected header"):
        resolve_and_validate_inputs(data_dir)


def test_transaction_identifier_remains_string() -> None:
    raw = pd.DataFrame(
        [["1000", "1", "transaction", "10", "000123"]],
        columns=["timestamp", "visitorid", "event", "itemid", "transactionid"],
        dtype="string",
    )
    parsed = normalize_chunk(raw, "events")
    assert str(parsed.frame["transactionid"].dtype) == "string"
    assert parsed.frame.loc[0, "transactionid"] == "000123"


def test_output_cannot_be_inside_raw_data(tmp_path: Path) -> None:
    raw = tmp_path / "data" / "raw"
    config = AuditConfig(data_dir=raw, output_dir=raw / "outputs")
    with pytest.raises(ValueError, match="output_dir"):
        config.validate()


def test_sequence_buckets_reconcile() -> None:
    events = pd.DataFrame(
        [
            [1000, 1, "view", 10],
            [2000, 2, "view", 10],
            [3000, 2, "addtocart", 10],
            [4000, 2, "transaction", 10],
            [5000, 2, "view", 20],
            [6000, 2, "view", 30],
        ],
        columns=["timestamp", "visitorid", "event", "itemid"],
    )
    metrics, _, buckets = user_sequence_tables(events)
    assert buckets.frame["user_count"].sum() == len(metrics.frame)
    assert (
        buckets.frame["behavior_count"].sum()
        == metrics.frame["total_behavior_length"].sum()
    )
    assert set(buckets.frame["sequence_length_bucket"]) == {
        "1",
        "2-4",
        "5-9",
        "10-19",
        "20-49",
        "50-99",
        "100+",
    }


def test_gini_known_values_and_zero_handling() -> None:
    assert gini([0, 0, 0]) == 0.0
    assert gini([1, 1, 1]) == pytest.approx(0.0)
    assert gini([0, 0, 1]) == pytest.approx(2 / 3)
    with pytest.raises(ValueError):
        gini([1, -1])


def test_category_cycle_detection_terminates() -> None:
    tree = pd.DataFrame(
        [[1, 2], [2, 1], [3, None], [4, 99], [5, 5]],
        columns=["categoryid", "parentid"],
    )
    summary, nodes = category_tree_audit(tree)
    row = summary.frame.iloc[0]
    assert row["root_count"] == 1
    assert row["missing_parent_node_count"] == 1
    assert row["self_loop_count"] == 1
    assert row["cycle_node_count"] == 3
    assert nodes.frame["is_cycle_node"].sum() == 3
    assert pd.isna(
        nodes.frame.loc[nodes.frame["categoryid"].eq(4), "resolved_depth"].iloc[0]
    )


def test_bounded_run_is_marked_non_evidentiary(tmp_path: Path) -> None:
    data_dir = tmp_path / "raw"
    output_dir = tmp_path / "smoke"
    _write_fixture(data_dir)
    config = AuditConfig(
        data_dir=data_dir,
        output_dir=output_dir,
        chunk_size=2,
        max_rows_per_file=10,
        fingerprint_mode="metadata",
    )
    written = run(config)
    assert output_dir / "smoke_audit_report.md" in written
    assert not (output_dir / "data_audit_report.md").exists()
    report = (output_dir / "smoke_audit_report.md").read_text(encoding="utf-8")
    assert "NON-EVIDENTIARY" in report
    manifest = json.loads(
        (output_dir / "audit_manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["data_scope"] == "smoke_non_evidentiary"
    assert manifest["configuration"]["max_rows_per_file"] == 10
    assert manifest["matrix_status"] == "withheld_pending_full_audit"
    assert manifest["partition_processing"]["status"] == "completed"
    assert (
        manifest["partition_processing"]["datasets"]["properties_item_property"][
            "rows"
        ]
        == 4
    )
    assert (
        manifest["partition_processing"]["datasets"][
            "properties_property_value_candidates"
        ]["rows"]
        <= 4
    )
    assert not (output_dir / "feature_feasibility_matrix.csv").exists()
    assert not (output_dir / "data_feasibility_matrix.csv").exists()
    assert not (output_dir / "data_feasibility_matrix.md").exists()
    assert not Path(manifest["temporary_workspace"]["run_dir"]).exists()
