"""Tests for evidence-gated feature and module feasibility matrices."""

from __future__ import annotations

import pandas as pd
import pytest

from src.data_audit.evidence import EvidenceTable
from src.data_audit.matrices import (
    DATASET_SUPPORT_VALUES,
    FEATURE_RULES,
    FEATURE_SUPPORT_VALUES,
    MODULE_RULES,
    RECOMMENDED_ACTION_VALUES,
    NonEvidentiaryMatrixError,
    generate_data_feasibility_matrix,
    generate_feature_feasibility_matrix,
    write_formal_matrices,
)


def _synthetic_catalog() -> list[EvidenceTable]:
    evidence_ids = {
        rule.evidence_id for rule in FEATURE_RULES
    } | {
        evidence_id
        for rule in MODULE_RULES
        for evidence_id in rule.evidence_ids
        if evidence_id != "G01_FEATURE_FEASIBILITY_MATRIX"
    }
    metrics_by_id: dict[str, set[str]] = {}
    for rule in FEATURE_RULES:
        metrics_by_id.setdefault(rule.evidence_id, set()).add(rule.coverage_metric)
    tables = []
    for evidence_id in sorted(evidence_ids):
        row = {
            "rows_read": 10,
            "overlap_item_count": 5,
            "union_item_category_coverage": 0.5,
            "transaction_event_count": 2,
            **{
                metric: 1
                for metric in metrics_by_id.get(evidence_id, set())
                if metric != "row_count"
            },
        }
        tables.append(
            EvidenceTable(
                evidence_id=evidence_id,
                title=evidence_id,
                population="synthetic unit-test population",
                denominator="synthetic unit-test denominator",
                frame=pd.DataFrame([row]),
            )
        )
    return tables


def test_feature_matrix_enums_and_evidence_ids() -> None:
    tables = _synthetic_catalog()
    matrix = generate_feature_feasibility_matrix(tables, "full")
    assert set(matrix["support_level"]) <= FEATURE_SUPPORT_VALUES
    assert set(matrix["feature_group"]) == {
        "user",
        "item",
        "context",
        "user_item_cross",
    }
    catalog = {table.evidence_id for table in tables}
    assert set(matrix["coverage_evidence_id"]) <= catalog


def test_module_matrix_enums_modules_and_evidence_ids() -> None:
    tables = _synthetic_catalog()
    feature = generate_feature_feasibility_matrix(tables, "full")
    tables.append(
        EvidenceTable(
            "G01_FEATURE_FEASIBILITY_MATRIX",
            "feature matrix",
            "synthetic rules",
            "one row per feature",
            feature,
        )
    )
    matrix = generate_data_feasibility_matrix(tables, "full")
    assert len(matrix) == 30
    assert matrix["module"].nunique() == 30
    assert set(matrix["dataset_support"]) <= DATASET_SUPPORT_VALUES
    assert set(matrix["recommended_action"]) <= RECOMMENDED_ACTION_VALUES
    for rule in MODULE_RULES:
        evidence = matrix.loc[matrix["module"].eq(rule.module), "evidence"].iloc[0]
        for evidence_id in rule.evidence_ids:
            assert evidence_id in evidence


def test_smoke_scope_withholds_both_formal_matrices() -> None:
    tables = _synthetic_catalog()
    with pytest.raises(NonEvidentiaryMatrixError):
        generate_feature_feasibility_matrix(tables, "smoke_non_evidentiary")
    with pytest.raises(NonEvidentiaryMatrixError):
        generate_data_feasibility_matrix(tables, "smoke_non_evidentiary")


def test_full_scope_writes_all_formal_matrix_files(tmp_path) -> None:
    tables = _synthetic_catalog()
    written = write_formal_matrices(tmp_path, tables, "full")
    assert {path.name for path in written} == {
        "feature_feasibility_matrix.csv",
        "data_feasibility_matrix.csv",
        "data_feasibility_matrix.md",
    }
    assert all(path.is_file() for path in written)
