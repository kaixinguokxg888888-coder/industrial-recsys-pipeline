"""Declarative, evidence-gated feasibility matrix generation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Final, Iterable, Literal

import pandas as pd

from .config import DataScope
from .evidence import EvidenceTable

FeatureSupport = Literal[
    "directly_available",
    "historically_constructible",
    "proxy_only",
    "not_reliably_constructible",
]
DatasetSupport = Literal[
    "fully_supported",
    "partially_supported",
    "weakly_supported",
    "unsupported",
]
RecommendedAction = Literal[
    "implement",
    "implement_with_constraints",
    "active_user_experiment_only",
    "synthetic_pipeline_test_only",
    "design_only",
    "remove",
]

FEATURE_SUPPORT_VALUES: Final[set[str]] = {
    "directly_available",
    "historically_constructible",
    "proxy_only",
    "not_reliably_constructible",
}
DATASET_SUPPORT_VALUES: Final[set[str]] = {
    "fully_supported",
    "partially_supported",
    "weakly_supported",
    "unsupported",
}
RECOMMENDED_ACTION_VALUES: Final[set[str]] = {
    "implement",
    "implement_with_constraints",
    "active_user_experiment_only",
    "synthetic_pipeline_test_only",
    "design_only",
    "remove",
}

FEATURE_MATRIX_COLUMNS: Final[list[str]] = [
    "feature_group",
    "feature_name",
    "source_fields",
    "construction_method",
    "support_level",
    "coverage_evidence_id",
    "coverage_value",
    "leakage_risk",
    "limitations",
    "recommended_usage",
    "notes",
]
MODULE_MATRIX_COLUMNS: Final[list[str]] = [
    "module",
    "required_data",
    "dataset_support",
    "evidence",
    "feasible_scope",
    "limitations",
    "leakage_risk",
    "recommended_action",
    "confidence",
    "notes",
]


class NonEvidentiaryMatrixError(RuntimeError):
    """Raised when a limited run attempts to publish formal conclusions."""


@dataclass(frozen=True)
class FeatureRule:
    feature_group: str
    feature_name: str
    source_fields: str
    construction_method: str
    support_level: FeatureSupport
    evidence_id: str
    coverage_metric: str
    leakage_risk: str
    limitations: str
    recommended_usage: str
    notes: str = ""


@dataclass(frozen=True)
class ModuleRule:
    module: str
    required_data: str
    dataset_support: DatasetSupport
    evidence_ids: tuple[str, ...]
    feasible_scope: str
    limitations: str
    leakage_risk: str
    recommended_action: RecommendedAction
    confidence: str
    notes: str = ""


FEATURE_RULES: Final[tuple[FeatureRule, ...]] = (
    FeatureRule("user", "visitor_identifier", "events.visitorid", "Use the observed identifier only for grouping.", "directly_available", "A01_FILE_PROFILES", "rows_read", "Identifier must not encode future behavior.", "Anonymous identifier; no profile semantics.", "grouping_key"),
    FeatureRule("user", "historical_event_counts", "events.timestamp,event,visitorid", "Count only events strictly before prediction time.", "historically_constructible", "C01_USER_SEQUENCE_METRICS", "row_count", "Future-window inclusion causes leakage.", "Sparse histories for short-sequence users.", "time_cutoff_feature"),
    FeatureRule("user", "historical_active_days", "events.timestamp,visitorid", "Count UTC active days strictly before prediction time.", "historically_constructible", "C02_USER_SEQUENCE_DISTRIBUTION", "row_count", "Must apply prediction-time cutoff.", "Observed-event activity only.", "time_cutoff_feature"),
    FeatureRule("user", "demographic_profile", "none", "No reliable construction.", "not_reliably_constructible", "A01_FILE_PROFILES", "row_count", "Fabrication risk.", "No demographic fields.", "exclude"),
    FeatureRule("item", "item_identifier", "events.itemid,item_properties.itemid", "Use observed identifier as entity key.", "directly_available", "A03_CROSS_SOURCE_ITEM_COVERAGE", "overlap_item_count", "Do not use future first-seen state.", "Coverage differs across sources.", "entity_key"),
    FeatureRule("item", "historical_popularity", "events.timestamp,event,itemid", "Aggregate observed events strictly before prediction time.", "historically_constructible", "D01_ITEM_METRICS", "row_count", "Full-period popularity leaks future behavior.", "Event counts are not true sales.", "time_cutoff_feature"),
    FeatureRule("item", "categoryid", "item_properties.property,value,timestamp", "Resolve explicit categoryid as-of prediction time.", "historically_constructible", "A04_ITEM_CATEGORY_COVERAGE", "union_item_category_coverage", "Latest global value may be future information.", "Only items with explicit categoryid records.", "asof_item_feature"),
    FeatureRule("item", "available", "item_properties.property,value,timestamp", "Resolve explicit available value as-of prediction time.", "historically_constructible", "E03_SPECIAL_PROPERTIES", "row_count", "Latest global value may be future information.", "Meaning limited to dataset's explicit property name.", "asof_item_feature"),
    FeatureRule("item", "brand", "none reliably identified", "Do not infer from hashed properties.", "not_reliably_constructible", "E01_PROPERTY_PROFILE", "row_count", "Unsupported semantic assignment.", "No verified brand field.", "exclude"),
    FeatureRule("item", "price", "none reliably identified", "Do not infer from numeric or hashed values.", "not_reliably_constructible", "E01_PROPERTY_PROFILE", "row_count", "Unsupported semantic assignment.", "No verified price field.", "exclude"),
    FeatureRule("context", "event_timestamp", "events.timestamp", "Use request/event time components available at prediction time.", "directly_available", "B02_EVENT_DAILY_TREND", "row_count", "Derived global future trends would leak.", "No request context beyond observed event time.", "request_time_only"),
    FeatureRule("context", "device_location_request_context", "none", "No reliable construction.", "not_reliably_constructible", "A01_FILE_PROFILES", "row_count", "Fabrication risk.", "No device, location, page, or request fields.", "exclude"),
    FeatureRule("context", "true_impression", "none", "Cannot reconstruct genuine recommendation exposure.", "not_reliably_constructible", "B01_EVENT_DISTRIBUTION", "row_count", "Using view as impression creates invalid denominators.", "No exposure or recommendation-request log.", "exclude"),
    FeatureRule("user_item_cross", "historical_interaction_counts", "events.visitorid,itemid,event,timestamp", "Aggregate same-pair history strictly before prediction time.", "historically_constructible", "B04_TRANSACTION_PRIOR_BEHAVIOR", "transaction_event_count", "Future pair events must be excluded.", "Only observed positive event histories.", "time_cutoff_cross_feature"),
    FeatureRule("user_item_cross", "view_to_transaction_interval_proxy", "events.visitorid,itemid,event,timestamp", "Use strictly prior same-pair view interval.", "proxy_only", "B05_PRIOR_BEHAVIOR_INTERVALS", "row_count", "Same-time and future events must be excluded.", "Not an impression-to-conversion interval.", "constrained_proxy"),
)


def _module_rules() -> tuple[ModuleRule, ...]:
    """Return the complete mandatory module rule table."""

    return (
        ModuleRule("Popularity retrieval", "time-bounded item event counts", "fully_supported", ("D01_ITEM_METRICS", "D02_ITEM_CONCENTRATION"), "offline retrieval baseline", "Popularity reflects observed events only.", "Use only pre-cutoff counts.", "implement", "high"),
        ModuleRule("Category retrieval", "as-of categoryid and user history", "partially_supported", ("A04_ITEM_CATEGORY_COVERAGE", "E03_SPECIAL_PROPERTIES"), "items with explicit category history", "Category coverage may be incomplete.", "Resolve categories as-of cutoff.", "implement_with_constraints", "medium"),
        ModuleRule("ItemCF", "time-ordered user-item interactions", "fully_supported", ("C01_USER_SEQUENCE_METRICS", "D01_ITEM_METRICS"), "observed-event collaborative retrieval", "Positive-only interaction data.", "Build co-occurrence from training history only.", "implement", "high"),
        ModuleRule("Swing", "multi-user item co-occurrence", "partially_supported", ("C03_USER_SEQUENCE_BUCKETS", "D01_ITEM_METRICS"), "users with sufficient histories", "Short histories may limit support.", "Training history only.", "active_user_experiment_only", "medium"),
        ModuleRule("Standard two-tower", "users, items, positives, defensible negatives", "partially_supported", ("C03_USER_SEQUENCE_BUCKETS", "A03_CROSS_SOURCE_ITEM_COVERAGE"), "retrieval with sampled non-interactions", "No true exposure negatives.", "Negative sampling must be time-aware.", "implement_with_constraints", "medium"),
        ModuleRule("SDM", "long sequential user histories", "weakly_supported", ("C02_USER_SEQUENCE_DISTRIBUTION", "C03_USER_SEQUENCE_BUCKETS"), "active-user subset", "Population support depends on sequence distribution.", "Sequences stop at prediction time.", "active_user_experiment_only", "medium"),
        ModuleRule("DIN", "candidate-conditioned histories", "weakly_supported", ("C02_USER_SEQUENCE_DISTRIBUTION", "C03_USER_SEQUENCE_BUCKETS"), "active-user subset", "Short histories and proxy labels.", "Histories stop at prediction time.", "active_user_experiment_only", "medium"),
        ModuleRule("DeepFM", "dense/sparse fields and labels", "weakly_supported", ("A01_FILE_PROFILES", "E01_PROPERTY_PROFILE"), "synthetic or constrained ranking pipeline", "Few verified semantic fields; no exposure labels.", "As-of property resolution required.", "synthetic_pipeline_test_only", "medium"),
        ModuleRule("LightGBM coarse ranking", "candidate features and proxy labels", "partially_supported", ("G01_FEATURE_FEASIBILITY_MATRIX", "F01_TIME_SPLIT_COMPARISON"), "offline proxy ranking", "No true impression negatives.", "All aggregates must be pre-cutoff.", "implement_with_constraints", "medium"),
        ModuleRule("Single-task CTR ranking", "impressions and clicks", "unsupported", ("B01_EVENT_DISTRIBUTION",), "design only", "No genuine impression denominator.", "View cannot be relabeled as impression.", "remove", "high"),
        ModuleRule("Single-task purchase-propensity ranking", "historical interactions and transaction-event proxy", "partially_supported", ("B04_TRANSACTION_PRIOR_BEHAVIOR", "D01_ITEM_METRICS"), "offline purchase-event propensity proxy", "Not true purchase probability or sales.", "Labels must be future-window and cutoff-safe.", "implement_with_constraints", "medium"),
        ModuleRule("MMoE", "multiple reliable task labels", "weakly_supported", ("B01_EVENT_DISTRIBUTION", "B04_TRANSACTION_PRIOR_BEHAVIOR"), "synthetic pipeline only", "No exposure-based CTR/CVR labels.", "Task windows must be cutoff-safe.", "synthetic_pipeline_test_only", "medium"),
        ModuleRule("PLE", "multiple reliable task labels", "weakly_supported", ("B01_EVENT_DISTRIBUTION", "B04_TRANSACTION_PRIOR_BEHAVIOR"), "synthetic pipeline only", "No exposure-based CTR/CVR labels.", "Task windows must be cutoff-safe.", "synthetic_pipeline_test_only", "medium"),
        ModuleRule("CTR+CVR", "impression, click, conversion labels", "unsupported", ("B01_EVENT_DISTRIBUTION",), "design only", "No genuine impression/click denominator.", "Cannot fabricate funnel labels.", "design_only", "high"),
        ModuleRule("CTR+CVR+CTCVR", "impression, click, cart, conversion labels", "unsupported", ("B01_EVENT_DISTRIBUTION",), "design only", "No genuine impression/click denominator.", "Cannot fabricate funnel labels.", "design_only", "high"),
        ModuleRule("ESMM", "impression-space CTR and CTCVR labels", "unsupported", ("B01_EVENT_DISTRIBUTION",), "design only", "Required impression space is absent.", "Cannot use view as impression.", "design_only", "high"),
        ModuleRule("ESCM2", "exposure/click/conversion causal labels", "unsupported", ("B01_EVENT_DISTRIBUTION",), "design only", "Required exposure and intervention data absent.", "Cannot fabricate exposure selection.", "design_only", "high"),
        ModuleRule("MMR", "candidate relevance and item similarity/diversity", "partially_supported", ("A04_ITEM_CATEGORY_COVERAGE", "E01_PROPERTY_PROFILE"), "category/property constrained reranking", "Semantic diversity fields are limited.", "Use as-of attributes.", "implement_with_constraints", "medium"),
        ModuleRule("User cold start", "user profile or context", "weakly_supported", ("C03_USER_SEQUENCE_BUCKETS", "A01_FILE_PROFILES"), "non-personalized fallback", "No user profile/context.", "Do not use future history.", "implement_with_constraints", "high"),
        ModuleRule("Item cold start", "item metadata available before first interaction", "partially_supported", ("E01_PROPERTY_PROFILE", "A04_ITEM_CATEGORY_COVERAGE"), "items with pre-interaction properties", "Hashed semantics and coverage limits.", "Use only properties available before cutoff.", "implement_with_constraints", "medium"),
        ModuleRule("Strict temporal split", "timestamps and ordered histories", "fully_supported", ("F01_TIME_SPLIT_COMPARISON",), "offline evaluation", "Operational choice still requires evidence-based selection.", "Enforce non-overlapping cutoffs.", "implement", "high"),
        ModuleRule("GAUC", "per-user labels and prediction scores", "partially_supported", ("C03_USER_SEQUENCE_BUCKETS", "F01_TIME_SPLIT_COMPARISON"), "eligible users with proxy labels", "Undefined for users without both classes.", "Score only future-window samples.", "implement_with_constraints", "medium"),
        ModuleRule("NDCG@K", "ranked candidates and held-out relevant items", "fully_supported", ("F01_TIME_SPLIT_COMPARISON",), "temporal offline evaluation", "Relevance is observed-event based.", "Hold out future interactions only.", "implement", "high"),
        ModuleRule("Recall@K", "ranked candidates and held-out relevant items", "fully_supported", ("F01_TIME_SPLIT_COMPARISON",), "temporal offline evaluation", "Relevance is observed-event based.", "Hold out future interactions only.", "implement", "high"),
        ModuleRule("MAP@K", "ranked candidates and held-out relevant items", "fully_supported", ("F01_TIME_SPLIT_COMPARISON",), "temporal offline evaluation", "Relevance is observed-event based.", "Hold out future interactions only.", "implement", "high"),
        ModuleRule("HitRate@K", "ranked candidates and held-out relevant items", "fully_supported", ("F01_TIME_SPLIT_COMPARISON",), "temporal offline evaluation", "Relevance is observed-event based.", "Hold out future interactions only.", "implement", "high"),
        ModuleRule("Coverage", "recommendation lists and item universe", "fully_supported", ("D01_ITEM_METRICS",), "offline catalog coverage", "Universe definition must be explicit.", "Use train-available item universe.", "implement", "high"),
        ModuleRule("Intra-List Diversity", "recommendation lists and reliable item similarity", "partially_supported", ("A04_ITEM_CATEGORY_COVERAGE", "E01_PROPERTY_PROFILE"), "items with usable category/property data", "Limited verified semantics.", "Use as-of item attributes.", "implement_with_constraints", "medium"),
        ModuleRule("Offline simulated A/B test", "counterfactual exposure and outcomes", "unsupported", ("B01_EVENT_DISTRIBUTION",), "synthetic pipeline demonstration", "Offline comparison is not an A/B test.", "Must not claim causal effects.", "synthetic_pipeline_test_only", "high"),
        ModuleRule("Real online A/B test", "online serving, randomization, exposure and outcomes", "unsupported", ("B01_EVENT_DISTRIBUTION",), "industrial extension design", "No online system or randomized experiment.", "Cannot fabricate online results.", "design_only", "high"),
    )


MODULE_RULES: Final[tuple[ModuleRule, ...]] = _module_rules()


def _catalog(tables: Iterable[EvidenceTable]) -> dict[str, EvidenceTable]:
    catalog: dict[str, EvidenceTable] = {}
    for table in tables:
        if table.evidence_id in catalog:
            raise ValueError(f"duplicate evidence ID: {table.evidence_id}")
        catalog[table.evidence_id] = table
    return catalog


def _require_full(data_scope: DataScope) -> None:
    if data_scope != "full":
        raise NonEvidentiaryMatrixError(
            "formal feasibility conclusions are withheld for non-full runs"
        )


def _coverage_value(table: EvidenceTable, metric: str) -> str:
    if metric == "row_count":
        return str(len(table.frame))
    if metric not in table.frame.columns or table.frame.empty:
        raise KeyError(
            f"coverage metric {metric!r} missing from evidence {table.evidence_id}"
        )
    values = table.frame[metric].dropna()
    if values.empty:
        return "0"
    return str(values.iloc[0])


def generate_feature_feasibility_matrix(
    tables: Iterable[EvidenceTable],
    data_scope: DataScope,
) -> pd.DataFrame:
    """Generate feature conclusions only when full-data evidence is available."""

    _require_full(data_scope)
    catalog = _catalog(tables)
    rows: list[dict[str, str]] = []
    for rule in FEATURE_RULES:
        if rule.evidence_id not in catalog:
            raise KeyError(
                f"missing evidence {rule.evidence_id} for feature {rule.feature_name}"
            )
        rows.append(
            {
                "feature_group": rule.feature_group,
                "feature_name": rule.feature_name,
                "source_fields": rule.source_fields,
                "construction_method": rule.construction_method,
                "support_level": rule.support_level,
                "coverage_evidence_id": rule.evidence_id,
                "coverage_value": _coverage_value(
                    catalog[rule.evidence_id], rule.coverage_metric
                ),
                "leakage_risk": rule.leakage_risk,
                "limitations": rule.limitations,
                "recommended_usage": rule.recommended_usage,
                "notes": rule.notes,
            }
        )
    result = pd.DataFrame(rows, columns=FEATURE_MATRIX_COLUMNS)
    invalid = set(result["support_level"]) - FEATURE_SUPPORT_VALUES
    if invalid:
        raise ValueError(f"invalid feature support values: {sorted(invalid)}")
    return result


def _evidence_summary(table: EvidenceTable) -> str:
    numeric = table.frame.select_dtypes(include="number")
    metrics: list[str] = []
    if not numeric.empty and not table.frame.empty:
        for column in list(numeric.columns)[:2]:
            value = numeric[column].dropna()
            if not value.empty:
                metrics.append(f"{column}={value.iloc[0]}")
    suffix = ",".join(metrics) if metrics else f"rows={len(table.frame)}"
    return f"{table.evidence_id}[{suffix}]"


def generate_data_feasibility_matrix(
    tables: Iterable[EvidenceTable],
    data_scope: DataScope,
) -> pd.DataFrame:
    """Apply the complete declarative module rule table to full evidence."""

    _require_full(data_scope)
    catalog = _catalog(tables)
    rows: list[dict[str, str]] = []
    for rule in MODULE_RULES:
        missing = [evidence for evidence in rule.evidence_ids if evidence not in catalog]
        if missing:
            raise KeyError(f"missing evidence {missing} for module {rule.module}")
        rows.append(
            {
                "module": rule.module,
                "required_data": rule.required_data,
                "dataset_support": rule.dataset_support,
                "evidence": "; ".join(
                    _evidence_summary(catalog[evidence])
                    for evidence in rule.evidence_ids
                ),
                "feasible_scope": rule.feasible_scope,
                "limitations": rule.limitations,
                "leakage_risk": rule.leakage_risk,
                "recommended_action": rule.recommended_action,
                "confidence": rule.confidence,
                "notes": rule.notes,
            }
        )
    result = pd.DataFrame(rows, columns=MODULE_MATRIX_COLUMNS)
    invalid_support = set(result["dataset_support"]) - DATASET_SUPPORT_VALUES
    invalid_action = set(result["recommended_action"]) - RECOMMENDED_ACTION_VALUES
    if invalid_support or invalid_action:
        raise ValueError(
            f"invalid module enums: support={invalid_support}, action={invalid_action}"
        )
    return result


def dataframe_to_markdown(frame: pd.DataFrame) -> str:
    """Render a simple Markdown table without an optional tabulate dependency."""

    columns = [str(column) for column in frame.columns]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in frame.itertuples(index=False, name=None):
        escaped = [str(value).replace("|", "\\|").replace("\n", " ") for value in row]
        lines.append("| " + " | ".join(escaped) + " |")
    return "\n".join(lines) + "\n"


def write_formal_matrices(
    output_dir: Path,
    tables: Iterable[EvidenceTable],
    data_scope: DataScope,
) -> list[Path]:
    """Write the three required formal matrix artifacts for a full run only."""

    table_list = list(tables)
    feature = generate_feature_feasibility_matrix(table_list, data_scope)
    feature_evidence = EvidenceTable(
        evidence_id="G01_FEATURE_FEASIBILITY_MATRIX",
        title="Evidence-gated feature feasibility matrix",
        population="declared feature rule catalog",
        denominator="one row per declared feature",
        frame=feature,
    )
    modules = generate_data_feasibility_matrix(
        [*table_list, feature_evidence], data_scope
    )
    feature_path = output_dir / "feature_feasibility_matrix.csv"
    module_csv_path = output_dir / "data_feasibility_matrix.csv"
    module_md_path = output_dir / "data_feasibility_matrix.md"
    feature.to_csv(feature_path, index=False)
    modules.to_csv(module_csv_path, index=False)
    module_md_path.write_text(
        "# Recommendation Module Feasibility Matrix\n\n"
        + dataframe_to_markdown(modules),
        encoding="utf-8",
    )
    return [feature_path, module_csv_path, module_md_path]
