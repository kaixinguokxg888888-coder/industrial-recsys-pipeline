"""Behavior, sequence, long-tail, funnel, and temporal-split audit functions."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .evidence import EvidenceTable

EXPECTED_EVENTS = ("view", "addtocart", "transaction")
SEQUENCE_BUCKETS = (
    (1, 1, "1"),
    (2, 4, "2-4"),
    (5, 9, "5-9"),
    (10, 19, "10-19"),
    (20, 49, "20-49"),
    (50, 99, "50-99"),
    (100, None, "100+"),
)


def _valid_events(events: pd.DataFrame) -> pd.DataFrame:
    required = ["timestamp", "visitorid", "event", "itemid"]
    return events.dropna(subset=required).copy()


def event_distribution(events: pd.DataFrame) -> EvidenceTable:
    """Count observed event values without calling views impressions."""

    counts = events["event"].fillna("<missing>").value_counts(dropna=False)
    total = int(counts.sum())
    frame = counts.rename_axis("event").reset_index(name="event_count")
    frame["event_share"] = frame["event_count"] / total if total else 0.0
    frame["is_expected_event"] = frame["event"].isin(EXPECTED_EVENTS)
    return EvidenceTable(
        evidence_id="B01_EVENT_DISTRIBUTION",
        title="Observed event-value distribution",
        population="rows read from events.csv for this run",
        denominator="all event rows read for this run",
        frame=frame,
    )


def event_time_trend(events: pd.DataFrame) -> EvidenceTable:
    """Count event values by UTC calendar day."""

    valid = events.dropna(subset=["timestamp", "event"]).copy()
    valid["date_utc"] = pd.to_datetime(
        valid["timestamp"], unit="ms", utc=True
    ).dt.strftime("%Y-%m-%d")
    frame = (
        valid.groupby(["date_utc", "event"], dropna=False)
        .size()
        .rename("event_count")
        .reset_index()
    )
    return EvidenceTable(
        evidence_id="B02_EVENT_DAILY_TREND",
        title="Observed event counts by UTC day",
        population="event rows with parseable timestamp and non-missing event",
        denominator="event rows in each UTC day",
        frame=frame,
    )


def behavior_combinations(events: pd.DataFrame) -> EvidenceTable:
    """Count per-user combinations of observed behavior types."""

    valid = events.dropna(subset=["visitorid", "event"])
    combinations = (
        valid.groupby("visitorid")["event"]
        .agg(lambda values: "|".join(sorted(set(str(value) for value in values))))
        .value_counts()
        .rename_axis("behavior_combination")
        .reset_index(name="user_count")
    )
    total_users = int(combinations["user_count"].sum())
    combinations["user_share"] = (
        combinations["user_count"] / total_users if total_users else 0.0
    )
    return EvidenceTable(
        evidence_id="B03_USER_BEHAVIOR_COMBINATIONS",
        title="User behavior-type combinations",
        population="users with at least one non-missing event value",
        denominator="users with at least one non-missing event value",
        frame=combinations,
    )


def distribution_summary(series: pd.Series, metric: str) -> pd.DataFrame:
    """Summarize a numeric series with the specification's percentiles."""

    values = pd.to_numeric(series, errors="coerce").dropna().astype(float)
    quantiles = {
        "P25": 0.25,
        "P50": 0.50,
        "P75": 0.75,
        "P90": 0.90,
        "P95": 0.95,
        "P99": 0.99,
    }
    row: dict[str, float | int | str] = {"metric": metric, "count": len(values)}
    if values.empty:
        row.update(
            {
                "mean": math.nan,
                "std": math.nan,
                "min": math.nan,
                **{name: math.nan for name in quantiles},
                "max": math.nan,
            }
        )
    else:
        row.update(
            {
                "mean": float(values.mean()),
                "std": float(values.std(ddof=1)) if len(values) > 1 else math.nan,
                "min": float(values.min()),
                **{
                    name: float(values.quantile(value, interpolation="linear"))
                    for name, value in quantiles.items()
                },
                "max": float(values.max()),
            }
        )
    return pd.DataFrame([row])


def user_sequence_tables(
    events: pd.DataFrame,
) -> tuple[EvidenceTable, EvidenceTable, EvidenceTable]:
    """Build per-user sequence metrics, distributions, and exhaustive buckets."""

    valid = _valid_events(events)
    valid["date_utc"] = pd.to_datetime(
        valid["timestamp"], unit="ms", utc=True
    ).dt.floor("D")
    grouped = valid.groupby("visitorid", sort=False)
    users = grouped.agg(
        total_behavior_length=("event", "size"),
        distinct_item_count=("itemid", "nunique"),
        active_day_count=("date_utc", "nunique"),
        first_timestamp_ms=("timestamp", "min"),
        last_timestamp_ms=("timestamp", "max"),
    )
    for event_value, column_name in (
        ("view", "view_length"),
        ("transaction", "transaction_event_count"),
        ("addtocart", "addtocart_event_count"),
    ):
        counts = (
            valid.loc[valid["event"].eq(event_value)]
            .groupby("visitorid")
            .size()
            .rename(column_name)
        )
        users = users.join(counts, how="left")
        users[column_name] = users[column_name].fillna(0).astype("int64")
    users["lifecycle_ms"] = (
        users["last_timestamp_ms"] - users["first_timestamp_ms"]
    )
    users = users.reset_index()

    metric_columns = (
        "total_behavior_length",
        "view_length",
        "distinct_item_count",
        "active_day_count",
        "lifecycle_ms",
        "transaction_event_count",
        "addtocart_event_count",
    )
    distribution = pd.concat(
        [distribution_summary(users[column], column) for column in metric_columns],
        ignore_index=True,
    )

    total_users = len(users)
    total_events = int(users["total_behavior_length"].sum())
    bucket_rows: list[dict[str, int | float | str]] = []
    assigned_users = 0
    assigned_events = 0
    for lower, upper, label in SEQUENCE_BUCKETS:
        mask = users["total_behavior_length"].ge(lower)
        if upper is not None:
            mask &= users["total_behavior_length"].le(upper)
        bucket_users = int(mask.sum())
        bucket_events = int(users.loc[mask, "total_behavior_length"].sum())
        assigned_users += bucket_users
        assigned_events += bucket_events
        bucket_rows.append(
            {
                "sequence_length_bucket": label,
                "user_count": bucket_users,
                "user_share": bucket_users / total_users if total_users else 0.0,
                "behavior_count": bucket_events,
                "behavior_contribution_share": (
                    bucket_events / total_events if total_events else 0.0
                ),
            }
        )
    if assigned_users != total_users or assigned_events != total_events:
        raise AssertionError("sequence buckets do not reconcile to user/event totals")

    return (
        EvidenceTable(
            evidence_id="C01_USER_SEQUENCE_METRICS",
            title="Per-user sequence metrics",
            population="users with valid user, item, event, and timestamp",
            denominator="one row per eligible user",
            frame=users,
        ),
        EvidenceTable(
            evidence_id="C02_USER_SEQUENCE_DISTRIBUTION",
            title="User-sequence distribution summary",
            population="users with valid user, item, event, and timestamp",
            denominator="eligible users for each metric; linear percentile interpolation",
            frame=distribution,
        ),
        EvidenceTable(
            evidence_id="C03_USER_SEQUENCE_BUCKETS",
            title="User sequence-length buckets",
            population="users with valid user, item, event, and timestamp",
            denominator="eligible users and their valid events",
            frame=pd.DataFrame(bucket_rows),
        ),
    )


def gini(values: pd.Series | np.ndarray) -> float:
    """Compute the Gini coefficient for non-negative values including zeros."""

    array = np.asarray(values, dtype=float)
    if array.size == 0:
        return math.nan
    if np.isnan(array).any() or (array < 0).any():
        raise ValueError("Gini values must be finite, non-negative numbers")
    total = float(array.sum())
    if total == 0:
        return 0.0
    ordered = np.sort(array)
    ranks = np.arange(1, len(ordered) + 1, dtype=float)
    return float(
        (2.0 * np.dot(ranks, ordered) / (len(ordered) * total))
        - (len(ordered) + 1.0) / len(ordered)
    )


def item_long_tail_tables(
    events: pd.DataFrame,
) -> tuple[EvidenceTable, EvidenceTable]:
    """Build per-item statistics and concentration metrics."""

    valid = _valid_events(events)
    valid["date_utc"] = pd.to_datetime(
        valid["timestamp"], unit="ms", utc=True
    ).dt.floor("D")
    items = valid.groupby("itemid", sort=False).agg(
        total_behavior_count=("event", "size"),
        active_day_count=("date_utc", "nunique"),
        first_timestamp_ms=("timestamp", "min"),
        last_timestamp_ms=("timestamp", "max"),
    )
    for event_value in EXPECTED_EVENTS:
        column = f"{event_value}_event_count"
        counts = (
            valid.loc[valid["event"].eq(event_value)]
            .groupby("itemid")
            .size()
            .rename(column)
        )
        items = items.join(counts, how="left")
        items[column] = items[column].fillna(0).astype("int64")
    items = items.reset_index()

    ordered = items.sort_values(
        ["total_behavior_count", "itemid"], ascending=[False, True]
    )
    total_behavior = int(ordered["total_behavior_count"].sum())
    concentration_rows: list[dict[str, float | int | str]] = []
    for percentage in (0.01, 0.05, 0.10, 0.20):
        top_n = max(1, math.ceil(len(ordered) * percentage)) if len(ordered) else 0
        contribution = int(ordered.head(top_n)["total_behavior_count"].sum())
        concentration_rows.append(
            {
                "top_item_percentage": percentage,
                "rounding_rule": "ceil; ties resolved by ascending itemid",
                "top_item_count": top_n,
                "behavior_count": contribution,
                "behavior_contribution_share": (
                    contribution / total_behavior if total_behavior else 0.0
                ),
            }
        )
    concentration_rows.append(
        {
            "top_item_percentage": math.nan,
            "rounding_rule": "all event-observed items",
            "top_item_count": len(items),
            "behavior_count": total_behavior,
            "behavior_contribution_share": gini(
                items["total_behavior_count"].to_numpy()
            ),
        }
    )
    concentration = pd.DataFrame(concentration_rows)
    concentration["metric"] = [
        "top_contribution",
        "top_contribution",
        "top_contribution",
        "top_contribution",
        "gini_coefficient",
    ]
    return (
        EvidenceTable(
            evidence_id="D01_ITEM_METRICS",
            title="Per-item observed behavior metrics",
            population="event-observed items with valid user, item, event, timestamp",
            denominator="one row per event-observed item",
            frame=items,
        ),
        EvidenceTable(
            evidence_id="D02_ITEM_CONCENTRATION",
            title="Item behavior concentration",
            population="event-observed items",
            denominator="all valid behavior events attached to event-observed items",
            frame=concentration,
        ),
    )


def _split_metrics(
    events: pd.DataFrame, scheme: str, split_labels: pd.Series
) -> pd.DataFrame:
    work = events.copy()
    work["split"] = split_labels.to_numpy()
    train = work.loc[work["split"].eq("train")]
    train_users = set(train["visitorid"].dropna().tolist())
    train_items = set(train["itemid"].dropna().tolist())
    rows: list[dict[str, int | float | str]] = []
    for split_name in ("train", "validation", "test"):
        subset = work.loc[work["split"].eq(split_name)]
        users = set(subset["visitorid"].dropna().tolist())
        items = set(subset["itemid"].dropna().tolist())
        cold_users = users - train_users if split_name != "train" else set()
        cold_items = items - train_items if split_name != "train" else set()
        user_event_counts = subset.groupby("visitorid").size()
        eligible = int((user_event_counts >= 2).sum())
        rows.append(
            {
                "scheme": scheme,
                "split": split_name,
                "event_count": len(subset),
                "user_count": len(users),
                "item_count": len(items),
                "cold_user_share": len(cold_users) / len(users) if users else 0.0,
                "cold_item_share": len(cold_items) / len(items) if items else 0.0,
                "users_with_training_history_share": (
                    len(users & train_users) / len(users) if users else 0.0
                ),
                "items_seen_in_training_share": (
                    len(items & train_items) / len(items) if items else 0.0
                ),
                "next_item_eligible_user_count": eligible,
                "leave_future_out_eligible_user_count": eligible,
            }
        )
    return pd.DataFrame(rows)


def time_split_audit(events: pd.DataFrame) -> EvidenceTable:
    """Compare three strictly chronological candidate split schemes."""

    valid = _valid_events(events).sort_values(
        ["timestamp", "visitorid", "itemid"], kind="mergesort"
    )
    if valid.empty:
        return EvidenceTable(
            evidence_id="F01_TIME_SPLIT_COMPARISON",
            title="Candidate temporal split comparison",
            population="valid event rows",
            denominator="events/users/items in each split",
            frame=pd.DataFrame(),
        )

    row_count = len(valid)
    train_end = math.floor(row_count * 0.70)
    validation_end = math.floor(row_count * 0.85)
    percentage_labels = pd.Series("test", index=valid.index)
    percentage_labels.iloc[:train_end] = "train"
    percentage_labels.iloc[train_end:validation_end] = "validation"

    frames = [
        _split_metrics(
            valid,
            "chronological_70_15_15_by_ordered_rows",
            percentage_labels,
        )
    ]
    max_timestamp = int(valid["timestamp"].max())
    day_ms = 86_400_000
    for days in (7, 14):
        test_start = max_timestamp - days * day_ms
        validation_start = test_start - days * day_ms
        labels = pd.Series("train", index=valid.index)
        labels.loc[valid["timestamp"].ge(validation_start)] = "validation"
        labels.loc[valid["timestamp"].ge(test_start)] = "test"
        frames.append(
            _split_metrics(
                valid,
                f"last_{days}d_test_previous_{days}d_validation",
                labels,
            )
        )
    return EvidenceTable(
        evidence_id="F01_TIME_SPLIT_COMPARISON",
        title="Candidate temporal split comparison",
        population="events with valid user, item, event, and timestamp",
        denominator="events/users/items in each strictly chronological split",
        frame=pd.concat(frames, ignore_index=True),
    )
