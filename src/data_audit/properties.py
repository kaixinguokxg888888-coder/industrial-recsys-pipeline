"""Item-property, cross-source coverage, and category-tree audit functions."""

from __future__ import annotations

import pandas as pd

from .evidence import EvidenceTable


def property_tables(
    properties: pd.DataFrame,
) -> tuple[EvidenceTable, EvidenceTable]:
    """Summarize property coverage, values, temporal change, and multiplicity."""

    valid_items = properties["itemid"].dropna().nunique()
    grouped = properties.groupby("property", dropna=False)
    summary = grouped.agg(
        record_count=("itemid", "size"),
        unique_item_count=("itemid", "nunique"),
        missing_value_count=("value", lambda values: int(values.isna().sum())),
        unique_value_count=("value", "nunique"),
    ).reset_index()
    summary["item_coverage"] = (
        summary["unique_item_count"] / valid_items if valid_items else 0.0
    )
    summary["missing_value_rate"] = (
        summary["missing_value_count"] / summary["record_count"]
    ).fillna(0.0)
    summary["semantic_status"] = summary["property"].map(
        lambda value: (
            "explicit_dataset_property"
            if value in {"categoryid", "available"}
            else "unverified_hashed_property"
        )
    )

    valid = properties.dropna(subset=["itemid", "property"])
    per_pair = (
        valid.groupby(["itemid", "property"], dropna=False)
        .agg(
            record_count=("timestamp", "size"),
            distinct_timestamp_count=("timestamp", "nunique"),
            distinct_value_count=("value", "nunique"),
        )
        .reset_index()
    )
    change_summary = (
        per_pair.groupby("property", dropna=False)
        .agg(
            item_property_pair_count=("itemid", "size"),
            pairs_with_multiple_timestamps=(
                "distinct_timestamp_count",
                lambda values: int((values > 1).sum()),
            ),
            pairs_with_multiple_values=(
                "distinct_value_count",
                lambda values: int((values > 1).sum()),
            ),
        )
        .reset_index()
    )
    return (
        EvidenceTable(
            evidence_id="E01_PROPERTY_PROFILE",
            title="Item-property profile",
            population="property records read for this run",
            denominator="distinct non-missing item identifiers in property records",
            frame=summary,
        ),
        EvidenceTable(
            evidence_id="E02_PROPERTY_CHANGE",
            title="Item-property temporal and multi-value profile",
            population="non-missing item-property pairs",
            denominator="item-property pairs for each property",
            frame=change_summary,
        ),
    )


def cross_source_item_coverage(
    events: pd.DataFrame, properties: pd.DataFrame
) -> EvidenceTable:
    """Measure directional item overlap across events and properties."""

    event_items = set(events["itemid"].dropna().tolist())
    property_items = set(properties["itemid"].dropna().tolist())
    overlap = event_items & property_items
    frame = pd.DataFrame(
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
    return EvidenceTable(
        evidence_id="A03_CROSS_SOURCE_ITEM_COVERAGE",
        title="Directional item coverage across events and properties",
        population="items observed in event/property rows read for this run",
        denominator="event items or property items as named by each share",
        frame=frame,
    )


def category_item_coverage(
    events: pd.DataFrame, properties: pd.DataFrame
) -> EvidenceTable:
    """Measure explicit categoryid-property coverage without semantic inference."""

    event_items = set(events["itemid"].dropna().tolist())
    property_items = set(properties["itemid"].dropna().tolist())
    category_rows = properties.loc[properties["property"].eq("categoryid")]
    category_items = set(category_rows["itemid"].dropna().tolist())
    union_items = event_items | property_items
    frame = pd.DataFrame(
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
    return EvidenceTable(
        evidence_id="A04_ITEM_CATEGORY_COVERAGE",
        title="Explicit categoryid item coverage",
        population="items observed in events or property rows for this run",
        denominator="event, property, or union items as named by each metric",
        frame=frame,
    )


def category_tree_audit(tree: pd.DataFrame) -> tuple[EvidenceTable, EvidenceTable]:
    """Audit roots, missing parents, self-loops, cycles, and depth safely."""

    valid = tree.dropna(subset=["categoryid"]).copy()
    category_ids = set(int(value) for value in valid["categoryid"])
    parent_by_node: dict[int, int | None] = {}
    duplicate_category_rows = int(valid["categoryid"].duplicated(keep=False).sum())
    for row in valid.itertuples(index=False):
        node = int(row.categoryid)
        parent = None if pd.isna(row.parentid) else int(row.parentid)
        parent_by_node.setdefault(node, parent)

    roots = {node for node, parent in parent_by_node.items() if parent is None}
    missing_parent_nodes = {
        node
        for node, parent in parent_by_node.items()
        if parent is not None and parent not in category_ids
    }
    self_loops = {
        node for node, parent in parent_by_node.items() if parent is not None and node == parent
    }

    cycle_nodes: set[int] = set()
    depths: dict[int, int | None] = {}
    for start in parent_by_node:
        path: list[int] = []
        positions: dict[int, int] = {}
        node: int | None = start
        terminal_depth: int | None = None
        while True:
            if node is None:
                terminal_depth = 0
                break
            if node not in parent_by_node:
                terminal_depth = None
                break
            if node in depths:
                terminal_depth = depths[node]
                break
            if node in positions:
                cycle_start = positions[node]
                cycle_nodes.update(path[cycle_start:])
                terminal_depth = None
                break
            positions[node] = len(path)
            path.append(node)
            node = parent_by_node[node]
        for path_node in reversed(path):
            if terminal_depth is None or path_node in cycle_nodes:
                depths[path_node] = None
            else:
                terminal_depth += 1
                depths[path_node] = terminal_depth

    summary = pd.DataFrame(
        [
            {
                "category_row_count": len(tree),
                "distinct_category_count": len(category_ids),
                "duplicate_category_row_count": duplicate_category_rows,
                "root_count": len(roots),
                "missing_parent_node_count": len(missing_parent_nodes),
                "self_loop_count": len(self_loops),
                "cycle_node_count": len(cycle_nodes),
                "max_resolved_depth": max(
                    (depth for depth in depths.values() if depth is not None),
                    default=0,
                ),
            }
        ]
    )
    node_details = pd.DataFrame(
        [
            {
                "categoryid": node,
                "parentid": parent_by_node[node],
                "is_root": node in roots,
                "has_missing_parent": node in missing_parent_nodes,
                "is_self_loop": node in self_loops,
                "is_cycle_node": node in cycle_nodes,
                "resolved_depth": depths.get(node),
            }
            for node in sorted(parent_by_node)
        ]
    )
    return (
        EvidenceTable(
            evidence_id="A05_CATEGORY_TREE_SUMMARY",
            title="Category-tree integrity summary",
            population="category_tree rows with non-missing categoryid",
            denominator="distinct observed category identifiers",
            frame=summary,
        ),
        EvidenceTable(
            evidence_id="A06_CATEGORY_TREE_NODES",
            title="Category-tree node diagnostics",
            population="distinct observed category identifiers",
            denominator="one row per distinct category identifier",
            frame=node_details,
        ),
    )
