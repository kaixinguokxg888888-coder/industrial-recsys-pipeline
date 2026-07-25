"""Item-property, cross-source coverage, and category-tree audit functions."""

from __future__ import annotations

import pandas as pd

from .evidence import EvidenceTable


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
