"""Artifact writing for full audits and explicitly non-evidentiary smoke runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .config import AuditConfig
from .evidence import EvidenceTable


def write_artifacts(
    config: AuditConfig,
    tables: Iterable[EvidenceTable],
    manifest: dict[str, Any],
) -> list[Path]:
    """Write annotated tables, a manifest, and a scope-safe Markdown report."""

    config.output_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = config.output_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    table_index: list[dict[str, str | int]] = []
    for table in tables:
        annotated = table.annotated(config.data_scope)
        output_path = tables_dir / f"{table.evidence_id.lower()}.csv"
        annotated.to_csv(output_path, index=False)
        written.append(output_path)
        table_index.append(
            {
                "evidence_id": table.evidence_id,
                "title": table.title,
                "population": table.population,
                "denominator": table.denominator,
                "row_count": len(table.frame),
                "path": str(output_path.relative_to(config.output_dir)),
            }
        )

    manifest = dict(manifest)
    manifest["data_scope"] = config.data_scope
    manifest["tables"] = table_index
    manifest_path = config.output_dir / "audit_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    written.append(manifest_path)

    report_name = (
        "smoke_audit_report.md" if config.is_smoke else "data_audit_report.md"
    )
    report_path = config.output_dir / report_name
    heading = (
        "# Smoke Audit Report — NON-EVIDENTIARY"
        if config.is_smoke
        else "# Full Data Feasibility Audit Report"
    )
    warning = (
        "\n> This run used per-file row limits. Its statistics are only pipeline "
        "validation outputs and must not support dataset or module conclusions.\n"
        if config.is_smoke
        else ""
    )
    lines = [
        heading,
        warning,
        "## Run scope",
        "",
        f"- Data scope: `{config.data_scope}`",
        f"- Chunk size: `{config.chunk_size}`",
        f"- Maximum rows per non-tree file: `{config.max_rows_per_file}`",
        "",
        "## Generated evidence tables",
        "",
    ]
    for item in table_index:
        lines.append(
            f"- `{item['evidence_id']}` — {item['title']} "
            f"(`{item['path']}`, {item['row_count']} rows)"
        )
    if config.is_smoke:
        lines.extend(
            [
                "",
                "## Prohibited interpretation",
                "",
                "Do not treat any value in this smoke output as a full-data finding, "
                "feasibility decision, model result, CTR, CVR, CTCVR, sales volume, "
                "or A/B test result.",
            ]
        )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    written.append(report_path)
    return written
