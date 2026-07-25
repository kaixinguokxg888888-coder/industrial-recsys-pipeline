# industrial-recsys-pipeline

`industrial-recsys-pipeline` is a recommendation-system engineering project based on the public RetailRocket e-commerce dataset. The project begins with a data feasibility audit so that later retrieval, ranking, multitask, reranking, and evaluation choices are supported by actual data rather than assumed fields or fabricated labels.

## Project objectives

- Build a trustworthy and reproducible recommendation pipeline.
- Prevent temporal data leakage in dataset splits, features, labels, and evaluation.
- Establish which recommendation modules the available data genuinely supports.
- Keep every important conclusion traceable to full-data statistics.

## Current stage

The current stage is **partitioned data feasibility audit implementation and validation**. Stable disk partitioning, exact duplicate comparison, partitioned user-item funnel processing, partitioned item-property aggregation, and bounded non-evidentiary smoke runs have been validated. The formal full-data audit, recommendation models, and experiment results have not yet been produced.

## RetailRocket data files

Place the following files locally under `data/raw/`:

- `events.csv`
- `item_properties_part1.csv`
- `item_properties_part2.csv`
- `category_tree.csv`

The raw files are local inputs and are excluded from Git. See `data/README.md` for the expected layout and handling rules.

## Initial project structure

```text
industrial-recsys-pipeline/
├── AGENTS.md
├── README.md
├── requirements.txt
├── data/
│   ├── README.md
│   └── raw/                         # local, read-only, ignored by Git
├── docs/
│   └── specs/
│       └── data_audit_spec.md
├── reports/                         # future auditable outputs
├── src/                             # future implementation
└── tests/                           # future automated tests
```

## Running

The full-data audit entry point is:

```bash
python -m src.data_audit.run_audit --data-dir data/raw --output-dir reports
```

Do not run the full-data command until the implementation's cross-chunk memory and exactness risks have been reviewed.

A bounded pipeline smoke test can be run without producing formal evidence:

```bash
python -m src.data_audit.run_audit \
  --data-dir data/raw \
  --output-dir data/tmp/partitioned_smoke_audit \
  --chunk-size 100000 \
  --partition-count 16 \
  --temp-dir data/tmp/data_audit \
  --max-memory-mb 1024 \
  --max-rows-per-file 100000 \
  --fingerprint-mode metadata
```

Any use of `--max-rows-per-file` marks all generated artifacts as `smoke_non_evidentiary`. Such outputs validate the pipeline only and must not support dataset or recommendation-module conclusions.

## Data Audit Architecture

See [Data Audit Data Flow](docs/architecture/data_audit_dataflow.md) for the implemented chunked reading, stable hash partitioning, exact aggregation, evidence generation, and formal-result publication flow.

## Status

### Completed

- Repository initialization rules and local-data ignore policy.
- Detailed data-audit specification for phases 3–5.
- Modular audit foundation, automated unit tests, and bounded smoke runs.
- Stable Parquet partitioning with exact original-field duplicate comparison.
- Partitioned user-item funnel and item-property temporal aggregation.
- Evidence-gated feature and module feasibility matrix generators.

### In development

- Formal full-data execution, result verification, and evidence-backed matrix publication.

### Planned

- Full-data audit and evidence-backed feasibility matrices.
- Validation of terminology, traceability, reproducibility, and leakage controls.
- Architecture freeze based on verified audit findings.

## Data truthfulness and reproducibility

This project does not invent exposure logs, user profiles, brands, prices, sales volume, CTR, CVR, CTCVR, model performance, or A/B test outcomes. A `view` is treated only as an observed view event, transaction counts are historical transaction-event proxies rather than true sales, and hashed properties receive no unsupported business meaning. Core findings must be computed from the complete source data and be reproducible from documented commands and configurations.
