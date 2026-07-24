# industrial-recsys-pipeline

`industrial-recsys-pipeline` is a recommendation-system engineering project based on the public RetailRocket e-commerce dataset. The project begins with a data feasibility audit so that later retrieval, ranking, multitask, reranking, and evaluation choices are supported by actual data rather than assumed fields or fabricated labels.

## Project objectives

- Build a trustworthy and reproducible recommendation pipeline.
- Prevent temporal data leakage in dataset splits, features, labels, and evaluation.
- Establish which recommendation modules the available data genuinely supports.
- Keep every important conclusion traceable to full-data statistics.

## Current stage

The current stage is **data feasibility audit implementation and validation**. The modular audit entry point and a bounded, explicitly non-evidentiary smoke run have been validated. The full-data audit, recommendation models, and experiment results have not yet been produced.

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
  --output-dir data/tmp/smoke_audit \
  --chunk-size 5000 \
  --max-rows-per-file 25000 \
  --fingerprint-mode metadata
```

Any use of `--max-rows-per-file` marks all generated artifacts as `smoke_non_evidentiary`. Such outputs validate the pipeline only and must not support dataset or recommendation-module conclusions.

## Status

### Completed

- Repository initialization rules and local-data ignore policy.
- Detailed data-audit specification for phases 3–5.
- Modular audit foundation, automated unit tests, and a bounded smoke run.

### In development

- Scalable full-data aggregation, exact cross-chunk reconciliation, and feasibility-matrix generation.

### Planned

- Full-data audit and evidence-backed feasibility matrices.
- Validation of terminology, traceability, reproducibility, and leakage controls.
- Architecture freeze based on verified audit findings.

## Data truthfulness and reproducibility

This project does not invent exposure logs, user profiles, brands, prices, sales volume, CTR, CVR, CTCVR, model performance, or A/B test outcomes. A `view` is treated only as an observed view event, transaction counts are historical transaction-event proxies rather than true sales, and hashed properties receive no unsupported business meaning. Core findings must be computed from the complete source data and be reproducible from documented commands and configurations.
