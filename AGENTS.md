# Repository Development Rules

## Project objective

- Build a trustworthy and reproducible industrial recommendation-system project with strict prevention of data leakage.
- Complete the data feasibility audit and freeze the evidence-supported system architecture before training any recommendation model.

## Data integrity and terminology

- Treat `data/raw/` as permanently read-only. Never modify, overwrite, move, delete, or commit its contents.
- Never fabricate fields, labels, data, model results, performance improvements, or A/B test results.
- Do not call a `view` event a true impression. The dataset does not contain genuine exposure or recommendation-request logs.
- Treat transaction counts only as a proxy for historical transaction-event counts; never call them true sales volume.
- Do not infer business semantics such as price or brand from hashed property formats without reliable evidence.
- Base core conclusions on the complete dataset. Clearly label every sampled analysis and do not substitute it for full-data evidence.

## Time and leakage controls

- Split datasets by time, never by random record-level partitioning.
- Construct every historical feature using only information available strictly before its prediction timestamp.
- Preserve temporal ordering when constructing labels, sequences, candidate sets, features, and evaluation samples.
- Record and validate cutoff timestamps and data availability for every split and feature.

## Engineering standards

- Use type annotations, structured logging, explicit exception handling, and automated tests.
- Make data parsing rules explicit, including selected columns, dtypes, invalid-value handling, and parse-failure counts.
- Keep computations reproducible and record input identity, configuration, runtime, and peak memory where applicable.
- Keep generated evidence traceable: report conclusions and feasibility-matrix entries must point to reproducible full-data tables.
- Update only the relevant README sections incrementally; do not rewrite the whole README without a reason.

## Stage completion and safety

- At the end of each stage, report changed files, commands run, test results, limitations, and unresolved issues.
- Do not run `git push`, delete important files, or modify raw data without explicit user confirmation.
- Do not train recommendation models until the data audit is complete and the project architecture has been frozen.
- Keep this file limited to durable repository rules; do not add chat transcripts or temporary progress notes.
