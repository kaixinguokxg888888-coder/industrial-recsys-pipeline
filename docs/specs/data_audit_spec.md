# Data Feasibility Audit Specification

## 1. Purpose, authority, and stage boundaries

This document is the single detailed task specification for phases 3, 4, and 5 of `industrial-recsys-pipeline`.

- **Phase 3 — implement and run:** implement the modular audit, pass a smoke test, run it against the complete dataset, and generate reports and feasibility matrices.
- **Phase 4 — verify:** validate code, outputs, terminology, evidence traceability, reproducibility, and temporal leakage controls.
- **Phase 5 — conclude and freeze:** report evidence-backed findings and freeze the recommendation-system architecture into supported implementation, constrained experiments, and design-only extensions.

This specification defines work to be performed later. It contains no audit result and authorizes no model training.

## 2. Non-negotiable data and evidence rules

1. `data/raw/` is read-only. Record input file identity before and after execution and fail validation if a raw input changes.
2. Core findings must use all valid records in the complete source files. Any sample must be explicitly labeled with its purpose and must not support a core conclusion.
3. A `view` is an observed view event, not a verified impression. Transaction counts are historical transaction-event-count proxies, not true sales.
4. Do not infer price, brand, or other business meaning solely from hashed property formats.
5. Do not fabricate exposure, request, user-profile, label, feature, result, CTR, CVR, CTCVR, or A/B-test data.
6. All splits are time-based. Features, histories, labels, and candidate evidence must respect their prediction cutoffs.
7. Every feasibility conclusion must cite a reproducible evidence table generated from the full audit.

## 3. Inputs, execution contract, and implementation constraints

### 3.1 Required inputs

| File | Required columns |
| --- | --- |
| `data/raw/events.csv` | `timestamp`, `visitorid`, `event`, `itemid`, `transactionid` |
| `data/raw/item_properties_part1.csv` | `timestamp`, `itemid`, `property`, `value` |
| `data/raw/item_properties_part2.csv` | `timestamp`, `itemid`, `property`, `value` |
| `data/raw/category_tree.csv` | `categoryid`, `parentid` |

The implementation must validate actual headers rather than silently assuming them.

### 3.2 Planned command

```bash
python -m src.data_audit.run_audit --data-dir data/raw --output-dir reports
```

The command must run from the repository root, reject missing or malformed inputs with actionable errors, and avoid writing inside `data/raw/`.

### 3.3 Processing constraints

- Read `events.csv` and both item-property files in bounded chunks with explicit `usecols` and dtypes.
- Load `category_tree.csv` in full unless measured constraints require otherwise.
- Count invalid parses and missing values explicitly.
- Preserve `transactionid` as an identifier; do not silently normalize it through ordinary floating-point representation.
- Use deterministic stable-hash partitioning and temporary Parquet only when a cross-chunk exact aggregation requires it.
- Record configuration, input identity, row accounting, elapsed time, and peak memory.
- Make reruns deterministic for identical inputs and configuration.

### 3.4 Global acceptance criteria

- Input row accounting reconciles valid, missing, invalid, and rejected records without unexplained loss.
- All core tables declare whether they use full data and include a stable evidence identifier.
- Every report number cited as evidence maps to a generated table, metric name, population, and computation configuration.
- No raw input is created, modified, moved, overwritten, or deleted.

## 4. A — Data quality audit

### Required work

- For every input, compute row and column counts, observed fields, parsed types, missing counts and rates, duplicate counts, timestamp range, and illegal-parse counts.
- Compute distinct users, items, and categories with clearly stated populations.
- Measure item overlap and directional coverage between `events` and the combined item-property data.
- Measure item-to-category coverage.
- For `category_tree`, identify root nodes, missing-parent/orphan nodes, self-loops, longer cycles, and nodes participating in cycles.

### Required evidence

- Per-file profile table.
- Entity-count table with population definitions.
- Cross-source item-coverage table.
- Category-coverage and tree-integrity tables.

### Acceptance criteria

- The two item-property parts are audited separately and jointly without double-counting unexplained duplicates.
- Coverage denominators are explicit.
- Timestamp parse failures and invalid identifiers are reported rather than silently dropped.
- Tree traversal terminates safely even if cycles exist.

## 5. B — Behavior and funnel audit

### Required work

- Count and proportion `view`, `addtocart`, `transaction`, and any unexpected event values; report time trends at documented granularity.
- Compute user behavior-type combinations.
- Audit temporal orders for the same `(visitorid, itemid)` pair.
- For each transaction event, determine whether a prior view and/or add-to-cart exists for that user-item pair under explicit same-history and cutoff rules.
- Characterize prior-view-to-transaction time intervals without using future events.
- Classify candidate labels as direct facts, reasonable proxy labels, or not reliably constructible.
- Decide whether exposure, click, add-to-cart, conversion, CTR, CVR, and CTCVR are constructible from this dataset.

### Acceptance criteria

- “Prior” always means a strictly earlier timestamp; tie handling is documented.
- Funnels are not presented as impression-based funnels unless genuine impression denominators exist.
- Event ratios are distinguished from CTR/CVR definitions.
- Each label decision cites full-data evidence and states its denominator and limitations.

## 6. C — User-sequence audit

### Required work

For each user compute:

- total behavior length;
- view-event length;
- distinct-item count;
- active-day count;
- lifecycle from first to last observed event;
- transaction-event count;
- add-to-cart-event count.

For each metric output `mean`, `std`, `min`, `P25`, `P50`, `P75`, `P90`, `P95`, `P99`, and `max`.

Bucket users by total behavior length:

- 1;
- 2–4;
- 5–9;
- 10–19;
- 20–49;
- 50–99;
- 100 or more.

For every bucket output user count, user share, behavior count, and behavior-contribution share.

### Feasibility decisions

Use the distributions to assess DIN, SDM, and two-tower support for short-sequence and active-user populations. Quantify how removing cold or short-history users would change population and event coverage, and identify resulting sample-selection bias.

### Acceptance criteria

- Bucket bounds are exhaustive, mutually exclusive, and reconcile to the full user and event populations.
- Percentiles use one documented convention.
- Model-support decisions cite sequence tables and do not generalize an active-user subset to all users.

## 7. D — Item and long-tail audit

### Required work

- Per item, compute view, add-to-cart, transaction, and total event counts; active days; and first/last appearance.
- Compute behavior contribution of the top 1%, 5%, 10%, and 20% of items under a documented tie and rounding rule.
- Compute a Gini coefficient with the formula, population, zero handling, and validation tests documented.
- Compute the share of items with no transaction events and the shares under documented low-frequency thresholds.
- Define and quantify new-item cohorts relative to candidate temporal cutoffs.
- Assess support for popularity retrieval, ItemCF, Swing, popularity features, item cold start, and MMR.

### Acceptance criteria

- Long-tail shares declare whether the item universe is event-observed, property-observed, or their union.
- New-item status is cutoff-relative and never uses future first-seen information.
- Each module assessment cites full-data long-tail evidence.

## 8. E — Item-property and category audit

### Required work

- Per property, compute record count, item coverage, missing count/rate, and value cardinality.
- Audit temporal changes and multiple values for the same item-property pair.
- Audit the special property names `categoryid` and `available` using observed records.
- Determine whether price, brand, or other semantic fields are reliably identifiable, using evidence rather than hash appearance.
- Compute category-tree depth with documented handling for roots, missing parents, and cycles, and measure item-category coverage.

### Acceptance criteria

- Property coverage denominators are explicit.
- Current/as-of property values for later features are resolved using only records available at the relevant cutoff.
- Unverified hashed values remain semantically unknown.
- Any semantic identification includes a reproducible validation rule and confidence/limitations.

## 9. F — Time-split audit

### Candidate schemes

1. Chronological 70% train, next 15% validation, final 15% test.
2. Final 7 days test, preceding 7 days validation, all earlier data train.
3. Final 14 days test, preceding 14 days validation, all earlier data train.

For each scheme compute by split:

- event count;
- user count;
- item count;
- cold-user share;
- cold-item share;
- share of test users with training history;
- share of test items observed in training;
- number of users eligible for next-item evaluation;
- number of users eligible for leave-future-out evaluation.

### Required recommendation

Recommend one scheme using full-data evidence. Explain temporal coverage, sample sizes, cold-start effects, seasonality limitations, evaluation eligibility, and operational interpretability.

### Acceptance criteria

- Split boundaries are exact, non-overlapping, ordered, and documented in timestamps.
- Validation information does not influence training features; test information influences neither training nor validation construction.
- Cold entities are defined relative to prior splits only.
- Eligibility rules for next-item and leave-future-out are explicit and reproducible.

## 10. G — Feature feasibility matrix

Classify user, item, context, and cross features into:

- directly usable;
- reasonably constructible from prior logs;
- proxy only;
- not reliably constructible.

Each row must contain:

| Field | Meaning |
| --- | --- |
| `feature` | Unique feature name |
| `feature_group` | User, item, context, or cross |
| `classification` | One of the four allowed classes |
| `raw_fields` | Source files and fields |
| `construction` | Reproducible as-of construction |
| `coverage` | Full-data coverage value and denominator |
| `evidence` | Evidence-table identifier and metric |
| `leakage_risk` | Temporal or label-leakage risk |
| `limitations` | Semantic and population limits |

### Acceptance criteria

- Every row has a source or explicitly states that no source exists.
- Historical aggregations specify prediction cutoff, window, and tie handling.
- Coverage values trace to full-data evidence.
- Unsupported price, brand, profile, exposure, and request-context features are not promoted to usable features.

## 11. H — Recommendation-system module feasibility matrix

### Mandatory modules

- Popularity retrieval
- Category retrieval
- ItemCF
- Swing
- Standard two-tower
- SDM
- DIN
- DeepFM
- LightGBM coarse ranking
- Single-task CTR ranking
- Single-task purchase-propensity ranking
- MMoE
- PLE
- CTR+CVR
- CTR+CVR+CTCVR
- ESMM
- ESCM2
- MMR
- User cold start
- Item cold start
- Strict temporal split
- GAUC
- NDCG@K
- Recall@K
- MAP@K
- HitRate@K
- Coverage
- Intra-List Diversity
- Offline simulated A/B test
- Real online A/B test

### Required schema

```text
module
required_data
dataset_support
evidence
feasible_scope
limitations
leakage_risk
recommended_action
confidence
notes
```

`dataset_support` must be exactly one of:

```text
fully_supported
partially_supported
weakly_supported
unsupported
```

`recommended_action` must be exactly one of:

```text
implement
implement_with_constraints
active_user_experiment_only
synthetic_pipeline_test_only
design_only
remove
```

### Acceptance criteria

- Every mandatory module appears exactly once.
- Enum fields contain only allowed values.
- Every row cites actual full-data statistics through evidence-table identifiers; qualitative-only evidence is invalid.
- `required_data` is compared with actual available fields before support is assigned.
- Offline evaluation is not described as a real A/B test, and a real online A/B test is not claimed without an online serving experiment.

## 12. I — Phase 3 implementation and outputs

### Planned outputs

```text
src/data_audit/
tests/test_data_audit.py
reports/data_audit_report.md
reports/data_feasibility_matrix.csv
reports/data_feasibility_matrix.md
reports/feature_feasibility_matrix.csv
reports/tables/
reports/figures/
```

### Execution order

1. Implement modular readers, validators, aggregations, evidence tables, matrices, and report rendering.
2. Run a bounded smoke test that is explicitly marked non-evidentiary.
3. Fix defects and rerun automated tests and the smoke test.
4. Run the audit on all source records.
5. Generate and reconcile all reports, tables, figures, and matrices.

### Phase 3 acceptance criteria

- The planned root command completes successfully on the full dataset.
- Automated tests and smoke tests pass before the full run.
- Required outputs exist and contain no placeholder evidence.
- Full-data row accounting, evidence references, runtime, peak memory, input identity, and configuration are recorded.
- No recommendation model is trained.

## 13. J — Phase 4 verification

Perform and record:

- automated test execution;
- proof that all core statistics use complete data;
- traceability of matrix evidence to generated tables;
- terminology scan confirming `view` is not called an impression;
- terminology scan confirming transaction counts are not called true sales;
- semantic scan confirming hashed properties are not assigned unsupported meanings;
- cutoff and split checks for all temporal features, labels, histories, and evaluations;
- before/after raw-input identity comparison;
- required-output completeness checks;
- clean rerun from the repository root using documented inputs and command.

### Phase 4 acceptance criteria

- All mandatory automated and reconciliation checks pass, or every failure is documented and blocks architecture freeze.
- Evidence references resolve to an existing table and metric.
- No future information is used in a historical feature or earlier split.
- Raw-data identity is unchanged.
- Repeated execution with identical inputs/configuration produces equivalent core tables, with any non-deterministic metadata excluded and documented.

## 14. K — Phase 5 report and architecture freeze

The final report must include:

1. added and modified files;
2. commands actually executed;
3. test and validation results;
4. the ten most important verified data findings;
5. module-feasibility conclusions;
6. the recommended genuinely implementable main path;
7. modules suitable only for active-user experiments;
8. modules suitable only for industrial extension design;
9. modules recommended for removal;
10. whether RetailRocket should remain the primary dataset;
11. unresolved questions.

Freeze the proposed architecture into:

- **A. Main path genuinely implementable with the current dataset**
- **B. Experiments limited to an active-user subset**
- **C. Modules included only as industrialization extension designs**

### Phase 5 acceptance criteria

- Every finding and architecture decision cites verified phase 3 evidence and passed phase 4 checks.
- Planned, synthetic, offline, and real-online capabilities are clearly distinguished.
- Limitations and unresolved questions are preserved rather than converted into unsupported claims.
- Architecture freeze occurs before any recommendation-model training.

## 15. Evidence traceability and completion checklist

Each generated evidence table must include or be accompanied by:

- stable evidence identifier;
- source population and full-data/sample flag;
- input identity and audit configuration;
- metric definitions and denominators;
- temporal cutoff where applicable;
- generation module or command.

The audit is complete only when:

- sections A–H have generated evidence-backed outputs;
- all phase 3 files are present and reconciled;
- all phase 4 verification checks pass or blocking exceptions are documented;
- the phase 5 report assigns every architecture component to A, B, or C;
- raw data remains unchanged;
- no unsupported experiment or business claim is present.
