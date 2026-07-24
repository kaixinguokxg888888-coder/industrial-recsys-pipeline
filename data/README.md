# Local data layout

RetailRocket source data is not committed to this Git repository. Obtain the dataset separately and place the four files at exactly:

```text
data/raw/events.csv
data/raw/item_properties_part1.csv
data/raw/item_properties_part2.csv
data/raw/category_tree.csv
```

`data/raw/` is permanently read-only. Do not modify, overwrite, move, delete, or commit any raw file.

Generated intermediate data must be written outside `data/raw/`:

- `data/interim/` for reusable intermediate representations;
- `data/processed/` for derived datasets;
- `data/tmp/` for disposable partitions and temporary aggregation data.

These local data directories are ignored by Git. Final, reviewable audit reports and evidence tables belong under `reports/`.
