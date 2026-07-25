"""Row-accounting table rendering utilities."""

from __future__ import annotations

from typing import Any

import pandas as pd

def profiles_to_frame(profiles: list[dict[str, Any]]) -> pd.DataFrame:
    """Flatten profiles for a traceable evidence table."""

    rows: list[dict[str, Any]] = []
    for profile in profiles:
        row = {
            key: value
            for key, value in profile.items()
            if key not in {"missing_counts", "missing_rates", "invalid_parse_counts"}
        }
        for column, value in profile["missing_counts"].items():
            row[f"missing_count__{column}"] = value
        for column, value in profile["missing_rates"].items():
            row[f"missing_rate__{column}"] = value
        for column, value in profile["invalid_parse_counts"].items():
            row[f"invalid_parse_count__{column}"] = value
        rows.append(row)
    return pd.DataFrame(rows)
