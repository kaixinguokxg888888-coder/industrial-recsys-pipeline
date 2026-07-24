"""Stable evidence identifiers and table metadata."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .config import DataScope


@dataclass(frozen=True)
class EvidenceTable:
    """A generated table and the metadata needed to interpret it."""

    evidence_id: str
    title: str
    population: str
    denominator: str
    frame: pd.DataFrame

    def annotated(self, data_scope: DataScope) -> pd.DataFrame:
        """Attach evidence identity and scope to every output row."""

        result = self.frame.copy()
        result.insert(0, "denominator_definition", self.denominator)
        result.insert(0, "population", self.population)
        result.insert(0, "data_scope", data_scope)
        result.insert(0, "evidence_id", self.evidence_id)
        return result
