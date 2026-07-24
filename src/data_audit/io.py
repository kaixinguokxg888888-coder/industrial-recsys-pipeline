"""Read-only CSV validation, parsing, chunking, and input fingerprinting."""

from __future__ import annotations

import csv
import hashlib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterator

import pandas as pd

from .config import AuditConfig, EXPECTED_SCHEMAS, INPUT_FILENAMES


class InputValidationError(ValueError):
    """Raised when an input file violates the declared contract."""


@dataclass(frozen=True)
class InputFingerprint:
    """Identity material recorded before and after a run."""

    path: str
    size_bytes: int
    modified_time_ns: int
    sha256: str | None

    def to_dict(self) -> dict[str, str | int | None]:
        """Return a JSON-serializable representation."""

        return asdict(self)


@dataclass(frozen=True)
class ParsedChunk:
    """A normalized chunk plus parse failures attributable to source values."""

    frame: pd.DataFrame
    invalid_parse_counts: dict[str, int]


def resolve_and_validate_inputs(data_dir: Path) -> dict[str, Path]:
    """Resolve required files and validate exact CSV headers."""

    resolved_dir = data_dir.resolve()
    if not resolved_dir.is_dir():
        raise InputValidationError(f"data directory does not exist: {resolved_dir}")

    paths: dict[str, Path] = {}
    for source_name, filename in INPUT_FILENAMES.items():
        path = resolved_dir / filename
        if not path.is_file():
            raise InputValidationError(f"required input file is missing: {path}")
        if path.is_symlink():
            raise InputValidationError(f"raw input must not be a symbolic link: {path}")

        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            try:
                header = tuple(next(reader))
            except StopIteration as exc:
                raise InputValidationError(f"input file is empty: {path}") from exc

        expected = EXPECTED_SCHEMAS[source_name]
        if header != expected:
            raise InputValidationError(
                f"unexpected header for {path.name}: expected {expected}, observed {header}"
            )
        paths[source_name] = path
    return paths


def fingerprint_file(path: Path, include_sha256: bool) -> InputFingerprint:
    """Fingerprint a file without modifying it."""

    stat = path.stat()
    digest: str | None = None
    if include_sha256:
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(block)
        digest = hasher.hexdigest()
    return InputFingerprint(
        path=str(path.resolve()),
        size_bytes=stat.st_size,
        modified_time_ns=stat.st_mtime_ns,
        sha256=digest,
    )


def collect_fingerprints(
    paths: dict[str, Path], include_sha256: bool
) -> dict[str, InputFingerprint]:
    """Fingerprint every declared input in stable source-name order."""

    return {
        source_name: fingerprint_file(paths[source_name], include_sha256)
        for source_name in INPUT_FILENAMES
    }


def iter_raw_chunks(
    path: Path,
    source_name: str,
    config: AuditConfig,
) -> Iterator[pd.DataFrame]:
    """Yield bounded raw string chunks with explicit columns and dtypes."""

    expected = list(EXPECTED_SCHEMAS[source_name])
    row_limit = None
    if source_name != "category_tree":
        row_limit = config.max_rows_per_file

    reader = pd.read_csv(
        path,
        usecols=expected,
        dtype={column: "string" for column in expected},
        chunksize=config.chunk_size,
        nrows=row_limit,
        keep_default_na=True,
        low_memory=False,
    )
    yield from reader


def _parse_integer_column(
    frame: pd.DataFrame, column: str
) -> tuple[pd.Series, int]:
    source = frame[column]
    numeric = pd.to_numeric(source, errors="coerce")
    fractional = numeric.notna() & numeric.mod(1).ne(0)
    invalid = int(((source.notna() & numeric.isna()) | fractional).sum())
    numeric = numeric.mask(fractional)
    return numeric.astype("Int64"), invalid


def normalize_chunk(raw: pd.DataFrame, source_name: str) -> ParsedChunk:
    """Normalize identifiers/timestamps while retaining parse-failure counts."""

    frame = raw.copy()
    invalid: dict[str, int] = {}

    integer_columns: tuple[str, ...]
    if source_name == "events":
        integer_columns = ("timestamp", "visitorid", "itemid")
    elif source_name.startswith("item_properties"):
        integer_columns = ("timestamp", "itemid")
    else:
        integer_columns = ("categoryid", "parentid")

    for column in integer_columns:
        frame[column], invalid[column] = _parse_integer_column(frame, column)

    # transactionid deliberately remains pandas StringDtype: it is an identifier,
    # not a floating-point measure. Other text fields also preserve source strings.
    return ParsedChunk(frame=frame, invalid_parse_counts=invalid)
