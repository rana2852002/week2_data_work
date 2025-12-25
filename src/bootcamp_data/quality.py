from __future__ import annotations

import pandas as pd


def require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = sorted(set(required) - set(df.columns))
    assert len(missing) == 0, f"Required columns missing: {missing}"


def assert_non_empty(df: pd.DataFrame, name: str = "df") -> None:
    assert df.shape[0] > 0, f"{name} is empty (0 rows)"


def assert_unique_key(df: pd.DataFrame, key: str, *, allow_na: bool = False) -> None:
    assert key in df.columns, f"Key column not found: {key}"

    s = df[key]

    if not allow_na:
        assert s.notna().all(), f"{key} contains missing values"

    dup_mask = s.notna() & s.duplicated(keep=False)
    n_dup = int(dup_mask.sum())
    assert n_dup == 0, f"{key} must be unique; found {n_dup} duplicate rows"


def assert_in_range(s: pd.Series, lo=None, hi=None, name: str = "value") -> None:
    x = s.dropna()

    if lo is not None:
        bad_lo = x < lo
        assert not bad_lo.any(), f"{name} has values below {lo}"

    if hi is not None:
        bad_hi = x > hi
        assert not bad_hi.any(), f"{name} has values above {hi}"
