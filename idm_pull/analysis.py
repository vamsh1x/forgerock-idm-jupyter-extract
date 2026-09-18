"""Starter analysis helpers for the extracted DataFrame.

These are the first-look checks I run on any IDM pull: how complete the
fields are, which values are duplicated, and where the identities might
be fake/dupes. Everything returns DataFrames so you can keep exploring
in the notebook.
"""

from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd


def completeness(df: pd.DataFrame, fields: Optional[Iterable[str]] = None) -> pd.DataFrame:
    """Null-rate per field — spot attributes that are barely populated."""
    cols = list(fields) if fields else [c for c in df.columns if not c.startswith("_")]
    out = pd.DataFrame(
        {
            "field": cols,
            "filled": [int(df[c].notna().sum()) for c in cols],
            "missing": [int(df[c].isna().sum()) for c in cols],
        }
    )
    out["fill_pct"] = (100.0 * out["filled"] / max(len(df), 1)).round(2)
    return out.sort_values("fill_pct").reset_index(drop=True)


def duplicates(df: pd.DataFrame, key: str, n: int = 20) -> pd.DataFrame:
    """Rows sharing the same value in ``key`` — duplicate/fake-profile leads."""
    dup_mask = df.duplicated(subset=[key], keep=False) & df[key].notna()
    dups = df[dup_mask].sort_values(key)
    counts = df[key].value_counts()
    summary = counts[counts > 1].head(n).reset_index()
    summary.columns = [key, "occurrences"]
    return summary, dups


def value_top(df: pd.DataFrame, field: str, n: int = 15) -> pd.DataFrame:
    """Most common values for a field (e.g. top OU, top city)."""
    vc = df[field].value_counts(dropna=True).head(n).reset_index()
    vc.columns = [field, "count"]
    return vc


def errors(df: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    """The fetch failures — usually bad input ids or deleted objects."""
    bad = df[df["_error"].notna() & (df["_error"] != "")]
    return bad[["_id_input", "_error"]].head(n)
