"""Bulk extract engine: input CSV -> ThreadPoolExecutor -> output CSV + DataFrame.

Workflow
--------
1. Read the *input* CSV — one identifier per row (e.g. a ``userName`` column).
2. Fan out ``client.read`` calls across a ``ThreadPoolExecutor`` with a
   ``rich`` progress bar.
3. Flatten each object's requested fields into one row of the *output* CSV.
4. Load the output CSV into a pandas DataFrame for further analysis.

Failures never kill the run: per-record errors are recorded in an
``_error`` column and summarized at the end.
"""

from __future__ import annotations

import csv
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional

import pandas as pd
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from .client import IdmClient

log = logging.getLogger(__name__)

ERROR_COLUMN = "_error"
SENTINEL = object()  # marks "attribute missing entirely"


def read_input_ids(path: str, id_column: str) -> List[str]:
    """Read one identifier per row from the input CSV."""
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if id_column not in (reader.fieldnames or []):
            raise ValueError(
                f"Input CSV {path!r} has no column {id_column!r}; "
                f"found: {reader.fieldnames}"
            )
        return [row[id_column].strip() for row in reader if row.get(id_column)]


def _pick(obj: dict, field: str):
    """Support dotted paths like ``address.city``; missing -> SENTINEL."""
    cur = obj
    for part in field.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return SENTINEL
    return cur


def fetch_one(client: IdmClient, resource: str, object_id: str, fields: List[str]) -> Dict[str, object]:
    """Fetch one object and flatten the requested fields into a row dict."""
    row: Dict[str, object] = {"_id_input": object_id, ERROR_COLUMN: ""}
    try:
        obj = client.read(resource, object_id, fields=fields)
        for field in fields:
            value = _pick(obj, field)
            row[field] = None if value is SENTINEL else value
    except Exception as exc:  # per-record failure, never fatal
        row[ERROR_COLUMN] = f"{type(exc).__name__}: {exc}"[:300]
        for field in fields:
            row[field] = None
    return row


def extract(
    client: IdmClient,
    resource: str,
    input_ids: Iterable[str],
    fields: List[str],
    output_csv: str,
    max_workers: int = 10,
    show_progress: bool = True,
) -> "pd.DataFrame":
    """Pull every id concurrently, write the output CSV, return the DataFrame.

    Parameters
    ----------
    input_ids:
        Iterable of object ids to fetch (from ``read_input_ids``).
    fields:
        Attribute names to pull; each becomes a column in the CSV.
    max_workers:
        ThreadPoolExecutor size. 8-16 is the sweet spot for IDM behind
        most load balancers; the client's rate limiter caps the real RPS.
    """
    ids = list(input_ids)
    columns = ["_id_input", ERROR_COLUMN, *fields]
    stats = {"ok": 0, "failed": 0}
    lock = threading.Lock()

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]Pulling from IDM"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        transient=not show_progress,
    )

    with progress, open(output_csv, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        task = progress.add_task("fetch", total=len(ids))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(fetch_one, client, resource, oid, fields): oid
                for oid in ids
            }
            for future in as_completed(futures):
                row = future.result()
                writer.writerow({k: row.get(k, "") for k in columns})
                with lock:
                    if row[ERROR_COLUMN]:
                        stats["failed"] += 1
                    else:
                        stats["ok"] += 1
                progress.advance(task)

    log.info("Extract complete: %d ok, %d failed -> %s", stats["ok"], stats["failed"], output_csv)
    df = pd.read_csv(output_csv)
    return df
