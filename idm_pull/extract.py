"""Bulk extract engine: input CSV -> ThreadPoolExecutor -> output CSV + DataFrame.

Adapted from a production-style ForgeRock IDM extraction workflow:
environment-driven config, a shared retrying ``requests`` session, a
``ThreadPoolExecutor`` fan-out over input ids with ``tqdm`` progress, rich
``Console`` summary tables (execution summary + failure breakdown), and
success/failed/metadata/failure-summary outputs written as both JSON and
CSV. Per-record failures are captured, never fatal.

On top of that baseline this adds: a shared rate limiter, dotted-path
field selection, and the output CSV loaded into a pandas DataFrame for
further analysis in the notebook.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from rich.console import Console
from rich.table import Table
from tqdm import tqdm
from urllib3.util import Retry

log = logging.getLogger(__name__)
console = Console()

ERROR_COLUMN = "_error"
_SENTINEL = object()  # marks "attribute missing entirely"

DEFAULT_FIELDS = [
    "userName", "_id", "givenName", "sn",
    "mail", "employeeNumber", "accountStatus",
]


# ------------------------------------------------------------------ config
def load_config(env_prefix: str = "IDM") -> Dict[str, Any]:
    """Build the run config from environment variables.

    Recognized variables (all optional — sane defaults everywhere else):
        IDM_BASE_URL      e.g. https://idm.example.com:8443/openidm/managed/user
        IDM_USERNAME / IDM_PASSWORD   service-account credentials
        IDM_INPUT_FILE    input CSV path (one id per row, or a column)
        IDM_INPUT_COLUMN  column holding the ids (default: first column)
        IDM_OUTPUT_BASE   base dir for output folders (default: ./idm_output)
        IDM_FIELDS        comma-separated fields to pull
        IDM_MAX_THREADS   ThreadPoolExecutor size (default: 10)
        IDM_VERIFY_SSL    true/false (default: true)
        IDM_RATE_LIMIT    max requests/sec across all threads (default: 20)
    """
    p = env_prefix
    fields = os.getenv(f"{p}_FIELDS")
    return {
        "base_url": os.getenv(f"{p}_BASE_URL", "https://idm.example.com:8443/openidm/managed/user"),
        "username": os.getenv(f"{p}_USERNAME", ""),
        "password": os.getenv(f"{p}_PASSWORD", ""),
        "input_file": os.getenv(f"{p}_INPUT_FILE", "notebooks/input_ids.example.csv"),
        "input_column": os.getenv(f"{p}_INPUT_COLUMN", ""),
        "output_base": os.getenv(f"{p}_OUTPUT_BASE", "./idm_output"),
        "fields": [f.strip() for f in fields.split(",")] if fields else list(DEFAULT_FIELDS),
        "max_threads": int(os.getenv(f"{p}_MAX_THREADS", "10")),
        "verify_ssl": os.getenv(f"{p}_VERIFY_SSL", "true").lower() != "false",
        "rate_limit_per_second": float(os.getenv(f"{p}_RATE_LIMIT", "20")),
        "request_timeout": float(os.getenv(f"{p}_TIMEOUT", "15")),
    }


def validate_config(cfg: Dict[str, Any]) -> None:
    """Fail fast when credentials are missing."""
    missing = [k for k in ("username", "password") if not cfg.get(k)]
    if missing:
        raise RuntimeError(
            f"Missing credentials in config: {missing}. "
            f"Set IDM_USERNAME and IDM_PASSWORD environment variables."
        )


# ----------------------------------------------------------------- session
def create_session(cfg: Dict[str, Any]) -> requests.Session:
    """Shared session: basic auth, JSON headers, retry on 429/5xx."""
    session = requests.Session()
    session.auth = (cfg["username"], cfg["password"])
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    retries = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class _RateLimiter:
    """Cross-thread token pacing: max N requests/sec overall."""

    def __init__(self, per_second: float) -> None:
        self._interval = 1.0 / max(per_second, 0.01)
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = self._next - now
            if delay > 0:
                time.sleep(delay)
            self._next = max(now, self._next) + self._interval


# ------------------------------------------------------------------ fetch
def read_input_ids(path: str, id_column: str = "") -> List[str]:
    """Read ids from an input CSV — named column, or first column."""
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if id_column and id_column not in (reader.fieldnames or []):
            raise ValueError(f"No column {id_column!r} in {path}; found {reader.fieldnames}")
        col = id_column or (reader.fieldnames or [""])[0]
        return [row[col].strip() for row in reader if row.get(col)]


def _pick(obj: dict, field: str):
    """Support dotted paths like ``address.city``; missing -> _SENTINEL."""
    cur = obj
    for part in field.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return _SENTINEL
    return cur


def fetch_record(session: requests.Session, limiter: _RateLimiter,
                 base_url: str, value: str, fields: List[str],
                 timeout: float, verify_ssl: bool):
    """GET one user by ``_queryFilter``; returns (status, record)."""
    limiter.wait()
    try:
        params = {
            "_queryFilter": f'userName eq "{value}"',
            "_fields": ",".join(fields),
        }
        url = base_url.rstrip("/") + "?" + urllib.parse.urlencode(params)
        response = session.get(url, timeout=timeout, verify=verify_ssl)
        response.raise_for_status()

        results = response.json().get("result", [])
        if not results:
            return "failed", {"lookup_value": value, "reason": "NOT_FOUND"}

        user = results[0]
        record = {field: user.get(field) for field in fields}
        record["lookup_value"] = value
        return "success", record

    except requests.exceptions.Timeout:
        return "failed", {"lookup_value": value, "reason": "TIMEOUT"}
    except requests.exceptions.ConnectionError:
        return "failed", {"lookup_value": value, "reason": "CONNECTION_ERROR"}
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        return "failed", {"lookup_value": value, "reason": f"HTTP_{status}"}
    except Exception as exc:  # never let one record kill the run
        return "failed", {"lookup_value": value, "reason": str(exc)[:200]}


# ----------------------------------------------------------------- outputs
def _write_json(folder: str, name: str, data) -> None:
    with open(os.path.join(folder, f"{name}.json"), "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, default=str)


def _write_csv(folder: str, name: str, data: List[dict]) -> None:
    if not data:
        return
    with open(os.path.join(folder, f"{name}.csv"), "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)


def print_summary(metadata: Dict[str, Any], failure_summary: Dict[str, int],
                  output_folder: str) -> None:
    """Rich execution-summary and failure-breakdown tables."""
    summary = Table(title="Execution Summary")
    summary.add_column("Metric", style="cyan")
    summary.add_column("Value", style="green")
    summary.add_row("Input Records", str(metadata["total_records"]))
    summary.add_row("Success", str(metadata["success"]))
    summary.add_row("Failed", str(metadata["failed"]))
    summary.add_row("Duration", f"{metadata['duration_seconds']} sec")
    summary.add_row("Throughput", f"{metadata['records_per_second']} rec/sec")
    summary.add_row("Threads", str(metadata["threads"]))
    summary.add_row("Output Folder", output_folder)

    console.print()
    console.print(summary)

    if failure_summary:
        failure_table = Table(title="Failure Breakdown")
        failure_table.add_column("Reason")
        failure_table.add_column("Count")
        for reason, count in sorted(failure_summary.items()):
            failure_table.add_row(reason, str(count))
        console.print()
        console.print(failure_table)


# -------------------------------------------------------------------- run
def run_extraction(cfg: Optional[Dict[str, Any]] = None,
                   session: Optional[requests.Session] = None) -> pd.DataFrame:
    """Full pipeline: load ids -> ThreadPoolExecutor pull -> outputs -> DataFrame."""
    cfg = cfg or load_config()
    validate_config(cfg)

    input_file = cfg["input_file"]
    output_folder = os.path.join(
        cfg["output_base"],
        os.path.splitext(os.path.basename(input_file))[0],
    )
    os.makedirs(output_folder, exist_ok=True)

    console.print("\n[bold cyan]Loading Input File...[/bold cyan]")
    values = read_input_ids(input_file, cfg["input_column"])
    console.print(f"[green]Loaded {len(values)} records[/green]")
    log.info("Loaded %d records from %s", len(values), input_file)

    own_session = session is None
    session = session or create_session(cfg)
    limiter = _RateLimiter(cfg["rate_limit_per_second"])
    fields = cfg["fields"]

    success, failed = [], []
    start_time = time.time()
    try:
        console.print("\n[bold cyan]Querying IDM...[/bold cyan]")
        with ThreadPoolExecutor(max_workers=cfg["max_threads"]) as executor:
            futures = [
                executor.submit(fetch_record, session, limiter, cfg["base_url"],
                                value, fields, cfg["request_timeout"], cfg["verify_ssl"])
                for value in values
            ]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
                status, result = future.result()
                (success if status == "success" else failed).append(result)
    finally:
        if own_session:
            session.close()

    duration = round(time.time() - start_time, 2)
    failure_summary: Dict[str, int] = {}
    for item in failed:
        reason = item.get("reason", "UNKNOWN")
        failure_summary[reason] = failure_summary.get(reason, 0) + 1

    metadata = {
        "total_records": len(values),
        "success": len(success),
        "failed": len(failed),
        "duration_seconds": duration,
        "records_per_second": round(len(values) / duration, 2) if duration else 0,
        "threads": cfg["max_threads"],
        "run_at": datetime.utcnow().isoformat() + "Z",
    }

    console.print("\n[bold cyan]Writing Output Files...[/bold cyan]")
    _write_json(output_folder, "success", success)
    _write_json(output_folder, "failed", failed)
    _write_json(output_folder, "execution_metadata", metadata)
    _write_json(output_folder, "failure_summary", failure_summary)
    _write_csv(output_folder, "success", success)
    _write_csv(output_folder, "failed", failed)

    print_summary(metadata, failure_summary, output_folder)
    log.info("Execution completed successfully")

    # The success CSV doubles as the analysis-ready DataFrame.
    success_csv = os.path.join(output_folder, "success.csv")
    return pd.read_csv(success_csv) if success else pd.DataFrame()
