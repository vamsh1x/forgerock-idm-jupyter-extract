# ForgeRock / PingIDM Jupyter Toolkit

Jupyter notebooks + a small Python library for pulling and analyzing
identity data at production scale: ForgeRock / PingIDM extraction,
Twilio OTP cost analysis, and Splunk log analysis for root cause.

## Notebooks

- `notebooks/idm_bulk_extract.ipynb` — Bulk-pull millions of IDM records: input CSV of ids to ThreadPoolExecutor GETs (requests + urllib.parse + urllib3 retries + cross-thread rate limiter), rich execution-summary / failure-breakdown tables, success/failed/metadata outputs (JSON + CSV), then a pandas DataFrame for identity-resolution analysis (duplicates, fake profiles).
- `notebooks/twilio_otp_analysis.ipynb` — Pull Twilio SMS logs via the REST API (paginated), then analyze OTP spend: cost by day, burst detection (same number, 3+ OTPs in 5 min = the classic AM-tree re-entry bug), delivery health, hourly patterns.
- `notebooks/splunk_data_analysis.ipynb` — Run SPL through the Splunk Python SDK, load results into pandas, and root-cause: top failure reasons, most-affected users, per-hour spikes; reusable patterns for recon failures, slow IDM queries, OTP storms.

## Libraries in play

- **requests** — HTTP sessions, retry adapters
- **urllib** — `urllib.parse` for safe, encoded URL building and pagination
- **concurrent.futures** — `ThreadPoolExecutor` multithreading
- **rich** — progress output and summary tables
- **tqdm** — progress bars over thread pools
- **pandas** — DataFrame analysis
- **twilio** / **splunk-sdk** — vendor APIs for the OTP and log notebooks

## Quick start

```bash
pip install -r requirements.txt
# credentials come from environment variables — never hardcode them
export IDM_USERNAME=... IDM_PASSWORD=...
export TWILIO_ACCOUNT_SID=... TWILIO_AUTH_TOKEN=...
export SPLUNK_HOST=... SPLUNK_USERNAME=... SPLUNK_PASSWORD=...
jupyter notebook notebooks/
```

## Files

- `notebooks/idm_bulk_extract.ipynb` — concurrent IDM extraction + duplicate analysis
- `notebooks/twilio_otp_analysis.ipynb` — OTP cost + burst/root-cause analysis
- `notebooks/splunk_data_analysis.ipynb` — Splunk SDK log analysis for root cause
- `notebooks/idm_config.example.json` — fake config template (never commit the real one)
- `notebooks/input_ids.example.csv` — fake input ids
- `idm_pull/client.py` — thread-safe IDM client (requests + urllib3 retry + rate limit)
- `idm_pull/extract.py` — env-driven ThreadPoolExecutor pipeline: fetch, JSON/CSV outputs, DataFrame
- `idm_pull/analysis.py` — starter analysis: completeness, duplicates, top values, errors

## Safety

- The `.gitignore` excludes `*_config.json`, `idm_extract.csv`, `.env` files, and output folders.
- Everything here is example data. Use service accounts with least privilege
  and respect your IDM's rate limits.
