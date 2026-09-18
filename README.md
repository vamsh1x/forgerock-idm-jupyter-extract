# ForgeRock / PingIDM Jupyter Bulk Extractor

Pull millions of records out of PingIDM / ForgeRock IDM from a Jupyter notebook —
concurrent threads, rich progress bars, then straight into a pandas DataFrame
for analysis.

## How it works

1. **Input CSV** with one identifier per row (e.g. `userName`).
2. `ThreadPoolExecutor` fans out `GET managed/user/<id>?_fields=...` calls —
   connection pooling, automatic retry on 429/5xx, and a cross-thread rate
   limiter so the pool doesn't trip the server throttle.
3. Each requested field becomes a column in the **output CSV** (per-record
   failures are captured in an `_error` column, never fatal).
4. The output CSV loads straight into a **pandas DataFrame** for analysis:
   field completeness, duplicate/fake-profile detection, top values.

## Libraries in play

- **requests** — HTTP session, urllib3 retry adapter
- **urllib** — `urllib.parse` for safe, encoded URL building
- **concurrent.futures** — `ThreadPoolExecutor` multithreading
- **rich** — live progress bars in the notebook
- **pandas** — DataFrame analysis

## Quick start

```bash
pip install -r requirements.txt
cp notebooks/idm_config.example.json notebooks/idm_config.json   # fill in real values
jupyter notebook notebooks/idm_bulk_extract.ipynb
```

Open `notebooks/idm_bulk_extract.ipynb`, set your `base_url`, credentials,
resource, input CSV, and the fields you want — then run all cells.

## Files

- `notebooks/idm_bulk_extract.ipynb` — the full workflow
- `notebooks/idm_config.example.json` — fake config template (never commit the real one)
- `notebooks/input_ids.example.csv` — fake input ids
- `idm_pull/client.py` — thread-safe IDM client (requests + urllib3 retry + rate limit)
- `idm_pull/extract.py` — ThreadPoolExecutor engine, input CSV → output CSV → DataFrame
- `idm_pull/analysis.py` — starter analysis: completeness, duplicates, top values, errors

## Safety

- The `.gitignore` excludes `*_config.json`, `idm_extract.csv`, and real journals.
- Everything here is example data. Use service accounts with least privilege
  and respect your IDM's rate limits.
