# Birdeye OHLCV Backfill

Backfills hourly Solana token OHLCV data from Birdeye into BigQuery for the
addresses in `tokens.json`. Existing rows are skipped by checking
`(token_address, price_timestamp, chain)` before appending.

The credentials used to run the script must be able to read and append to the
target table. For the duplicate check, grant at least BigQuery Data Viewer on
the dataset or table. For loading rows, grant BigQuery Data Editor or another
role with append permissions.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Authenticate BigQuery with Application Default Credentials. `gcloud auth login`
logs in the CLI, but Python client libraries use ADC:

```powershell
gcloud auth application-default login
gcloud auth application-default set-quota-project crypto-trading-474111
```

Or use a service account key:

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\service-account.json"
```

## Run

```powershell
$env:BIRDEYE_API_KEY="your-birdeye-key"
$env:BIGQUERY_TABLE="crypto-trading-474111.core.token_ohlcv"
$env:BIGQUERY_LOCATION="europe-central2"
$env:CHAIN="sol"
$env:BIRDEYE_CHAIN="solana"
python .\backfill_birdeye_ohlcv.py
```

Defaults:

- Start time: `2026-02-05T00:00:00Z`
- End time: current UTC hour, exclusive
- BigQuery chain value: `sol`
- Birdeye API chain header: `solana`
- Interval: `1H`
- Currency: `usd`
- Birdeye rate limit: `60` requests per minute
- BigQuery location: `europe-central2`

If the Birdeye fetch/parsing loop fails after rows have been collected, the
script writes the pending rows to BigQuery before exiting with a non-zero code.

Useful test run:

```powershell
python .\backfill_birdeye_ohlcv.py --limit-tokens 2 --dry-run
```

Pull new data from `2026-05-20` using token addresses already present in
BigQuery instead of `tokens.json`:

```powershell
python .\backfill_birdeye_ohlcv.py --tokens-from-bigquery --start-date 2026-05-20
```

Pull new data from `2026-05-20` using token addresses from the inference
features view:

```powershell
python .\backfill_birdeye_ohlcv.py `
  --tokens-query 'SELECT DISTINCT token_address FROM `crypto-trading-474111.20m_eval.rl_inference_features_next_open_v`' `
  --start-date 2026-05-20
```

Backfill only gaps exported from the BigQuery gap query:

```powershell
python .\backfill_birdeye_ohlcv.py --gaps-csv "C:\Users\pzdan\Downloads\bquxjob_72a5dd6_19f283d8c5b.csv"
```

When `--gaps-csv` is set, the script uses the CSV `token_address`, `chain`,
`missing_from`, and `missing_to` columns. Rows are appended to BigQuery after
each token finishes, rather than waiting for the whole run.

You can also have the script detect gaps directly in BigQuery and skip the CSV
export:

```powershell
python .\backfill_birdeye_ohlcv.py --detect-gaps
```

This detects gaps for `chain = 'sol'` where consecutive hourly candles are more
than `24` hours apart. To change the threshold:

```powershell
python .\backfill_birdeye_ohlcv.py --detect-gaps --min-gap-hours 48
```

After a restart, first generate a smaller CSV containing only gaps still missing
from BigQuery:

```powershell
python .\backfill_birdeye_ohlcv.py `
  --gaps-csv "C:\Users\pzdan\Downloads\bquxjob_72a5dd6_19f283d8c5b.csv" `
  --remaining-gaps-csv ".\remaining_gaps.csv" `
  --remaining-gaps-only
```

Then backfill from the smaller file:

```powershell
python .\backfill_birdeye_ohlcv.py --gaps-csv ".\remaining_gaps.csv"
```

The same remaining-gap workflow works without a source CSV:

```powershell
python .\backfill_birdeye_ohlcv.py `
  --detect-gaps `
  --remaining-gaps-csv ".\remaining_gaps.csv" `
  --remaining-gaps-only
```

To change the request pacing:

```powershell
python .\backfill_birdeye_ohlcv.py --rate-limit-rpm 60
```

If the account can append but cannot read the table, you can bypass the
duplicate check, but this may create duplicate rows:

```powershell
python .\backfill_birdeye_ohlcv.py --skip-existing-check
```
