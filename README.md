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

---

## Parameterised candles ingest — 15m sol + hourly EVM (added 2026-08-10, challenger project)

`ingest_candles.py` + `Dockerfile.ingest_candles` → two Cloud Run jobs:
**`birdeye-15m-ingest`** (INTERVAL=15m, sol → `core.token_ohlcv_15m`), Scheduler
`birdeye-15m-ingest-q` at :05/:20/:35/:50 UTC; and **`birdeye-evm-ingest`**
(INTERVAL=1H, CHAINS=eth|base|bsc → `core.token_ohlcv_evm`), Scheduler
`birdeye-evm-ingest-q` at :25 UTC (slots chosen to avoid the
existing hourly chain at :01/:02, dbt :08, shadow :12).

- **Target**: `core.token_ohlcv_15m` (day-partitioned, clustered chain+token_address,
  same column schema as `core.token_ohlcv`). Created 2026-08-10 by cross-region copy of
  the frozen `core_prices.token_ohlcv_15m` (EU, last bar 2026-01-11) into
  europe-central2, so 15m data is joinable with the live hourly store; the EU table was
  left untouched as an archive.
- **Why a separate entrypoint** rather than extending `backfill_birdeye_ohlcv.py`: that
  script hard-codes `type="1H"` and feeds live production; per DEPLOYMENT.md §5 rule 1
  this change is purely additive and cannot affect the hourly path.
- **Idempotent**: skips existing `(token_address, price_timestamp)` in the lookback
  window, so re-runs/overlaps can't duplicate. `HOURS_BACK=6` in steady state; raise it
  for gap fills.
- **Bulk writes**: >20k rows use a load job, falling back to chunked streaming if the
  runtime SA lacks dataset-level `tables.create` (it holds table-level rights only).
  One-time gap fill of 2026-07-20→08-10 (240,136 rows) was run locally on 2026-08-10.
- **Rate**: `RATE_LIMIT_RPM=600`, top-500 tokens/run — polite alongside the hourly ingest.
- Runtime SA: `challenger-paper@` with `roles/bigquery.dataEditor` on the target table only.

Table state after commissioning: 51.49M rows, coverage 2021-12-09 → current hour.

### EVM hourly (B2a, 2026-08-10)

`core.token_ohlcv_evm` — NEW table, deliberately separate from `core.token_ohlcv`:
that table has only ever contained `chain='sol'`, and silently introducing eth/base/bsc
rows would change the universe of any consumer that does not filter by chain
(DEPLOYMENT.md §5 rule 1). A UNION view can be added if a combined surface is ever wanted.

Contents: 13.98M rows — 13.09M migrated from `core_prices.token_ohlcv_1h` (EU, chains
eth/base/bsc, history to 2025-10-22; source left untouched) + 631,358 rows of real OHLCV
backfill for 2026-04→08 pulled with this script. Coverage now runs to the current hour.

Gotchas fixed while commissioning (both were latent in the 15m path too):
- `num()` returned Python `None`, which `str()` turned into the literal `"None"` —
  BigQuery rejects that as an invalid NUMERIC (surfaced on sparse EVM `volume`). It now
  emits JSON null.
- `CHAINS` accepts `|` as well as `,` (commas are awkward in `gcloud --set-env-vars`).

### Ingest universe decoupled from the model's feature view (B3, 2026-08-10)

**Symptom**: `core.token_ohlcv` breadth fell 1306 → 385 distinct tokens/day on 2026-07-21 and
decayed to ~150 by 2026-08-10. The job itself was healthy the whole time (exit 0 hourly).

**Cause**: `hourly_ingest.sh` defaulted `TOKENS_QUERY` to
`SELECT DISTINCT token_address FROM rl_prod.rl_prod_inference_features_v` — so the estate's
*ingestion* breadth was bound to one model's *inference* universe. That model legitimately
narrowed to its top ~120 tokens; ingestion should not have followed it down.

**Fix**: `TOKENS_QUERY` set on the `birdeye-hourly-ingest` job to a UNION that is a strict
superset — the model's view is kept as one leg, so its coverage is unchanged by construction:

    rl_prod.rl_prod_inference_features_v                                  (~120, unchanged)
  ∪ raw.candidate_universe, most recent 8d of pulls, chain='sol'          (~500)
  ∪ core.token_ohlcv tokens with >= $200k dollar-volume over 30d          (~647)
  → ~700-800 unique tokens/run.

**Why not simply "everything" (~1,477 tokens)?** Measured 2026-08-10: of the 1,477 tokens
with bars in the last pre-collapse week, only 543 traded >= $50k/7d and 280 cleared the
$350k strategy floor — ~994 are zombies. Ingesting them costs API budget and buys nothing.

**Why not just the model's ~120?** Measured over 2026-04→07: **28-75 tokens per week newly
cross the $350k floor**. A token becomes tradeable only once it has trailing history
(7d volume, age >= 7d, >= 100 nonzero-volume hours), so ingest must cover the *pipeline* of
near-eligible names, not just today's eligible ones. Ingesting narrowly also truncates the
death tail — rugs and fades must stay in the record or backtests inflate (they are the
left tail challenger's scoring depends on).

The $200k/30d floor is the compromise: ~2.5x headroom below the trading floor (so newcomers
arrive with history), keeps dying tokens while they fade, and drops the zombie half of the
tail. Roughly half the API cost of an unfiltered universe.

Backbone is the estate's own weekly discovery pull (`birdeye-candidate-pull-weekly`), so
breadth is now self-sustaining and cannot be silently narrowed by a downstream consumer.
Cost: ~1,400 requests/hour at RATE_LIMIT_RPM=800 (~2 min) — the pre-collapse norm.

**Rollback** (restores the old behaviour exactly — the job previously set no TOKENS_QUERY):

    gcloud run jobs update birdeye-hourly-ingest --region=europe-central2 \
      --remove-env-vars TOKENS_QUERY

Applied as an env-var change only; image, schedule and code are untouched.

**Address guard (2026-08-10)**: the broader universe surfaced a latent landmine — a bogus
`btcusdt` row that has long existed in `core.token_ohlcv`. Birdeye returns HTTP 400
("address is invalid format") for it and `backfill_birdeye_ohlcv.py` raises RuntimeError,
aborting the whole run (it never triggered before because the model's view does not contain
it). `TOKENS_QUERY` now ends with a base58 sanity filter:

    AND LENGTH(token_address) BETWEEN 32 AND 44
    AND REGEXP_CONTAINS(token_address, r'^[1-9A-HJ-NP-Za-km-z]+$')

Worth noting for future work: a single malformed address anywhere in the universe kills an
entire hourly cycle. Making the fetch loop skip-and-continue on HTTP 400 would be a genuine
robustness improvement to the hourly script (not done here — that script is untouched by
design).
