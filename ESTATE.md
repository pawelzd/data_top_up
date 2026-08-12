# ESTATE — data_top_up live components

Registered 2026-08-12 in response to GOV/INBOX.md T-001 ("register your live
components... so GOV monitors what you declare rather than what GOV
guessed"). This sleeve owns Birdeye OHLCV ingestion into
`crypto-trading-474111.core.token_ohlcv*` and the weekly universe-discovery
pull. Keep this file in sync when a job's schedule, table, or entrypoint
changes — GOV's duplicate-writer monitor and freshness checks read against
what's declared here.

All writers below go through `bq_merge_upsert.py`'s atomic MERGE (T-001,
2026-08-12): concurrent/overlapping runs targeting the same
`(token_address, price_timestamp, chain)` cannot produce duplicate rows,
regardless of scheduling overlap. See that module's docstring for why.

| Component | Type | Schedule (UTC) | Entrypoint | Target table | Notes |
|---|---|---|---|---|---|
| `birdeye-hourly-ingest` | Cloud Run Job | `1 * * * *` (Scheduler `birdeye-hourly-ingest-hourly`) | `hourly_ingest.sh` → `backfill_birdeye_ohlcv.py` | `core.token_ohlcv` (chain=sol) | `TOKENS_QUERY` = UNION of `rl_prod_inference_features_v` ∪ `raw.candidate_universe` (8d) ∪ `core.token_ohlcv` tokens ≥$200k/30d dv. `HOURS_BACK=3`. The legacy duplicate `-hourly` scheduler (2026-08-11 incident) is paused, not deleted — confirm it stays paused. |
| `birdeye-15m-ingest` | Cloud Run Job | `:05,:20,:35,:50` (Scheduler `birdeye-15m-ingest-q`) | `ingest_candles.py` (`INTERVAL=15m`) | `core.token_ohlcv_15m` (chain=sol) | `TOP_N=500`, `RATE_LIMIT_RPM=600`, `HOURS_BACK=6`. |
| `birdeye-evm-ingest` | Cloud Run Job | `:25` (Scheduler `birdeye-evm-ingest-q`) | `ingest_candles.py` (`INTERVAL=1H`, `CHAINS=eth,base,bsc`) | `core.token_ohlcv_evm` | Not in GOV's original list but live and sharing the same writer code path — included for completeness. |
| `birdeye-gate-wide-ingest` | Cloud Run Job | `:35` hourly (Scheduler `birdeye-gate-wide-q`) | `ingest_candles.py` (`GATE_SQL` + `TOKENS_SQL`) | `core.token_ohlcv_15m` | Event-driven: makes 0 Birdeye calls unless SOL 24h return ≤ -3%. `HOURS_BACK=6`, ranks ~150-420 by volume when open. |
| `birdeye-candidate-pull` | Cloud Run Job | weekly (Scheduler `birdeye-candidate-pull-weekly`) | `candidate_pull.py` | `raw.raw_birdeye_market_data`, `raw.candidate_universe`, and (best-effort, subprocess) `backfill_birdeye_ohlcv.py` → `core.token_ohlcv` for newly-discovered tokens | Snapshot writes (step 2) are fail-loud; the OHLCV backfill (step 3) is best-effort/non-fatal. Delegates to `backfill_birdeye_ohlcv.py` for OHLCV, so it inherits the same MERGE write — it is not a separate write path. |
| `birdeye-breadth-daily` | Cloud Run Job (assumed) | 03:20 UTC (per GOV, 2026-08-12 incident report) | **unconfirmed** — not represented by any script/deploy config checked into this repo; likely another invocation of `ingest_candles.py` or `backfill_birdeye_ohlcv.py` with `HOURS_BACK=24`, deployed directly via `gcloud` | **unconfirmed** — incident report says it wrote the same 16:00 bar as the hourly job for 134 tokens, consistent with `core.token_ohlcv` or `core.token_ohlcv_15m` | New 2026-08-12, added on an operator decision outside this sleeve's checked-in code (see INBOX.md T-001 "Not asked" section — cadence/universe not mine to revisit). **GOV/whoever deployed it should fill in the exact image, env vars, and schedule here** — I have no `gcloud` access in an unattended session to inspect the live Cloud Run/Scheduler config directly. |

## Known non-owned writer of the same table

`btcusdt` rows in `core.token_ohlcv` (chain=sol) are written hourly by
`btc-data-ingest-hourly`, owned by the separate `btc-data` repo — not this
sleeve. Mentioned here only because it shares the destination table T-001 is
about; do not delete that pseudo-token row (see README.md "Address guard").

## Idempotent-write mechanism (T-001)

`bq_merge_upsert.py` — a shared `merge_upsert()` used by both
`ingest_candles.py` (`write_rows()`) and `backfill_birdeye_ohlcv.py`
(`append_rows()`, and transitively `candidate_pull.py` which shells out to
it). Replaces the old "SELECT existing keys, then WRITE_APPEND" pattern
(racy under concurrency — its read-check window closes before either
concurrent writer commits) with a single `MERGE ... USING
(SELECT * FROM UNNEST(@rows)) ... WHEN NOT MATCHED THEN INSERT` per batch.
BigQuery serializes DML statements against the same destination table, so
the merge key itself acts as the concurrency guard — no separate lock table,
and overlapping backfills are safe without a pause/resume procedure.

**Not yet verified against live BigQuery** — this session has no BigQuery
access (unattended-session hard limit). Recommend a supervised dry run
(e.g. `--limit-tokens 1` against the real table, or a short `HOURS_BACK`
window) before trusting this broadly. See INBOX.md T-001 status for details.
