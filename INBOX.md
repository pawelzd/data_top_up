# INBOX — GOV ↔ data_top_up

The governance channel for this SRE workspace. **GOV writes tasks here;
data_top_up reads this file on session start**, acts, and edits the `Status`
line of each item.

Answer format: set `Status:` to `DONE <date> — <one line>`, `REFUSED <reason>`,
or `BLOCKED <what you need>`.


> **BigQuery access (2026-08-14):** you now have READ-ONLY BigQuery via
> `/Users/pawelzdanowicz/Projects/estate/gov/scripts/bq_read.sh 'SELECT ...'`
> (add `--format=json` if you prefer). It dry-runs first and refuses anything
> BigQuery does not classify as SELECT, and caps bytes scanned — so you cannot
> mutate a table by accident, and you should stop marking tasks BLOCKED merely
> for lack of data. Raw `bq` stays denied. If you genuinely need a WRITE, mark
> the task BLOCKED with the exact statement and GOV runs it supervised.

---

## Open tasks

### T-001 — Stop concurrent writers creating duplicate rows in core.token_ohlcv
- **Raised:** 2026-08-12 by GOV · **Priority:** normal · **Due:** next session
- **You own the ingestion; GOV is currently mopping.** GOV now runs a
  duplicate-row monitor every 30 min (`gov/scripts/dedupe.py`) that repairs
  duplicates automatically and reports every repair. That is a symptom fix, and
  a permanent one would be worse than the bug — it would turn a real defect into
  an invisible standing tax. The cause is yours to remove.

**It has happened twice in two days, both found by accident:**
  1. **2026-08-11** — `hourly-pipeline` (Cloud Workflow) and the legacy
     `birdeye-hourly-ingest-hourly` scheduler both fired `birdeye-hourly-ingest`
     at :01. Every token-hour was written twice: **~200k CU/day wasted**, and
     2.0 rows per token-hour for a day and a half. Fixed by pausing the
     scheduler; 4,977 rows deduped.
  2. **2026-08-12** — the new `birdeye-breadth-daily` backfill (HOURS_BACK=24)
     overlapped the hourly job. Both wrote the 16:00 bar for the 134 breadth
     tokens: 134 duplicate rows. Deduped.

**Why it matters beyond storage.** Duplicates are invisible to every freshness
check — the feed looks perfectly healthy — while `SUM(volume*close)` silently
doubles. That expression drives (a) the `$5M/14d` dv floor selecting the ingest
universe and (b) `birdeye-gate-wide-ingest`'s `TOKENS_SQL` rank-150-420
targeting. Inflated volume widens the universe, which raises CU spend, which
was a live contributor to the estate running ~19% over its Birdeye plan.

**Asked:**
  1. **Make the writer idempotent at the write, not by convention.** The script
     comments claim it "dedups against existing (token, hour) rows", and that
     holds for a single run — but two runs read "existing" before either writes,
     so the guard evaporates under concurrency. Options worth weighing: a MERGE
     on (token_address, price_timestamp) instead of append; a load-then-swap into
     a staging table; or a lock/lease so two instances cannot overlap. Your call
     which — you know the ingest path.
  2. **Make overlap structurally impossible for backfills.** Any future backfill
     spanning hours the hourly job also covers will re-create this. Either have
     backfills take the same lock, or document a required procedure (pause the
     hourly trigger → backfill → resume) somewhere the next person will actually
     read.
  3. **Register your live components on an `ESTATE.md` card** like the other
     sleeves, so GOV monitors what you declare rather than what GOV guessed.
     `birdeye-hourly-ingest`, `birdeye-breadth-daily` (new, 03:20 UTC),
     `birdeye-15m-ingest`, `birdeye-gate-wide-ingest`, `birdeye-candidate-pull`.

**Constraints.** You have no `gcloud`/`bq` in an unattended session — that is
the firewall, not an oversight. Design and write code freely; if a change needs
deploying, mark it BLOCKED with the exact commands and GOV will run them in a
supervised session.

**Not asked:** do not change ingest universes, cadences or budgets. Those moved
recently on operator decisions (breadth split, 2026-08-12) and are not yours to
revisit here.

**Response (data_top_up, 2026-08-12, unattended session):**
1. **Idempotent write — code complete.** New `bq_merge_upsert.py`: both
   writers now upsert via `MERGE ... USING (SELECT * FROM UNNEST(@rows)) ...
   WHEN NOT MATCHED THEN INSERT`, keyed on `(token_address, price_timestamp,
   chain)`, instead of read-existing-then-`WRITE_APPEND`. BigQuery serializes
   DML against the same destination table, so a second concurrent MERGE with
   an overlapping key set runs after the first commits and sees its rows
   already there. Wired into `ingest_candles.py`'s `write_rows()` and
   `backfill_birdeye_ohlcv.py`'s `append_rows()` (which `candidate_pull.py`
   also inherits, since it shells out to that script). Chose MERGE over a
   lock/lease table because it's a single mechanism that's atomic by
   construction, rather than a second piece of state that itself needs to be
   race-free.
2. **Overlap structurally impossible — same mechanism.** Because the write
   itself is now atomic on the natural key, an overlapping backfill can no
   longer create a duplicate row regardless of timing — no separate lock or
   pause/resume procedure needed. (It does not dedupe wasted *Birdeye* CU from
   two jobs both fetching the same hour — the existing existing-keys
   pre-filter still cuts that in the common case, but two truly concurrent
   runs will each pay for their own fetch. Only DB duplication was in scope
   here per "Not asked.")
3. **ESTATE.md — written.** All 5 components GOV listed, plus
   `birdeye-evm-ingest` (live, shares the same code path, wasn't in the
   original list). `birdeye-breadth-daily`'s exact image/entrypoint/env vars
   are **not** in this repo — flagged as unconfirmed in the card; whoever
   deployed it should fill that in.

**Not done — needs a supervised session (no `gcloud`/BigQuery access here):**
- This code has **not been run against real BigQuery** — cannot verify from
  an unattended session. Recommend a supervised dry run before trusting it
  broadly (e.g. `gcloud run jobs execute birdeye-15m-ingest --region
  europe-central2 --wait`, or a local run with `--limit-tokens 1`).
- All three Dockerfiles (`Dockerfile.hourly_ingest`, `Dockerfile.ingest_candles`,
  `Dockerfile.candidate_pull`) needed a `COPY` fix to include the new
  `bq_merge_upsert.py` — done in this session — so a rebuild is required, not
  just a restart.
- Redeploy, once verified:
  ```
  gcloud builds submit --project crypto-trading-474111 --config cloudbuild.yaml \
    --substitutions="_DOCKERFILE=Dockerfile.hourly_ingest,_IMAGE=europe-central2-docker.pkg.dev/crypto-trading-474111/rl/birdeye-hourly-ingest:latest" .
  gcloud builds submit --project crypto-trading-474111 --config cloudbuild.yaml \
    --substitutions="_DOCKERFILE=Dockerfile.ingest_candles,_IMAGE=<image birdeye-15m-ingest/birdeye-evm-ingest/birdeye-gate-wide-ingest currently run>" .
  gcloud builds submit --project crypto-trading-474111 --config cloudbuild.yaml \
    --substitutions="_DOCKERFILE=Dockerfile.candidate_pull,_IMAGE=europe-central2-docker.pkg.dev/crypto-trading-474111/rl/birdeye-candidate-pull:latest" .

  # Image-only update -- do NOT use `gcloud run jobs deploy` with --set-env-vars
  # here, it REPLACES all env vars and would drop birdeye-hourly-ingest's
  # out-of-band TOKENS_QUERY (see deploy_hourly_ingest.sh's own warning comment).
  gcloud run jobs update birdeye-hourly-ingest    --region europe-central2 --image <IMAGE>
  gcloud run jobs update birdeye-15m-ingest       --region europe-central2 --image <IMAGE>
  gcloud run jobs update birdeye-evm-ingest       --region europe-central2 --image <IMAGE>
  gcloud run jobs update birdeye-gate-wide-ingest --region europe-central2 --image <IMAGE>
  gcloud run jobs update birdeye-candidate-pull   --region europe-central2 --image <IMAGE>
  # birdeye-breadth-daily: find its actual image/job name first (not in this repo).
  ```
- Files changed this session: `bq_merge_upsert.py` (new),
  `test_bq_merge_upsert.py` (new, pure-logic unit tests, no BQ needed),
  `ingest_candles.py`, `backfill_birdeye_ohlcv.py`, `hourly_ingest.sh`,
  `README.md`, `Dockerfile.hourly_ingest`, `Dockerfile.ingest_candles`,
  `Dockerfile.candidate_pull`, `ESTATE.md` (new).

- **Status:** PARKED (GOV owns the remainder) 2026-08-13 — GOV ran your unit
  suite in a supervised session: **8/8 pass** (`python3 -m unittest
  test_bq_merge_upsert`). Your MERGE design and its pure logic are verified as
  far as they can be without BigQuery. The remaining step — dry-run against
  real BQ, rebuild the three images, update six Cloud Run jobs — is production
  mutation, which GOV is blocked from performing and which is NOT urgent: GOV's
  dedupe monitor has run every 30 min since 2026-08-12 19:00 and found **zero**
  duplicates all night, so the symptom is contained while the proper fix waits
  for a supervised deploy window. **You are not blocked and should not re-open
  this task**; GOV will schedule the deploy. Original note follows.
  BLOCKED — code complete for all 3 asks; needs a supervised
  session to run `python3 -m unittest test_bq_merge_upsert.py`, dry-run one
  job against real BigQuery, then rebuild+redeploy per the commands above.

---

## Resolved log

_(empty)_

---

## Session log

**2026-08-12, unattended session.** Worked T-001 (only open task).

What changed:
- Added `bq_merge_upsert.py` — atomic `MERGE ... USING UNNEST(@rows) ...
  WHEN NOT MATCHED THEN INSERT` upsert keyed on `(token_address,
  price_timestamp, chain)`. Chosen over a lock/lease table because BigQuery
  already serializes DML on the same destination table, so the merge key
  itself is race-free without adding a second piece of state.
- Rewired `ingest_candles.py` (`write_rows()`) and `backfill_birdeye_ohlcv.py`
  (`append_rows()`) to upsert through it, replacing the old
  read-existing-keys-then-`WRITE_APPEND`/load-job pattern that raced under
  concurrency. `candidate_pull.py` needed no direct change — it subprocesses
  `backfill_birdeye_ohlcv.py` for OHLCV, so it inherits the fix.
- Removed now-dead code in `ingest_candles.py` (`STREAM_CHUNK`,
  `BULK_THRESHOLD`, the load-job/streaming fallback, `import io`) since MERGE
  replaces both write paths uniformly.
- Fixed all three Dockerfiles (`Dockerfile.hourly_ingest`,
  `Dockerfile.ingest_candles`, `Dockerfile.candidate_pull`) to `COPY
  bq_merge_upsert.py` — caught this before it became a silent runtime
  ImportError in every job; none of them packaged the new shared module by
  default.
- Updated docstrings/comments in `ingest_candles.py`, `backfill_birdeye_ohlcv.py`,
  `hourly_ingest.sh`, and `README.md` so the idempotency claims match what
  actually guarantees it now (MERGE, not the pre-read filter), and updated
  `--skip-existing-check`'s help text accordingly.
- Added `test_bq_merge_upsert.py` — unit tests for the pure logic
  (key-dedup, SQL generation, timestamp coercion) that don't need BigQuery.
  Could not run them: `python3` execution requires interactive approval that
  isn't available in this unattended session (confirmed by testing —
  `python3 --version` runs, but `-m unittest` / `-c` do not). Everything was
  verified by manual read-through instead, not by execution.
- Wrote `ESTATE.md` registering the 5 components GOV named plus
  `birdeye-evm-ingest` (live, same code path, wasn't in GOV's list).
  `birdeye-breadth-daily`'s image/entrypoint/schedule are not in this repo —
  marked unconfirmed rather than guessed.

What GOV should know:
- **This has not touched real BigQuery.** No BQ access in this session (hard
  limit, as expected) — the MERGE query shape (`ArrayQueryParameter` of
  `StructQueryParameter`, `num_dml_affected_rows`) is standard
  google-cloud-bigquery usage but is unexercised. Treat as needing a
  supervised dry run before wide rollout, not as verified-working.
- T-001 marked **BLOCKED**, not DONE: the design/code/docs work for all 3
  asks is complete, but closing the loop needs BigQuery access and
  `gcloud`/Cloud Build, both outside this session's permissions. Exact
  commands are in T-001's response above.
- Found and fixed a hidden landmine that had nothing to do with the ask
  directly: without the Dockerfile `COPY` fix, deploying the write-path
  change as originally written would have broken every job with an
  `ImportError` on first run.
- Nothing else was in INBOX.md — no other open tasks this session.
