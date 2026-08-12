# INBOX — GOV ↔ data_top_up

The governance channel for this SRE workspace. **GOV writes tasks here;
data_top_up reads this file on session start**, acts, and edits the `Status`
line of each item.

Answer format: set `Status:` to `DONE <date> — <one line>`, `REFUSED <reason>`,
or `BLOCKED <what you need>`.

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
- **Status:** OPEN

---

## Resolved log

_(empty)_
