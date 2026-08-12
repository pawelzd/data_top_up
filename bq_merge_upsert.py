"""Atomic upsert helper for Birdeye OHLCV writers (T-001, 2026-08-12).

ingest_candles.py and backfill_birdeye_ohlcv.py both used to read "existing
(token, hour) keys" from the target table and then WRITE_APPEND whatever
wasn't in that set. Two runs each read "existing" before either had written,
so the guard evaporated under concurrency -- which is exactly what happened
twice in 48h: the legacy hourly scheduler racing the Cloud Workflow, then the
new birdeye-breadth-daily backfill racing the hourly job.

Both writers now go through merge_upsert() instead: a single
`MERGE ... USING (SELECT * FROM UNNEST(@rows)) ... WHEN NOT MATCHED THEN
INSERT` statement per batch. BigQuery serializes DML statements against the
same destination table (queues and auto-retries conflicting ones rather than
interleaving them), so a second concurrent MERGE targeting an overlapping key
set is guaranteed to run after the first commits and see its rows already
there -- WHEN NOT MATCHED then correctly skips them. The merge key IS the
lock: no separate lease/lock table is needed, and this also makes a backfill
that overlaps the hourly job's window safe, with no pause-then-resume
procedure required.

This also sidesteps a real IAM gap noted in ingest_candles.py: BigQuery load
jobs require `bigquery.tables.create` on the DATASET even when appending to a
table that already exists, and this project's runtime SAs only hold
table-level grants. A parameterised MERGE query needs only
tables.getData/updateData on the target table -- which the pre-existing
"existing keys" SELECT already proves every writer here has.

Row-fetch-then-filter (existing_keys()/fetch_existing_keys() in the two
callers) is kept as a cost optimisation only -- it avoids re-fetching hours
already covered from Birdeye -- but correctness no longer depends on it.
"""
from __future__ import annotations

from datetime import datetime
from typing import Sequence

from google.cloud import bigquery

# Keep each MERGE's UNNEST(@rows) payload comfortably under BigQuery's query
# request-size limits, independent of whatever batch size a caller uses.
MAX_ROWS_PER_MERGE = 5000

KEY_COLUMNS = ("token_address", "price_timestamp", "chain")

_FIELD_TYPES = {
    "token_address": "STRING",
    "price_timestamp": "TIMESTAMP",
    "open": "NUMERIC",
    "high": "NUMERIC",
    "low": "NUMERIC",
    "close": "NUMERIC",
    "volume": "NUMERIC",
    "chain": "STRING",
}


def _dedupe_last_wins(rows: Sequence[dict]) -> list[dict]:
    """MERGE raises if a WHEN MATCHED clause matches >1 source row, and would
    silently insert >1 row if WHEN NOT MATCHED matches >1 source row sharing a
    key -- either way a batch must already be unique on KEY_COLUMNS before it
    is staged."""
    by_key: dict[tuple, dict] = {}
    for row in rows:
        by_key[tuple(row[c] for c in KEY_COLUMNS)] = row
    return list(by_key.values())


def _coerce(column: str, value):
    if value is not None and _FIELD_TYPES[column] == "TIMESTAMP" and isinstance(value, str):
        return datetime.fromisoformat(value)
    return value


def _row_struct_param(row: dict, columns: Sequence[str]) -> bigquery.StructQueryParameter:
    return bigquery.StructQueryParameter(
        None,
        *[bigquery.ScalarQueryParameter(c, _FIELD_TYPES[c], _coerce(c, row.get(c)))
          for c in columns],
    )


def build_merge_sql(table: str, columns: Sequence[str]) -> str:
    on_clause = " AND ".join(f"T.{c} = S.{c}" for c in KEY_COLUMNS)
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join(f"S.{c}" for c in columns)
    return (
        f"MERGE `{table}` T "
        f"USING (SELECT * FROM UNNEST(@rows)) S "
        f"ON {on_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def merge_upsert(
    client: bigquery.Client,
    table: str,
    rows: Sequence[dict],
    location: str,
) -> int:
    """Upsert `rows` into `table`, keyed on KEY_COLUMNS. Rows whose key
    already exists in `table` are silently skipped. Returns the number of
    rows actually inserted. Safe to call concurrently -- from multiple
    processes/jobs/schedules -- without producing duplicate rows."""
    rows = _dedupe_last_wins(rows)
    if not rows:
        return 0

    columns = list(rows[0].keys())
    sql = build_merge_sql(table, columns)
    inserted = 0
    for start in range(0, len(rows), MAX_ROWS_PER_MERGE):
        batch = rows[start:start + MAX_ROWS_PER_MERGE]
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter(
                "rows", "STRUCT", [_row_struct_param(r, columns) for r in batch]
            )
        ])
        job = client.query(sql, job_config=job_config, location=location)
        job.result()
        inserted += job.num_dml_affected_rows or 0
    return inserted
