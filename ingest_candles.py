#!/usr/bin/env python3
"""Birdeye OHLCV ingest (interval- and chain-parameterised)  (ADDED 2026-08-10).

Serves two jobs: birdeye-15m-ingest (INTERVAL=15m -> core.token_ohlcv_15m) and
birdeye-evm-ingest (INTERVAL=1H, CHAINS=eth,base,bsc -> core.token_ohlcv_evm).

Companion to backfill_birdeye_ohlcv.py (hourly). Deliberately a SEPARATE entrypoint:
the hourly script is battle-tested and hard-codes type="1H"; per DEPLOYMENT.md §5 rule 1
(backward compatible only) this job adds capability without touching that path.

Target table: crypto-trading-474111.core.token_ohlcv_15m
  (day-partitioned on price_timestamp, clustered chain+token_address; same column
   schema as core.token_ohlcv). Created 2026-08-10 by migrating the frozen
   core_prices.token_ohlcv_15m (EU, last bar 2026-01-11) into europe-central2 so 15m
   data is finally joinable with the live hourly store.

Universe: Birdeye tokenlist top-N by v24hUSD (default 500) — same source the hourly
pipeline's candidate pull uses, so breadth does not depend on the (currently degraded)
token set already present in core.token_ohlcv.

Idempotent: existing (token_address, price_timestamp) rows in the lookback window are
skipped, so re-runs and overlapping schedules cannot duplicate.

Env:
  BIRDEYE_API_KEY   (required)   BIGQUERY_TABLE   default core.token_ohlcv_15m
  HOURS_BACK        default 6    TOP_N            default 500
  CHAIN             default sol  BIRDEYE_CHAIN    default solana
  INTERVAL          default 15m  CHAINS  optional "sol:solana,eth:ethereum" multi-chain
  RATE_LIMIT_RPM    default 600
"""
from __future__ import annotations

import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from google.cloud import bigquery

try:
    from obs_log import Logger            # shared estate logger
except Exception:                          # pragma: no cover - local runs
    class Logger:                          # minimal stand-in
        def __init__(self, *a, **k): pass
        def event(self, name, **kw): print(json.dumps({"event": name, **kw}, default=str))
        def error(self, name, **kw): self.event(name, level="ERROR", **kw)

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "crypto-trading-474111")
TABLE = os.getenv("BIGQUERY_TABLE", f"{PROJECT}.core.token_ohlcv_15m")
LOCATION = os.getenv("BIGQUERY_LOCATION", "europe-central2")
CHAIN = os.getenv("CHAIN", "sol")
BE_CHAIN = os.getenv("BIRDEYE_CHAIN", "solana")
HOURS_BACK = int(os.getenv("HOURS_BACK", "6"))
TOP_N = int(os.getenv("TOP_N", "500"))
RPM = int(os.getenv("RATE_LIMIT_RPM", "600"))
# Optional BQ-driven control plane (costs 0 CU, unlike /defi/tokenlist at 30 CU/call):
#   TOKENS_SQL  universe from BigQuery instead of the Birdeye toplist
#   GATE_SQL    run only when this query's first column is TRUE (event-driven ingest)
TOKENS_SQL = os.getenv("TOKENS_SQL")
GATE_SQL = os.getenv("GATE_SQL")
INTERVAL = os.getenv("INTERVAL", "15m")
CANDLE_SECONDS = {"15m": 900, "1H": 3600, "1h": 3600}.get(INTERVAL, 900)
_chains_raw = os.getenv("CHAINS", "").replace("|", ",")   # "|" allowed: commas are
CHAINS = [tuple(p.split(":")) for p in _chains_raw.split(",") if ":" in p] \
         or [(CHAIN, BE_CHAIN)]                            # awkward in gcloud env flags
STREAM_CHUNK = 2000        # rows per streaming insert request
BULK_THRESHOLD = 20000     # above this, use a load job instead of streaming
Q = Decimal("0.000000001")

log = Logger(os.getenv("SERVICE_NAME", "birdeye-candles-ingest"))
bq = bigquery.Client(project=PROJECT, location=LOCATION)
_sleep = 60.0 / RPM if RPM else 0.0


def be_get(path: str, params: dict, be_chain: str = None, retries: int = 4):
    url = f"https://public-api.birdeye.so{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={
        "X-API-KEY": os.environ["BIRDEYE_API_KEY"],
        "x-chain": be_chain or BE_CHAIN, "accept": "application/json"})
    for i in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.load(r)
            if _sleep:
                time.sleep(_sleep)
            return d
        except Exception:
            time.sleep(1.5 * (i + 1))
    return None


def universe(n: int, be_chain: str) -> list[str]:
    if TOKENS_SQL:
        toks = [r[0] for r in bq.query(TOKENS_SQL).result() if r[0]]
        log.event("universe_from_sql", n=len(toks))
        return toks
    out = []
    for off in range(0, n, 50):
        d = be_get("/defi/tokenlist", dict(sort_by="v24hUSD", sort_type="desc",
                                           offset=off, limit=50), be_chain=be_chain)
        out += ((d or {}).get("data") or {}).get("tokens") or []
    return [t["address"] for t in out if t.get("address")]


def existing_keys(t_from: datetime, t_to: datetime, chain: str) -> set:
    q = f"""SELECT token_address, UNIX_SECONDS(price_timestamp) ts
            FROM `{TABLE}`
            WHERE price_timestamp >= @a AND price_timestamp < @b AND chain = @c"""
    job = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("a", "TIMESTAMP", t_from),
        bigquery.ScalarQueryParameter("b", "TIMESTAMP", t_to),
        bigquery.ScalarQueryParameter("c", "STRING", chain)]))
    return {(r.token_address, int(r.ts)) for r in job.result()}


def num(v):
    """NUMERIC-safe string, or None -> JSON null. Never the literal "None":
    BigQuery rejects it as an invalid NUMERIC (seen on sparse EVM volume fields)."""
    try:
        if v is None:
            return None
        return str(Decimal(str(v)).quantize(Q))
    except Exception:
        return None


def main() -> int:
    t_to = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    t_from = t_to - timedelta(hours=HOURS_BACK)
    if GATE_SQL:
        try:
            open_ = bool(list(bq.query(GATE_SQL).result())[0][0])
        except Exception as e:
            log.error("gate_sql_failed", error=str(e)[:200])
            return 1
        if not open_:
            log.event("gate_closed_skip", detail="no Birdeye calls made")
            return 0
        log.event("gate_open", detail="proceeding with wide pull")
    log.event("run_start", table=TABLE, interval=INTERVAL,
              chains=[c for c, _ in CHAINS], hours_back=HOURS_BACK,
              window=f"{t_from.isoformat()}..{t_to.isoformat()}")
    rows = []
    for chain, be_chain in CHAINS:
        toks = universe(TOP_N, be_chain)
        if not toks:
            log.error("universe_empty", chain=chain)
            continue
        have = existing_keys(t_from, t_to, chain)
        for i, addr in enumerate(toks):
            # /defi/ohlcv is a flat 35 CU; /defi/v3/ohlcv is 60-120 CU (scales with response
            # size). Identical candle payload at our window sizes -> use the cheap one.
            d = be_get("/defi/ohlcv", dict(address=addr, type=INTERVAL,
                                           time_from=int(t_from.timestamp()),
                                           time_to=int(t_to.timestamp()),
                                           currency="usd"), be_chain=be_chain)
            items = ((d or {}).get("data") or {}).get("items") or []
            for it in items:
                ts = int(it.get("unix_time") or it.get("unixTime") or 0)
                if not ts or (addr, ts) in have:
                    continue
                rows.append(dict(
                    token_address=addr,
                    price_timestamp=datetime.fromtimestamp(ts, timezone.utc).isoformat(),
                    open=num(it.get("o")), high=num(it.get("h")),
                    low=num(it.get("l")), close=num(it.get("c")),
                    volume=num(it.get("v") if it.get("v") is not None else 0),
                    chain=chain))
            if i % 100 == 0:
                log.event("progress", chain=chain, done=i, total=len(toks),
                          pending_rows=len(rows))
    if not rows:
        log.event("no_new_rows")
        return 0
    if not write_rows(rows):
        return 1
    log.event("run_end", appended=len(rows))
    return 0


def write_rows(rows: list[dict]) -> bool:
    """Small batches stream; bulk (gap-fill) goes through a load job — streaming
    inserts cap out well below a multi-week backfill."""
    if len(rows) <= BULK_THRESHOLD:
        for i in range(0, len(rows), STREAM_CHUNK):
            errors = bq.insert_rows_json(TABLE, rows[i:i + STREAM_CHUNK])
            if errors:
                log.error("bq_insert_failed", errors=str(errors)[:500])
                return False
        return True
    buf = io.BytesIO("\n".join(json.dumps(r) for r in rows).encode())
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    try:
        bq.load_table_from_file(buf, TABLE, job_config=cfg, location=LOCATION).result()
        log.event("bulk_loaded", rows=len(rows))
        return True
    except Exception as e:
        # Load jobs need dataset-level bigquery.tables.create; the runtime SA holds
        # table-level rights only (least privilege). Fall back to chunked streaming.
        log.event("bq_load_failed_falling_back", level="WARNING", error=str(e)[:300])
        for i in range(0, len(rows), STREAM_CHUNK):
            errors = bq.insert_rows_json(TABLE, rows[i:i + STREAM_CHUNK])
            if errors:
                log.error("bq_insert_failed", errors=str(errors)[:500])
                return False
        log.event("bulk_streamed", rows=len(rows))
        return True


if __name__ == "__main__":
    sys.exit(main())
