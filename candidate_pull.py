#!/usr/bin/env python3
"""birdeye-candidate-pull — weekly universe discovery fetcher (Cloud Run Job).

Implements steps 1-3 of the live-universe spec
(rl-crypto/docs/universe_live_birdeye_2026-07-12.md), moving discovery off the
external Databricks `public_tokens_to_monitor` stream into a Cloud Run job:

  1. DISCOVER — pull Birdeye Token List V3 (a deliberately LOOSE candidate
     screen: top-by-24h-volume with a pool-liquidity floor). This is NOT the
     universe; it is the set the downstream membership rule gets evaluated on
     (the rule lives in the dbt model, never here — see the live-universe spec
     doc referenced above for its definition).
  2. PERSIST — append the snapshot to `raw.raw_birdeye_market_data`, the exact
     table the dbt `stg_birdeye_market_data` model already reads as its backfill
     source. So NO dbt change is needed — this simply supplies the source the
     Databricks stream used to. Append-only: every weekly run is a point-in-time
     record (Token List V3 has no as-of-date, so accumulated snapshots ARE the
     history).
  3. BACKFILL — for tokens discovered this run that have no OHLCV yet, reuse
     backfill_birdeye_ohlcv.py (via subprocess, unchanged) so the membership
     rule has trailing stats for brand-new candidates.

Runs to completion (Cloud Run Job). The snapshot (step 2) is mandatory — the job
exits non-zero if it fails so the weekly Cloud Workflow branches to its fail-loud
alert. The OHLCV backfill (step 3) is best-effort (logged, non-fatal).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
from typing import Any

import requests
from google.cloud import bigquery

from obs_log import Logger, log_event

SERVICE = "birdeye-candidate-pull"
TOKEN_LIST_URL = os.getenv(
    "BIRDEYE_TOKEN_LIST_URL", "https://public-api.birdeye.so/defi/v3/token/list")

# The raw.raw_birdeye_market_data columns (a 1:1 dump of Token List V3 item
# fields + `chain`). Item keys that are not columns are dropped; columns absent
# from an item become NULL. Kept explicit so a Birdeye response shape change
# can't silently widen the load.
TABLE_COLUMNS = (
    "chain", "address", "logo_uri", "name", "symbol", "decimals",
    "market_cap", "fdv", "total_supply", "circulating_supply", "liquidity",
    "last_trade_unix_time",
    "volume_1h_usd", "volume_1h_change_percent", "volume_2h_usd",
    "volume_2h_change_percent", "volume_4h_usd", "volume_4h_change_percent",
    "volume_8h_usd", "volume_8h_change_percent", "volume_24h_usd",
    "volume_24h_change_percent",
    "trade_1h_count", "trade_2h_count", "trade_4h_count", "trade_8h_count",
    "trade_24h_count", "buy_24h", "buy_24h_change_percent", "volume_buy_24h_usd",
    "volume_buy_24h_change_percent", "sell_24h", "sell_24h_change_percent",
    "volume_sell_24h_usd", "volume_sell_24h_change_percent",
    "unique_wallet_24h", "unique_wallet_24h_change_percent",
    "price", "price_change_1h_percent", "price_change_2h_percent",
    "price_change_4h_percent", "price_change_8h_percent",
    "price_change_24h_percent", "holder", "recent_listing_time",
)


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def fetch_token_list(session: requests.Session, api_key: str, chain_header: str,
                     sort_by: str, min_liquidity: float, min_market_cap: float,
                     max_tokens: int, page_size: int = 100,
                     max_attempts: int = 5) -> list[dict]:
    """Paginate Token List V3 (sorted desc by `sort_by`, liquidity-floored) and
    return up to `max_tokens` item dicts."""
    headers = {"X-API-KEY": api_key, "x-chain": chain_header,
               "accept": "application/json"}
    items: list[dict] = []
    offset = 0
    while len(items) < max_tokens:
        limit = min(page_size, max_tokens - len(items))
        params: dict[str, Any] = {
            "sort_by": sort_by, "sort_type": "desc",
            "offset": offset, "limit": limit,
        }
        if min_liquidity > 0:
            params["min_liquidity"] = min_liquidity
        if min_market_cap > 0:
            params["min_market_cap"] = min_market_cap

        page = _get_with_retry(session, headers, params, max_attempts)
        data = page.get("data") or {}
        page_items = data.get("items") or data.get("tokens") or []
        if not page_items:
            break
        items.extend(page_items)
        offset += len(page_items)
        if len(page_items) < limit:
            break  # last page
    return items[:max_tokens]


def _get_with_retry(session, headers, params, max_attempts) -> dict:
    for attempt in range(1, max_attempts + 1):
        resp = session.get(TOKEN_LIST_URL, headers=headers, params=params, timeout=30)
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            if attempt == max_attempts:
                resp.raise_for_status()
            retry_after = resp.headers.get("Retry-After")
            time.sleep(float(retry_after) if retry_after else min(2 ** attempt, 30))
            continue
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("success") is False:
            raise RuntimeError(f"Birdeye token/list success=false: {payload}")
        return payload
    raise RuntimeError("Birdeye token/list failed after retries")


def to_row(item: dict, chain: str) -> dict:
    """Project a Token List V3 item onto the raw table columns."""
    row = {c: item.get(c) for c in TABLE_COLUMNS}
    row["chain"] = chain  # not in the item; set per-request
    return row


def append_snapshot(client: bigquery.Client, table: str, rows: list[dict],
                    location: str, batch_size: int = 5000) -> int:
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    inserted = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        client.load_table_from_json(batch, table, job_config=job_config,
                                    location=location).result()
        inserted += len(batch)
    return inserted


def ensure_candidate_table(client: bigquery.Client, table: str, location: str) -> None:
    """Create the clean candidate-universe table (idempotent). Partitioned by pull
    date + clustered by token so the hourly Raydium cost-probe can cheaply read the
    latest weekly set. This is the DURABLE '~500 list' record the probe consumes —
    raw.raw_birdeye_market_data is append-only with no as-of column, so a snapshot
    cannot be identified there."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table}` (
      pulled_at TIMESTAMP NOT NULL,
      week_start DATE,
      rank INT64,
      token_address STRING NOT NULL,
      chain STRING,
      symbol STRING,
      name STRING,
      decimals INT64,
      market_cap FLOAT64,
      liquidity FLOAT64,
      volume_24h_usd FLOAT64,
      price FLOAT64
    )
    PARTITION BY DATE(pulled_at)
    CLUSTER BY token_address
    """
    client.query(ddl, location=location).result()


def to_candidate_row(item: dict, chain: str, pulled_at: str, week_start: str,
                     rank: int) -> dict:
    """Clean, typed projection of a Token List V3 item for the candidate table."""
    def _f(key: str):
        v = item.get(key)
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None
    dec = item.get("decimals")
    return {
        "pulled_at": pulled_at,
        "week_start": week_start,
        "rank": rank,
        "token_address": item.get("address"),
        "chain": chain,
        "symbol": item.get("symbol"),
        "name": item.get("name"),
        "decimals": int(dec) if dec is not None else None,
        "market_cap": _f("market_cap"),
        "liquidity": _f("liquidity"),
        "volume_24h_usd": _f("volume_24h_usd"),
        "price": _f("price"),
    }


def persist_candidate_list(client: bigquery.Client, table: str, rows: list[dict],
                           location: str) -> int:
    """Append this run's clean candidate list (one tagged snapshot per weekly pull)."""
    schema = [
        bigquery.SchemaField("pulled_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("week_start", "DATE"),
        bigquery.SchemaField("rank", "INT64"),
        bigquery.SchemaField("token_address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chain", "STRING"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("decimals", "INT64"),
        bigquery.SchemaField("market_cap", "FLOAT64"),
        bigquery.SchemaField("liquidity", "FLOAT64"),
        bigquery.SchemaField("volume_24h_usd", "FLOAT64"),
        bigquery.SchemaField("price", "FLOAT64"),
    ]
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
    )
    client.load_table_from_json(rows, table, job_config=job_config,
                                location=location).result()
    return len(rows)


def new_tokens_without_ohlcv(client: bigquery.Client, discovered: list[str],
                             ohlcv_table: str, location: str) -> list[str]:
    """Discovered addresses that have no rows yet in the OHLCV table."""
    if not discovered:
        return []
    sql = f"SELECT DISTINCT token_address FROM `{ohlcv_table}`"
    try:
        existing = {r["token_address"] for r
                    in client.query(sql, location=location).result()}
    except Exception as exc:  # table missing / transient — treat all as new
        log_event(SERVICE, "ohlcv_read_failed", level="WARNING",
                  table=ohlcv_table, error=repr(exc))
        existing = set()
    return [a for a in discovered if a not in existing]


def backfill_ohlcv(new_addrs: list[str], ohlcv_table: str) -> None:
    """Best-effort: reuse backfill_birdeye_ohlcv.py (unchanged) for new tokens."""
    script = _env("BACKFILL_SCRIPT", os.path.join(os.path.dirname(__file__),
                                                  "backfill_birdeye_ohlcv.py"))
    extra = shlex.split(_env("BACKFILL_EXTRA_ARGS", ""))
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(new_addrs, f)
        tokens_path = f.name
    cmd = [sys.executable, script, "--tokens", tokens_path, "--table", ohlcv_table, *extra]
    log_event(SERVICE, "backfill_start", n=len(new_addrs))
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        log_event(SERVICE, "backfill_failed", level="WARNING", error=str(exc),
                  note="snapshot already persisted; membership picks these up once OHLCV lands")
    finally:
        os.unlink(tokens_path)


def pull_chain(client: bigquery.Client, session: requests.Session, api_key: str,
               chain_store: str, chain_header: str, market_table: str,
               candidate_table: str, location: str, sort_by: str,
               min_liquidity: float, min_market_cap: float, max_tokens: int,
               log: "Logger", write_market_snapshot: bool = True) -> tuple[list[str], int]:
    """Pull one chain: fetch the Birdeye token list and persist the timestamped
    candidate list. Returns (discovered_addresses, n_candidates); best-effort on
    the candidate write.

    write_market_snapshot: append to raw_birdeye_market_data too. Kept SOLANA-ONLY
    by the caller — the stg_birdeye_market_data model reads that raw table WITHOUT
    a chain filter and feeds the Solana OHLCV/feature pipeline, so EVM rows there
    would pollute it. The candidate_universe (0x-filtered downstream) already
    carries mcap/liquidity/volume/price per token, so nothing is lost for the
    other chains."""
    items = fetch_token_list(session, api_key, chain_header, sort_by,
                             min_liquidity, min_market_cap, max_tokens)
    if not items:
        log.warn("no_candidates_chain", chain=chain_store, chain_header=chain_header)
        return [], 0
    rows = [to_row(it, chain_store) for it in items if it.get("address")]
    discovered = [r["address"] for r in rows]
    if write_market_snapshot:
        inserted = append_snapshot(client, market_table, rows, location)
        log.info("persisted_raw", chain=chain_store, rows=inserted, table=market_table)
    n_cand = 0
    try:
        now = dt.datetime.now(dt.timezone.utc)
        pulled_at = now.isoformat()
        today = now.date()
        week_start = (today - dt.timedelta(days=today.weekday())).isoformat()  # Monday
        cand_items = [it for it in items if it.get("address")]
        cand_rows = [to_candidate_row(it, chain_store, pulled_at, week_start, i + 1)
                     for i, it in enumerate(cand_items)]
        ensure_candidate_table(client, candidate_table, location)
        n_cand = persist_candidate_list(client, candidate_table, cand_rows, location)
        log.info("persisted_candidates", chain=chain_store, rows=n_cand,
                 pulled_at=pulled_at, week_start=week_start)
    except Exception as exc:
        log.warn("candidate_persist_failed", chain=chain_store, error=repr(exc),
                 note="raw snapshot already landed; probe uses previous week's list")
    return discovered, n_cand


def main() -> int:
    log = Logger(SERVICE)
    api_key = _env("BIRDEYE_API_KEY")
    if not api_key:
        log.error("config_error", error="BIRDEYE_API_KEY is required")
        return 2
    project = _env("BQ_PROJECT_ID", "crypto-trading-474111")
    market_table = _env("MARKET_DATA_TABLE", f"{project}.raw.raw_birdeye_market_data")
    ohlcv_table = _env("OHLCV_TABLE", f"{project}.core.token_ohlcv")
    location = _env("BIGQUERY_LOCATION", "europe-central2")
    sort_by = _env("SORT_BY", "volume_24h_usd")
    min_liquidity = float(_env("MIN_LIQUIDITY_USD", "50000"))
    min_market_cap = float(_env("MIN_MARKET_CAP_USD", "0"))  # 0 = no floor; rule applies it
    max_tokens = int(_env("MAX_TOKENS", "500"))
    do_backfill = _env("BACKFILL_NEW_TOKENS", "true").lower() in ("1", "true", "yes")
    candidate_table = _env("CANDIDATE_TABLE", f"{project}.raw.candidate_universe")

    # Chains to pull, as "store:header" pairs (store = the `chain` column value,
    # header = Birdeye x-chain). Default = the 4 chains Birdeye's token-list
    # endpoint supports (arb/op/poly/avax return HTTP 400). A legacy explicit
    # CHAIN/BIRDEYE_CHAIN still forces single-chain, for back-compat.
    if _env("CHAIN") or _env("BIRDEYE_CHAIN"):
        chains = [(_env("CHAIN", "sol"), _env("BIRDEYE_CHAIN", "solana"))]
    else:
        chains = [tuple(p.split(":", 1)) for p in
                  _env("CHAINS", "sol:solana,eth:ethereum,base:base,bsc:bsc").split(",")
                  if ":" in p]

    session = requests.Session()
    client = bigquery.Client(project=project)
    log.info("cycle_start", chains=[c[0] for c in chains], sort_by=sort_by,
             min_liquidity=min_liquidity, min_market_cap=min_market_cap, max_tokens=max_tokens)

    # Only the live-traded Solana list drives the OHLCV backfill; other chains are
    # captured (candidate + raw market snapshot) for future cross-chain use.
    sol_discovered: list[str] = []
    total_cand = 0
    for chain_store, chain_header in chains:
        try:
            discovered, n_cand = pull_chain(
                client, session, api_key, chain_store, chain_header, market_table,
                candidate_table, location, sort_by, min_liquidity, min_market_cap,
                max_tokens, log, write_market_snapshot=(chain_store == "sol"))
            total_cand += n_cand
            if chain_store == "sol":
                sol_discovered = discovered
        except Exception as exc:  # one chain failing must not kill the others
            log.error("chain_failed", chain=chain_store, error=repr(exc))

    if do_backfill and sol_discovered:
        new_addrs = new_tokens_without_ohlcv(client, sol_discovered, ohlcv_table, location)
        log.info("new_tokens_no_ohlcv", n=len(new_addrs))
        if new_addrs:
            backfill_ohlcv(new_addrs, ohlcv_table)

    if total_cand == 0:
        log.error("no_candidates", error="no candidates persisted for any chain")
        return 1
    # n_chains, NOT chains: `cycle_start` logs chains=["sol","eth",...] (an array),
    # so the BigQuery log sink typed jsonPayload.chains as STRING REPEATED. Sending
    # an INT under the same key made the sink reject this row outright --
    # "Repeated value added outside of an array, field: chains" -- silently losing
    # the run summary on every multi-chain pull since 2026-08-06 (three so far:
    # 08-06, 08-10, 08-18). One key, one type.
    log.info("cycle_done", n_chains=len(chains), total_candidates=total_cand)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
