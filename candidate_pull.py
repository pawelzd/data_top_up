#!/usr/bin/env python3
"""birdeye-candidate-pull — weekly universe discovery fetcher (Cloud Run Job).

Implements steps 1-3 of the live-universe spec
(rl-crypto/docs/universe_live_birdeye_2026-07-12.md), moving discovery off the
external Databricks `public_tokens_to_monitor` stream into a Cloud Run job:

  1. DISCOVER — pull Birdeye Token List V3 (a deliberately LOOSE candidate
     screen: top-by-24h-volume with a pool-liquidity floor). This is NOT the
     universe; it is the set the membership rule gets evaluated on. The actual
     mktcap>=$20M / volume-rank<=120 rule is applied downstream by the v1 dbt
     model, never here.
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
        print(f"[candidate-pull] could not read {ohlcv_table} ({exc!r}); "
              f"treating all discovered tokens as new")
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
    print(f"[candidate-pull] backfilling OHLCV for {len(new_addrs)} new token(s): "
          f"{' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        print(f"[candidate-pull] WARNING: OHLCV backfill failed ({exc}); snapshot "
              f"already persisted, membership will pick these up once OHLCV lands")
    finally:
        os.unlink(tokens_path)


def main() -> int:
    api_key = _env("BIRDEYE_API_KEY")
    if not api_key:
        print("BIRDEYE_API_KEY is required", file=sys.stderr)
        return 2
    project = _env("BQ_PROJECT_ID", "crypto-trading-474111")
    market_table = _env("MARKET_DATA_TABLE", f"{project}.raw.raw_birdeye_market_data")
    ohlcv_table = _env("OHLCV_TABLE", f"{project}.core.token_ohlcv")
    location = _env("BIGQUERY_LOCATION", "europe-central2")
    chain_store = _env("CHAIN", "sol")            # stored in the `chain` column
    chain_header = _env("BIRDEYE_CHAIN", "solana")  # Birdeye x-chain header
    sort_by = _env("SORT_BY", "volume_24h_usd")
    min_liquidity = float(_env("MIN_LIQUIDITY_USD", "50000"))
    min_market_cap = float(_env("MIN_MARKET_CAP_USD", "0"))  # 0 = no floor; rule applies it
    max_tokens = int(_env("MAX_TOKENS", "500"))
    do_backfill = _env("BACKFILL_NEW_TOKENS", "true").lower() in ("1", "true", "yes")

    session = requests.Session()
    print(f"[candidate-pull] Token List V3: sort_by={sort_by} min_liquidity={min_liquidity} "
          f"min_market_cap={min_market_cap} max={max_tokens}")
    items = fetch_token_list(session, api_key, chain_header, sort_by,
                             min_liquidity, min_market_cap, max_tokens)
    if not items:
        print("[candidate-pull] ERROR: Token List V3 returned no candidates", file=sys.stderr)
        return 1
    rows = [to_row(it, chain_store) for it in items if it.get("address")]
    discovered = [r["address"] for r in rows]
    print(f"[candidate-pull] discovered {len(rows)} candidates")

    client = bigquery.Client(project=project)
    inserted = append_snapshot(client, market_table, rows, location)
    print(f"[candidate-pull] persisted {inserted} rows -> {market_table}")

    if do_backfill:
        new_addrs = new_tokens_without_ohlcv(client, discovered, ohlcv_table, location)
        print(f"[candidate-pull] {len(new_addrs)} discovered token(s) have no OHLCV yet")
        if new_addrs:
            backfill_ohlcv(new_addrs, ohlcv_table)

    print("[candidate-pull] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
