#!/usr/bin/env python3
"""Backfill hourly Birdeye OHLCV candles into BigQuery.

Reads token addresses from tokens.json and appends missing hourly candles to a
BigQuery table with this schema:

token_address STRING, price_timestamp TIMESTAMP, close NUMERIC, high NUMERIC,
open NUMERIC, low NUMERIC, volume NUMERIC, chain STRING
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery


BIRDEYE_OHLCV_URL = "https://public-api.birdeye.so/defi/v3/ohlcv"
DEFAULT_START = "2026-02-05"
MAX_CANDLES_PER_REQUEST = 5000
SECONDS_PER_HOUR = 3600
BIGQUERY_NUMERIC_QUANT = Decimal("0.000000001")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill hourly Birdeye OHLCV data into BigQuery."
    )
    parser.add_argument("--tokens", default="tokens.json", help="Path to tokens JSON.")
    parser.add_argument(
        "--tokens-from-bigquery",
        action="store_true",
        help=(
            "In normal backfill mode, read distinct token_address values from "
            "the target BigQuery table instead of --tokens."
        ),
    )
    parser.add_argument(
        "--tokens-query",
        default=os.getenv("BIGQUERY_TOKENS_QUERY"),
        help=(
            "In normal backfill mode, read token_address values from this "
            "BigQuery SQL query instead of --tokens. Defaults to "
            "BIGQUERY_TOKENS_QUERY."
        ),
    )
    parser.add_argument(
        "--gaps-csv",
        help=(
            "Path to a BigQuery gap CSV with token_address, chain, missing_from, "
            "and missing_to columns. When set, only these ranges are backfilled."
        ),
    )
    parser.add_argument(
        "--detect-gaps",
        action="store_true",
        help=(
            "Query BigQuery for gaps larger than --min-gap-hours and backfill "
            "those ranges directly, without using --gaps-csv."
        ),
    )
    parser.add_argument(
        "--min-gap-hours",
        type=int,
        default=24,
        help="Minimum gap size, in hours, for --detect-gaps. Default: 24.",
    )
    parser.add_argument(
        "--remaining-gaps-csv",
        help=(
            "Write the still-missing gap ranges after checking BigQuery. "
            "Useful for resuming from a smaller CSV."
        ),
    )
    parser.add_argument(
        "--remaining-gaps-only",
        action="store_true",
        help=(
            "Only compute --remaining-gaps-csv from --gaps-csv or "
            "--detect-gaps, then exit. No Birdeye API key or fetch is required."
        ),
    )
    parser.add_argument(
        "--table",
        default=os.getenv("BIGQUERY_TABLE"),
        help="Target BigQuery table as project.dataset.table. Defaults to BIGQUERY_TABLE.",
    )
    parser.add_argument(
        "--bigquery-location",
        default=os.getenv("BIGQUERY_LOCATION", "europe-central2"),
        help="BigQuery job location. Default: BIGQUERY_LOCATION or europe-central2.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("BIRDEYE_API_KEY"),
        help="Birdeye API key. Defaults to BIRDEYE_API_KEY.",
    )
    parser.add_argument(
        "--chain",
        default=os.getenv("CHAIN", "sol"),
        help="Chain value stored in BigQuery. Default: CHAIN or sol.",
    )
    parser.add_argument(
        "--api-chain",
        default=os.getenv("BIRDEYE_CHAIN", "solana"),
        help="Chain header sent to Birdeye. Default: BIRDEYE_CHAIN or solana.",
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START,
        help=f"UTC start date/time, inclusive. Default: {DEFAULT_START}",
    )
    parser.add_argument(
        "--end-date",
        help=(
            "UTC end date/time, exclusive. Defaults to the current UTC hour, "
            "so the in-progress hourly candle is skipped."
        ),
    )
    parser.add_argument(
        "--currency",
        default="usd",
        choices=("usd", "native"),
        help="Birdeye OHLCV currency.",
    )
    parser.add_argument(
        "--insert-batch-size",
        type=int,
        default=50000,
        help="Rows per BigQuery append job.",
    )
    parser.add_argument(
        "--flush-row-threshold",
        type=int,
        default=25000,
        help=(
            "Buffer rows until this many are pending before writing to "
            "BigQuery. Keeps large runs under the per-table update quota. "
            "Use 1 to write after every token. Default: 25000."
        ),
    )
    parser.add_argument(
        "--rate-limit-rpm",
        type=float,
        default=60.0,
        help="Maximum Birdeye requests per minute. Default: 60.",
    )
    parser.add_argument(
        "--request-sleep",
        type=float,
        help="Deprecated alias for a fixed sleep between Birdeye requests.",
    )
    parser.add_argument(
        "--limit-tokens",
        type=int,
        help="Process only the first N tokens. Useful for testing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and compare data, but do not write rows to BigQuery.",
    )
    parser.add_argument(
        "--skip-existing-check",
        action="store_true",
        help=(
            "Do not query the target table before loading. This can create duplicate "
            "rows and should only be used when you cannot read the table."
        ),
    )
    return parser.parse_args()


UTC = timezone.utc


def parse_utc(value: Optional[str], default_to_current_hour: bool = False) -> datetime:
    if value is None:
        if not default_to_current_hour:
            raise ValueError("missing date/time value")
        now = datetime.now(UTC)
        return now.replace(minute=0, second=0, microsecond=0)

    normalized = value.strip()
    if not normalized:
        raise ValueError("empty date/time value")
    if normalized.endswith(" UTC"):
        normalized = normalized[: -len(" UTC")] + "+00:00"

    try:
        if len(normalized) == 10:
            dt = datetime.strptime(normalized, "%Y-%m-%d")
        else:
            normalized = normalized.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%d-%b-%Y", "%d-%B-%Y"):
            try:
                dt = datetime.strptime(value.strip(), fmt)
                break
            except ValueError:
                continue
        else:
            raise

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def load_token_addresses(path: str, limit: Optional[int]) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    addresses: List[str] = []
    if not isinstance(payload, list):
        raise ValueError(f"{path} must contain a JSON array")

    for item in payload:
        if isinstance(item, str):
            address = item
        elif isinstance(item, dict):
            address = item.get("address") or item.get("token_address")
        else:
            address = None

        if not isinstance(address, str) or not address.strip():
            raise ValueError(f"invalid token entry: {item!r}")
        addresses.append(address.strip())

    deduped = list(dict.fromkeys(addresses))
    if limit is not None:
        return deduped[:limit]
    return deduped


def fetch_token_addresses_from_bigquery(
    client: bigquery.Client,
    table: str,
    chain: str,
    location: str,
    limit: Optional[int],
) -> List[str]:
    query = f"""
        SELECT DISTINCT token_address
        FROM `{table}`
        WHERE chain = @chain
          AND token_address IS NOT NULL
          AND token_address != ''
        ORDER BY token_address
    """
    query_parameters = [
        bigquery.ScalarQueryParameter("chain", "STRING", chain),
    ]
    if limit is not None:
        query += "\n        LIMIT @limit"
        query_parameters.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    return [
        row.token_address
        for row in client.query(
            query, job_config=job_config, location=location
        ).result()
    ]


def fetch_token_addresses_from_query(
    client: bigquery.Client,
    query: str,
    location: str,
    limit: Optional[int],
) -> List[str]:
    wrapped_query = f"""
        SELECT DISTINCT token_address
        FROM ({query})
        WHERE token_address IS NOT NULL
          AND token_address != ''
        ORDER BY token_address
    """
    query_parameters = []
    if limit is not None:
        wrapped_query += "\n        LIMIT @limit"
        query_parameters.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    return [
        row.token_address
        for row in client.query(
            wrapped_query, job_config=job_config, location=location
        ).result()
    ]


def load_gap_tasks(
    path: str,
    default_chain: str,
    limit_tokens: Optional[int],
) -> Tuple[List[str], List[Tuple[str, str, datetime, datetime]]]:
    token_order: List[str] = []
    seen_tokens: Set[str] = set()
    tasks: List[Tuple[str, str, datetime, datetime]] = []

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required_columns = {"token_address", "missing_from", "missing_to"}
        missing_columns = required_columns.difference(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                f"{path} is missing required columns: {sorted(missing_columns)}"
            )

        for row in reader:
            token_address = (row.get("token_address") or "").strip()
            if not token_address:
                continue
            if limit_tokens is not None and token_address not in seen_tokens:
                if len(token_order) >= limit_tokens:
                    continue

            chain = (row.get("chain") or default_chain).strip() or default_chain
            start_dt = parse_utc(row.get("missing_from"))
            # CSV missing_to is the last missing hourly candle, so make it exclusive.
            end_dt = parse_utc(row.get("missing_to")).replace(
                minute=0, second=0, microsecond=0
            ) + timedelta(hours=1)
            if end_dt <= start_dt:
                continue

            if token_address not in seen_tokens:
                seen_tokens.add(token_address)
                token_order.append(token_address)
            tasks.append((token_address, chain, start_dt, end_dt))

    return token_order, tasks


def detect_gap_tasks_from_bigquery(
    client: bigquery.Client,
    table: str,
    chain: str,
    min_gap_hours: int,
    location: str,
    limit_tokens: Optional[int],
) -> Tuple[List[str], List[Tuple[str, str, datetime, datetime]]]:
    query = f"""
        WITH points AS (
          SELECT DISTINCT
            token_address,
            chain,
            price_timestamp
          FROM `{table}`
          WHERE price_timestamp IS NOT NULL
            AND chain = @chain
        ),

        ordered AS (
          SELECT
            token_address,
            chain,
            price_timestamp,
            LAG(price_timestamp) OVER (
              PARTITION BY token_address, chain
              ORDER BY price_timestamp
            ) AS previous_timestamp
          FROM points
        )

        SELECT
          token_address,
          chain,
          TIMESTAMP_ADD(previous_timestamp, INTERVAL 1 HOUR) AS missing_from,
          TIMESTAMP_SUB(price_timestamp, INTERVAL 1 HOUR) AS missing_to,
          TIMESTAMP_DIFF(price_timestamp, previous_timestamp, HOUR) - 1
            AS missing_hour_count
        FROM ordered
        WHERE previous_timestamp IS NOT NULL
          AND TIMESTAMP_DIFF(price_timestamp, previous_timestamp, HOUR) > @min_gap_hours
        ORDER BY missing_hour_count DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("chain", "STRING", chain),
            bigquery.ScalarQueryParameter("min_gap_hours", "INT64", min_gap_hours),
        ]
    )

    token_order: List[str] = []
    seen_tokens: Set[str] = set()
    tasks: List[Tuple[str, str, datetime, datetime]] = []

    for row in client.query(query, job_config=job_config, location=location).result():
        token_address = row.token_address
        if limit_tokens is not None and token_address not in seen_tokens:
            if len(token_order) >= limit_tokens:
                continue

        if token_address not in seen_tokens:
            seen_tokens.add(token_address)
            token_order.append(token_address)

        start_dt = row.missing_from.astimezone(UTC)
        # Query returns the last missing hourly candle; make it exclusive.
        end_dt = row.missing_to.astimezone(UTC) + timedelta(hours=1)
        tasks.append((token_address, row.chain, start_dt, end_dt))

    return token_order, tasks


def fetch_existing_keys(
    client: bigquery.Client,
    table: str,
    token_addresses: List[str],
    chain: Optional[str],
    start_dt: datetime,
    end_dt: datetime,
    location: str,
) -> Set[Tuple[str, int, str]]:
    if not token_addresses:
        return set()

    query = f"""
        SELECT
          token_address,
          UNIX_SECONDS(price_timestamp) AS price_ts,
          chain
        FROM `{table}`
        WHERE price_timestamp >= @start_dt
          AND price_timestamp < @end_dt
          AND token_address IN UNNEST(@token_addresses)
    """
    if chain is not None:
        query += "\n          AND chain = @chain"
    query_parameters = [
        bigquery.ScalarQueryParameter("start_dt", "TIMESTAMP", start_dt),
        bigquery.ScalarQueryParameter("end_dt", "TIMESTAMP", end_dt),
        bigquery.ArrayQueryParameter("token_addresses", "STRING", token_addresses),
    ]
    if chain is not None:
        query_parameters.append(bigquery.ScalarQueryParameter("chain", "STRING", chain))

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )

    return {
        (row.token_address, int(row.price_ts), row.chain)
        for row in client.query(
            query, job_config=job_config, location=location
        ).result()
    }


def fetch_matching_chain_counts(
    client: bigquery.Client,
    table: str,
    token_addresses: List[str],
    start_dt: datetime,
    end_dt: datetime,
    location: str,
) -> List[dict]:
    if not token_addresses:
        return []

    query = f"""
        SELECT
          chain,
          COUNT(*) AS row_count,
          MIN(price_timestamp) AS min_price_timestamp,
          MAX(price_timestamp) AS max_price_timestamp
        FROM `{table}`
        WHERE price_timestamp >= @start_dt
          AND price_timestamp < @end_dt
          AND token_address IN UNNEST(@token_addresses)
        GROUP BY chain
        ORDER BY row_count DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_dt", "TIMESTAMP", start_dt),
            bigquery.ScalarQueryParameter("end_dt", "TIMESTAMP", end_dt),
            bigquery.ArrayQueryParameter("token_addresses", "STRING", token_addresses),
        ]
    )

    return [
        {
            "chain": row.chain,
            "row_count": row.row_count,
            "min_price_timestamp": row.min_price_timestamp,
            "max_price_timestamp": row.max_price_timestamp,
        }
        for row in client.query(
            query, job_config=job_config, location=location
        ).result()
    ]


def iter_hour_timestamps(start_dt: datetime, end_dt: datetime) -> List[int]:
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    return list(range(start_ts, end_ts, SECONDS_PER_HOUR))


def find_remaining_tasks(
    token_address: str,
    tasks: List[Tuple[str, datetime, datetime]],
    existing_keys: Set[Tuple[str, int, str]],
) -> List[Tuple[str, datetime, datetime]]:
    remaining: List[Tuple[str, datetime, datetime]] = []

    for chain, start_dt, end_dt in tasks:
        missing_start_ts: Optional[int] = None
        previous_missing_ts: Optional[int] = None

        for ts in iter_hour_timestamps(start_dt, end_dt):
            key = (token_address, ts, chain)
            if key not in existing_keys:
                if missing_start_ts is None:
                    missing_start_ts = ts
                previous_missing_ts = ts
                continue

            if missing_start_ts is not None and previous_missing_ts is not None:
                remaining.append(
                    (
                        chain,
                        datetime.fromtimestamp(missing_start_ts, UTC),
                        datetime.fromtimestamp(previous_missing_ts + SECONDS_PER_HOUR, UTC),
                    )
                )
                missing_start_ts = None
                previous_missing_ts = None

        if missing_start_ts is not None and previous_missing_ts is not None:
            remaining.append(
                (
                    chain,
                    datetime.fromtimestamp(missing_start_ts, UTC),
                    datetime.fromtimestamp(previous_missing_ts + SECONDS_PER_HOUR, UTC),
                )
            )

    return remaining


def task_hour_count(tasks: List[Tuple[str, datetime, datetime]]) -> int:
    total = 0
    for _chain, start_dt, end_dt in tasks:
        total += int((end_dt - start_dt).total_seconds() // SECONDS_PER_HOUR)
    return total


def write_gap_tasks_csv(
    path: str,
    rows: List[Tuple[str, str, datetime, datetime]],
) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "token_address",
                "chain",
                "missing_from",
                "missing_to",
                "missing_hour_count",
            ],
        )
        writer.writeheader()
        for token_address, chain, start_dt, end_dt in rows:
            inclusive_end = end_dt - timedelta(hours=1)
            writer.writerow(
                {
                    "token_address": token_address,
                    "chain": chain,
                    "missing_from": start_dt.isoformat(),
                    "missing_to": inclusive_end.isoformat(),
                    "missing_hour_count": int(
                        (end_dt - start_dt).total_seconds() // SECONDS_PER_HOUR
                    ),
                }
            )


def iter_time_windows(start_dt: datetime, end_dt: datetime) -> List[Tuple[int, int]]:
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    max_window_seconds = MAX_CANDLES_PER_REQUEST * SECONDS_PER_HOUR

    windows: List[Tuple[int, int]] = []
    cursor = start_ts
    while cursor < end_ts:
        window_end = min(cursor + max_window_seconds, end_ts)
        windows.append((cursor, window_end))
        cursor = window_end
    return windows


def request_birdeye_ohlcv(
    session: requests.Session,
    api_key: str,
    token_address: str,
    chain: str,
    currency: str,
    time_from: int,
    time_to: int,
    max_attempts: int = 5,
) -> List[dict]:
    headers = {
        "X-API-KEY": api_key,
        "x-chain": chain,
        "accept": "application/json",
    }
    params = {
        "address": token_address,
        "type": "1H",
        "currency": currency,
        "time_from": time_from,
        "time_to": time_to,
        "mode": "range",
    }

    for attempt in range(1, max_attempts + 1):
        response = session.get(
            BIRDEYE_OHLCV_URL, headers=headers, params=params, timeout=30
        )
        if response.status_code == 429 or 500 <= response.status_code < 600:
            if attempt == max_attempts:
                break
            retry_after = response.headers.get("Retry-After")
            sleep_seconds = float(retry_after) if retry_after else min(2**attempt, 30)
            time.sleep(sleep_seconds)
            continue

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"Birdeye HTTP {response.status_code} for {token_address} "
                f"from {time_from} to {time_to}: {response.text}"
            ) from exc
        payload = response.json()
        if payload.get("success") is False:
            raise RuntimeError(f"Birdeye returned success=false: {payload}")
        return extract_ohlcv_items(payload)

    response.raise_for_status()
    raise RuntimeError(f"Birdeye request failed after {max_attempts} attempts")


def extract_ohlcv_items(payload: dict) -> List[dict]:
    data = payload.get("data")
    if isinstance(data, dict):
        items = data.get("items") or data.get("list") or data.get("ohlcv")
    else:
        items = data

    if items is None:
        return []
    if not isinstance(items, list):
        raise ValueError(f"unexpected Birdeye OHLCV response shape: {payload}")
    return items


def first_present(item: dict, names: Tuple[str, ...]) -> Any:
    for name in names:
        if name in item and item[name] is not None:
            return item[name]
    return None


def parse_candle_timestamp(item: dict) -> Optional[int]:
    value = first_present(
        item,
        (
            "unixTime",
            "unix_time",
            "time",
            "timestamp",
            "price_timestamp",
            "t",
        ),
    )
    if value is None:
        return None

    if isinstance(value, str) and not value.isdigit():
        dt = parse_utc(value)
        return int(dt.timestamp())

    ts = int(value)
    if ts > 10_000_000_000:
        ts = ts // 1000
    return ts


def to_numeric_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"invalid numeric value from Birdeye: {value!r}") from None
    if not parsed.is_finite():
        raise ValueError(f"non-finite numeric value from Birdeye: {value!r}")
    if parsed.as_tuple().exponent < -9:
        parsed = parsed.quantize(BIGQUERY_NUMERIC_QUANT)
    return format(parsed, "f")


def to_bigquery_row(
    token_address: str,
    chain: str,
    candle: dict,
    start_ts: int,
    end_ts: int,
) -> Optional[Tuple[Tuple[str, int, str], dict]]:
    candle_ts = parse_candle_timestamp(candle)
    if candle_ts is None or candle_ts < start_ts or candle_ts >= end_ts:
        return None

    row = {
        "token_address": token_address,
        "price_timestamp": datetime.fromtimestamp(candle_ts, UTC).isoformat(),
        "close": to_numeric_string(first_present(candle, ("c", "close"))),
        "high": to_numeric_string(first_present(candle, ("h", "high"))),
        "open": to_numeric_string(first_present(candle, ("o", "open"))),
        "low": to_numeric_string(first_present(candle, ("l", "low"))),
        "volume": to_numeric_string(first_present(candle, ("v", "volume"))),
        "chain": chain,
    }
    return (token_address, candle_ts, chain), row


def append_rows(
    client: bigquery.Client,
    table: str,
    rows: List[dict],
    batch_size: int,
    location: str,
) -> int:
    inserted = 0
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        job = client.load_table_from_json(
            batch, table, job_config=job_config, location=location
        )
        job.result()
        inserted += len(batch)

    return inserted


def flush_pending_rows(
    client: bigquery.Client,
    table: str,
    rows: List[dict],
    batch_size: int,
    location: str,
    dry_run: bool,
    reason: str,
) -> int:
    if not rows:
        print(f"No pending rows to write {reason}.")
        return 0

    if dry_run:
        print(f"Dry run enabled; {len(rows)} pending rows were not written {reason}.")
        rows.clear()
        return 0

    print(f"Writing {len(rows)} pending rows to BigQuery {reason}...")
    inserted = append_rows(client, table, rows, batch_size, location)
    rows.clear()
    print(f"Inserted {inserted} rows into {table}.")
    return inserted


def main() -> int:
    args = parse_args()
    gap_mode = bool(args.gaps_csv or args.detect_gaps)
    if args.gaps_csv and args.detect_gaps:
        print("Use either --gaps-csv or --detect-gaps, not both.", file=sys.stderr)
        return 2
    if args.tokens_from_bigquery and gap_mode:
        print(
            "--tokens-from-bigquery is only used in normal backfill mode; "
            "gap modes derive tokens from the detected/provided gaps.",
            file=sys.stderr,
        )
        return 2
    if args.tokens_query and gap_mode:
        print(
            "--tokens-query is only used in normal backfill mode; "
            "gap modes derive tokens from the detected/provided gaps.",
            file=sys.stderr,
        )
        return 2
    if args.tokens_from_bigquery and args.tokens_query:
        print(
            "Use either --tokens-from-bigquery or --tokens-query, not both.",
            file=sys.stderr,
        )
        return 2
    if args.remaining_gaps_only and not gap_mode:
        print(
            "--remaining-gaps-only requires --gaps-csv or --detect-gaps",
            file=sys.stderr,
        )
        return 2
    if args.remaining_gaps_only and not args.remaining_gaps_csv:
        print("--remaining-gaps-only requires --remaining-gaps-csv", file=sys.stderr)
        return 2
    if not args.api_key and not args.remaining_gaps_only:
        print("BIRDEYE_API_KEY or --api-key is required", file=sys.stderr)
        return 2
    if not args.table:
        print("BIGQUERY_TABLE or --table is required", file=sys.stderr)
        return 2
    if args.insert_batch_size <= 0:
        print("--insert-batch-size must be positive", file=sys.stderr)
        return 2

    tasks_by_token: Dict[str, List[Tuple[str, datetime, datetime]]] = {}
    client = bigquery.Client(location=args.bigquery_location)
    if args.gaps_csv:
        token_addresses, gap_tasks = load_gap_tasks(
            args.gaps_csv, args.chain, args.limit_tokens
        )
        if not gap_tasks:
            print(f"No gap tasks found in {args.gaps_csv}", file=sys.stderr)
            return 2
        for token_address, chain, task_start_dt, task_end_dt in gap_tasks:
            tasks_by_token.setdefault(token_address, []).append(
                (chain, task_start_dt, task_end_dt)
            )
        start_dt = min(task[2] for task in gap_tasks)
        end_dt = max(task[3] for task in gap_tasks)
    elif args.detect_gaps:
        print(
            f"Detecting gaps in {args.table} for chain={args.chain!r} "
            f"larger than {args.min_gap_hours} hours..."
        )
        try:
            token_addresses, gap_tasks = detect_gap_tasks_from_bigquery(
                client,
                args.table,
                args.chain,
                args.min_gap_hours,
                args.bigquery_location,
                args.limit_tokens,
            )
        except Forbidden as exc:
            print(
                f"BigQuery denied access while detecting gaps in {args.table}.",
                file=sys.stderr,
            )
            print(f"Original error: {exc}", file=sys.stderr)
            return 1
        except NotFound as exc:
            print(
                f"BigQuery could not find {args.table} in location "
                f"{args.bigquery_location}.",
                file=sys.stderr,
            )
            print(f"Original error: {exc}", file=sys.stderr)
            return 1
        if not gap_tasks:
            print("No matching gaps found.")
            return 0
        for token_address, chain, task_start_dt, task_end_dt in gap_tasks:
            tasks_by_token.setdefault(token_address, []).append(
                (chain, task_start_dt, task_end_dt)
            )
        start_dt = min(task[2] for task in gap_tasks)
        end_dt = max(task[3] for task in gap_tasks)
    else:
        start_dt = parse_utc(args.start_date)
        end_dt = parse_utc(args.end_date, default_to_current_hour=True)
        if end_dt <= start_dt:
            print("--end-date must be after --start-date", file=sys.stderr)
            return 2
        if args.tokens_query:
            print("Reading token addresses from --tokens-query...")
            try:
                token_addresses = fetch_token_addresses_from_query(
                    client,
                    args.tokens_query,
                    args.bigquery_location,
                    args.limit_tokens,
                )
            except Forbidden as exc:
                print(
                    "BigQuery denied access while reading tokens from "
                    "--tokens-query.",
                    file=sys.stderr,
                )
                print(f"Original error: {exc}", file=sys.stderr)
                return 1
            except NotFound as exc:
                print(
                    "BigQuery could not resolve a table or view referenced by "
                    "--tokens-query.",
                    file=sys.stderr,
                )
                print(f"Original error: {exc}", file=sys.stderr)
                return 1
            if not token_addresses:
                print("No tokens returned by --tokens-query.", file=sys.stderr)
                return 2
        elif args.tokens_from_bigquery:
            print(
                f"Reading token addresses from {args.table} "
                f"for chain={args.chain!r}..."
            )
            try:
                token_addresses = fetch_token_addresses_from_bigquery(
                    client,
                    args.table,
                    args.chain,
                    args.bigquery_location,
                    args.limit_tokens,
                )
            except Forbidden as exc:
                print(
                    f"BigQuery denied access while reading tokens from {args.table}.",
                    file=sys.stderr,
                )
                print(f"Original error: {exc}", file=sys.stderr)
                return 1
            except NotFound as exc:
                print(
                    f"BigQuery could not find {args.table} in location "
                    f"{args.bigquery_location}.",
                    file=sys.stderr,
                )
                print(f"Original error: {exc}", file=sys.stderr)
                return 1
            if not token_addresses:
                print(
                    f"No tokens found in {args.table} for chain={args.chain!r}.",
                    file=sys.stderr,
                )
                return 2
        else:
            token_addresses = load_token_addresses(args.tokens, args.limit_tokens)
        for token_address in token_addresses:
            tasks_by_token[token_address] = [(args.chain, start_dt, end_dt)]

    print(
        f"Processing {len(token_addresses)} tokens from "
        f"{start_dt.isoformat()} to {end_dt.isoformat()}."
    )
    if args.gaps_csv:
        print(f"Gap CSV: {args.gaps_csv}; gap ranges: {sum(len(v) for v in tasks_by_token.values())}")
    if args.detect_gaps:
        print(f"Detected gap ranges: {sum(len(v) for v in tasks_by_token.values())}")
    if args.tokens_query:
        print("Token source: --tokens-query")
    elif args.tokens_from_bigquery:
        print("Token source: BigQuery")
    elif not gap_mode:
        print(f"Token source: {args.tokens}")
    print(
        f"Target BigQuery table: {args.table}; "
        f"job location: {args.bigquery_location}"
    )
    print(f"BigQuery chain value: {args.chain}; Birdeye API chain: {args.api_chain}")

    existing_keys: Set[Tuple[str, int, str]] = set()
    if args.skip_existing_check:
        print("Skipping existing-row check; duplicate rows may be appended.")
    elif gap_mode:
        print("Gap mode: existing-row checks will run one token at a time.")
    else:
        print("Reading existing BigQuery keys...")
        try:
            existing_keys = fetch_existing_keys(
                client,
                args.table,
                token_addresses,
                args.chain,
                start_dt,
                end_dt,
                args.bigquery_location,
            )
        except Forbidden as exc:
            print(
                "BigQuery denied read access to the target table. To skip rows "
                "that already exist, the credentials running this script need "
                f"permission to query {args.table}. Grant BigQuery Data Viewer "
                "on the dataset/table, or run with --skip-existing-check if you "
                "accept possible duplicates.",
                file=sys.stderr,
            )
            print(f"Original error: {exc}", file=sys.stderr)
            return 1
        except NotFound as exc:
            print(
                f"BigQuery could not find {args.table} in location "
                f"{args.bigquery_location}. Check BIGQUERY_TABLE and "
                "BIGQUERY_LOCATION.",
                file=sys.stderr,
            )
            print(f"Original error: {exc}", file=sys.stderr)
            return 1
    if not gap_mode:
        print(f"Found {len(existing_keys)} existing hourly candles.")
    if not gap_mode and not args.skip_existing_check and not existing_keys:
        matching_chain_counts = fetch_matching_chain_counts(
            client,
            args.table,
            token_addresses,
            start_dt,
            end_dt,
            args.bigquery_location,
        )
        if matching_chain_counts:
            print(
                "No rows matched the exact chain filter, but matching "
                "token/date rows exist with these chain values:"
            )
            for item in matching_chain_counts:
                print(
                    "  "
                    f"chain={item['chain']!r}, rows={item['row_count']}, "
                    f"min={item['min_price_timestamp']}, "
                    f"max={item['max_price_timestamp']}"
                )
        else:
            print(
                "No rows matched these token addresses in the requested "
                "date range, even without the chain filter."
            )

    pending_rows: List[dict] = []
    seen_keys: Set[Tuple[str, int, str]] = set(existing_keys)
    fetched_candles = 0
    total_new_rows = 0
    total_inserted_rows = 0
    remaining_gap_rows: List[Tuple[str, str, datetime, datetime]] = []
    min_request_interval = 60.0 / args.rate_limit_rpm if args.rate_limit_rpm else 0.0
    if args.request_sleep is not None:
        min_request_interval = args.request_sleep

    try:
        with requests.Session() as session:
            for index, token_address in enumerate(token_addresses, start=1):
                token_new_rows = 0
                token_tasks = tasks_by_token[token_address]
                fetch_tasks = token_tasks
                if gap_mode and not args.skip_existing_check:
                    token_start_dt = min(task[1] for task in token_tasks)
                    token_end_dt = max(task[2] for task in token_tasks)
                    print(
                        f"[{index}/{len(token_addresses)}] {token_address}: "
                        "reading existing BigQuery keys..."
                    )
                    token_existing_keys = fetch_existing_keys(
                        client,
                        args.table,
                        [token_address],
                        None,
                        token_start_dt,
                        token_end_dt,
                        args.bigquery_location,
                    )
                    seen_keys.update(token_existing_keys)
                    fetch_tasks = find_remaining_tasks(
                        token_address, token_tasks, token_existing_keys
                    )
                    remaining_gap_rows.extend(
                        (token_address, chain, task_start_dt, task_end_dt)
                        for chain, task_start_dt, task_end_dt in fetch_tasks
                    )
                    print(
                        f"[{index}/{len(token_addresses)}] {token_address}: "
                        f"found {len(token_existing_keys)} existing hourly candles; "
                        f"{len(fetch_tasks)} remaining ranges, "
                        f"{task_hour_count(fetch_tasks)} remaining hours"
                    )
                elif gap_mode and args.remaining_gaps_csv:
                    remaining_gap_rows.extend(
                        (token_address, chain, task_start_dt, task_end_dt)
                        for chain, task_start_dt, task_end_dt in fetch_tasks
                    )

                if args.remaining_gaps_only:
                    continue

                for row_chain, task_start_dt, task_end_dt in fetch_tasks:
                    task_start_ts = int(task_start_dt.timestamp())
                    task_end_ts = int(task_end_dt.timestamp())
                    for time_from, time_to in iter_time_windows(task_start_dt, task_end_dt):
                        items = request_birdeye_ohlcv(
                            session=session,
                            api_key=args.api_key,
                            token_address=token_address,
                            chain=args.api_chain,
                            currency=args.currency,
                            time_from=time_from,
                            time_to=time_to,
                        )
                        fetched_candles += len(items)

                        for candle in items:
                            parsed = to_bigquery_row(
                                token_address,
                                row_chain,
                                candle,
                                task_start_ts,
                                task_end_ts,
                            )
                            if parsed is None:
                                continue
                            key, row = parsed
                            if key in seen_keys:
                                continue
                            seen_keys.add(key)
                            pending_rows.append(row)
                            token_new_rows += 1

                        if min_request_interval:
                            time.sleep(min_request_interval)

                print(
                    f"[{index}/{len(token_addresses)}] {token_address}: "
                    f"{token_new_rows} new rows"
                )
                total_new_rows += token_new_rows
                if len(pending_rows) >= args.flush_row_threshold:
                    total_inserted_rows += flush_pending_rows(
                        client,
                        args.table,
                        pending_rows,
                        args.insert_batch_size,
                        args.bigquery_location,
                        args.dry_run,
                        f"after token {index}/{len(token_addresses)}",
                    )
    except (Exception, KeyboardInterrupt) as exc:
        print(
            f"Backfill interrupted by {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        try:
            flush_pending_rows(
                client,
                args.table,
                pending_rows,
                args.insert_batch_size,
                args.bigquery_location,
                args.dry_run,
                "after the error",
            )
        except Exception as write_exc:
            print(
                f"Failed to write pending rows after the error: {write_exc}",
                file=sys.stderr,
            )
        return 1

    if args.remaining_gaps_csv:
        write_gap_tasks_csv(args.remaining_gaps_csv, remaining_gap_rows)
        print(
            f"Wrote {len(remaining_gap_rows)} remaining gap ranges "
            f"({task_hour_count([(chain, start_dt, end_dt) for _token, chain, start_dt, end_dt in remaining_gap_rows])} hours) "
            f"to {args.remaining_gaps_csv}."
        )

    if args.remaining_gaps_only:
        return 0

    print(
        f"Fetched {fetched_candles} candles; "
        f"{total_new_rows} new rows identified; "
        f"{total_inserted_rows} rows inserted."
    )

    if pending_rows:
        flush_pending_rows(
            client,
            args.table,
            pending_rows,
            args.insert_batch_size,
            args.bigquery_location,
            args.dry_run,
            "after successful fetch",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
