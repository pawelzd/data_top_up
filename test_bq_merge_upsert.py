"""Unit tests for the pure (non-BigQuery) logic in bq_merge_upsert.py.

Deliberately does not touch bigquery.Client / merge_upsert() itself -- this
workspace has no BigQuery access in an unattended session. Run with:
    python3 -m unittest test_bq_merge_upsert.py -v
"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone

from bq_merge_upsert import KEY_COLUMNS, _coerce, _dedupe_last_wins, build_merge_sql


class DedupeLastWinsTests(unittest.TestCase):
    def test_keeps_last_row_for_duplicate_key(self):
        rows = [
            {"token_address": "A", "price_timestamp": "t1", "chain": "sol", "open": "1"},
            {"token_address": "A", "price_timestamp": "t1", "chain": "sol", "open": "2"},
            {"token_address": "B", "price_timestamp": "t1", "chain": "sol", "open": "3"},
        ]
        out = _dedupe_last_wins(rows)
        self.assertEqual(len(out), 2)
        by_addr = {r["token_address"]: r for r in out}
        self.assertEqual(by_addr["A"]["open"], "2")
        self.assertEqual(by_addr["B"]["open"], "3")

    def test_empty_input(self):
        self.assertEqual(_dedupe_last_wins([]), [])

    def test_distinct_chain_is_a_distinct_key(self):
        rows = [
            {"token_address": "A", "price_timestamp": "t1", "chain": "sol", "open": "1"},
            {"token_address": "A", "price_timestamp": "t1", "chain": "eth", "open": "2"},
        ]
        self.assertEqual(len(_dedupe_last_wins(rows)), 2)


class BuildMergeSqlTests(unittest.TestCase):
    def test_on_clause_covers_all_key_columns(self):
        sql = build_merge_sql("proj.ds.tbl", ["token_address", "price_timestamp", "open", "chain"])
        for col in KEY_COLUMNS:
            self.assertIn(f"T.{col} = S.{col}", sql)

    def test_insert_columns_and_values_match(self):
        columns = ["token_address", "price_timestamp", "open", "chain"]
        sql = build_merge_sql("proj.ds.tbl", columns)
        self.assertIn("INSERT (token_address, price_timestamp, open, chain)", sql)
        self.assertIn("VALUES (S.token_address, S.price_timestamp, S.open, S.chain)", sql)
        self.assertIn("USING (SELECT * FROM UNNEST(@rows)) S", sql)
        self.assertIn("WHEN NOT MATCHED THEN", sql)
        self.assertIn("MERGE `proj.ds.tbl` T", sql)


class CoerceTests(unittest.TestCase):
    def test_timestamp_string_becomes_datetime(self):
        iso = datetime(2026, 8, 12, 16, 0, tzinfo=timezone.utc).isoformat()
        out = _coerce("price_timestamp", iso)
        self.assertIsInstance(out, datetime)
        self.assertEqual(out, datetime(2026, 8, 12, 16, 0, tzinfo=timezone.utc))

    def test_non_timestamp_passthrough(self):
        self.assertEqual(_coerce("token_address", "abc"), "abc")
        self.assertEqual(_coerce("volume", "0.000000001"), "0.000000001")

    def test_none_stays_none(self):
        self.assertIsNone(_coerce("price_timestamp", None))
        self.assertIsNone(_coerce("volume", None))


if __name__ == "__main__":
    unittest.main()
