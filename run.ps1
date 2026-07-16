$env:BIRDEYE_API_KEY="<set-me>"
$env:BIGQUERY_TABLE="crypto-trading-474111.core.token_ohlcv_no_partition"
$env:BIGQUERY_LOCATION="europe-central2"
$env:CHAIN="sol"
$env:BIRDEYE_CHAIN="solana"

#.\venv\Scripts\python.exe .\backfill_birdeye_ohlcv.py `
#  --tokens-query 'SELECT DISTINCT token_address FROM `crypto-trading-474111.20m_eval.rl_inference_features_next_open_v`' `
#  --start-date 2025-12-09 --rate-limit-rpm 900
  
  
.\venv\Scripts\python.exe .\backfill_birdeye_ohlcv.py --gaps-csv ".\remaining_gaps.csv" --rate-limit-rpm 900 --skip-existing-check --start-date 2025-12-01 