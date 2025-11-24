-- models/input/stg_btc_ohlc.sql
-- Staging for daily BTC candlestick data (volatility inputs)

SELECT
    timestamp,
    open,
    high,
    low,
    close,
    coin_id,
    date
FROM {{ source('raw', 'coin_gecko_ohlc') }}
WHERE coin_id = '{{ var("coin_id", "bitcoin") }}'
