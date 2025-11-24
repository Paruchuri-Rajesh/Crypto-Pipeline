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
FROM CAR_SALES_DBB.raw.coin_gecko_ohlc
WHERE coin_id = 'bitcoin'