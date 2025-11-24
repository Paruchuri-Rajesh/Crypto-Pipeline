-- models/input/stg_btc_market.sql
-- Staging for daily BTC market data (trend inputs: price, market cap, volume)

SELECT
    timestamp,
    price,
    market_cap,
    volume,
    coin_id,
    date
FROM CAR_SALES_DBB.raw.coin_gecko_market_daily
WHERE coin_id = 'bitcoin'