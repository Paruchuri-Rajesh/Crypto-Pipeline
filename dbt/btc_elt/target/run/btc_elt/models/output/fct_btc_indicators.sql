
  
    

        create or replace transient table CAR_SALES_DBB.analytics.fct_btc_indicators
         as
        (-- models/output/fct_btc_indicators.sql
-- Goal: BI-ready table for “Bitcoin’s market behavior, volatility, and price trends”.

WITH  __dbt__cte__stg_btc_market as (
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
),  __dbt__cte__stg_btc_ohlc as (
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
), market_data AS (
    -- Staging: BTC daily trend inputs (may contain multiple rows per calendar date)
    SELECT * FROM __dbt__cte__stg_btc_market
),
market_ranked AS (
    -- Rank rows per date so we can take the LAST (latest timestamp) for that date
    SELECT
        date,
        timestamp,
        price,
        market_cap,
        volume,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY timestamp DESC) AS rn_last
    FROM market_data
),
market_daily AS (
    -- Exactly one row per date from market:
    -- take the row with latest timestamp (rn_last = 1)
    SELECT
        date,
        timestamp,
        price,
        market_cap,
        volume
    FROM market_ranked
    WHERE rn_last = 1
),
ohlc_data AS (
    -- Staging: BTC candlestick inputs (may contain multiple rows per date)
    SELECT * FROM __dbt__cte__stg_btc_ohlc
),
ohlc_ranked AS (
    -- Rank rows per date so we can pick the first (open) and last (close)
    SELECT
        date,
        timestamp,
        open,
        high,
        low,
        close,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY timestamp ASC)  AS rn_open,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY timestamp DESC) AS rn_close
    FROM ohlc_data
),
ohlc_daily AS (
    -- One row per date from OHLC:
    --   open  = earliest ts, close = latest ts, high=max, low=min
    SELECT
        d.date,
        MIN(d.low)  AS low_price,
        MAX(d.high) AS high_price,
        MAX(CASE WHEN d.rn_open  = 1 THEN d.open  END) AS open_price,
        MAX(CASE WHEN d.rn_close = 1 THEN d.close END) AS close_price
    FROM ohlc_ranked d
    GROUP BY d.date
),
joined_data AS (
    -- Join de-duplicated market + OHLC on date → keep only dates present in BOTH (clean BI)
    SELECT
        m.date         AS trading_date,
        m.price,
        m.market_cap,
        m.volume,
        o.open_price,
        o.high_price,
        o.low_price,
        o.close_price
    FROM market_daily m
    INNER JOIN ohlc_daily o
      ON m.date = o.date
),
moving_averages AS (
    -- Price trends: short & medium moving averages (7d and 30d)
    SELECT
        *,
        AVG(price) OVER (ORDER BY trading_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS avg_price_7d,
        AVG(price) OVER (ORDER BY trading_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_price_30d
    FROM joined_data
),
rsi_and_deltas AS (
    -- Prepare RSI + momentum inputs: yesterday’s price and simple daily return
    SELECT
        *,
        LAG(price) OVER (ORDER BY trading_date) AS previous_price,
        CASE
            WHEN LAG(price) OVER (ORDER BY trading_date) IS NULL THEN NULL
            WHEN LAG(price) OVER (ORDER BY trading_date) = 0     THEN NULL
            ELSE (price - LAG(price) OVER (ORDER BY trading_date))
                 /  LAG(price) OVER (ORDER BY trading_date)
        END AS daily_return_pct
    FROM moving_averages
),
gains_losses AS (
    -- Split daily price change into gain/loss (non-negative) for RSI math
    SELECT
        *,
        CASE WHEN price > previous_price THEN price - previous_price ELSE 0 END AS gain,
        CASE WHEN price < previous_price THEN previous_price - price ELSE 0 END AS loss
    FROM rsi_and_deltas
),
rsi_window AS (
    -- RSI(14): rolling averages of gains and losses over last 14 days
    SELECT
        trading_date, price, market_cap, volume,
        open_price, high_price, low_price, close_price,
        avg_price_7d, avg_price_30d, daily_return_pct,
        AVG(gain) OVER (ORDER BY trading_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
        AVG(loss) OVER (ORDER BY trading_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14
    FROM gains_losses
),
momentum_window AS (
    -- Price momentum signals: 7-day level & percent changes
    SELECT
        *,
        LAG(price, 7)  OVER (ORDER BY trading_date) AS price_7d_ago
    FROM rsi_window
),
final_output AS (
    -- Final indicators:
    -- - avg_price_7d / avg_price_30d: price trends
    -- - rsi_strength_14d: strength vs weakness (RSI)
    -- - intraday_range_pct: simple daily volatility proxy
    -- - price_momentum_7d / price_return_7d_pct: momentum views
    SELECT
        trading_date                                            AS date,
        price,
        market_cap,
        volume,
        open_price                                              AS open,
        high_price                                              AS high,
        low_price                                               AS low,
        close_price                                             AS close,
        avg_price_7d,
        avg_price_30d,
        CASE
            WHEN NULLIF(avg_loss_14, 0) IS NULL THEN NULL
            ELSE 100 - (100 / (1 + (avg_gain_14 / NULLIF(avg_loss_14, 0))))
        END                                                     AS rsi_strength_14d,
        CASE
            WHEN open_price IS NOT NULL AND open_price <> 0
                 THEN (high_price - low_price) / open_price
            ELSE NULL
        END                                                     AS intraday_range_pct,
        CASE
            WHEN price_7d_ago IS NULL THEN NULL
            ELSE price - price_7d_ago
        END                                                     AS price_momentum_7d,
        CASE
            WHEN price_7d_ago IS NULL OR price_7d_ago = 0 THEN NULL
            ELSE (price / price_7d_ago) - 1
        END                                                     AS price_return_7d_pct
    FROM momentum_window
)

SELECT *
FROM final_output
ORDER BY date
        );
      
  