# -----------------------------
# Import Libraries and Snowflake Connections
# -----------------------------
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # <-- NEW
from datetime import datetime, timedelta
import pandas as pd
import requests
import snowflake.connector  # optional, kept for consistency

# -----------------------------
# Snowflake Helper Function
# -----------------------------
# This function establishes a connection to Snowflake using Airflow's SnowflakeHook
# It returns a cursor object that can be used to execute SQL queries
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()


# =====================  MARKET_CHART BRANCH (A)  ======================

# -----------------------------
# Step 1A: Extraction Task (Market Chart)
# -----------------------------
# Fetches daily CoinGecko market chart data for last N days
# Returns prices, market_caps, total_volumes as dictionaries
@task
def extract_coin_gecko_data(coin_id, vs_currency, days):
    url = (
        f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
        f"?vs_currency={vs_currency}&days={days}&interval=daily"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    print("Market chart keys:", list(data.keys()))

    return {
        "prices": data.get("prices", []),
        "market_caps": data.get("market_caps", []),
        "total_volumes": data.get("total_volumes", [])
    }


# -----------------------------
# Step 2A: Transformation Task (Market Chart)
# -----------------------------
@task
def transform_coin_gecko_data(market_data, coin_id="bitcoin"):
    df_prices = pd.DataFrame(market_data.get("prices", []), columns=["timestamp", "price"])
    df_market_caps = pd.DataFrame(market_data.get("market_caps", []), columns=["timestamp", "market_cap"])
    df_volumes = pd.DataFrame(market_data.get("total_volumes", []), columns=["timestamp", "volume"])

    df = df_prices.merge(df_market_caps, on="timestamp", how="inner").merge(df_volumes, on="timestamp", how="inner")
    df["coin_id"] = coin_id
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.date

    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Keep last 90 full days
    df = df.tail(90)

    print(f"[market_chart] Transformed {coin_id}: last {len(df)} full days")
    print(df.head(3))
    print(df.tail(3))

    return df


# -----------------------------
# Step 3A: Load Task (Market Chart)
# -----------------------------
# Loads DataFrame into Snowflake with transactional full-refresh
@task
def load_coin_gecko_data(df, table_name="raw.coin_gecko_market_daily"):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        # Create table if not exists
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                timestamp BIGINT,
                price FLOAT,
                market_cap FLOAT,
                volume FLOAT,
                coin_id STRING,
                date DATE,
                PRIMARY KEY(timestamp, coin_id)
            );
        """)
        # Full refresh
        cur.execute(f"DELETE FROM {table_name};")
        print(f"Deleted old records from {table_name}.")

        # Insert new records
        for r in df.to_dict(orient='records'):
            cur.execute(f"""
                INSERT INTO {table_name} (timestamp, price, market_cap, volume, coin_id, date)
                VALUES (
                    {int(r['timestamp'])}, {float(r['price'])}, {float(r['market_cap'])}, {float(r['volume'])},
                    '{r['coin_id']}', '{r['date']}'
                );
            """)

        cur.execute("COMMIT;")
        print(f"Loaded {len(df)} rows into {table_name} successfully.")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Error during load (market_chart), transaction rolled back:", e)
        raise
    finally:
        cur.close()


# ========================  OHLC BRANCH (B)  ===========================

# -----------------------------
# Step 1B: Extraction Task (OHLC)
# -----------------------------
# Fetches OHLC candlestick data for last N days
# Returns list of [timestamp, open, high, low, close]
@task
def extract_coin_gecko_ohlc(coin_id, vs_currency, days):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency={vs_currency}&days={days}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()  # list of [ts, open, high, low, close]

    print(f"OHLC records fetched: {len(data)}")

    return {"ohlc": data}


# -----------------------------
# Step 2B: Transformation Task (OHLC)
# -----------------------------
# Converts OHLC list into tidy DataFrame
@task
def transform_coin_gecko_ohlc(ohlc_payload, coin_id="bitcoin"):
    ohlc_list = ohlc_payload.get("ohlc", [])
    df = pd.DataFrame(ohlc_list, columns=["timestamp", "open", "high", "low", "close"])

    # Ensure correct types
    df["timestamp"] = df["timestamp"].astype("int64")
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)

    df["coin_id"] = coin_id
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.date

    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Keep last 90 full days (to mirror market branch)
    df = df.tail(90)

    print(f"[ohlc] Transformed {coin_id}: last {len(df)} full days")
    print(df.head(3))
    print(df.tail(3))

    return df


# -----------------------------
# Step 3B: Load Task (OHLC)
# -----------------------------
# Loads OHLC DataFrame into Snowflake with transactional full-refresh
@task
def load_coin_gecko_ohlc(df, table_name="raw.coin_gecko_ohlc"):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        # Create table if not exists
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                timestamp BIGINT,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                coin_id STRING,
                date DATE,
                PRIMARY KEY(timestamp, coin_id)
            );
        """)
        # Full refresh
        cur.execute(f"DELETE FROM {table_name};")
        print(f"Deleted old records from {table_name}.")

        # Insert new records
        for r in df.to_dict(orient='records'):
            cur.execute(f"""
                INSERT INTO {table_name} (timestamp, open, high, low, close, coin_id, date)
                VALUES (
                    {int(r['timestamp'])}, {float(r['open'])}, {float(r['high'])},
                    {float(r['low'])}, {float(r['close'])}, '{r['coin_id']}', '{r['date']}'
                );
            """)

        cur.execute("COMMIT;")
        print(f"Loaded {len(df)} rows into {table_name} successfully.")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Error during load (ohlc), transaction rolled back:", e)
        raise
    finally:
        cur.close()

# -----------------------------
# Step 4: DAG Definition
# -----------------------------
# This DAG executes the ETL pipeline and then TRIGGERS the ELT (dbt) DAG.
with DAG(
    dag_id='coin_gecko_etl_v1',
    start_date=datetime(2025, 10, 1),
    catchup=False,
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM
    tags=['ETL', 'CoinGecko'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:

    # Airflow Variables
    coin_id = Variable.get("coin_id", default_var="bitcoin")
    vs_currency = Variable.get("vs_currency", default_var="usd")
    days = int(Variable.get("days", default_var="90"))

    # -------- Branch A: market_chart --------
    market_data = extract_coin_gecko_data(coin_id, vs_currency, days)
    df_market   = transform_coin_gecko_data(market_data, coin_id)
    load_market = load_coin_gecko_data(df_market)   # defaults to raw.coin_gecko_market_daily

    # -------- Branch B: ohlc --------
    ohlc_raw  = extract_coin_gecko_ohlc(coin_id, vs_currency, days)
    df_ohlc   = transform_coin_gecko_ohlc(ohlc_raw, coin_id)
    load_ohlc = load_coin_gecko_ohlc(df_ohlc)       # defaults to raw.coin_gecko_ohlc

    # -------- Trigger ELT (dbt) DAG after ETL completes --------
    trigger_elt_dag = TriggerDagRunOperator(
        task_id='trigger_btc_elt_dbt_v1',
        trigger_dag_id='btc_elt_dbt_v1',  # must match ELT DAG's dag_id
        wait_for_completion=False
    )

    # dependencies
    market_data >> df_market >> load_market
    ohlc_raw    >> df_ohlc   >> load_ohlc

    # fan-out to trigger once both loads are done
    [load_market, load_ohlc] >> trigger_elt_dag
