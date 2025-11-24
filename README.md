**End-to-End Bitcoin Analytics Pipeline**

This repository presents a complete, automated data analytics pipeline designed to extract, transform, and visualize Bitcoin market data using modern data engineering and analytics tools. The project integrates Apache Airflow, Snowflake, dbt, and Preset (Apache Superset) into a unified, Dockerized workflow that delivers near real-time insights from the CoinGecko API.

The pipeline is composed of two orchestrated stages. The ETL layer (Extract, Transform, Load) uses Airflow to fetch raw Bitcoin data from CoinGecko’s /market_chart and /ohlc endpoints and loads it into the RAW schema in Snowflake. Once the data ingestion completes, Airflow automatically triggers the ELT layer (Extract, Load, Transform), where dbt performs analytical transformations, computes derived metrics, and manages data quality checks. Analytical models calculate key indicators such as the Relative Strength Index (RSI), 7-day and 30-day moving averages, price momentum, and trading volume patterns, enabling deep exploration of Bitcoin’s market behavior.

The transformed data resides in the ANALYTICS schema and is visualized through Preset dashboards, providing interactive charts that reveal trends in Bitcoin’s RSI strength, daily trading activity, and moving-average crossovers. These visual insights help users understand market volatility and identify bullish or bearish signals effectively.

The system emphasizes automation, reproducibility, and modularity—all managed under a Docker environment to ensure consistency across deployments. Airflow Connections and Variables enable parameterized, secure configurations, while dbt snapshots maintain historical versioning for time-based analytics.

This repository demonstrates an end-to-end data engineering and analytics solution that connects raw cryptocurrency APIs to actionable business intelligence for Bitcoin trend analysis.
