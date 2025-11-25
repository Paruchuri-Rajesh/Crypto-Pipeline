dbt Tests Folder

This folder stores custom (singular) dbt tests for the Bitcoin Analytics project.

Most of the data quality checks in this project are handled through generic tests defined in
models/schema.yml — for example:

not_null and unique on the date column

not_null on the price column

These ensure the core daily indicators table is valid and has a clean one-row-per-day structure.

Because the Bitcoin indicators (RSI, momentum, moving averages, etc.) naturally contain NULL values for the first few days of the time series (due to required look-back windows), we do not enforce not_null tests on those columns.

Custom SQL tests can be added in this folder in the future if needed (e.g., checking for negative volume or duplicate (date, coin_id) pairs), but for this project the generic schema tests already cover the required validation.
