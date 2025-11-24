{% snapshot snap_btc_market %}

{{
  config(
    target_schema='snapshot',
    unique_key='business_key',
    strategy='check',
    check_cols=['price','market_cap','volume'],
    invalidate_hard_deletes=True
  )
}}

select
  timestamp,
  price,
  market_cap,
  volume,
  coin_id,
  date,
  -- compose a stable unique key for this row
  concat(coin_id, '|', to_varchar(timestamp)) as business_key
from {{ source('raw','coin_gecko_market_daily') }}

{% endsnapshot %}
