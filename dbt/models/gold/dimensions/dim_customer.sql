select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    _bronze_snapshot_id as customers_bronze_snapshot_id
from {{ source('olist_silver', 'customers') }}
