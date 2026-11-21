select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    _bronze_snapshot_id as sellers_bronze_snapshot_id
from {{ source('olist_silver', 'sellers') }}
