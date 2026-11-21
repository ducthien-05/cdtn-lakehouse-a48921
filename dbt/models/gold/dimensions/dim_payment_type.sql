select
    '__UNKNOWN__' as payment_type_key,
    'Unknown' as payment_type
union all
select distinct
    payment_type as payment_type_key,
    payment_type
from {{ source('olist_silver', 'order_payments') }}
