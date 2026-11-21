select
    '__UNKNOWN__' as status_key,
    'UNKNOWN' as order_status,
    false as is_delivered,
    false as is_cancelled,
    false as is_unavailable
union all
select distinct
    order_status as status_key,
    order_status,
    order_status = 'delivered' as is_delivered,
    order_status = 'canceled' as is_cancelled,
    order_status = 'unavailable' as is_unavailable
from {{ source('olist_silver', 'orders') }}
