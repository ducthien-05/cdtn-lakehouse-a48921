select
    concat(p.order_id, ':', cast(p.payment_sequential as string)) as payment_key,
    p.order_id,
    p.payment_sequential,
    coalesce(t.payment_type_key, '__UNKNOWN__') as payment_type_key,
    p.payment_installments,
    cast(p.payment_value as decimal(18,2)) as payment_value,
    cast(p._bronze_snapshot_id as bigint) as order_payments_bronze_snapshot_id
from {{ source('olist_silver', 'order_payments') }} as p
left join {{ ref('dim_payment_type') }} as t
  on t.payment_type = p.payment_type
