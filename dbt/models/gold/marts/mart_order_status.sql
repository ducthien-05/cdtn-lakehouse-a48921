with grouped as (
    select
        order_status,
        count(*) as order_count,
        cast(sum(order_value) as decimal(18,2)) as order_value,
        cast(avg(order_value) as decimal(18,2)) as average_order_value
    from {{ ref('fct_orders') }}
    group by order_status
)
select
    order_status,
    order_count,
    order_value,
    average_order_value,
    cast(cast(order_count as decimal(18,6)) / cast(sum(order_count) over () as decimal(18,6)) as decimal(9,6)) as order_share
from grouped
