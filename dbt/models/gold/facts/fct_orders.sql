with item_agg as (
    select
        order_id,
        count(*) as order_item_count,
        cast(sum(item_value) as decimal(18,2)) as item_value,
        cast(sum(freight_value) as decimal(18,2)) as freight_value,
        max(order_items_bronze_snapshot_id) as order_items_bronze_snapshot_id
    from {{ ref('fct_order_items') }}
    group by order_id
), payment_agg as (
    select
        order_id,
        count(*) as payment_count,
        cast(sum(payment_value) as decimal(18,2)) as payment_value,
        max(order_payments_bronze_snapshot_id) as order_payments_bronze_snapshot_id
    from {{ ref('fct_order_payments') }}
    group by order_id
), review_agg as (
    select
        order_id,
        count(*) as review_count,
        cast(avg(cast(review_score as decimal(4,2))) as decimal(4,2)) as average_review_score,
        max(order_reviews_bronze_snapshot_id) as order_reviews_bronze_snapshot_id
    from {{ ref('fct_order_reviews') }}
    group by order_id
), base as (
    select
        o.order_id,
        o.customer_id,
        coalesce(c.customer_state, 'UNKNOWN') as customer_state,
        coalesce(s.status_key, '__UNKNOWN__') as status_key,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        cast(date_format(cast(o.order_purchase_timestamp as date), 'yyyyMMdd') as int) as order_purchase_date_key,
        cast(date_format(cast(o.order_estimated_delivery_date as date), 'yyyyMMdd') as int) as estimated_delivery_date_key,
        cast(date_trunc('month', o.order_purchase_timestamp) as date) as order_purchase_month,
        o._bronze_snapshot_id as orders_bronze_snapshot_id,
        i.order_item_count,
        i.item_value,
        i.freight_value,
        i.order_items_bronze_snapshot_id,
        p.payment_count,
        p.payment_value,
        p.order_payments_bronze_snapshot_id,
        r.review_count,
        r.average_review_score,
        r.order_reviews_bronze_snapshot_id
    from {{ source('olist_silver', 'orders') }} as o
    left join {{ ref('dim_customer') }} as c on c.customer_id = o.customer_id
    left join {{ ref('dim_order_status') }} as s on s.status_key = o.order_status
    left join item_agg as i on i.order_id = o.order_id
    left join payment_agg as p on p.order_id = o.order_id
    left join review_agg as r on r.order_id = o.order_id
)
select
    order_id,
    customer_id,
    customer_state,
    status_key,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    order_purchase_date_key,
    estimated_delivery_date_key,
    order_purchase_month,
    coalesce(order_item_count, cast(0 as bigint)) as order_item_count,
    coalesce(item_value, cast(0 as decimal(18,2))) as item_value,
    coalesce(freight_value, cast(0 as decimal(18,2))) as freight_value,
    cast(coalesce(item_value, cast(0 as decimal(18,2))) + coalesce(freight_value, cast(0 as decimal(18,2))) as decimal(18,2)) as order_value,
    coalesce(payment_count, cast(0 as bigint)) as payment_count,
    coalesce(payment_value, cast(0 as decimal(18,2))) as payment_value,
    coalesce(review_count, cast(0 as bigint)) as review_count,
    average_review_score,
    case when order_status = 'delivered' then true else false end as is_delivered,
    case when order_status = 'canceled' then true else false end as is_cancelled,
    case when order_status = 'unavailable' then true else false end as is_unavailable,
    case when order_delivered_customer_date is not null
              and order_estimated_delivery_date is not null
              and cast(order_delivered_customer_date as date) <= cast(order_estimated_delivery_date as date)
         then true else false end as is_delivered_on_time,
    case when order_delivered_carrier_date is not null
         then datediff(cast(order_delivered_carrier_date as date), cast(order_purchase_timestamp as date)) else null end as purchase_to_carrier_days,
    case when order_delivered_customer_date is not null
         then datediff(cast(order_delivered_customer_date as date), cast(order_purchase_timestamp as date)) else null end as purchase_to_customer_days,
    case when order_delivered_customer_date is not null and order_delivered_carrier_date is not null
         then datediff(cast(order_delivered_customer_date as date), cast(order_delivered_carrier_date as date)) else null end as carrier_to_customer_days,
    case when order_estimated_delivery_date is not null
         then datediff(cast(order_estimated_delivery_date as date), cast(order_purchase_timestamp as date)) else null end as estimated_delivery_days,
    orders_bronze_snapshot_id,
    coalesce(order_items_bronze_snapshot_id, orders_bronze_snapshot_id) as order_items_bronze_snapshot_id,
    order_payments_bronze_snapshot_id,
    order_reviews_bronze_snapshot_id
from base
