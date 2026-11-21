{{ config(enabled=(target.name == 'gold'), tags=['gold']) }}
-- Star-schema Gold consistency checks. A non-empty result fails the test.
with order_dupes as (
    select 'fct_orders_duplicate' as rule, order_id as business_key
    from {{ ref('fct_orders') }} group by order_id having count(*) > 1
), item_dupes as (
    select 'fct_order_items_duplicate' as rule, order_item_key as business_key
    from {{ ref('fct_order_items') }} group by order_item_key having count(*) > 1
), payment_dupes as (
    select 'fct_order_payments_duplicate' as rule, payment_key as business_key
    from {{ ref('fct_order_payments') }} group by payment_key having count(*) > 1
), review_dupes as (
    select 'fct_order_reviews_duplicate' as rule, review_key as business_key
    from {{ ref('fct_order_reviews') }} group by review_key having count(*) > 1
), order_value_bad as (
    select 'fct_orders_value_formula' as rule, order_id as business_key
    from {{ ref('fct_orders') }}
    where order_value <> item_value + freight_value
       or item_value < 0 or freight_value < 0 or order_item_count < 0
), monthly_expected as (
    select order_purchase_month, count(*) as order_count,
           cast(sum(order_value) as decimal(18,2)) as order_value
    from {{ ref('fct_orders') }} group by order_purchase_month
), monthly_bad as (
    select 'mart_orders_monthly' as rule, cast(m.order_purchase_month as string) as business_key
    from {{ ref('mart_orders_monthly') }} m left join monthly_expected e
      on e.order_purchase_month = m.order_purchase_month
    where e.order_purchase_month is null or m.order_count <> e.order_count or m.order_value <> e.order_value
), state_expected as (
    select customer_state, count(*) as order_count,
           cast(sum(order_value) as decimal(18,2)) as order_value
    from {{ ref('fct_orders') }} group by customer_state
), state_bad as (
    select 'mart_orders_by_state' as rule, m.customer_state as business_key
    from {{ ref('mart_orders_by_state') }} m left join state_expected e
      on e.customer_state = m.customer_state
    where e.customer_state is null or m.order_count <> e.order_count or m.order_value <> e.order_value
), status_expected as (
    select order_status, count(*) as order_count,
           cast(sum(order_value) as decimal(18,2)) as order_value
    from {{ ref('fct_orders') }} group by order_status
), status_bad as (
    select 'mart_order_status' as rule, m.order_status as business_key
    from {{ ref('mart_order_status') }} m left join status_expected e
      on e.order_status = m.order_status
    where e.order_status is null or m.order_count <> e.order_count or m.order_value <> e.order_value
), status_share_bad as (
    select 'mart_order_status_share' as rule, 'all' as business_key
    from (select sum(order_share) as total_share from {{ ref('mart_order_status') }})
    where abs(total_share - cast(1 as decimal(9,6))) > cast(0.000010 as decimal(9,6))
), product_bad as (
    select 'mart_product_sales' as rule, m.product_id as business_key
    from {{ ref('mart_product_sales') }} m
    left join (select product_id, count(*) as item_count,
                      cast(sum(item_value) as decimal(18,2)) as item_value,
                      cast(sum(freight_value) as decimal(18,2)) as freight_value
               from {{ ref('fct_order_items') }} group by product_id) e
      on e.product_id = m.product_id
    where e.product_id is null or m.item_count <> e.item_count
       or m.item_value <> e.item_value or m.freight_value <> e.freight_value
), seller_bad as (
    select 'mart_seller_sales' as rule, m.seller_id as business_key
    from {{ ref('mart_seller_sales') }} m
    left join (select seller_id, count(*) as item_count,
                      cast(sum(item_value) as decimal(18,2)) as item_value,
                      cast(sum(freight_value) as decimal(18,2)) as freight_value
               from {{ ref('fct_order_items') }} group by seller_id) e
      on e.seller_id = m.seller_id
    where e.seller_id is null or m.item_count <> e.item_count
       or m.item_value <> e.item_value or m.freight_value <> e.freight_value
), customer_bad as (
    select 'mart_customer_value' as rule, m.customer_id as business_key
    from {{ ref('mart_customer_value') }} m
    left join (select customer_id, count(*) as order_count,
                      cast(sum(order_value) as decimal(18,2)) as customer_value
               from {{ ref('fct_orders') }} group by customer_id) e
      on e.customer_id = m.customer_id
    where e.customer_id is null or m.order_count <> e.order_count or m.customer_value <> e.customer_value
), category_share_bad as (
    select 'mart_category_share_monthly' as rule,
           cast(order_purchase_month as string) as business_key
    from {{ ref('mart_category_share_monthly') }}
    group by order_purchase_month
    having abs(sum(category_share) - cast(1 as decimal(9,6))) > cast(0.000010 as decimal(9,6))
)
select rule, business_key from order_dupes
union all select rule, business_key from item_dupes
union all select rule, business_key from payment_dupes
union all select rule, business_key from review_dupes
union all select rule, business_key from order_value_bad
union all select rule, business_key from monthly_bad
union all select rule, business_key from state_bad
union all select rule, business_key from status_bad
union all select rule, business_key from status_share_bad
union all select rule, business_key from product_bad
union all select rule, business_key from seller_bad
union all select rule, business_key from customer_bad
union all select rule, business_key from category_share_bad
