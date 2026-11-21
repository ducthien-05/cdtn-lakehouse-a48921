{{ config(enabled=(target.name == 'silver'), tags=['silver']) }}
-- Fixed expected outcomes exercise the same selector with updates, deletion,
-- tombstones, redelivery at a new offset, recreation and a composite key.
with customer_fixture as (
  select * from values
    ('{"customer_id":"updated"}', 'r', false, 1),
    ('{"customer_id":"updated"}', 'u', false, 2),
    ('{"customer_id":"updated"}', 'u', false, 3),
    ('{"customer_id":"deleted"}', 'c', false, 10),
    ('{"customer_id":"deleted"}', 'd', false, 11),
    ('{"customer_id":"deleted"}', cast(null as string), true, 12),
    ('{"customer_id":"recreated"}', 'c', false, 20),
    ('{"customer_id":"recreated"}', 'd', false, 21),
    ('{"customer_id":"recreated"}', 'c', false, 22),
    ('{"customer_id":"snapshot"}', 'r', false, 30)
  as events(key_json, op, is_tombstone, kafka_offset)
), item_fixture as (
  select * from values
    ('{"order_id":"o1","order_item_id":1}', 'r', false, 40),
    ('{"order_id":"o1","order_item_id":2}', 'r', false, 41),
    ('{"order_id":"o1","order_item_id":1}', 'u', false, 42)
  as events(key_json, op, is_tombstone, kafka_offset)
), actual_fixture as (
  select concat('customer:', get_json_object(key_json, '$.customer_id')) as fixture_key, op, kafka_offset
  from ({{ olist_latest_cdc('customer_fixture', ['customer_id']) }}) as selected_customers
  union all
  select concat('item:', get_json_object(key_json, '$.order_id'), ':', get_json_object(key_json, '$.order_item_id')), op, kafka_offset
  from ({{ olist_latest_cdc('item_fixture', ['order_id', 'order_item_id']) }}) as selected_items
), expected_fixture as (
  select * from values
    ('customer:updated', 'u', 3), ('customer:recreated', 'c', 22),
    ('customer:snapshot', 'r', 30), ('item:o1:1', 'u', 42), ('item:o1:2', 'r', 41)
  as expected(fixture_key, op, kafka_offset)
), unexpected as (
  select * from actual_fixture except all select * from expected_fixture
), missing as (
  select * from expected_fixture except all select * from actual_fixture
)
select 'cdc_fixture_unexpected' as rule, fixture_key as business_key from unexpected
union all
select 'cdc_fixture_missing', fixture_key from missing
union all
select 'order_items_duplicate_key', concat(order_id, ':', cast(order_item_id as string))
from {{ ref('order_items') }} group by order_id, order_item_id having count(*) > 1
union all
select 'order_items_positive_id_or_money', concat(order_id, ':', cast(order_item_id as string))
from {{ ref('order_items') }} where order_item_id < 1 or price < 0 or freight_value < 0
union all
select 'customer_zip_or_state_shape', customer_id from {{ ref('customers') }}
where not (customer_zip_code_prefix rlike '^[0-9]{5}$') or length(customer_state) <> 2
union all
select 'empty_business_id', customer_id from {{ ref('customers') }}
where length(customer_id) = 0 or length(customer_unique_id) = 0
union all
select 'empty_business_id', order_id from {{ ref('orders') }}
where length(order_id) = 0 or length(customer_id) = 0
union all
select 'empty_business_id', concat(order_id, ':', cast(order_item_id as string)) from {{ ref('order_items') }}
where length(order_id) = 0 or length(product_id) = 0 or length(seller_id) = 0
union all
select 'order_payments_duplicate_key', concat(order_id, ':', cast(payment_sequential as string))
from {{ ref('order_payments') }} group by order_id, payment_sequential having count(*) > 1
union all
select 'order_reviews_duplicate_key', concat(review_id, ':', order_id)
from {{ ref('order_reviews') }} group by review_id, order_id having count(*) > 1
union all
select 'product_nonnegative_attribute', product_id
from {{ ref('products') }}
where coalesce(product_name_lenght, 0) < 0
   or coalesce(product_description_lenght, 0) < 0
   or coalesce(product_photos_qty, 0) < 0
   or coalesce(product_weight_g, 0) < 0
   or coalesce(product_length_cm, 0) < 0
   or coalesce(product_height_cm, 0) < 0
   or coalesce(product_width_cm, 0) < 0
union all
select 'payment_value_or_id', concat(order_id, ':', cast(payment_sequential as string))
from {{ ref('order_payments') }}
where payment_sequential < 1 or payment_installments < 0 or payment_value < 0
union all
select 'review_score_range', concat(review_id, ':', order_id)
from {{ ref('order_reviews') }}
where review_score < 1 or review_score > 5
union all
select 'seller_zip_or_state_shape', seller_id
from {{ ref('sellers') }}
where not (seller_zip_code_prefix rlike '^[0-9]{5}$') or length(seller_state) <> 2
union all
select 'empty_seller_id', seller_id
from {{ ref('sellers') }}
where length(seller_id) = 0
union all
select 'empty_product_id', product_id
from {{ ref('products') }}
where length(product_id) = 0
union all
select 'translation_key', product_category_name
from {{ ref('product_category_name_translation') }}
where length(product_category_name) = 0
