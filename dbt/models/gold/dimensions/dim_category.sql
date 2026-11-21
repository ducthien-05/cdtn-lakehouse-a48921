select
    '__UNKNOWN__' as category_key,
    cast(null as string) as product_category_name,
    'Unknown' as product_category_name_english,
    cast(null as timestamp) as reference_loaded_at
union all
select
    product_category_name as category_key,
    product_category_name,
    coalesce(product_category_name_english, 'Unknown') as product_category_name_english,
    reference_loaded_at
from {{ source('olist_silver', 'product_category_name_translation') }}
