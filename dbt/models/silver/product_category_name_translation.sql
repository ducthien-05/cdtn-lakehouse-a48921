{{ config(
    tblproperties={
      'format-version': '2',
      'write.format.default': 'parquet',
      'write.parquet.compression-codec': 'zstd',
      'write.target-file-size-bytes': '134217728',
      'write.distribution-mode': 'none',
      'silver.contract.version': '1',
      'silver.source.table': 'product_category_name_translation',
      'silver.source.kind': 'static-reference'
    }
) }}

select
    cast(product_category_name as string) as product_category_name,
    cast(product_category_name_english as string) as product_category_name_english,
    cast(reference_loaded_at as timestamp) as reference_loaded_at
from {{ source('olist_reference', 'product_category_name_translation') }}
