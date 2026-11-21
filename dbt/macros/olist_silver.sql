{# Shared source contracts and CDC ordering for all seven Olist Silver CDC tables. #}
{% macro olist_contract(table) %}
  {% set contracts = {
    'customers': [
      {'name': 'customer_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'customer_unique_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'customer_zip_code_prefix', 'kind': 'text', 'nullable': false, 'max_length': 5},
      {'name': 'customer_city', 'kind': 'text', 'nullable': false, 'max_length': none},
      {'name': 'customer_state', 'kind': 'text', 'nullable': false, 'max_length': 2}
    ],
    'sellers': [
      {'name': 'seller_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'seller_zip_code_prefix', 'kind': 'text', 'nullable': false, 'max_length': 5},
      {'name': 'seller_city', 'kind': 'text', 'nullable': false, 'max_length': none},
      {'name': 'seller_state', 'kind': 'text', 'nullable': false, 'max_length': 2}
    ],
    'products': [
      {'name': 'product_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'product_category_name', 'kind': 'text', 'nullable': true, 'max_length': 100},
      {'name': 'product_name_lenght', 'kind': 'integer', 'nullable': true, 'max_length': none, 'minimum': 0},
      {'name': 'product_description_lenght', 'kind': 'integer', 'nullable': true, 'max_length': none, 'minimum': 0},
      {'name': 'product_photos_qty', 'kind': 'integer', 'nullable': true, 'max_length': none, 'minimum': 0},
      {'name': 'product_weight_g', 'kind': 'integer', 'nullable': true, 'max_length': none, 'minimum': 0},
      {'name': 'product_length_cm', 'kind': 'integer', 'nullable': true, 'max_length': none, 'minimum': 0},
      {'name': 'product_height_cm', 'kind': 'integer', 'nullable': true, 'max_length': none, 'minimum': 0},
      {'name': 'product_width_cm', 'kind': 'integer', 'nullable': true, 'max_length': none, 'minimum': 0}
    ],
    'orders': [
      {'name': 'order_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'customer_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'order_status', 'kind': 'text', 'nullable': false, 'max_length': 20},
      {'name': 'order_purchase_timestamp', 'kind': 'timestamp', 'nullable': false, 'max_length': none},
      {'name': 'order_approved_at', 'kind': 'timestamp', 'nullable': true, 'max_length': none},
      {'name': 'order_delivered_carrier_date', 'kind': 'timestamp', 'nullable': true, 'max_length': none},
      {'name': 'order_delivered_customer_date', 'kind': 'timestamp', 'nullable': true, 'max_length': none},
      {'name': 'order_estimated_delivery_date', 'kind': 'timestamp', 'nullable': false, 'max_length': none}
    ],
    'order_items': [
      {'name': 'order_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'order_item_id', 'kind': 'integer', 'nullable': false, 'max_length': none, 'minimum': 1},
      {'name': 'product_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'seller_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'shipping_limit_date', 'kind': 'timestamp', 'nullable': false, 'max_length': none},
      {'name': 'price', 'kind': 'decimal', 'nullable': false, 'max_length': none, 'minimum': 0},
      {'name': 'freight_value', 'kind': 'decimal', 'nullable': false, 'max_length': none, 'minimum': 0}
    ],
    'order_payments': [
      {'name': 'order_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'payment_sequential', 'kind': 'integer', 'nullable': false, 'max_length': none, 'minimum': 1},
      {'name': 'payment_type', 'kind': 'text', 'nullable': false, 'max_length': 30},
      {'name': 'payment_installments', 'kind': 'integer', 'nullable': false, 'max_length': none, 'minimum': 0},
      {'name': 'payment_value', 'kind': 'decimal', 'nullable': false, 'max_length': none, 'minimum': 0}
    ],
    'order_reviews': [
      {'name': 'review_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'order_id', 'kind': 'text', 'nullable': false, 'max_length': 32},
      {'name': 'review_score', 'kind': 'integer', 'nullable': false, 'max_length': none, 'minimum': 1, 'maximum': 5},
      {'name': 'review_comment_title', 'kind': 'text', 'nullable': true, 'max_length': none},
      {'name': 'review_comment_message', 'kind': 'text', 'nullable': true, 'max_length': none},
      {'name': 'review_creation_date', 'kind': 'timestamp', 'nullable': false, 'max_length': none},
      {'name': 'review_answer_timestamp', 'kind': 'timestamp', 'nullable': true, 'max_length': none}
    ]
  } %}
  {% if table not in contracts %}
    {{ exceptions.raise_compiler_error('Unsupported Silver CDC table: ' ~ table) }}
  {% endif %}
  {{ return(contracts[table]) }}
{% endmacro %}

{% macro olist_keys(table) %}
  {% set keys = {
    'customers': ['customer_id'],
    'sellers': ['seller_id'],
    'products': ['product_id'],
    'orders': ['order_id'],
    'order_items': ['order_id', 'order_item_id'],
    'order_payments': ['order_id', 'payment_sequential'],
    'order_reviews': ['review_id', 'order_id']
  } %}
  {% if table not in keys %}{{ exceptions.raise_compiler_error('Unsupported Silver key table: ' ~ table) }}{% endif %}
  {{ return(keys[table]) }}
{% endmacro %}

{% macro olist_snapshot(table) %}
  {% set snapshots = var('bronze_snapshots', {}) %}
  {% set value = snapshots.get(table) if snapshots is mapping else none %}
  {% if value is not integer or value <= 0 %}
    {% if execute and target.name == 'silver' %}
      {{ exceptions.raise_compiler_error('Capture bronze_snapshots for all seven tables before building Silver; invalid snapshot for ' ~ table) }}
    {% endif %}
    {{ return(0) }}
  {% endif %}
  {{ return(value) }}
{% endmacro %}

{% macro olist_topic(table) %}
  {% set prefix = var('bronze_topic_prefix', 'olist_pg.olist.') %}
  {% if prefix is not string or not modules.re.fullmatch('[A-Za-z0-9_.-]+[.]', prefix) %}
    {{ exceptions.raise_compiler_error('Invalid bronze_topic_prefix') }}
  {% endif %}
  {{ return(prefix ~ table) }}
{% endmacro %}

{% macro olist_silver_properties(table) %}
  {# dbt replaces, rather than merges, a model's tblproperties dictionary. #}
  {% set properties = {'format-version': '2', 'write.format.default': 'parquet',
                       'write.parquet.compression-codec': 'zstd',
                       'write.target-file-size-bytes': '134217728', 'write.distribution-mode': 'none'} %}
  {% do properties.update(config.get('tblproperties', {})) %}
  {% do properties.update({'silver.contract.version': '1', 'silver.source.table': table,
                          'silver.bronze.snapshot-id': olist_snapshot(table) | string}) %}
  {{ return(properties) }}
{% endmacro %}

{% macro olist_timestamp(expression) -%}
  case when try_cast({{ expression }} as bigint) between -62135596800000000 and 253402300799999999
       then cast(timestamp_micros(try_cast({{ expression }} as bigint)) as timestamp_ntz)
       else cast(null as timestamp_ntz) end
{%- endmacro %}

{% macro olist_latest_cdc(relation, key_columns, snapshot=none) %}
  select * from (
    select *, row_number() over (
      partition by {% for key in key_columns %}get_json_object(key_json, '$.{{ key }}'){% if not loop.last %}, {% endif %}{% endfor %}
      order by kafka_offset desc
    ) as _silver_rank
    from {{ relation }}{% if snapshot is not none %} version as of {{ snapshot }}{% endif %}
    where is_tombstone = false and op in ('r', 'c', 'u', 'd')
  ) as _silver_ranked
  where _silver_rank = 1 and op <> 'd'
{% endmacro %}

{% macro olist_silver_preflight(table) %}
  {% if not execute %}{{ return('select 1') }}{% endif %}
  {# The static translation model is loaded from lakehouse.reference, not Kafka. #}
  {% if table == 'product_category_name_translation' %}
    {{ return('select 1') }}
  {% endif %}
  {% if target.name != 'silver' or target.schema != 'silver' %}
    {{ exceptions.raise_compiler_error('Silver requires target=silver and schema=silver') }}
  {% endif %}
  {% set session = run_query('select current_catalog(), current_timezone()') %}
  {% if session.rows[0][0] != 'lakehouse' or session.rows[0][1] != 'UTC' %}
    {{ exceptions.raise_compiler_error('Silver requires catalog lakehouse and UTC session for decoding local source timestamps') }}
  {% endif %}
  {% set fields = olist_contract(table) %}
  {% set keys = olist_keys(table) %}
  {% set snapshot = olist_snapshot(table) %}
  {% set relation = source('olist_bronze', table) %}
  {% set properties_result = run_query('show tblproperties ' ~ relation) %}
  {% set properties = {} %}
  {% for row in properties_result.rows %}{% do properties.update({row[0]: row[1]}) %}{% endfor %}
  {% if properties.get('bronze.contract.version') != '1' %}
    {{ exceptions.raise_compiler_error('Expected Bronze contract version 1: ' ~ table) }}
  {% endif %}
  {% set validation_sql %}
    select count(*) as events,
      coalesce(sum(case when
        parse_error is not null or raw_key is null or key_json is null
        or kafka_topic is null or kafka_topic <> '{{ olist_topic(table) }}'
        or kafka_partition is null or kafka_partition < 0
        or kafka_offset is null or kafka_offset < 0 or ingested_at is null
        or is_tombstone is null
        or (is_tombstone and (raw_value is not null or op is not null
                              or before_json is not null or after_json is not null))
        or (source_lsn is not null and source_lsn < 0)
        or not (sort_array(map_keys(from_json(key_json, 'map<string,string>'))) <=>
                sort_array(array({% for key in keys %}'{{ key }}'{% if not loop.last %}, {% endif %}{% endfor %})))
        {% for key in keys %}
        or get_json_object(key_json, '$.{{ key }}') is null
        or length(get_json_object(key_json, '$.{{ key }}')) = 0
        {% endfor %}
        or (not is_tombstone and (
          raw_value is null or op is null or op not in ('r','c','u','d')
          or source_db is null or length(source_db) = 0
          or source_schema is null or source_schema <> 'olist'
          or source_table is null or source_table <> '{{ table }}'
          or (op = 'd' and (after_json is not null or before_json is null))
          {% for key in keys %}
          or not (get_json_object(key_json, '$.{{ key }}') <=>
                  get_json_object(case when op = 'd' then before_json else after_json end, '$.{{ key }}'))
          {% endfor %}
          or (op in ('r','c','u') and (
            after_json is null or from_json(after_json, 'map<string,string>') is null
            or not (sort_array(map_keys(from_json(after_json, 'map<string,string>'))) <=>
                    sort_array(array({% for field in fields %}'{{ field.name }}'{% if not loop.last %}, {% endif %}{% endfor %})))
            {% for field in fields %}
              {% set value = "get_json_object(after_json, '$." ~ field.name ~ "')" %}
              {% if not field.nullable %}or {{ value }} is null{% endif %}
              {% if field.kind == 'text' and field.max_length is not none %}
                or ({{ value }} is not null and length({{ value }}) > {{ field.max_length }})
              {% elif field.kind == 'integer' %}
                or ({{ value }} is not null and (not ({{ value }} rlike '^[0-9]+$') or try_cast({{ value }} as int) is null{% if field.minimum is defined and field.minimum is not none %} or try_cast({{ value }} as int) < {{ field.minimum }}{% endif %}{% if field.maximum is defined and field.maximum is not none %} or try_cast({{ value }} as int) > {{ field.maximum }}{% endif %}))
              {% elif field.kind == 'decimal' %}
                or ({{ value }} is not null and (not ({{ value }} rlike '^[0-9]+([.][0-9]{1,2})?$') or try_cast({{ value }} as decimal(12,2)) is null{% if field.minimum is defined and field.minimum is not none %} or try_cast({{ value }} as decimal(12,2)) < {{ field.minimum }}{% endif %}))
              {% elif field.kind == 'timestamp' %}
                or ({{ value }} is not null and (not ({{ value }} rlike '^[-]?[0-9]+$')
                    or try_cast({{ value }} as bigint) is null
                    or try_cast({{ value }} as bigint) < -62135596800000000
                    or try_cast({{ value }} as bigint) > 253402300799999999))
              {% endif %}
            {% endfor %}
          ))
        )) then 1 else 0 end), 0) as invalid,
      count(distinct case when not is_tombstone then source_db end) as source_databases
    from {{ relation }} version as of {{ snapshot }}
  {% endset %}
  {% set validation = run_query(validation_sql) %}
  {% if validation.rows[0][0] == 0 or validation.rows[0][1] != 0 or validation.rows[0][2] != 1 %}
    {{ exceptions.raise_compiler_error('Bronze input failed for ' ~ table ~ ': events=' ~ validation.rows[0][0] ~ ', invalid=' ~ validation.rows[0][1] ~ ', source_databases=' ~ validation.rows[0][2]) }}
  {% endif %}
  {% set identity_sql %}
    select count(*) from (
      select kafka_topic, kafka_partition, kafka_offset
      from {{ relation }} version as of {{ snapshot }}
      group by kafka_topic, kafka_partition, kafka_offset having count(*) > 1
    ) as duplicate_identities
  {% endset %}
  {% if run_query(identity_sql).rows[0][0] != 0 %}
    {{ exceptions.raise_compiler_error('Duplicate Kafka identities in Bronze: ' ~ table) }}
  {% endif %}
  {% set partition_sql %}
    select count(*) from (
      select {% for key in keys %}get_json_object(key_json, '$.{{ key }}'){% if not loop.last %}, {% endif %}{% endfor %}
      from {{ relation }} version as of {{ snapshot }}
      group by {% for key in keys %}get_json_object(key_json, '$.{{ key }}'){% if not loop.last %}, {% endif %}{% endfor %}
      having count(distinct kafka_partition) > 1
    ) as moved_keys
  {% endset %}
  {% if run_query(partition_sql).rows[0][0] != 0 %}
    {{ exceptions.raise_compiler_error('A business key spans Kafka partitions in ' ~ table ~ '; cross-partition ordering is not defined') }}
  {% endif %}
  {{ return('select 1') }}
{% endmacro %}
