-- Synthetic connection probe; it reads no PostgreSQL or Bronze rows.
-- Rebuilding this table must replace the same three rows, rather than append.
-- dbt renders a two-part name; refuse to write through a different catalog.
{% if execute %}
  {% set catalog_result = run_query('SELECT current_catalog()') %}
  {% if catalog_result.columns[0].values()[0] != 'lakehouse' %}
    {{ exceptions.raise_compiler_error('connection_probe requires current_catalog() = lakehouse') }}
  {% endif %}
{% endif %}

select
    cast(probe_id as int) as probe_id,
    cast(probe_label as string) as probe_label,
    cast(probe_amount as decimal(12, 2)) as probe_amount,
    cast(probe_at as timestamp) as probe_at
from values
    (1, 'spark_dbt_iceberg', '10.25', '2026-11-17 09:00:00'),
    (2, 'spark_dbt_iceberg', '20.50', '2026-11-17 09:01:00'),
    (3, 'spark_dbt_iceberg', '30.75', '2026-11-17 09:02:00')
as probe_rows(probe_id, probe_label, probe_amount, probe_at)
