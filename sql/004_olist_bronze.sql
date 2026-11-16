-- Run explicitly with Spark SQL, not psql. This is not a PostgreSQL init script.
-- One append-only CDC event table per Olist business topic. No translation topic.
-- Raw Kafka bytes, including NULL values for tombstones, are retained verbatim.
-- Logical identity is (kafka_topic, kafka_partition, kafka_offset); Iceberg does
-- not enforce a UNIQUE/PK constraint. The checker verifies uniqueness/NULLs.
-- IF NOT EXISTS does not migrate an existing schema; the job validates it first.
-- Unpartitioned local baseline; 128 MiB is a target, not a minimum file size.
CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.customers (
    kafka_topic               STRING,
    kafka_partition           INT,
    kafka_offset              BIGINT,
    kafka_timestamp           TIMESTAMP,
    kafka_timestamp_type      INT,
    raw_key                   BINARY,
    raw_value                 BINARY,
    key_json                  STRING,
    key_has_schema            BOOLEAN,
    value_has_schema          BOOLEAN,
    op                        STRING,
    source_db                 STRING,
    source_schema             STRING,
    source_table              STRING,
    source_lsn                BIGINT,
    source_tx_id              BIGINT,
    source_ts_ms              BIGINT,
    event_ts_ms               BIGINT,
    source_snapshot           STRING,
    before_json               STRING,
    after_json                STRING,
    is_tombstone              BOOLEAN,
    parse_error               STRING,
    ingested_at               TIMESTAMP
)
USING iceberg
COMMENT 'Append-only Olist CDC records; source table olist.customers'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'none',
    'bronze.contract.version' = '1'
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.sellers (
    kafka_topic               STRING,
    kafka_partition           INT,
    kafka_offset              BIGINT,
    kafka_timestamp           TIMESTAMP,
    kafka_timestamp_type      INT,
    raw_key                   BINARY,
    raw_value                 BINARY,
    key_json                  STRING,
    key_has_schema            BOOLEAN,
    value_has_schema          BOOLEAN,
    op                        STRING,
    source_db                 STRING,
    source_schema             STRING,
    source_table              STRING,
    source_lsn                BIGINT,
    source_tx_id              BIGINT,
    source_ts_ms              BIGINT,
    event_ts_ms               BIGINT,
    source_snapshot           STRING,
    before_json               STRING,
    after_json                STRING,
    is_tombstone              BOOLEAN,
    parse_error               STRING,
    ingested_at               TIMESTAMP
)
USING iceberg
COMMENT 'Append-only Olist CDC records; source table olist.sellers'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'none',
    'bronze.contract.version' = '1'
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.products (
    kafka_topic               STRING,
    kafka_partition           INT,
    kafka_offset              BIGINT,
    kafka_timestamp           TIMESTAMP,
    kafka_timestamp_type      INT,
    raw_key                   BINARY,
    raw_value                 BINARY,
    key_json                  STRING,
    key_has_schema            BOOLEAN,
    value_has_schema          BOOLEAN,
    op                        STRING,
    source_db                 STRING,
    source_schema             STRING,
    source_table              STRING,
    source_lsn                BIGINT,
    source_tx_id              BIGINT,
    source_ts_ms              BIGINT,
    event_ts_ms               BIGINT,
    source_snapshot           STRING,
    before_json               STRING,
    after_json                STRING,
    is_tombstone              BOOLEAN,
    parse_error               STRING,
    ingested_at               TIMESTAMP
)
USING iceberg
COMMENT 'Append-only Olist CDC records; source table olist.products'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'none',
    'bronze.contract.version' = '1'
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.orders (
    kafka_topic               STRING,
    kafka_partition           INT,
    kafka_offset              BIGINT,
    kafka_timestamp           TIMESTAMP,
    kafka_timestamp_type      INT,
    raw_key                   BINARY,
    raw_value                 BINARY,
    key_json                  STRING,
    key_has_schema            BOOLEAN,
    value_has_schema          BOOLEAN,
    op                        STRING,
    source_db                 STRING,
    source_schema             STRING,
    source_table              STRING,
    source_lsn                BIGINT,
    source_tx_id              BIGINT,
    source_ts_ms              BIGINT,
    event_ts_ms               BIGINT,
    source_snapshot           STRING,
    before_json               STRING,
    after_json                STRING,
    is_tombstone              BOOLEAN,
    parse_error               STRING,
    ingested_at               TIMESTAMP
)
USING iceberg
COMMENT 'Append-only Olist CDC records; source table olist.orders'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'none',
    'bronze.contract.version' = '1'
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.order_items (
    kafka_topic               STRING,
    kafka_partition           INT,
    kafka_offset              BIGINT,
    kafka_timestamp           TIMESTAMP,
    kafka_timestamp_type      INT,
    raw_key                   BINARY,
    raw_value                 BINARY,
    key_json                  STRING,
    key_has_schema            BOOLEAN,
    value_has_schema          BOOLEAN,
    op                        STRING,
    source_db                 STRING,
    source_schema             STRING,
    source_table              STRING,
    source_lsn                BIGINT,
    source_tx_id              BIGINT,
    source_ts_ms              BIGINT,
    event_ts_ms               BIGINT,
    source_snapshot           STRING,
    before_json               STRING,
    after_json                STRING,
    is_tombstone              BOOLEAN,
    parse_error               STRING,
    ingested_at               TIMESTAMP
)
USING iceberg
COMMENT 'Append-only Olist CDC records; source table olist.order_items'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'none',
    'bronze.contract.version' = '1'
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.order_payments (
    kafka_topic               STRING,
    kafka_partition           INT,
    kafka_offset              BIGINT,
    kafka_timestamp           TIMESTAMP,
    kafka_timestamp_type      INT,
    raw_key                   BINARY,
    raw_value                 BINARY,
    key_json                  STRING,
    key_has_schema            BOOLEAN,
    value_has_schema          BOOLEAN,
    op                        STRING,
    source_db                 STRING,
    source_schema             STRING,
    source_table              STRING,
    source_lsn                BIGINT,
    source_tx_id              BIGINT,
    source_ts_ms              BIGINT,
    event_ts_ms               BIGINT,
    source_snapshot           STRING,
    before_json               STRING,
    after_json                STRING,
    is_tombstone              BOOLEAN,
    parse_error               STRING,
    ingested_at               TIMESTAMP
)
USING iceberg
COMMENT 'Append-only Olist CDC records; source table olist.order_payments'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'none',
    'bronze.contract.version' = '1'
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.order_reviews (
    kafka_topic               STRING,
    kafka_partition           INT,
    kafka_offset              BIGINT,
    kafka_timestamp           TIMESTAMP,
    kafka_timestamp_type      INT,
    raw_key                   BINARY,
    raw_value                 BINARY,
    key_json                  STRING,
    key_has_schema            BOOLEAN,
    value_has_schema          BOOLEAN,
    op                        STRING,
    source_db                 STRING,
    source_schema             STRING,
    source_table              STRING,
    source_lsn                BIGINT,
    source_tx_id              BIGINT,
    source_ts_ms              BIGINT,
    event_ts_ms               BIGINT,
    source_snapshot           STRING,
    before_json               STRING,
    after_json                STRING,
    is_tombstone              BOOLEAN,
    parse_error               STRING,
    ingested_at               TIMESTAMP
)
USING iceberg
COMMENT 'Append-only Olist CDC records; source table olist.order_reviews'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'none',
    'bronze.contract.version' = '1'
);
