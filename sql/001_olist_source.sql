-- Olist source schema: eight CSV-backed tables, before Debezium CDC.
-- Apply once to a fresh PostgreSQL database with psql -v ON_ERROR_STOP=1 -f sql/001_olist_source.sql.
-- Column names and order intentionally mirror the CSV headers for direct \copy loading.
-- Source timestamps contain no timezone; use TIMESTAMP WITHOUT TIME ZONE.
-- PostgreSQL stores source rows only; do not add generated analytics columns here.

BEGIN;

CREATE SCHEMA IF NOT EXISTS olist;

CREATE TABLE olist.product_category_name_translation (
    product_category_name         varchar(100) PRIMARY KEY,
    product_category_name_english varchar(100)
);

CREATE TABLE olist.customers (
    customer_id              varchar(32) PRIMARY KEY,
    customer_unique_id       varchar(32) NOT NULL,
    customer_zip_code_prefix varchar(5)  NOT NULL
        CHECK (customer_zip_code_prefix ~ '^[0-9]{5}$'),
    customer_city            text         NOT NULL,
    customer_state           varchar(2)   NOT NULL
        CHECK (length(customer_state) = 2)
);

CREATE INDEX customers_unique_id_idx ON olist.customers (customer_unique_id);

CREATE TABLE olist.sellers (
    seller_id              varchar(32) PRIMARY KEY,
    seller_zip_code_prefix varchar(5) NOT NULL
        CHECK (seller_zip_code_prefix ~ '^[0-9]{5}$'),
    seller_city            text        NOT NULL,
    seller_state           varchar(2)  NOT NULL
        CHECK (length(seller_state) = 2)
);

CREATE TABLE olist.products (
    product_id                 varchar(32) PRIMARY KEY,
    product_category_name      varchar(100),
    product_name_lenght        integer CHECK (product_name_lenght >= 0),
    product_description_lenght integer CHECK (product_description_lenght >= 0),
    product_photos_qty         integer CHECK (product_photos_qty >= 0),
    product_weight_g           integer CHECK (product_weight_g >= 0),
    product_length_cm          integer CHECK (product_length_cm >= 0),
    product_height_cm          integer CHECK (product_height_cm >= 0),
    product_width_cm           integer CHECK (product_width_cm >= 0)
);

-- No FK from products to translation: 610 products have no category and
-- 13 products use two category names absent from the 71-row translation CSV.
CREATE INDEX products_category_name_idx ON olist.products (product_category_name);

CREATE TABLE olist.orders (
    order_id                      varchar(32) PRIMARY KEY,
    customer_id                   varchar(32) NOT NULL
        REFERENCES olist.customers (customer_id),
    order_status                  varchar(20) NOT NULL,
    order_purchase_timestamp      timestamp without time zone NOT NULL,
    order_approved_at             timestamp without time zone,
    order_delivered_carrier_date  timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date timestamp without time zone NOT NULL
);

CREATE INDEX orders_customer_id_idx ON olist.orders (customer_id);
CREATE INDEX orders_purchase_timestamp_idx
    ON olist.orders (order_purchase_timestamp);

CREATE TABLE olist.order_items (
    order_id            varchar(32) NOT NULL
        REFERENCES olist.orders (order_id),
    order_item_id       integer NOT NULL CHECK (order_item_id >= 1),
    product_id          varchar(32) NOT NULL
        REFERENCES olist.products (product_id),
    seller_id           varchar(32) NOT NULL
        REFERENCES olist.sellers (seller_id),
    shipping_limit_date timestamp without time zone NOT NULL,
    price               numeric(12, 2) NOT NULL CHECK (price >= 0),
    freight_value       numeric(12, 2) NOT NULL CHECK (freight_value >= 0),
    CONSTRAINT order_items_pkey PRIMARY KEY (order_id, order_item_id)
);

CREATE INDEX order_items_product_id_idx ON olist.order_items (product_id);
CREATE INDEX order_items_seller_id_idx ON olist.order_items (seller_id);

CREATE TABLE olist.order_payments (
    order_id             varchar(32) NOT NULL
        REFERENCES olist.orders (order_id),
    payment_sequential   integer NOT NULL CHECK (payment_sequential >= 1),
    payment_type         varchar(30) NOT NULL,
    payment_installments integer NOT NULL CHECK (payment_installments >= 0),
    payment_value        numeric(12, 2) NOT NULL CHECK (payment_value >= 0),
    CONSTRAINT order_payments_pkey PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE olist.order_reviews (
    review_id               varchar(32) NOT NULL,
    order_id                varchar(32) NOT NULL
        REFERENCES olist.orders (order_id),
    review_score            integer NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title    text,
    review_comment_message  text,
    review_creation_date    timestamp without time zone NOT NULL,
    review_answer_timestamp timestamp without time zone,
    CONSTRAINT order_reviews_pkey PRIMARY KEY (review_id, order_id)
);

CREATE INDEX order_reviews_order_id_idx ON olist.order_reviews (order_id);

COMMENT ON COLUMN olist.products.product_name_lenght IS
    'CSV header spelling preserved; rename to product_name_length in Silver.';
COMMENT ON COLUMN olist.products.product_description_lenght IS
    'CSV header spelling preserved; rename to product_description_length in Silver.';
COMMENT ON COLUMN olist.customers.customer_unique_id IS
    'Repeated across orders for the same customer; deliberately not UNIQUE.';
COMMENT ON TABLE olist.order_reviews IS
    'review_id repeats in the source; (review_id, order_id) is unique in the profiled CSV.';

COMMIT;