-- Apply once as the PostgreSQL role from .env, after loading all 8 Olist tables.
-- This role already owns the source tables and has REPLICATION in the local setup.
-- The translation table stays outside CDC.
BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = current_user
          AND (rolreplication OR rolsuper)
    ) THEN
        RAISE EXCEPTION 'Current role % needs REPLICATION privilege', current_user;
    END IF;
END
$$;

CREATE PUBLICATION olist_cdc_pub
FOR TABLE
    olist.customers,
    olist.sellers,
    olist.products,
    olist.orders,
    olist.order_items,
    olist.order_payments,
    olist.order_reviews
WITH (publish = 'insert, update, delete');

COMMIT;