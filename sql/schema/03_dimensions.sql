-- Dimensions: the "who / what / how" around each sale.
-- Each has a small integer surrogate key (used by the fact table) and the source's natural key.

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key     integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id      bigint  NOT NULL UNIQUE,   -- natural key from the source
    first_order_date date    NOT NULL,          -- first order seen in this dataset
    last_order_date  date    NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key      integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku              text    NOT NULL UNIQUE,
    category         text    NOT NULL,          -- latest known category (SCD Type 1)
    first_seen_date  date    NOT NULL,
    last_seen_date   date    NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_payment_method (
    payment_method_key integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_method     text    NOT NULL UNIQUE,  -- as in the source, e.g. 'cod', 'Easypay'
    payment_type       text    NOT NULL          -- 'Cash on delivery' / 'Prepaid' / 'Store credit / internal'
);
