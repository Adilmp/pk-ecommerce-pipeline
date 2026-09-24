-- Staging tables: Spark writes one month here, then SQL moves it into the star schema.
-- UNLOGGED = no write-ahead log, so writes are faster. That's safe here because staging is
-- rebuilt on every run anyway.
CREATE UNLOGGED TABLE IF NOT EXISTS staging.stg_order_items (
    item_id            bigint,
    order_id           text,
    customer_id        bigint,
    sku                text,
    category           text,
    status             text,
    status_group       text,
    payment_method     text,
    payment_type       text,
    order_date         date,
    qty                integer,
    unit_price         numeric(12,2),
    discount_amount    numeric(12,2),
    gross_amount       numeric(14,2),
    net_amount         numeric(14,2),
    order_grand_total  numeric(14,2),
    is_zero_price      boolean,
    dq_warnings        text,
    source_file        text,
    order_month        text
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging.stg_quarantine (
    item_id          text,
    increment_id     text,
    customer_id      text,
    sku              text,
    status           text,
    created_at       text,
    price            text,
    qty_ordered      text,
    discount_amount  text,
    grand_total      text,
    category_name_1  text,
    payment_method   text,
    error_codes      text,
    source_file      text,
    order_month      text
);
