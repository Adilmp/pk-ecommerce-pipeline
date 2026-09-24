-- The fact table. Grain: ONE ROW PER ORDER ITEM (decision D11).
-- grand_total is deliberately absent: it is an order-level value and would be double-counted
-- at this grain (decision D18). Revenue measures are computed per item.
CREATE TABLE IF NOT EXISTS dw.fact_order_items (
    item_id            bigint        PRIMARY KEY,
    order_id           text          NOT NULL,  -- degenerate dimension: the order number
    date_key           integer       NOT NULL REFERENCES dw.dim_date (date_key),
    customer_key       integer       NOT NULL REFERENCES dw.dim_customer (customer_key),
    product_key        integer       NOT NULL REFERENCES dw.dim_product (product_key),
    payment_method_key integer       NOT NULL REFERENCES dw.dim_payment_method (payment_method_key),
    order_month        text          NOT NULL,  -- the load unit: each month is replaced as a whole
    status             text          NOT NULL,
    status_group       text          NOT NULL,  -- completed / in_progress / refunded / cancelled
    qty                integer       NOT NULL,
    unit_price         numeric(12,2) NOT NULL,
    discount_amount    numeric(12,2) NOT NULL,
    gross_amount       numeric(14,2) NOT NULL,  -- unit_price x qty
    net_amount         numeric(14,2) NOT NULL,  -- gross_amount - discount_amount
    is_zero_price      boolean       NOT NULL,
    dq_warnings        text,                    -- e.g. 'ZERO_PRICE,UNKNOWN_CATEGORY'
    loaded_at          timestamptz   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fact_order_month ON dw.fact_order_items (order_month);
CREATE INDEX IF NOT EXISTS ix_fact_date        ON dw.fact_order_items (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_customer    ON dw.fact_order_items (customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_product     ON dw.fact_order_items (product_key);
CREATE INDEX IF NOT EXISTS ix_fact_order       ON dw.fact_order_items (order_id);
