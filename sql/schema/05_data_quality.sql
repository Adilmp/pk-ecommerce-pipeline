-- Quarantined rows: raw values exactly as received, plus every rule they failed (decision D9).
CREATE TABLE IF NOT EXISTS dq.quarantine (
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
    error_codes      text        NOT NULL,   -- e.g. 'MISSING_SKU,DISCOUNT_EXCEEDS_LINE'
    order_month      text        NOT NULL,   -- the monthly batch the row arrived in
    source_file      text        NOT NULL,
    quarantined_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_quarantine_month ON dq.quarantine (order_month);

-- One row per step per run: what ran, when, how long, and what it produced.
CREATE TABLE IF NOT EXISTS dq.pipeline_runs (
    run_id       bigint        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_month  text          NOT NULL,
    step         text          NOT NULL,   -- 'silver' or 'gold'
    status       text          NOT NULL CHECK (status IN ('success', 'failed')),
    started_at   timestamptz   NOT NULL,
    finished_at  timestamptz   NOT NULL,
    duration_s   numeric(10,2) NOT NULL,
    metrics      jsonb,
    error        text
);
CREATE INDEX IF NOT EXISTS ix_runs_month_step ON dq.pipeline_runs (order_month, step, finished_at);
