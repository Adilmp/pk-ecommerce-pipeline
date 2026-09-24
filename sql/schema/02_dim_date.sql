-- Date dimension: one row per calendar day, generated once rather than loaded from the source.
-- The key is a readable integer (20170315) instead of a meaningless surrogate.
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key      integer  PRIMARY KEY,
    full_date     date     NOT NULL UNIQUE,
    year          smallint NOT NULL,
    quarter       smallint NOT NULL,
    month         smallint NOT NULL,
    month_name    text     NOT NULL,
    year_month    text     NOT NULL,   -- '2017-03'
    day_of_month  smallint NOT NULL,
    day_of_week   smallint NOT NULL,   -- ISO: 1 = Monday ... 7 = Sunday
    day_name      text     NOT NULL,
    is_weekend    boolean  NOT NULL,
    fiscal_year   text     NOT NULL    -- Pakistan's fiscal year runs July-June: July 2016 is in FY17
);

INSERT INTO dw.dim_date (
    date_key, full_date, year, quarter, month, month_name, year_month,
    day_of_month, day_of_week, day_name, is_weekend, fiscal_year
)
SELECT
    to_char(d, 'YYYYMMDD')::integer,
    d,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    trim(to_char(d, 'Month')),
    to_char(d, 'YYYY-MM'),
    EXTRACT(DAY FROM d),
    EXTRACT(ISODOW FROM d),
    trim(to_char(d, 'Day')),
    EXTRACT(ISODOW FROM d) IN (6, 7),
    'FY' || to_char(d + interval '6 months', 'YY')
FROM (
    SELECT generate_series(date '2016-01-01', date '2019-12-31', interval '1 day')::date AS d
) AS days
ON CONFLICT (date_key) DO NOTHING;
