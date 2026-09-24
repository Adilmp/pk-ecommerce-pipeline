-- One schema per purpose, so it's obvious what each table is for.
CREATE SCHEMA IF NOT EXISTS staging;    -- temporary landing tables for each load (overwritten every run)
CREATE SCHEMA IF NOT EXISTS dw;         -- the star schema (gold layer)
CREATE SCHEMA IF NOT EXISTS dq;         -- data quality: quarantined rows and the pipeline run log
CREATE SCHEMA IF NOT EXISTS analytics;  -- business-facing views
