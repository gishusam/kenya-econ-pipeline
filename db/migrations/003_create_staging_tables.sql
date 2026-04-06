CREATE TABLE IF NOT EXISTS staging.stg_worldbank_gdp (
    id              SERIAL PRIMARY KEY,
    country_code    VARCHAR(10)  NOT NULL,
    country_name    VARCHAR(100) NOT NULL,
    indicator_id    VARCHAR(50)  NOT NULL,
    year            INTEGER      NOT NULL,
    value_usd       NUMERIC(20,2),
    valid_from      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    valid_to        TIMESTAMPTZ,
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
    loaded_at       TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (country_code, indicator_id, year, valid_from)
);

CREATE TABLE IF NOT EXISTS staging.stg_worldbank_inflation (
    id              SERIAL PRIMARY KEY,
    country_code    VARCHAR(10)  NOT NULL,
    country_name    VARCHAR(100) NOT NULL,
    indicator_id    VARCHAR(50)  NOT NULL,
    year            INTEGER      NOT NULL,
    inflation_pct   NUMERIC(8,4),
    valid_from      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    valid_to        TIMESTAMPTZ,
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
    loaded_at       TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (country_code, indicator_id, year, valid_from)
);

CREATE TABLE IF NOT EXISTS staging.stg_fx_rates (
    id              SERIAL PRIMARY KEY,
    base_currency   VARCHAR(5)   NOT NULL,
    target_currency VARCHAR(5)   NOT NULL,
    rate            NUMERIC(18,8) NOT NULL,
    usd_per_kes     NUMERIC(18,8),
    rate_timestamp  TIMESTAMPTZ  NOT NULL,
    loaded_at       TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_fx_rate_timestamp
    ON staging.stg_fx_rates (rate_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_stg_gdp_year
    ON staging.stg_worldbank_gdp (country_code, year);