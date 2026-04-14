CREATE TABLE IF NOT EXISTS raw.worldbank_gdp (
    id              SERIAL PRIMARY KEY,
    country_code    VARCHAR(10),
    country_name    VARCHAR(100),
    indicator_id    VARCHAR(50),
    indicator_name  VARCHAR(200),
    year            VARCHAR(4),
    value           NUMERIC,
    source_file     TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (country_code,indicator_id,year)
);

CREATE TABLE IF NOT EXISTS raw.worldbank_inflation (
    id              SERIAL PRIMARY KEY,
    country_code    VARCHAR(10),
    country_name    VARCHAR(100),
    indicator_id    VARCHAR(50),
    indicator_name  VARCHAR(200),
    year            VARCHAR(4),
    value           NUMERIC,
    source_file     TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (country_code, indicator_id, year)
    


);

CREATE TABLE IF NOT EXISTS raw.fx_rates (
    id              SERIAL PRIMARY KEY,
    base_currency   VARCHAR(5),
    target_currency VARCHAR(5),
    rate            NUMERIC(18, 8),
    rate_timestamp  TIMESTAMPTZ,
    source_file     TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);