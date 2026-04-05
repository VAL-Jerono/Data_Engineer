"""
Kenya Food Prices Data Engineering Capstone
Author: Rene Bosire | Everything Data Bootcamp
Module: load.py — Data Load Layer (PostgreSQL + Snowflake)

Supports:
  - Full load to raw.raw_food_prices
  - Upsert / incremental load to staging.stg_food_prices
  - Optional mirror load to Snowflake
  - Dimension population (dim_market, dim_commodity)
"""

import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

log = logging.getLogger("load")
load_dotenv(Path(__file__).resolve().parents[1] / ".env")
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _pg_engine() -> Engine:
    """Build a PostgreSQL SQLAlchemy engine from environment variables."""
    required = ["PG_HOST", "PG_DATABASE", "PG_USER", "PG_PASSWORD"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(
            f"Missing PostgreSQL environment variables: {joined}. "
            "Set them in your shell or in a project .env file before running the pipeline."
        )

    host     = os.environ["PG_HOST"]
    port     = os.environ.get("PG_PORT", "5432")
    db       = os.environ["PG_DATABASE"]
    user     = os.environ["PG_USER"]
    password = os.environ["PG_PASSWORD"]
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url, pool_pre_ping=True)

    try:
        with engine.connect():
            pass
    except OperationalError as exc:
        raise RuntimeError(
            "Could not connect to PostgreSQL at "
            f"{host}:{port}/{db}. Start PostgreSQL first, then rerun the pipeline. "
            "If you are using Docker, try `docker compose up -d postgres` "
            "or `docker compose up -d` from the project root."
        ) from exc

    return engine


def _sf_engine() -> Engine:
    """Build a Snowflake SQLAlchemy engine from environment variables."""
    from snowflake.sqlalchemy import URL as SnowflakeURL  # type: ignore

    return create_engine(
        SnowflakeURL(
            account   = os.environ["SF_ACCOUNT"],
            user      = os.environ["SF_USER"],
            password  = os.environ["SF_PASSWORD"],
            database  = os.environ["SF_DATABASE"],
            schema    = os.environ.get("SF_SCHEMA", "PUBLIC"),
            warehouse = os.environ.get("SF_WAREHOUSE", "COMPUTE_WH"),
            role      = os.environ.get("SF_ROLE", "SYSADMIN"),
        )
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_raw(df: pd.DataFrame) -> int:
    """Append the raw CSV shape into raw.raw_food_prices (PostgreSQL)."""
    engine = _pg_engine()
    subset = df.copy()
    subset.columns = [c.strip().lower() for c in subset.columns]
    subset = subset.rename(
        columns={
            "county": "admin1",
            "district": "admin2",
            "source": "source_file",
            "price_kes": "price",
            "price_usd": "usdprice",
        }
    )
    if "source_file" not in subset.columns:
        source_name = df.attrs.get("source_name")
        subset["source_file"] = Path(source_name).name if source_name else None

    raw_cols = [
        "date",
        "admin1",
        "admin2",
        "market",
        "market_id",
        "latitude",
        "longitude",
        "category",
        "commodity",
        "commodity_id",
        "unit",
        "priceflag",
        "pricetype",
        "currency",
        "price",
        "usdprice",
        "source_file",
    ]
    subset = subset[[c for c in raw_cols if c in subset.columns]].copy()

    subset.to_sql(
        "raw_food_prices",
        engine,
        schema="raw",
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )
    log.info("Loaded %d rows to raw.raw_food_prices.", len(subset))
    return len(subset)


def ensure_database_objects(engine: Engine | None = None) -> None:
    """
    Create the schemas and tables required by the pipeline without dropping
    existing objects.

    This makes the first local run smoother while avoiding privilege issues
    from replaying destructive setup SQL as a non-owner database user.
    """
    engine = engine or _pg_engine()
    required_tables = [
        ("raw", "raw_food_prices"),
        ("staging", "stg_food_prices"),
        ("warehouse", "dim_date"),
        ("warehouse", "dim_market"),
        ("warehouse", "dim_commodity"),
        ("warehouse", "fact_prices"),
    ]

    with engine.connect() as conn:
        missing = []
        for schema_name, table_name in required_tables:
            exists = conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = :schema_name
                          AND table_name = :table_name
                    )
                    """
                ),
                {"schema_name": schema_name, "table_name": table_name},
            ).scalar()
            if not exists:
                missing.append(f"{schema_name}.{table_name}")

    if not missing:
        log.info("Required database objects already exist.")
        return

    log.info("Missing database objects detected: %s", ", ".join(missing))
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS marts"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.raw_food_prices (
                id              SERIAL PRIMARY KEY,
                date            DATE,
                admin1          VARCHAR(100),
                admin2          VARCHAR(100),
                market          VARCHAR(150),
                market_id       INTEGER,
                latitude        NUMERIC(10, 6),
                longitude       NUMERIC(10, 6),
                category        VARCHAR(100),
                commodity       VARCHAR(150),
                commodity_id    INTEGER,
                unit            VARCHAR(50),
                priceflag       VARCHAR(50),
                pricetype       VARCHAR(50),
                currency        VARCHAR(10),
                price           NUMERIC(12, 4),
                usdprice        NUMERIC(12, 4),
                source_file     VARCHAR(255),
                loaded_at       TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_date ON raw.raw_food_prices (date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_market ON raw.raw_food_prices (market)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_commodity ON raw.raw_food_prices (commodity)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_admin1 ON raw.raw_food_prices (admin1)"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS staging.stg_food_prices (
                id                  SERIAL PRIMARY KEY,
                price_date          DATE NOT NULL,
                year                INT NOT NULL,
                month               INT NOT NULL,
                county              VARCHAR(100),
                district            VARCHAR(100),
                market              VARCHAR(150) NOT NULL,
                source_market_id    INTEGER,
                latitude            NUMERIC(10, 6),
                longitude           NUMERIC(10, 6),
                category            VARCHAR(100),
                commodity           VARCHAR(150) NOT NULL,
                source_commodity_id INTEGER,
                unit                VARCHAR(50),
                priceflag           VARCHAR(50),
                pricetype           VARCHAR(50),
                currency            VARCHAR(10),
                price_kes           NUMERIC(12, 4),
                price_usd           NUMERIC(12, 4),
                price_per_kg        NUMERIC(12, 4),
                source_file         VARCHAR(255),
                is_valid_price      BOOLEAN DEFAULT TRUE,
                loaded_at           TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_staging_food_prices_business_key
            ON staging.stg_food_prices (price_date, market, commodity, unit, pricetype)
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.dim_market (
                market_id        SERIAL PRIMARY KEY,
                market_name      VARCHAR(150) NOT NULL,
                district         VARCHAR(100),
                county           VARCHAR(100),
                source_market_id INTEGER,
                latitude         NUMERIC(10, 6),
                longitude        NUMERIC(10, 6),
                created_at       TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_market_business_key
            ON warehouse.dim_market (market_name, district, county)
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.dim_commodity (
                commodity_id        SERIAL PRIMARY KEY,
                commodity_name      VARCHAR(150) NOT NULL,
                category            VARCHAR(100),
                unit                VARCHAR(50),
                source_commodity_id INTEGER,
                created_at          TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_commodity_business_key
            ON warehouse.dim_commodity (commodity_name, unit)
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.dim_date (
                date_id       INT PRIMARY KEY,
                full_date     DATE NOT NULL UNIQUE,
                year          INT NOT NULL,
                quarter       INT NOT NULL,
                month         INT NOT NULL,
                month_name    VARCHAR(15) NOT NULL,
                day_of_month  INT NOT NULL,
                is_year_start BOOLEAN DEFAULT FALSE,
                is_year_end   BOOLEAN DEFAULT FALSE
            )
        """))
        conn.execute(text("""
            INSERT INTO warehouse.dim_date (
                date_id, full_date, year, quarter, month, month_name,
                day_of_month, is_year_start, is_year_end
            )
            SELECT
                TO_CHAR(d, 'YYYYMMDD')::INT,
                d,
                EXTRACT(YEAR FROM d)::INT,
                EXTRACT(QUARTER FROM d)::INT,
                EXTRACT(MONTH FROM d)::INT,
                TO_CHAR(d, 'Month'),
                EXTRACT(DAY FROM d)::INT,
                EXTRACT(MONTH FROM d) = 1 AND EXTRACT(DAY FROM d) = 1,
                EXTRACT(MONTH FROM d) = 12 AND EXTRACT(DAY FROM d) = 31
            FROM generate_series('2006-01-01'::DATE, '2026-12-31'::DATE, '1 day') AS d
            ON CONFLICT (date_id) DO NOTHING
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS warehouse.fact_prices (
                fact_id         BIGSERIAL PRIMARY KEY,
                date_id         INT REFERENCES warehouse.dim_date(date_id),
                market_id       INT REFERENCES warehouse.dim_market(market_id),
                commodity_id    INT REFERENCES warehouse.dim_commodity(commodity_id),
                price_kes       NUMERIC(12, 4),
                price_usd       NUMERIC(12, 4),
                price_per_kg    NUMERIC(12, 4),
                pricetype       VARCHAR(50),
                is_valid_price  BOOLEAN,
                loaded_at       TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_date ON warehouse.fact_prices (date_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_market ON warehouse.fact_prices (market_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_commodity ON warehouse.fact_prices (commodity_id)"))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_prices_business_key
            ON warehouse.fact_prices (date_id, market_id, commodity_id, pricetype)
        """))

    log.info("Database objects ensured with non-destructive bootstrap SQL.")


def load_staging(df: pd.DataFrame, incremental: bool = True) -> int:
    """
    Load cleaned data into staging.stg_food_prices.

    If incremental=True, only rows whose natural business key is not
    already present in staging are inserted.
    """
    engine = _pg_engine()

    if incremental:
        df = _filter_existing_business_keys(df, engine)
        if df.empty:
            log.info("No new data to load (incremental check).")
            return 0

    staging_cols = [
        "date",
        "year",
        "month",
        "county",
        "district",
        "market",
        "source_market_id",
        "latitude",
        "longitude",
        "category",
        "commodity",
        "source_commodity_id",
        "unit",
        "priceflag",
        "pricetype",
        "currency",
        "price_kes",
        "price_usd",
        "price_per_kg",
        "is_valid_price",
        "source_file",
    ]
    subset = df[[c for c in staging_cols if c in df.columns]].copy()
    subset.rename(columns={"date": "price_date"}, inplace=True)

    subset.to_sql(
        "stg_food_prices",
        engine,
        schema="staging",
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )
    log.info("Loaded %d rows to staging.stg_food_prices (incremental=%s).", len(subset), incremental)
    return len(subset)


def populate_dimensions(engine: Engine | None = None) -> None:
    """
    Populate warehouse dimensions from staging data.
    Uses INSERT … ON CONFLICT DO NOTHING for idempotency.
    """
    engine = engine or _pg_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.dim_market (
                market_name, district, county, source_market_id, latitude, longitude
            )
            SELECT DISTINCT
                market,
                district,
                county,
                source_market_id,
                latitude,
                longitude
            FROM staging.stg_food_prices
            WHERE market IS NOT NULL
            ON CONFLICT (market_name, district, county) DO NOTHING;
        """))

        conn.execute(text("""
            INSERT INTO warehouse.dim_commodity (
                commodity_name, category, unit, source_commodity_id
            )
            SELECT DISTINCT
                commodity,
                category,
                unit,
                source_commodity_id
            FROM staging.stg_food_prices
            WHERE commodity IS NOT NULL
            ON CONFLICT (commodity_name, unit) DO NOTHING;
        """))
    log.info("Dimensions populated (warehouse.dim_market, warehouse.dim_commodity).")


def load_fact_table(engine: Engine | None = None) -> None:
    """
    Populate warehouse.fact_prices by joining staging data
    to dimension keys.
    """
    engine = engine or _pg_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.fact_prices
                (date_id, market_id, commodity_id,
                 price_kes, price_usd, price_per_kg, pricetype, is_valid_price)
            SELECT
                d.date_id,
                m.market_id,
                c.commodity_id,
                s.price_kes,
                s.price_usd,
                s.price_per_kg,
                s.pricetype,
                s.is_valid_price
            FROM staging.stg_food_prices s
            JOIN warehouse.dim_date d
              ON d.full_date = s.price_date
            JOIN warehouse.dim_market m
              ON m.market_name = s.market
             AND m.district IS NOT DISTINCT FROM s.district
             AND m.county IS NOT DISTINCT FROM s.county
            JOIN warehouse.dim_commodity c
              ON c.commodity_name = s.commodity
             AND c.unit IS NOT DISTINCT FROM s.unit
            WHERE s.is_valid_price = TRUE
              AND NOT EXISTS (
                  SELECT 1
                  FROM warehouse.fact_prices f
                  WHERE f.date_id = d.date_id
                    AND f.market_id = m.market_id
                    AND f.commodity_id = c.commodity_id
                    AND f.pricetype IS NOT DISTINCT FROM s.pricetype
              );
        """))
    log.info("Fact table populated.")


def load_snowflake(df: pd.DataFrame) -> int:
    """
    Mirror the cleaned staging data to Snowflake.
    Uses write_pandas for best performance with large datasets.
    """
    try:
        from snowflake.connector.pandas_tools import write_pandas  # type: ignore
        import snowflake.connector as sf  # type: ignore
    except ImportError:
        log.warning("snowflake-connector-python not installed — skipping Snowflake load.")
        return 0

    conn = sf.connect(
        account   = os.environ["SF_ACCOUNT"],
        user      = os.environ["SF_USER"],
        password  = os.environ["SF_PASSWORD"],
        database  = os.environ["SF_DATABASE"],
        schema    = os.environ.get("SF_SCHEMA", "PUBLIC"),
        warehouse = os.environ.get("SF_WAREHOUSE", "COMPUTE_WH"),
        role      = os.environ.get("SF_ROLE", "SYSADMIN"),
    )

    # Snowflake wants UPPER CASE column names
    sf_df = df.copy()
    sf_df.columns = [c.upper() for c in sf_df.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn,
        sf_df,
        table_name="STG_FOOD_PRICES",
        auto_create_table=True,
        overwrite=False,
    )
    conn.close()
    log.info("Snowflake load: success=%s, chunks=%d, rows=%d", success, nchunks, nrows)
    return nrows


# ---------------------------------------------------------------------------
# Incremental helper
# ---------------------------------------------------------------------------

def _filter_existing_business_keys(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Return only rows whose natural business key is not already in staging."""
    key_cols = ["date", "market", "commodity", "unit", "pricetype"]
    missing = [col for col in key_cols if col not in df.columns]
    if missing:
        log.warning("Missing key columns for incremental load: %s — loading all rows.", missing)
        return df

    try:
        existing = pd.read_sql(
            text("""
                SELECT price_date AS date, market, commodity, unit, pricetype
                FROM staging.stg_food_prices
            """),
            engine,
        )
    except Exception as exc:
        log.warning("Could not query existing staging keys: %s — loading all rows.", exc)
        return df

    if existing.empty:
        log.info("Staging is empty — loading all rows.")
        return df

    incoming = df.copy()
    incoming["_etl_key"] = (
        incoming["date"].astype("string")
        + "|"
        + incoming["market"].astype("string")
        + "|"
        + incoming["commodity"].astype("string")
        + "|"
        + incoming["unit"].astype("string")
        + "|"
        + incoming["pricetype"].astype("string")
    )
    existing["_etl_key"] = (
        existing["date"].astype("string")
        + "|"
        + existing["market"].astype("string")
        + "|"
        + existing["commodity"].astype("string")
        + "|"
        + existing["unit"].astype("string")
        + "|"
        + existing["pricetype"].astype("string")
    )

    filtered = incoming.loc[~incoming["_etl_key"].isin(existing["_etl_key"])].drop(columns="_etl_key")
    log.info("Incremental filter kept %d of %d rows.", len(filtered), len(df))
    return filtered
