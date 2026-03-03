"""
db.py — SQLAlchemy engines and a simple query helper.
"""
from sqlalchemy import create_engine, text
import pandas as pd
from config import LOCAL_DB_URL, AI_DB_URL, DWH_DB_URL

local_engine = create_engine(
    LOCAL_DB_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    connect_args={"connect_timeout": 10},
)

ai_engine = create_engine(
    AI_DB_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    connect_args={"connect_timeout": 15},
)

dwh_engine = create_engine(
    DWH_DB_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    connect_args={"connect_timeout": 15},
)


def query_df(engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    """Execute a SQL string and return a DataFrame. Raises on error."""
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        rows = result.fetchall()
        cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)
