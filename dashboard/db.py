import os

import duckdb
import pandas as pd
import psycopg2
import streamlit as st

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://cartracker@postgres:5432/cartracker",
)

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/analytics/analytics.duckdb")


@st.cache_resource
def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


@st.cache_resource
def get_duckdb_connection():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        get_connection.clear()
        conn = get_connection()
        return pd.read_sql(sql, conn, params=params)


def run_duckdb_query(sql: str, params=None) -> pd.DataFrame:
    con = get_duckdb_connection()
    if params:
        return con.execute(sql, params).df()
    return con.execute(sql).df()
