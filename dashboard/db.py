import os

import pandas as pd
import psycopg2
import streamlit as st

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://cartracker@postgres:5432/cartracker",
)


@st.cache_resource
def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


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
