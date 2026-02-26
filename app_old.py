import streamlit as st
import psycopg
import pandas as pd
from sqlalchemy import create_engine, text

# ---------- 1) Read secrets ----------
cfg = st.secrets["postgres"]

HOST = cfg["host"]
PORT = cfg["port"]
DBNAME = cfg["dbname"]
USER = cfg["user"]
PASSWORD = cfg["password"]

st.title("PostgreSQL (Streamlit secrets) test")

# ---------- 2) psycopg2 connection test ----------
try:
    conn = psycopg.connect(
        host=HOST,
        port=PORT,
        dbname=DBNAME,
        user=USER,
        password=PASSWORD,
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()[0]
    st.success("Database connection OK")
    st.write("PostgreSQL version:", version)

    cur.close()
    conn.close()

except Exception as e:
    st.error(f"psycopg connection failed: {e}")
    st.stop()

# ---------- 3) SQLAlchemy engine ----------
# IMPORTANT: encode special chars in password if you build URL manually.
# Best: use sqlalchemy URL parts OR ensure password is URL-encoded.
#connection_string = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
connection_string = f"postgresql+psycopg://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
try:
    engine = create_engine(connection_string)
    # simple test
    with engine.connect() as c:
        c.execute(text("SELECT 1"))
    st.success("SQLAlchemy engine OK")
except Exception as e:
    st.error(f"SQLAlchemy engine failed: {e}")
    st.stop()

# ---------- 4) Read a table ----------
schema_name = st.text_input("Schema", value="dummy_schema")
table_name = st.text_input("Table", value="dummy_table")

if st.button("Load table"):
    try:
        df = pd.read_sql_table(table_name=table_name, con=engine, schema=schema_name)
        st.write(f"Rows: {len(df)}")
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Reading table failed: {e}")
        st.info("Check that the schema/table exist and that your user has USAGE on schema + SELECT on table.")