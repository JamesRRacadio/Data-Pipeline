import streamlit as st
import duckdb
from pyiceberg.catalog import load_catalog

st.set_page_config(page_title="Lakehouse SQL Studio", layout="wide")
st.title(" Financial Transaction Lakehouse Studio")

AWS_ACCESS_KEY = "AKIA25LOKDBAE2LLBTXV"
AWS_SECRET_KEY= "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"
BUCKET_NAME = "my-portfolio-pipeline-data-2026"
TARGET_REGION = "us-east-2"

@st.cache_resource
def get_arrow_table():

    catalog_properties = {
    "type": "sql",
    "uri": "sqlite:///c:/Users/Racadio/Desktop/iceberg_catalog.db",
    "warehouse": f"s3://{BUCKET_NAME}/warehouse",
    "s3.access-key-id": AWS_ACCESS_KEY,
    "s3.secret-access-key": AWS_SECRET_KEY,
    "s3.region": "us-east-2",
    "s3.endpoint": f"https://s3.us-east-2.amazonaws.com",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
}
    catalog = load_catalog("default", **catalog_properties)
    return catalog.load_table("financial.transactions").scan().to_arrow()

try:
    arrow_table = get_arrow_table()

    #Initialize DuckDB in-memory and register table.
    con = duckdb.connect(database=':memory:')
    con.register("transactions", arrow_table)

    #Fetch metrics safely with COALESCE to handle empty tables.
    result = con.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0.0) FROM transactions").fetchone()
    total_count = result[0] if result else 0
    total_vol = float(result[1]) if result and result[1] is not None else 0.0

    #KPI metrics row.
    col1, col2 = st.columns(2)
    col1.metric("Total Transactions", f"{total_count:,}" if total_count else "0")
    col2.metric("Total Volume ($)", f"${total_vol:,.2f}" if total_vol else "$0.00")

    st.markdown("---")

    #--- Interactice SQL Console ---
    st.subheader("SQLQueryConsole")
    default_query = "SELECT transaction_id, timestamp, amount, currency, merchant_category FROM transactions LIMIT 10"
    user_query = st.text_area("Write your SQL query below", value=default_query, height=120)

    if st.button("Run Query", type="primary"):
        try:
            result_df = con.execute(user_query).fetchdf()
            st.success(f"Query returned {len(result_df)} rows.")
            st.dataframe(result_df, width='stretch')
        except Exception as e:
            st.error(f"SQL Error: {e}")

    st.markdown("---")

    #Full Data Review
    st.subheader("Complete Dataset Viewer")
    st.dataframe(arrow_table.to_pandas(), width='stretch')

except Exception as e:
    st.error(
        f"Could not load Iceberg table. Make sure your consumer.py script has "
        f"successfully initialized the table and written metadata files to your "
        f"live AWS S3 bucket ({BUCKET_NAME}). \n\nError Details: {e}"
    )
        