import duckdb
from pyiceberg.catalog import load_catalog

#Load the Iceberg catalog
catalog = load_catalog("default")
table_name = "financial.transactions"

try: 
    print(f"Loading Iceberg table: {table_name}")
    table = catalog.load_table(table_name)
except Exception as e:
    print(f"Error loading Iceberg table: {e}. Check your catalog settings or ensure data has been written.")
    exit(1)

#Scan the Iceberg table data into a PyArrow table
arrow_table = table.scan().to_arrow()
print(f"Successfully loaded {len(arrow_table)} rows into PyArroq from LocalStack S3!")

#Initializa DuckDB and register the table.
con = duckdb.connect(database=':memory:')
con.register("transactions", arrow_table)

print("\n" + "="*80)
print(" DUCKDB SQL LAKEHOUSE QUERY RESULTS")
print("\n" + "="*80)

#Query 1: Summary
summary = con.execute("""
    SELECT
        COUNT(*)
        ROUND(SUM(amount), 2) as total_volume,
        ROUND(AVG(amount), 2) as average_amount,
        Max(amount) as max_transaction
    FROM transactions
""").fetchdf()

print("1. Transaction Volume Summary")
print(summary.to_string(index=False))
print("\n" + "="*80)