import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType, TimestampType
from pyiceberg.exceptions import NoSuchTableError
import pyarrow.fs as pafs
import json
from datetime import datetime, timedelta, timezone
import time
import os
from confluent_kafka import Consumer, KafkaError

#pafs.initialize_s3(pafs.S3LogLevel.Debug)

BUCKET_NAME = "my-portfolio-pipeline-data-2026"
REGION = "us-east-2"


catalog_properties = {
    "type": "glue",
    "warehouse": f"s3://{BUCKET_NAME}/warehouse",
    "glue.region": REGION,
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    "s3.region": REGION,
    "s3.endpoint": f"https://s3.{REGION}.amazonaws.com",
}


catalog = load_catalog("default", **catalog_properties)


table_name = "financial.transactions"
schema = Schema(
    NestedField(1, "transaction_id", StringType(), required=True),
    NestedField(2, "transaction_type", StringType(), required=True),
    NestedField(3, "timestamp", TimestampType(), required=True),
    NestedField(4, "account_number", StringType(), required=True),
    NestedField(5, "amount", DoubleType(), required=True),
    NestedField(6, "currency", StringType(), required=True),
    NestedField(7, "merchant_name", StringType(), required=True),
    NestedField(8, "merchant_category", StringType(), required=True),
    NestedField(9, "merchant_address", StringType(), required=True),
    NestedField(10, "merchant_country", StringType(), required=True),
    NestedField(11, "is_international", StringType(), required=True),
    NestedField(12, "fraud_score", DoubleType(), required=True)
)

try:
    table = catalog.load_table(table_name)
    table.metadata
    print(f"Loaded exisiting Icebergtable: {table_name}")
except (NoSuchTableError) as e:
    try:
        catalog.create_namespace("financial")
    except Exception:
        pass
    table = catalog.create_table(table_name, schema=schema)
    print(f"Created new Iceberg table: {table_name}")

conf ={
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'financial-transaction-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit':False
}

consumer = Consumer(conf)
consumer.subscribe(['financial-transactions'])

#Set the run time.
RUN_DURATION_MINUTES = 1
start_time = datetime.now()
end_time = start_time + timedelta(minutes=RUN_DURATION_MINUTES)

print("Listening for financial transactions...Press Ctrl+C to exit.")

batch = []
BATCH_SIZE = 100

def parse_timestamp(ts_value):
    dt = None
    if isinstance(ts_value, (int, float)):
        dt = datetime.fromtimestamp(ts_value, tz=timezone.utc)
    elif isinstance(ts_value, str):
        try: 
            #handle standard ISO format strings.
            dt = datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
        except ValueError:
            pass
    if dt is None:
        dt = datetime.now(tz=timezone.utc)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

try: 
    while True:
        if datetime.now() >= end_time:
            print(f"\nTime limit of {RUN_DURATION_MINUTES} minutes reached. Stopping script...")
            break

        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        raw_transaction = json.loads(msg.value().decode('utf-8'))

        #Explicitly map and cast fields to match the Iceberg schema types.
        cleaned_transaction = {
            "transaction_id": str(raw_transaction.get("transaction_id", "")),
            "transaction_type": str(raw_transaction.get("transaction_type", "")),
            "timestamp": parse_timestamp(raw_transaction.get("timestamp", "")),
            "account_number": str(raw_transaction.get("account_number", "")),
            "amount": float(raw_transaction.get("amount", 0.0)),
            "currency": str(raw_transaction.get("currency", "")),
            "merchant_name": str(raw_transaction.get("merchant_name", "")),
            "merchant_category": str(raw_transaction.get("merchant_category", "")),
            "merchant_address": str(raw_transaction.get("merchant_address", "")),
            "merchant_country": str(raw_transaction.get("merchant_country", "")),
            "is_international": str(raw_transaction.get("is_international", False)),
            "fraud_score": float(raw_transaction.get("fraud_score", 0.0))
        }

        print_friendly = cleaned_transaction.copy()
        print_friendly["timestamp"] = cleaned_transaction["timestamp"].isoformat()
        #print(f"Received transaction: {json.dumps(print_friendly, indent=2)}")
        batch.append(cleaned_transaction)

        if len(batch) >= BATCH_SIZE:
            arrow_table = pa.Table.from_pylist(batch, schema=table.schema().as_arrow())
            table.upsert(arrow_table, join_cols=["transaction_id"])
            consumer.commit(asynchronous=False)
            print(f"Upserted {len(batch)} transactions to Iceberg table: {table_name}")
            batch.clear()

except KeyboardInterrupt:
    print("\nScript interrupted by use.")

finally:
    #Flush any remaining items in the batch on exit
    if batch:
        arrow_table = pa.Table.from_pylist(batch, schema=table.schema().as_arrow())
        table.upsert(arrow_table, join_cols=["transaction_id"])
        consumer.commit(asynchronous=False)
        print(f"Appended {len(batch)} transactions to Iceberg table: {table_name}")
        batch.clear()
    consumer.close()
    print("Consumer closed successfully")
    consumer.close()
    print("Consumer closed successfully")
