# src/main.py
import duckdb
from elt.export import RPCClient
from elt.load import Loader
from storage.client import StorageClient
import os

RPC_ENDPOINT = os.getenv("RPC_ENDPOINT")
STORAGE_USER = os.getenv("MINIO_USER")
STORAGE_PASSWORD = os.getenv("MINIO_PASSWORD")
STORAGE_ENDPOINT = "localhost:9000"

# Validate environment variables
if not RPC_ENDPOINT:
    raise EnvironmentError("RPC_ENDPOINT environment variable not set")
if not STORAGE_USER or not STORAGE_PASSWORD:
    raise EnvironmentError("STORAGE_USER and STORAGE_PASSWORD must be set")

def main():
    # Initialize storage client and RPC client
    storage_client = StorageClient(access_key=STORAGE_USER, secret_key=STORAGE_PASSWORD)
    rpc = RPCClient(RPC_ENDPOINT)
    loader = Loader(storage_client.get_client())

    # Configure DuckDB's S3 settings first
    duckdb.sql("""
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
        SET s3_use_ssl='false';
        SET s3_url_style='path';
    """)
    
    # Connect to database stored in MinIO
    con = duckdb.connect('s3://warehouse/blockchain.db')
    
    start_block = 390000
    end_block = 390010

    print(f"Fetching blocks {start_block} to {end_block}...")

    # Extract and Load phase
    block_number_range = range(start_block, end_block)
    for block_number in block_number_range:
        try:
            block = rpc.get_block_by_number(block_number)
            loader.write_block(block_number, block)
        except Exception as e:
            print(f"Error processing blocks: {e}")
            raise
    print(f"Successfully loaded blocks {start_block} to {end_block}")

    # Transform phase - Create normalized tables from JSON data
    print("Creating normalized tables in DuckDB...")
    
    # Create blocks table
    con.sql("""
        CREATE TABLE IF NOT EXISTS blocks AS 
        SELECT * FROM read_json_auto('s3://raw/blocks/*.json')
    """)

    # Create transaction_receipts table
    con.sql("""
        CREATE TABLE IF NOT EXISTS transaction_receipts AS 
        SELECT * FROM read_json_auto('s3://raw/transaction_receipts/*.json')
    """)

    # Create address_codes table
    con.sql("""
        CREATE TABLE IF NOT EXISTS address_codes AS 
        SELECT * FROM read_json_auto('s3://raw/address_codes/*.json')
    """)

    # Verify tables were created
    print("Created tables:")
    print(con.sql("SHOW TABLES").fetchall())

    # Optional: Sample data verification
    print("\nSample data from blocks table:")
    print(con.sql("SELECT * FROM blocks LIMIT 5").fetchall())

if __name__ == "__main__":
    main()
