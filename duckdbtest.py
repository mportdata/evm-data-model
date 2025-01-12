import duckdb

# Connect to the database
duckdb.connect("blockchain_data_warehouse.db") 

# Configure S3/MinIO connection
duckdb.sql(
    """
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl='false';
    SET s3_url_style='path';
"""
)

# Create tables from JSON data
duckdb.sql(
    """
    CREATE TABLE blocks AS 
    SELECT * FROM read_json_auto('s3://raw/blocks/*.json')
"""
)

duckdb.sql(
    """
    CREATE TABLE transaction_receipts AS 
    SELECT * FROM read_json_auto('s3://raw/transaction_receipts/*.json')
"""
)

duckdb.sql(
    """
    CREATE TABLE address_codes AS 
    SELECT * FROM read_json_auto('s3://raw/address_codes/*.json')
"""
)

# Verify tables were created
print(duckdb.sql("SHOW TABLES").fetchall())

# Check data
print(duckdb.sql("SELECT * FROM blocks LIMIT 5").fetchall())
