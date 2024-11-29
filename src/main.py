import requests
import json
import os
from dataclasses import dataclass
from typing import List, Optional
import pandas as pd
from datetime import datetime
from decimal import Decimal
import duckdb

# Environment setup
RPC_URL = os.getenv("QUICKNODE_URL")
if not RPC_URL:
    raise EnvironmentError("QUICKNODE_URL environment variable not set")


def hex_to_int(hex_str: str) -> int:
    return int(hex_str, 16) if hex_str and hex_str.startswith("0x") else 0


def hex_to_decimal(hex_str: str, decimals: int = 18) -> Decimal:
    """Convert hex string to decimal, considering ETH's 18 decimals"""
    wei_value = hex_to_int(hex_str)
    return Decimal(wei_value) / Decimal(10**decimals)


class RPCClient:
    def __init__(self, url: str):
        self.url = url
        self.headers = {"Content-Type": "application/json"}

    def get_block(self, block_number: int) -> dict:
        hex_block = hex(block_number)
        payload = {
            "method": "eth_getBlockByNumber",
            "params": [hex_block, True],
            "id": 1,
            "jsonrpc": "2.0",
        }

        response = requests.post(url=self.url, headers=self.headers, json=payload)

        if response.status_code != 200:
            raise Exception(f"RPC call failed: {response.text}")

        return response.json()["result"]

    def get_transaction_receipt(self, tx_hash: str) -> dict:
        payload = {
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash],
            "id": 1,
            "jsonrpc": "2.0",
        }

        response = requests.post(url=self.url, headers=self.headers, json=payload)

        if response.status_code != 200:
            raise Exception(f"RPC call failed: {response.text}")

        return response.json()["result"]

    def get_block_with_receipts(self, block_number: int) -> dict:
        block = self.get_block(block_number)
        if block and block.get("transactions"):
            for tx in block["transactions"]:
                receipt = self.get_transaction_receipt(tx["hash"])
                tx["receipt"] = receipt
        return block

    def get_block_range(self, start_block: int, end_block: int) -> List[dict]:
        blocks = []
        for block_num in range(start_block, end_block + 1):
            block = self.get_block_with_receipts(block_num)
            blocks.append(block)
            print(
                f"Fetched block {block_num} with {len(block.get('transactions', [])) if block else 0} transactions"
            )
        return blocks


class BronzeWriter:
    def __init__(self, base_path: str = "./data/bronze"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

    def write_blocks(self, blocks: List[dict], batch_id: str):
        """Write raw block data to bronze layer"""
        df = pd.DataFrame(blocks)
        path = f"{self.base_path}/blocks/batch={batch_id}/data.parquet"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)

    def write_transactions(self, blocks: List[dict], batch_id: str):
        """Write transaction data to bronze layer"""
        all_transactions = []
        for block in blocks:
            if block and block.get("transactions"):
                all_transactions.extend(block["transactions"])

        if all_transactions:
            df = pd.DataFrame(all_transactions)
            path = f"{self.base_path}/transactions/batch={batch_id}/data.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_parquet(path, index=False)


class GoldWriter:
    def __init__(self, warehouse_path: str = "./data/gold"):
        self.warehouse_path = os.path.abspath(warehouse_path)
        os.makedirs(warehouse_path, exist_ok=True)
        self.con = duckdb.connect()

        print(f"Warehouse path: {self.warehouse_path}")

        try:
            self.con.execute("INSTALL iceberg;")
            self.con.execute("LOAD iceberg;")
            print("Successfully loaded Iceberg extension")
        except Exception as e:
            print(f"Error with Iceberg: {e}")

        self._create_tables()

        print("\nVerifying tables after creation:")
        print(self.con.execute("SHOW TABLES").fetchdf())

    def _create_tables(self):
        try:
            self.con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS block_dim (
                    block_key VARCHAR,
                    parent_block_hash VARCHAR,
                    block_number BIGINT,
                    block_timestamp TIMESTAMP,
                    miner_account_key VARCHAR,
                    block_gas_limit BIGINT
                ) WITH (
                    location = '{self.warehouse_path}/block_dim',
                    format = 'iceberg'
                )
            """
            )

            self.con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS transaction_fact (
                    transaction_hash_key VARCHAR,
                    block_key VARCHAR,
                    from_account_key VARCHAR,
                    to_account_key VARCHAR,
                    transaction_value DECIMAL(38,18),
                    gas_used BIGINT,
                    gas_price BIGINT,
                    transaction_status BIGINT,
                    block_number BIGINT
                ) WITH (
                    location = '{self.warehouse_path}/transaction_fact',
                    format = 'iceberg'
                )
            """
            )

            self.con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS account_dim (
                    account_key VARCHAR,
                    account_address VARCHAR,
                    is_contract_flag BOOLEAN,
                    first_seen_timestamp TIMESTAMP,
                    last_seen_timestamp TIMESTAMP
                ) WITH (
                    location = '{self.warehouse_path}/account_dim',
                    format = 'iceberg'
                )
            """
            )
            print("Successfully created tables")
        except Exception as e:
            print(f"Error creating tables: {e}")

    def write_dimensional_model(self, blocks: List[dict], batch_id: str):
        """Transform and write all dimensional tables"""
        block_dim = self._transform_block_dim(blocks)
        transaction_fact = self._transform_transaction_fact(blocks)
        account_dim = self._transform_account_dim(blocks)

        self._write_to_iceberg(block_dim, "block_dim")
        self._write_to_iceberg(transaction_fact, "transaction_fact")
        self._write_to_iceberg(account_dim, "account_dim")

    def _transform_block_dim(self, blocks: List[dict]) -> pd.DataFrame:
        transformed = []
        for block in blocks:
            if not block:
                continue
            transformed.append(
                {
                    "block_key": block["hash"],
                    "parent_block_hash": block["parentHash"],
                    "block_number": hex_to_int(block["number"]),
                    "block_timestamp": datetime.fromtimestamp(
                        hex_to_int(block["timestamp"])
                    ),
                    "miner_account_key": block["miner"],
                    "block_gas_limit": hex_to_int(block["gasLimit"]),
                }
            )
        return pd.DataFrame(transformed)

    def _transform_transaction_fact(self, blocks: List[dict]) -> pd.DataFrame:
        transformed = []
        for block in blocks:
            if not block or "transactions" not in block:
                continue
            block_number = hex_to_int(block["number"])
            for tx in block["transactions"]:
                if not tx.get("receipt"):
                    continue
                transformed.append(
                    {
                        "transaction_hash_key": tx["hash"],
                        "block_key": tx["blockHash"],
                        "from_account_key": tx["from"],
                        "to_account_key": tx["to"] if tx["to"] else None,
                        "transaction_value": hex_to_decimal(tx["value"]),
                        "gas_used": hex_to_int(tx["receipt"]["gasUsed"]),
                        "gas_price": hex_to_int(tx["gasPrice"]),
                        "transaction_status": hex_to_int(tx["receipt"]["status"]),
                        "block_number": block_number,
                    }
                )
        return pd.DataFrame(transformed)

    def _transform_account_dim(self, blocks: List[dict]) -> pd.DataFrame:
        accounts = set()
        for block in blocks:
            if not block or "transactions" not in block:
                continue
            accounts.add(block["miner"])
            for tx in block["transactions"]:
                accounts.add(tx["from"])
                if tx["to"]:
                    accounts.add(tx["to"])

        transformed = [
            {
                "account_key": addr,
                "account_address": addr,
                "is_contract_flag": False,
                "first_seen_timestamp": datetime.fromtimestamp(
                    hex_to_int(blocks[0]["timestamp"])
                ),
                "last_seen_timestamp": datetime.fromtimestamp(
                    hex_to_int(blocks[-1]["timestamp"])
                ),
            }
            for addr in accounts
        ]

        return pd.DataFrame(transformed)

    def _write_to_iceberg(self, df: pd.DataFrame, table_name: str):
        if not df.empty:
            try:
                self.con.register("temp_df", df)
                print(f"\nWriting {len(df)} rows to {table_name}")
                self.con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                print(f"Successfully wrote to {table_name}")
                count = self.con.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                print(f"Table {table_name} now has {count} rows")
                self.con.unregister("temp_df")
            except Exception as e:
                print(f"Error writing to {table_name}: {e}")

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query against the dimensional model"""
        return self.con.execute(sql).fetchdf()


def main():
    # Initialize clients
    rpc = RPCClient(RPC_URL)
    bronze = BronzeWriter()
    gold = GoldWriter()

    # Example: Load blocks 390000 to 390010
    end_block = 21281966
    start_block = end_block - 3

    print(f"Fetching blocks {start_block} to {end_block}...")
    blocks = rpc.get_block_range(start_block, end_block)

    # Write to bronze layer
    batch_id = f"{start_block}-{end_block}"
    bronze.write_blocks(blocks, batch_id)
    bronze.write_transactions(blocks, batch_id)
    print(f"Wrote {len(blocks)} blocks to bronze layer")

    # Write to gold layer
    gold.write_dimensional_model(blocks, batch_id)
    print("Wrote dimensional model to gold layer")

    # Example query
    result = gold.query(
        """
        SELECT 
            b.block_number,
            COUNT(t.transaction_hash_key) as tx_count,
            SUM(t.transaction_value) as total_value
        FROM transaction_fact t
        JOIN block_dim b ON t.block_key = b.block_key
        GROUP BY b.block_number
        ORDER BY b.block_number
    """
    )
    print("\nTransaction summary by block:")
    print(result)


if __name__ == "__main__":
    main()
