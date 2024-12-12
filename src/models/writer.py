# src/silver/writer.py
import duckdb
import os
from typing import List
import pandas as pd
from .models import Block, BlockRoots, BlockConsensus, BlockMetadata, BlockRunningTotals


class SlvWriter:
    def __init__(self, warehouse_path: str = "./data/silver"):
        self.warehouse_path = os.path.abspath(warehouse_path)
        os.makedirs(warehouse_path, exist_ok=True)
        self.con = duckdb.connect()

        # Enable Iceberg
        self.con.execute("INSTALL iceberg;")
        self.con.execute("LOAD iceberg;")

        self._create_tables()

    def _create_tables(self):
        """Create Iceberg tables if they don't exist"""
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS blocks (
                hash VARCHAR,
                parent_hash VARCHAR,
                number BIGINT,
                timestamp TIMESTAMP,
                miner VARCHAR,
                gas_limit BIGINT,
                gas_used BIGINT,
                base_fee_per_gas BIGINT,
                size BIGINT,
                difficulty BIGINT
            ) WITH (
                location = '{self.warehouse_path}/blocks',
                format = 'iceberg'
            )
        """
        )

        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS block_roots (
                block_hash VARCHAR,
                receipts_root VARCHAR,
                state_root VARCHAR,
                transactions_root VARCHAR
            ) WITH (
                location = '{self.warehouse_path}/block_roots',
                format = 'iceberg'
            )
        """
        )

        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS block_consensus (
                block_hash VARCHAR,
                nonce VARCHAR,
                mix_hash VARCHAR,
                sha3_uncles VARCHAR
            ) WITH (
                location = '{self.warehouse_path}/block_consensus',
                format = 'iceberg'
            )
        """
        )

        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS block_metadata (
                block_hash VARCHAR,
                logs_bloom VARCHAR,
                extra_data VARCHAR
            ) WITH (
                location = '{self.warehouse_path}/block_metadata',
                format = 'iceberg'
            )
        """
        )

        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS block_running_totals (
                block_hash VARCHAR,
                total_difficulty BIGINT,
                cumulative_gas_used BIGINT
            ) WITH (
                location = '{self.warehouse_path}/block_running_totals',
                format = 'iceberg'
            )
        """
        )

    def write_blocks(self, blocks: List[Block]):
        if not blocks:
            return
        df = pd.DataFrame([vars(b) for b in blocks])
        self.con.execute("CREATE TEMPORARY TABLE temp_blocks AS SELECT * FROM df")
        self.con.execute("INSERT INTO blocks SELECT * FROM temp_blocks")
        self.con.execute("DROP TABLE temp_blocks")

    def write_block_roots(self, roots: List[BlockRoots]):
        if not roots:
            return
        df = pd.DataFrame([vars(r) for r in roots])
        self.con.execute("CREATE TEMPORARY TABLE temp_roots AS SELECT * FROM df")
        self.con.execute("INSERT INTO block_roots SELECT * FROM temp_roots")
        self.con.execute("DROP TABLE temp_roots")

    def write_to_tables(self, blocks: List[Block], roots: List[BlockRoots]):
        self.write_blocks(self, blocks)
        self.write_block_roots(self, roots)
