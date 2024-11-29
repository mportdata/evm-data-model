# src/layer_1_brz/writer.py
import os
from typing import List, Dict
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BronzeWriter:
    def __init__(self, base_path: str = "./data/layer_1_brz"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

    def write_blocks(self, blocks: List[dict], batch_id: str):
        """Write raw block data to bronze layer"""
        # Remove nested transaction data for clean block storage
        blocks_without_txs = []
        for block in blocks:
            block_copy = block.copy()
            block_copy.pop("transactions", None)
            blocks_without_txs.append(block_copy)

        df = pd.DataFrame(blocks_without_txs)
        path = f"{self.base_path}/blocks/batch={batch_id}/data.parquet"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Wrote blocks to {path}")

    def write_transactions(self, blocks: List[dict], batch_id: str):
        """Write transaction data with receipts and code to bronze layer"""
        all_transactions = []
        for block in blocks:
            if block and block.get("transactions"):
                for tx in block["transactions"]:
                    # Flatten receipt data into transaction
                    tx_copy = tx.copy()
                    receipt_data = tx_copy.pop("receipt", {})
                    tx_copy.update({f"receipt_{k}": v for k, v in receipt_data.items()})
                    all_transactions.append(tx_copy)

        if all_transactions:
            df = pd.DataFrame(all_transactions)
            path = f"{self.base_path}/transactions/batch={batch_id}/data.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_parquet(path, index=False)
            logger.info(f"Wrote {len(all_transactions)} transactions to {path}")

    def write_address_codes(self, blocks: List[dict], batch_id: str):
        """Write address code data to bronze layer"""
        address_codes = {}
        for block in blocks:
            if block and block.get("transactions"):
                for tx in block["transactions"]:
                    if tx.get("to_code"):
                        address_codes[tx["to"]] = tx["to_code"]
                    if tx.get("from_code"):
                        address_codes[tx["from"]] = tx["from_code"]

        if address_codes:
            df = pd.DataFrame(
                [
                    {"address": addr, "code": code}
                    for addr, code in address_codes.items()
                ]
            )
            path = f"{self.base_path}/address_codes/batch={batch_id}/data.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_parquet(path, index=False)
            logger.info(f"Wrote {len(address_codes)} address codes to {path}")
