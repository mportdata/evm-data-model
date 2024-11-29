# src/layer_1_brz/client.py
import requests
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


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

    def get_code(self, address: str) -> str:
        payload = {
            "method": "eth_getCode",
            "params": [address, "latest"],
            "id": 2,
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
            "id": 3,
            "jsonrpc": "2.0",
        }

        response = requests.post(url=self.url, headers=self.headers, json=payload)
        if response.status_code != 200:
            raise Exception(f"RPC call failed: {response.text}")
        return response.json()["result"]

    def get_full_block_data(self, block_number: int) -> dict:
        """Get block with all related data (receipts and contract code)"""
        # Get the block first
        block = self.get_block(block_number)
        if not block:
            return None

        # For each transaction, get receipt and check if addresses have code
        if block.get("transactions"):
            address_codes = {}  # Cache for address codes

            for tx in block["transactions"]:
                # Get receipt
                tx["receipt"] = self.get_transaction_receipt(tx["hash"])

                # Check 'to' address for code if it exists
                if tx.get("to"):
                    if tx["to"] not in address_codes:
                        address_codes[tx["to"]] = self.get_code(tx["to"])
                    tx["to_code"] = address_codes[tx["to"]]

                # Check 'from' address for code
                if tx["from"] not in address_codes:
                    address_codes[tx["from"]] = self.get_code(tx["from"])
                tx["from_code"] = address_codes[tx["from"]]

        return block

    def get_block_range(self, start_block: int, end_block: int) -> List[dict]:
        """Get range of blocks with all related data"""
        blocks = []
        for block_num in range(start_block, end_block + 1):
            try:
                block = self.get_full_block_data(block_num)
                blocks.append(block)
                logger.info(
                    f"Fetched block {block_num} with {len(block.get('transactions', [])) if block else 0} transactions"
                )
            except Exception as e:
                logger.error(f"Error fetching block {block_num}: {e}")
                raise

        return blocks
