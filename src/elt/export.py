# src/layer_1_brz/client.py
import requests
import logging

logger = logging.getLogger(__name__)


class RPCClient:
    def __init__(self, url: str):
        self.url = url
        self.headers = {"Content-Type": "application/json"}

    def get_block_by_number(self, block_number: int) -> dict:
        hex_block = hex(block_number)
        payload = {
            "method": "eth_getBlockByNumber",
            "params": [hex_block, True],
            "id": 1,
            "jsonrpc": "2.0",
        }
        block = self._call_rpc_endpoint(payload=payload)
        return block

    def get_address_code(self, address: str) -> dict:
        payload = {
            "method": "eth_getCode",
            "params": [address, "latest"],
            "id": 2,
            "jsonrpc": "2.0",
        }
        code = self._call_rpc_endpoint(payload=payload)
        return code

    def get_tx_receipt(self, tx_hash: str) -> dict:
        payload = {
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash],
            "id": 3,
            "jsonrpc": "2.0",
        }
        tx_receipt = self._call_rpc_endpoint(payload=payload)
        return tx_receipt

    def _call_rpc_endpoint(self, payload: dict) -> dict:
        response = requests.post(url=self.url, headers=self.headers, json=payload)
        if response.status_code != 200:
            raise Exception(f"RPC call failed: {response.text}")
        return response.json()["result"]
