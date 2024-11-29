# tests/01_read_rpc.py
import os
import requests
import json

BASE_RPC_ENDPOINT = os.getenv("RPC_ENDPOINT")
if not BASE_RPC_ENDPOINT:
    raise EnvironmentError("RPC_ENDPOINT environment variable not set")


def test_base_rpc():
    # Test block retrieval
    block_payload = {
        "method": "eth_getBlockByNumber",
        "params": ["0x5F4E6", True],
        "id": 1,
        "jsonrpc": "2.0",
    }

    # Test account balance retrieval
    balance_payload = {
        "method": "eth_getBalance",
        "params": ["0x4200000000000000000000000000000000000011", "latest"],
        "id": 2,
        "jsonrpc": "2.0",
    }

    # Test if address is contract
    code_payload = {
        "method": "eth_getCode",
        "params": ["0x4200000000000000000000000000000000000011", "latest"],
        "id": 3,
        "jsonrpc": "2.0",
    }

    # Test transaction receipt retrieval
    receipt_payload = {
        "method": "eth_getTransactionReceipt",
        "params": [
            "0x2d3750f7944818e1a4e6b897b6f6ea4d975fcf46720990eb4eda0c00d926a8fd"
        ],
        "id": 4,
        "jsonrpc": "2.0",
    }

    headers = {"Content-Type": "application/json"}

    # Make the calls
    for payload in [block_payload, balance_payload, code_payload, receipt_payload]:
        response = requests.post(BASE_RPC_ENDPOINT, headers=headers, json=payload)
        if response.status_code != 200:
            raise Exception(f"RPC call failed: {response.text}")

        print(f"\nTesting {payload['method']}:")
        print(json.dumps(response.json()["result"], indent=2))


if __name__ == "__main__":
    try:
        test_base_rpc()
    except Exception as e:
        print(f"Error: {e}")
