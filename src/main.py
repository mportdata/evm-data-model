# src/main.py
from elt.export import RPCClient
from elt.load import Loader
from elt.transform_1_normalize import Blocks_Normalizer
from storage.client import StorageClient
from compute.client import ComputeClient
import os

RPC_ENDPOINT = os.getenv("RPC_ENDPOINT")
STORAGE_USER = os.getenv("MINIO_USER")
STORAGE_PASSWORD = os.getenv("MINIO_PASSWORD")
STORAGE_ENDPOINT = "localhost:9000"
COMPUTE_ENDPOINT = "localhost:15002"

# Validate environment variables
if not RPC_ENDPOINT:
    raise EnvironmentError("RPC_ENDPOINT environment variable not set")
if not STORAGE_USER or not STORAGE_PASSWORD:
    raise EnvironmentError("STORAGE_USER and STORAGE_PASSWORD must be set")


def main():
    # Initialize clients
    storage_client = StorageClient(access_key=STORAGE_USER, secret_key=STORAGE_PASSWORD)

    compute_client = ComputeClient(
        compute_endpoint=COMPUTE_ENDPOINT,
        storage_endpoint=STORAGE_ENDPOINT,
        access_key=STORAGE_USER,
        secret_key=STORAGE_PASSWORD,
    )

    rpc = RPCClient(RPC_ENDPOINT)
    loader = Loader(storage_client.get_client())

    # Initialize normalizer
    blocks_normalizer = Blocks_Normalizer(
        storage_client=storage_client.get_client(),
        compute_client=compute_client.get_session(),
    )

    start_block = 390000
    end_block = 390010

    print(f"Fetching blocks {start_block} to {end_block}...")

    block_number_range = range(start_block, end_block)

    for block_number in block_number_range:
        try:
            block = rpc.get_block_by_number(block_number)
            loader.write_block(block_number, block)
        except Exception as e:
            print(f"Error processing blocks: {e}")
            raise
    print(f"Successfully loaded blocks {start_block} to {end_block}")

    # Transform blocks to integration
    blocks = []
    for block_number in block_number_range:
        block = blocks_normalizer.read_from_bronze(block_number)
        blocks.append(block)

    # Batch write to integration
    blocks_normalizer.batch_write_to_silver(blocks)


if __name__ == "__main__":
    main()
