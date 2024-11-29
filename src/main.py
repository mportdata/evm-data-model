# src/main.py
from layer_1_brz.client import RPCClient
from layer_1_brz.writer import BronzeWriter

import os
from typing import List
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment setup
RPC_ENDPOINT = os.getenv("RPC_ENDPOINT")
if not RPC_ENDPOINT:
    raise EnvironmentError("RPC_ENDPOINT environment variable not set")


def main():
    # Initialize clients
    rpc = RPCClient(RPC_ENDPOINT)
    bronze = BronzeWriter()

    # Example: Load blocks 390000 to 390010
    start_block = 390000
    end_block = 390010

    logger.info(f"Fetching blocks {start_block} to {end_block}...")
    try:
        # Get full block data including receipts and contract code
        blocks = rpc.get_block_range(start_block, end_block)

        # Write to bronze layer
        batch_id = f"{start_block}-{end_block}"
        bronze.write_blocks(blocks, batch_id)
        bronze.write_transactions(blocks, batch_id)
        bronze.write_address_codes(blocks, batch_id)

        logger.info(
            f"Successfully wrote block range {start_block}-{end_block} to bronze layer"
        )

    except Exception as e:
        logger.error(f"Error processing blocks: {e}")
        raise


if __name__ == "__main__":
    main()
