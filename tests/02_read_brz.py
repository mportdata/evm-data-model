# tests/02_read_brz.py
import pandas as pd
import os
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_bronze_data(batch_id: str) -> Dict[str, pd.DataFrame]:
    """Read all bronze layer data for a specific batch"""
    base_path = "./data/layer_1_brz"
    data = {}

    # Read blocks
    block_path = f"{base_path}/blocks/batch={batch_id}/data.parquet"
    if os.path.exists(block_path):
        data["blocks"] = pd.read_parquet(block_path)
        logger.info(f"Read {len(data['blocks'])} blocks")
        logger.info("\nBlock columns:")
        logger.info(data["blocks"].columns.tolist())

    # Read transactions
    tx_path = f"{base_path}/transactions/batch={batch_id}/data.parquet"
    if os.path.exists(tx_path):
        data["transactions"] = pd.read_parquet(tx_path)
        logger.info(f"\nRead {len(data['transactions'])} transactions")
        logger.info("\nTransaction columns:")
        logger.info(data["transactions"].columns.tolist())

    # Read address codes
    code_path = f"{base_path}/address_codes/batch={batch_id}/data.parquet"
    if os.path.exists(code_path):
        data["address_codes"] = pd.read_parquet(code_path)
        logger.info(f"\nRead {len(data['address_codes'])} address codes")
        logger.info("\nAddress code columns:")
        logger.info(data["address_codes"].columns.tolist())

    return data


def examine_data(data: Dict[str, pd.DataFrame]):
    """Print some basic analysis of the data"""
    for name, df in data.items():
        logger.info(f"\n{name.upper()} Summary:")
        logger.info(f"Shape: {df.shape}")
        logger.info("\nSample data:")
        logger.info(df.head())
        logger.info("\nData types:")
        logger.info(df.dtypes)


def main():
    batch_id = "390000-390010"  # Same as in our main script
    try:
        data = read_bronze_data(batch_id)
        examine_data(data)
    except Exception as e:
        logger.error(f"Error reading data: {e}")
        raise


if __name__ == "__main__":
    main()
