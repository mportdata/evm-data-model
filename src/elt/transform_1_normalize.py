# src/elt/transform_1_normalize.py
from datetime import datetime
from typing import Optional
from dataclasses import asdict
import json
from models.models import Block_Datatype
from pyspark.sql import SparkSession
from minio import Minio
import logging
from io import BytesIO

logger = logging.getLogger(__name__)


class Blocks_Normalizer:
    def __init__(
        self,
        storage_client: Minio,
        compute_client: SparkSession,
        warehouse_path: str = "./warehouse",
    ):
        self.storage_client = storage_client
        self.warehouse_path = warehouse_path
        self.compute_client = compute_client

        self._create_table()

    def _create_table(self):
        """Create or get the Iceberg table"""
        try:
            self.compute_client.sql(
                """
                CREATE TABLE IF NOT EXISTS integration.blocks (
                    hash STRING,
                    parent_hash STRING,
                    number BIGINT,
                    timestamp TIMESTAMP,
                    miner STRING,
                    gas_limit BIGINT,
                    gas_used BIGINT,
                    base_fee_per_gas BIGINT,
                    size BIGINT,
                    difficulty BIGINT
                ) USING iceberg
                PARTITIONED BY (days(timestamp))
            """
            )
            logger.info("Successfully created/verified blocks table")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def read_from_bronze(self, block_number: int) -> Optional[Block_Datatype]:
        """Read block from staging bucket"""
        try:
            key_string = str(block_number).zfill(10)
            object_path = f"blocks/block_{key_string}.json"

            # Get object from MinIO
            data = self.storage_client.get_object("staging", object_path)

            # Read JSON data
            raw_block = json.loads(data.read())

            # Transform raw data into Block dataclass
            return Block_Datatype(
                hash=raw_block["hash"],
                parent_hash=raw_block["parentHash"],
                number=int(raw_block["number"], 16),
                timestamp=datetime.fromtimestamp(int(raw_block["timestamp"], 16)),
                miner=raw_block["miner"],
                gas_limit=int(raw_block["gasLimit"], 16),
                gas_used=int(raw_block["gasUsed"], 16),
                base_fee_per_gas=int(raw_block["baseFeePerGas"], 16),
                size=int(raw_block["size"], 16),
                difficulty=int(raw_block["difficulty"], 16),
            )
        except Exception as e:
            logger.error(f"Error reading block {block_number} from bronze: {e}")
            raise

    def write_to_silver(self, block: Block_Datatype):
        """Write block to Iceberg table"""
        if not block:
            return

        try:
            # Convert block to dict
            block_dict = asdict(block)

            # Create DataFrame from block
            df = self.compute_client.createDataFrame([block_dict])

            # Write to Iceberg table
            df.writeTo("local.integration.blocks").append()

            logger.info(
                f"Successfully wrote block {block.number} to integration.blocks"
            )

        except Exception as e:
            logger.error(f"Error writing block to silver: {e}")
            raise

    def batch_write_to_silver(self, blocks: list[Block_Datatype]):
        """Write multiple blocks to Iceberg table in batch"""
        if not blocks:
            return

        try:
            # Convert blocks to dicts
            block_dicts = [asdict(block) for block in blocks]

            # Create DataFrame from blocks
            df = self.compute_client.createDataFrame(block_dicts)

            # Write to Iceberg table
            df.writeTo("local.integration.blocks").append()

            logger.info(
                f"Successfully wrote {len(blocks)} blocks to integration.blocks"
            )

        except Exception as e:
            logger.error(f"Error batch writing blocks to silver: {e}")
            raise
