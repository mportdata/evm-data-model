# src/layer_1_brz/writer.py
from typing import List, Dict
import logging
import json
from minio import Minio
from io import BytesIO

logger = logging.getLogger(__name__)


class Loader:
    def __init__(self, client: Minio, bucket: str = "staging"):
        self.client = client
        self.bucket = bucket

        # Ensure bucket exists
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)

    def write_block(self, block_number: int, block: dict):
        key_string = str(block_number).zfill(10)
        self._write_from_rpc_result(name="block", key_string=key_string, result=block)

    def write_address_code(self, address: int, address_code: dict):
        self._write_from_rpc_result(
            name="address_code", key_string=str(address), result=address_code
        )

    def _write_from_rpc_result(self, name: str, key_string: str, result: dict):
        # Construct the object path (key) in MinIO
        object_path = f"{name}s/{name}_{key_string}.json"

        # Convert dictionary to JSON string and then to bytes
        json_data = json.dumps(result, indent=2).encode("utf-8")

        # Create a BytesIO object
        data_stream = BytesIO(json_data)

        try:
            # Upload to MinIO
            self.client.put_object(
                bucket_name=self.bucket,
                object_name=object_path,
                data=data_stream,
                length=len(json_data),
                content_type="application/json",
            )
            logger.info(
                f"Successfully wrote {name} {key_string} to s3://{self.bucket}/{object_path}"
            )
        except Exception as e:
            logger.error(f"Error writing to MinIO: {e}")
            raise

    def list_objects(self, prefix: str = ""):
        """Utility method to list objects in bucket"""
        try:
            objects = self.client.list_objects(self.bucket, prefix=prefix)
            for obj in objects:
                print(f"{obj.bucket_name}/{obj.object_name}")
        except Exception as e:
            logger.error(f"Error listing objects: {e}")
            raise
