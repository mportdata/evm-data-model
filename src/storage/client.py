# src/storage/client.py
from minio import Minio
import logging

logger = logging.getLogger(__name__)


class StorageClient:
    def __init__(self, access_key: str, secret_key: str):
        logger.info("Initializing storage client")
        self.client = Minio(
            "localhost:9000",
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )

    def get_client(self) -> Minio:
        return self.client
