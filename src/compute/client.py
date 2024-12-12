# src/compute/client.py
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


class ComputeClient:
    def __init__(
        self,
        compute_endpoint: str = "localhost:15002",
        storage_endpoint: str = "localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
    ):
        logger.info(f"Initializing remote compute client with host: {compute_endpoint}")

        spark_remote_address = f"sc://{compute_endpoint}"
        print(f"Connecting to Spark remote address: {spark_remote_address}")
        self.spark = SparkSession.builder.remote(spark_remote_address).getOrCreate()
        print(f"Connected to Spark remote address: {spark_remote_address}")

    def get_session(self) -> SparkSession:
        return self.spark
