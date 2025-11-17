import hashlib
import os
from pathlib import Path

from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # points to src/
raw_loc = f"{BASE_DIR}/input/raw/"
silver_loc = f"{BASE_DIR}/output/silver/health_partner_data"
quarantine_loc = f"{BASE_DIR}/output/quarantine/"
stream_checkpoint_loc = f"{BASE_DIR}/output/checkpoint/data/"
audit_log = f"{BASE_DIR}/output/checkpoint/audit/"
query_name = "health_care_ingest_stream"


def get_spark_session(name: str) -> SparkSession:
    return (
        SparkSession.builder.master("local")
        .appName(name)
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.databricks.delta.commitInfo.userMetadata",
            f"{os.getenv('USER', 'unknown')}",
        )
        .getOrCreate()
    )


def get_file_checksum(file_path: str) -> str:
    """Calculate MD5 checksum of file content."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
