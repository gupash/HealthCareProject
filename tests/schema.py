from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Define schema for the input test DataFrame
source_schema = StructType(
    [
        StructField("member_id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("dob", StringType()),
        StructField("gender", StringType()),
        StructField("phone", StringType()),
        StructField("email", StringType()),
        StructField("zip5", StringType()),
        StructField("plan_id", StringType()),
        StructField("file_path", StringType()),
    ]
)

# Define expected schema
expected_schema = StructType(
    [
        StructField("member_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("dob", DateType(), True),
        StructField("gender", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("zip5", IntegerType(), True),
        StructField("plan_id", StringType(), True),
        StructField("file_name", StringType(), False),
        StructField("client_id", StringType(), True),
        StructField("creation_date", DateType(), True),
        StructField("ingestion_time", TimestampType(), False),
    ]
)
