from typing import List

from delta import DeltaTable
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

from src.utils import *

spark = get_spark_session("HealthCareIngest")

input_schema = (
    "member_id string, first_name string, last_name string, dob string, gender string, phone string, "
    "email string, zip5 string, plan_id string"
)

df = spark.readStream.csv(
    header=True,
    path=raw_loc,
    mode="PERMISSIVE",
    columnNameOfCorruptRecord="_corrupt",
    dateFormat="yyyy-MM-dd",
    emptyValue="",
    schema=input_schema,
)


def parse_df(df: DataFrame) -> DataFrame:
    df_filtered = df.withColumn("total_rows", count("*").over(Window.partitionBy("file_path"))).filter(
        df.member_id.isNotNull() & df.plan_id.isNotNull() & df.plan_id.startswith("PLN")
    )
    return (
        df_filtered.withColumn(
            "gender",
            when(df_filtered.gender.isin("F", "f"), "FEMALE")
            .when(df_filtered.gender.isin("M", "m"), "MALE")
            .otherwise(upper(df_filtered.gender)),
        )
        .withColumn("phone", regexp_replace("phone", r"[()\-\s]", ""))
        .withColumn("member_id", col("member_id").try_cast("int"))
        .withColumn("zip5", substring(trim("zip5"), 1, 5).try_cast("int"))
        .withColumn(
            "dob",
            when(
                regexp_like("dob", lit(r"\d{2}\/\d{2}\/\d{4}")),
                to_date("dob", "MM/dd/yyyy"),
            )
            .when(
                regexp_like("dob", lit(r"\d{4}\/\d{2}\/\d{2}")),
                to_date("dob", "yyyy/dd/MM"),
            )
            .when(col("dob").contains(" "), to_date("dob", "dd MMM yyyy"))
            .when(
                regexp_like("dob", lit(r"\d{4}-\d{2}-\d{2}")),
                to_date("dob", "yyyy-MM-dd"),
            )
            .otherwise(lit(None)),
        )
        .withColumn("file_name", element_at(split(col("file_path"), "/"), -1))
        .withColumn("client_id", element_at(split(col("file_name"), "_"), 1))
        .withColumn(
            "creation_date",
            to_date(substring(element_at(split(col("file_name"), "_"), 2), 0, 10)),
        )
        .withColumn("ingestion_time", current_timestamp())
        .filter(col("member_id").isNotNull())
        .drop("file_path")
        .drop_duplicates()
    )


def generate_merge_cond(columns: List[str]) -> str:
    return " and ".join([f"source.{clm} <=> target.{clm}" for clm in columns])


def merge(source_df: DataFrame, _: int) -> None:
    parsed_df = parse_df(source_df).cache()
    final_df = parsed_df.drop("total_rows")

    if not DeltaTable.isDeltaTable(spark, silver_loc):
        final_df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").save(silver_loc)
        print("Initial Commit")
    else:
        delta_table = DeltaTable.forPath(spark, path=silver_loc)
        delta_table.alias("target").merge(
            final_df.alias("source"),
            generate_merge_cond(["member_id", "first_name", "last_name"]),
        ).whenMatchedUpdate(
            condition="source.creation_date > target.creation_date",  # Only update if newer
            set={
                "phone": "source.phone",
                "email": "source.email",
                "zip5": "source.zip5",
                "plan_id": "source.plan_id",
                "creation_date": "source.creation_date",
                "ingestion_time": "source.ingestion_time",
            },
        ).whenNotMatchedInsertAll().execute()

    audit_df = (
        parsed_df.groupby("client_id", "file_name", "total_rows")
        .agg(count("*").alias("valid_count"))
        .withColumn("invalid_count", col("total_rows") - col("valid_count"))
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("run_id", lit(query_name))
    )
    audit_df.write.format("delta").mode("append").save(audit_log)
    print("checkpoint saved")
    parsed_df.unpersist()


# Check if Delta table exists
query = (
    df.withColumn("file_path", input_file_name())
    .writeStream.format("delta")
    .queryName(query_name)
    .option("checkpointLocation", stream_checkpoint_loc)
    .trigger(availableNow=True)
    .foreachBatch(merge)
    .start()
)

query.awaitTermination()

print("Parsed all new files")
