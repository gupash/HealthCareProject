import os
from itertools import chain
from typing import List

from delta import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import *

from healthcare.utils import (
    audit_log,
    get_file_checksum,
    get_spark_session,
    query_name,
    raw_loc,
    silver_loc,
)

spark = get_spark_session("HealthCareIngest")

all_files = os.listdir(raw_loc)
all_files_with_checksum = {file: get_file_checksum(f"{raw_loc}/{file}") for file in all_files}
files_to_process = all_files_with_checksum
all_files_df = spark.createDataFrame(list(all_files_with_checksum.items()), schema=["file_name", "checksum"])
files_checksum_map = create_map([lit(x) for x in chain(*all_files_with_checksum.items())])

if DeltaTable.isDeltaTable(spark, audit_log):
    audit_df = spark.read.format("delta").load(audit_log)
    if not audit_df.isEmpty():
        filtered_df = all_files_df.join(
            broadcast(audit_df.select("file_name", "checksum")),
            on=(all_files_df.file_name == audit_df.file_name) & (all_files_df.checksum == audit_df.checksum),
            how="left_anti",
        )
        files_to_process = {row[0]: row[1] for row in filtered_df.select("file_name", "checksum").collect()}
        print(f"Files To Process: {files_to_process}")

input_schema = (
    "member_id string, first_name string, last_name string, dob string, gender string, phone string, "
    "email string, zip5 string, plan_id string"
)

df = spark.read.csv(
    header=True,
    path=[f"{raw_loc}/{file}" for file in files_to_process.keys()],
    mode="PERMISSIVE",
    columnNameOfCorruptRecord="_corrupt",
    dateFormat="yyyy-MM-dd",
    emptyValue="",
    schema=input_schema,
)

df_filtered = (
    df.withColumn("file_path", input_file_name())
    .withColumn("total_rows", count("*").over(Window.partitionBy("file_path")))
    .filter(df.member_id.isNotNull() & df.plan_id.isNotNull() & df.plan_id.startswith("PLN"))
)

df_parsed = (
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
    .withColumn("checksum", files_checksum_map[col("file_name")])
    .filter(col("member_id").isNotNull())
    .drop("file_path")
    .drop_duplicates()
).cache()


def generate_merge_cond(columns: List[str]) -> str:
    return " and ".join([f"source.{clm} <=> target.{clm}" for clm in columns])


if not df_parsed.isEmpty():
    # Check if Delta table exists
    if os.path.exists(silver_loc):
        deltaTable = DeltaTable.forPath(spark, path=silver_loc)
        deltaTable.alias("target").merge(
            df_parsed.alias("source"),
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
    else:
        # Create new Delta table with first batch
        df_parsed.write.format("delta").mode("overwrite").save(silver_loc)

    print("Parsed all new files")

    audit_df = (
        df_parsed.groupby("client_id", "file_name", "checksum", "total_rows")
        .agg(count("*").alias("valid_count"))
        .withColumn("invalid_count", col("total_rows") - col("valid_count"))
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("run_id", lit(query_name))
    )

    # TODO: Update logic to include run_id, valid_count, invalid_count. error sample (first N errors)
    audit_df.write.format("delta").mode("append").save(audit_log)
    print("checkpoint saved")
else:
    print("No new files")
