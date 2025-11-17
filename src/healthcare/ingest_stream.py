from typing import List, Tuple

from delta import DeltaTable
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

from healthcare.utils import *


def parse_df(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    # Define initial validation condition
    initial_filter_condition = (
        col("member_id").isNotNull() & col("plan_id").isNotNull() & col("plan_id").startswith("PLN")
    )

    # Apply transformations and identify quarantine reasons
    transformed_df = (
        df.withColumn("total_rows", count("*").over(Window.partitionBy("file_path")))
        .withColumn(
            "gender",
            when(df.gender.isin("F", "f"), "FEMALE").when(df.gender.isin("M", "m"), "MALE").otherwise(upper(df.gender)),
        )
        .withColumn("phone", regexp_replace("phone", r"[()\-\s]", ""))
        .withColumn("zip5", substring(trim("zip5"), 1, 5).try_cast("int"))
        .withColumn(
            "dob",
            when(
                regexp_like("dob", lit(r"\d{2}\/\d{2}\/\d{4}")),
                to_date(try_to_timestamp("dob", lit("MM/dd/yyyy"))),
            )
            .when(
                regexp_like("dob", lit(r"\d{4}\/\d{2}\/\d{2}")),
                to_date(try_to_timestamp("dob", lit("yyyy/dd/MM"))),
            )
            .when(
                col("dob").contains(" "),
                to_date(try_to_timestamp("dob", lit("dd MMM yyyy"))),
            )
            .when(
                regexp_like("dob", lit(r"\d{4}-\d{2}-\d{2}")),
                to_date(try_to_timestamp("dob", lit("yyyy-MM-dd"))),
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
        # Cast member_id after checking for castability
        .withColumn("member_id", col("member_id").try_cast("int"))
        # Add quarantine reason column
        .withColumn(
            "quarantine_reason_raw",
            array(
                when(
                    ~initial_filter_condition,
                    lit("Missing or invalid member_id/plan_id"),
                ),
                when(
                    col("member_id").isNull(),
                    lit("Invalid member_id format (cannot cast to int)"),
                ),
                when(col("dob").isNull(), lit("Invalid or unparsable dob")),
            ),
        )
        .withColumn(
            "quarantine_reason",
            expr("filter(quarantine_reason_raw, x -> x is not null)"),
        )
        .drop("quarantine_reason_raw")
    )

    # Split DataFrame into valid and quarantined
    valid_df = (
        transformed_df.filter((col("quarantine_reason").isNull()) | (array_size(col("quarantine_reason")) == 0))
        .drop("quarantine_reason", "file_path")
        .drop_duplicates()
    )

    # Select original columns for quarantine plus the reason
    quarantine_df = transformed_df.filter(
        (col("quarantine_reason").isNotNull()) & (array_size(col("quarantine_reason")) > 0)
    ).select(*df.columns, "quarantine_reason", "ingestion_time")

    return valid_df, quarantine_df


def generate_merge_cond(columns: List[str]) -> str:
    return " and ".join([f"source.{clm} <=> target.{clm}" for clm in columns])


def merge(source_df: DataFrame, _: int) -> None:
    parsed_df, quarantine_df = parse_df(source_df)
    parsed_df.cache()

    # Write quarantined records to the quarantine table
    if not quarantine_df.isEmpty():
        quarantine_df.write.format("delta").mode("append").save(quarantine_loc)

    final_df = parsed_df.drop("total_rows")

    if not DeltaTable.isDeltaTable(parsed_df.sparkSession, silver_loc):
        final_df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").save(silver_loc)
        print("Initial Commit")
    else:
        delta_table = DeltaTable.forPath(parsed_df.sparkSession, path=silver_loc)
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


def start_stream() -> None:
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


if __name__ == "__main__":
    start_stream()
