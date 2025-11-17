import pytest
from pyspark.sql import SparkSession
from sample_data import source_data
from schema import source_schema

from healthcare.ingest_stream import generate_merge_cond, parse_df


@pytest.fixture(scope="session")
def spark():
    """Fixture for creating a Spark session for all tests."""
    session = SparkSession.builder.master("local[2]").appName("IngestStreamTests").getOrCreate()
    yield session
    session.stop()


def test_generate_merge_cond():
    """Test the generate_merge_cond function."""
    columns = ["member_id", "client_id"]
    expected = "source.member_id <=> target.member_id and source.client_id <=> target.client_id"
    assert generate_merge_cond(columns) == expected


def test_parse_df(spark):
    """Test the main data transformation logic in parse_df."""

    source_df = spark.createDataFrame(data=source_data, schema=source_schema)

    # Apply the transformation
    result_df, _ = parse_df(source_df)

    # Collect results and check
    result_data = result_df.drop("ingestion_time").collect()
    assert len(result_data) == 3  # Expect 3 unique, valid rows

    # Check first row's transformations
    row1 = result_df.filter("member_id = 101").first()
    assert row1 is not None
    assert row1["gender"] == "MALE"
    assert row1["phone"] == "5551234567"
    assert row1["zip5"] == 12345
    assert row1["client_id"] == "clientA"
    assert str(row1["creation_date"]) == "2024-01-10"

    # Check second row's transformations
    row2 = result_df.filter("member_id = 102").first()
    assert row2 is not None
    assert row2["gender"] == "FEMALE"
    assert str(row2["dob"]) == "1985-03-20"
    assert row2["client_id"] == "clientB"

    # Check third row's transformations
    row3 = result_df.filter("member_id = 103").first()
    assert row3 is not None
    assert row3["gender"] == "OTHER"
    assert str(row3["dob"]) == "2000-01-12"  # yyyy/dd/MM format
    assert row3["client_id"] == "clientA"
