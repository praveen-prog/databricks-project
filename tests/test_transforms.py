import pytest
from pyspark.sql import SparkSession
from src.transformations.customer_transform import clean_customer_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()

def test_clean_customer_data(spark, tmp_path):
    input_data = [(1, " Alice ", True), (2, "Bob", False), (1, "Alice", True)]
    df = spark.createDataFrame(input_data, ["customer_id", "customer_name", "is_active"])
    input_path = str(tmp_path / "input")
    output_path = str(tmp_path / "output")

    df.write.format("delta").mode("overwrite").save(input_path)

    clean_customer_data(spark, input_path, output_path)

    result = spark.read.format("delta").load(output_path)
    assert result.count() == 1  # Only 1 active unique customer