from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim
from src.common.logging_utils import get_logger

logger = get_logger(__name__)

def clean_customer_data(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Reads raw customer data, applies transformations, and writes cleaned data.
    """
    logger.info(f"Reading customer data from {input_path}")
    df: DataFrame = spark.read.format("delta").load(input_path)

    logger.info("Applying transformations...")
    df_cleaned = (
        df.withColumn("customer_name", trim(col("customer_name")))
          .dropDuplicates(["customer_id"])
          .filter(col("is_active") == True)
    )

    logger.info(f"Writing cleaned data to {output_path}")
    df_cleaned.write.format("delta").mode("overwrite").save(output_path)
    logger.info("Transformation completed successfully.")