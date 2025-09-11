# Databricks notebook source
from pyspark.sql import SparkSession
from src.transformations.customer_transform import clean_customer_data
from src.validations.schema_checks import validate_customer_schema
from src.common.logging_utils import get_logger

logger = get_logger("clean_customers")

spark = SparkSession.builder.getOrCreate()

input_path = "/mnt/dev/raw/customers"
output_path = "/mnt/dev/clean/customers"

# Read data
df = spark.read.format("delta").load(input_path)

# Validate schema
if validate_customer_schema(df):
    clean_customer_data(spark, input_path, output_path)
else:
    logger.error("Schema validation failed. Aborting pipeline.")


# This is the change
# Change 2
