from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dp.table(
    name="finguard_streaming_project.silver.transactions",
    comment="parsed and cleaned data")
@dp.expect_or_drop()
def silver_transactions() -> DataFrame:

    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("payment_channel", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("is_international", BooleanType(), True),
        StructField("status", StringType(), True),
    ])
  
    
    bronze_df = spark.readStream.table("finguard_streaming_project.bronze.transactions")

    transformed_df = bronze_df.select(
        from_json("value", transaction_schema).alias("data"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
    ).select(
        col("data.*"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("bronze_ingestion_timestamp"),
        current_timestamp().alias("silver_ingestion_timestamp")
    )

    return transformed_df