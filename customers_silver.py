from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json, to_date


@dp.table(
    name="finguard_streaming_project.silver.customers",
    comment="parsed and cleaned customer data")
@dp.expect_or_drop("valid customer id", "customer_id IS NOT NULL")
def customer_transactions() -> DataFrame:
    
    bronze_df = spark.readStream.table("finguard_streaming_project.bronze.customers")

    transformed_df = bronze_df.select(
       col("customer_id"),
       col("first_name"),
       col("last_name"),
       col("gender"),
       col("age"),
       col("city"),
       col("state"),
       col("email"),
       col("country"),
       col("annual_income"),
       col("customer_segment"),
       to_date(col("account_open_date"), "yyyy-MM-dd").alias("account_open_date"),
       col("risk_score"),
       col("preferred_spending_min"),
       col("preferred_spending_max"),
       col("preferred_city"),
       col("preferred_country"),
       col("trusted_device_id"),
       col("card_number"),
       col("card_type"),
       col("transaction_limit"),
       col("update_timestamp"),
       current_timestamp().alias("silver_ingest_timestamp")
    )

    return transformed_df