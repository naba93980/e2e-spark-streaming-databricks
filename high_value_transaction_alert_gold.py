from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json, to_date, concat_ws, lit


@dp.table(
    name="finguard_streaming_project.gold.high_value_transactions_alert",
    comment="Alert details where transactions are above the customer's spending limit")
def high_value_transactions() -> DataFrame:
    
    customers = spark.read.table("finguard_streaming_project.silver.customers")

    transactions = spark.readStream.table("finguard_streaming_project.silver.transactions")
    
    high_value_transactions_df = (
        transactions.join(customers, on="customer_id", how="left")
        .filter(col("amount") > col("transaction_limit"))
        .select(
            concat_ws("-", lit("ALERT"), col("transaction_id")).alias("alert_id"),
            lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
            current_timestamp().alias("alert_timestamp"),
            col("customer_id"),
            col("transaction_id"),
            col("transaction_timestamp"),
            col("amount"),
            col("email").alias("customer_email"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("customer_name"),
            col("transaction_limit"),
            col("merchant_name"),
            col("payment_channel"),
            col("status")
        )
    )

    return high_value_transactions_df