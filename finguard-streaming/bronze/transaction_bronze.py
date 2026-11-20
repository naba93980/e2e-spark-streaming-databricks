from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp


@dp.table(
    name="finguard_streaming_project.bronze.transactions",
    comment="Transactions from the kafka")
def bronze_transactions() -> DataFrame:
    bootstrap_servers='pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
    topic='credit_card_transactions'
    kafka_api_key = dbutils.secrets.get(
        catalog="finguard_streaming_project",
        schema="bronze",
        key="kafka-api-key"
    )
    kafka_api_secret = dbutils.secrets.get(
        catalog="finguard_streaming_project",
        schema="bronze",
        key="kafka-api-secret"
    )
    jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";'

    streaming_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", jaas_config) \
        .option("startingOffsets", "earliest") \
        .option("subscribe", topic) \
        .load()

    parsed_streaming_df=streaming_df.select(
        col("key").cast("string"),
        col("value").cast("string"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("timestampType"),
        current_timestamp().alias("ingestion_timestamp")
    )

    return parsed_streaming_df
    
  


