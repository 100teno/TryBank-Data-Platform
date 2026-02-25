import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    count,
    sum as spark_sum,
    avg,
    abs as spark_abs,
    rand,
    max,
    to_date,
    current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DataType, TimestampType, LongType

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverToGold")
        .getOrCreate()
    )

METADATA_PATH = "data_lake/metadata/pipeline_control/"
PIPELINE_NAME = "silver_to_gold"

def get_last_processed_date(spark, METADATA_PATH, PIPELINE_NAME):
    if os.path.exists(METADATA_PATH):
        df_meta = spark.read.parquet(METADATA_PATH)
        df_filtered = df_meta.filter(col("pipeline_name") == PIPELINE_NAME)

        if df_filtered.count() > 0:
            return df_filtered.select("last_processed_date").collect()[0][0]
    return None

def update_metadata(
        spark,
        metadata_path,
        pipeline_name,
        last_processed_date,
        rows_processed,
        status
):
    new_metadata = spark.createDataFrame(
        [(pipeline_name, last_processed_date, status, rows_processed)],
        ["pipeline_name", "last_processed_date", "status", "rows_processed"]
    ).withColumn("last_run_timestamp", current_timestamp())

    if os.path.exists(metadata_path):
        existing = spark.read.parquet(metadata_path)
        updated = (
            existing.filter(col("pipeline_name") != pipeline_name)
            .unionByName(new_metadata)
        )
    else:
        updated = new_metadata
    
    updated.write.mode("overwrite").parquet(metadata_path)

def main():
    spark = create_spark_session()

    SILVER_PATH = "data_lake/silver/transactions"
    GOLD_TRANSACTIONS_PATH = "data_lake/gold/transactions"
    GOLD_METRICS_PATH = "data_lake/gold/customer_metrics"

    df_silver = spark.read.parquet(SILVER_PATH)

    # GARANTE transaction_date
    if "transaction_date" not in df_silver.columns:
        df_silver = df_silver.withColumn(
            "transaction_date",
            to_date(col("timestamp"))
        )

    # INCREMENTAL FILTER
    last_date = get_last_processed_date(
        spark,
        METADATA_PATH,
        PIPELINE_NAME
    )

    if last_date:
        df_silver = df_silver.filter(
            col("transaction_date") > last_date
        )
        print(f"Processing data after {last_date}")
    else: 
        print("First run - full load.")

    # Se não houver novos dados
    if df_silver.count() == 0:
        print("No new data to process.")
        spark.stop()
        return

    # Feature Engineering

    customer_stats = (
        df_silver
        .groupBy("customer_id")
        .agg(
            count("*").alias("customer_total_transactions"),
            spark_sum("amount").alias("customer_total_amount"),
            avg("amount").alias("customer_avg_amount"),
            spark_sum(
                when(col("is_international") == True, 1).otherwise(0)
            ).alias("customer_international_count")
        )
    )

    df_features = df_silver.join(
        customer_stats,
        on="customer_id",
        how="left"
    )

    df_features = df_features.withColumn(
        "amount_deviation_from_avg",
        spark_abs(col("amount") - col("customer_avg_amount"))
    )

    df_features = df_features.withColumn(
        "international_ratio",
        when(
            col("customer_total_transactions") > 0,
            col("customer_international_count") /
            col("customer_total_transactions")
        ).otherwise(0.0)
    )

    df_gold = (
        df_features
        .withColumn(
            "amount_risk",
            when(col("amount") > 3000, 0.3).otherwise(0.0)
        )
        .withColumn(
            "international_risk",
            when(col("is_international") == True, 0.3).otherwise(0.0)
        )
        .withColumn(
            "category_risk",
            when(col("merchant_category") == "electronics", 0.2).otherwise(0.0)
        )
        .withColumn(
            "deviation_risk",
            when(col("amount_deviation_from_avg") > 2000, 0.2).otherwise(0.0)
        )
    )

    df_gold = df_gold.withColumn(
        "fraud_probability",
        col("amount_risk") +
        col("international_risk") +
        col("category_risk") +
        col("deviation_risk")
    )

    df_gold = df_gold.withColumn(
        "fraud_flag",
        when(rand() < col("fraud_probability"), 1).otherwise(0)
    )

    # GARANTE transaction_date NO GOLD
    df_gold = df_gold.withColumn(
        "transaction_date",
        to_date(col("timestamp"))
    )

    # Customer Metrics Incremental

    customer_metrics = (
        df_gold
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_transactions"),
            spark_sum("amount").alias("total_amount"),
            spark_sum("fraud_flag").alias("fraud_count")
        )
    )

    #  Append Transactions
    df_gold.write.mode("append").parquet(GOLD_TRANSACTIONS_PATH)

    #  Merge Metrics
    if os.path.exists(GOLD_METRICS_PATH):
        existing_metrics = spark.read.parquet(GOLD_METRICS_PATH)

        updated_metrics = (
            existing_metrics.alias("e")
            .join(customer_metrics.alias("n"), "customer_id", "outer")
            .selectExpr(
                "coalesce(e.customer_id, n.customer_id) as customer_id",
                "coalesce(e.total_transactions, 0) + coalesce(n.total_transactions, 0) as total_transactions",
                "coalesce(e.total_amount, 0) + coalesce(n.total_amount, 0) as total_amount",
                "coalesce(e.fraud_count, 0) + coalesce(n.fraud_count, 0) as fraud_count"
            )
        )

        updated_metrics.write.mode("overwrite").parquet(GOLD_METRICS_PATH)
    else:
        customer_metrics.write.mode("overwrite").parquet(GOLD_METRICS_PATH)

    print("Gold layer updated successfully.")

    rows_processed = df_gold.count()

    max_date = df_gold.agg(max("transaction_date")).collect()[0][0]

    update_metadata(
        spark,
        METADATA_PATH,
        PIPELINE_NAME,
        max_date,
        rows_processed,
        "SUCCESS"
    )

    spark.stop()


if __name__ == "__main__":
    main()