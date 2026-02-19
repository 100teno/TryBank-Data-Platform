from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    count,
    sum as spark_sum,
    avg,
    abs as spark_abs
)


# -----------------------------
# Spark Session
# -----------------------------
def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverToGold")
        .getOrCreate()
    )


# Main Pipeline

def main():
    spark = create_spark_session()

    # 1. Read Silver Layer

    df_silver = spark.read.parquet("data_lake/silver/transactions")


    # 2. Customer Aggregated Features

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

    # 3. Behavioral Features

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

    # 4. Risk Components

    df_gold = (
        df_features
        .withColumn(
            "amount_risk",
            when(col("amount") > 3000, 0.4).otherwise(0.0)
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
            when(col("amount_deviation_from_avg") > 2000, 0.3).otherwise(0.0)
        )
    )

    # 5. Fraud Score

    df_gold = df_gold.withColumn(
        "fraud_score",
        col("amount_risk") +
        col("international_risk") +
        col("category_risk") +
        col("deviation_risk")
    )

    df_gold = df_gold.withColumn(
        "fraud_flag",
        when(col("fraud_score") >= 0.7, 1).otherwise(0)
    )

    # 6. Customer Metrics (Gold Aggregation)

    customer_metrics = (
        df_gold
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_transactions"),
            spark_sum("amount").alias("total_amount"),
            spark_sum("fraud_flag").alias("fraud_count")
        )
    )

    # 7. Write Gold Layer
  
    df_gold.write.mode("overwrite").parquet(
        "data_lake/gold/transactions"
    )

    customer_metrics.write.mode("overwrite").parquet(
        "data_lake/gold/customer_metrics"
    )

    print("Gold layer created successfully.")

    spark.stop()


if __name__ == "__main__":
    main()
