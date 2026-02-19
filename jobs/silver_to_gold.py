from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as spark_sum, lit

# criando a sessão do Spark
def create_spark_session():
    return(
        SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    # leitura da silver 
    df = spark.read.parquet("data_lake/silver/transactions")

    # Score components
    df_gold = df.withColumn(
        "amount_risk",
        when(col("amount") > 3000, 0.4).otherwise(0.0)
    ).withColumn(
        "international_risk",
        when(col("is_international") == True, 0.3).otherwise(0.0)
    ).withColumn(
        "category_risk",
        when(col("merchant_category") == "electronics", 0.2).otherwise(0.0)
    )
    
    # Somar score
    df_gold = df_gold.withColumn(
        "fraud_score",
        col("amount_risk") +
        col("international_risk") +
        col("category_risk")
    )
    
    # Definir flag final
    df_gold = df_gold.withColumn(
        "fraud_flag",
        when(col("fraud_score") >= 0.7, 1).otherwise(0)
    )

    # metricas feitas por cliente
    metrics = (
        df_gold.groupBy("customer_id")
        .agg(
            count("*").alias("total_transactions"),
            spark_sum("amount").alias("total_amount"),
            spark_sum("fraud_flag").alias("fraud_count")
        )
    )

    df_gold.write.mode("overwrite").parquet("data_lake/gold/transactions")
    metrics.write.mode("overwrite").parquet("data_lake/gold/customer_metrics")

    print("Gold layer created successfully.")

    spark.stop()

if __name__ == "__main__":
    main()
