from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# criando a sessão do Spark
def create_spark_session():
    return(
        SparkSession.builder \
        .appName("BronzeToSilver") \
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    # leitura da bronze 
    df = spark.read.json("data_lake/bronze/transactions.json")

    # transformação dos dados
    df_silver = (
        df.withcolumn("amount", col("amount").cast(DoubleType()))
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("is_international", col("is_international").cast(BooleanType()))
    )

    # filtragem de dados inválidos
    df_silver = df_silver.filter(
        col("customer_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    )

    # coluna de particionamento
    df_silver = df_silver.withColumn("transaction_date", to_date(col("timestamp"))
    )

    # escrita no silver particionando
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .parquet("data_lake/silver/transactions")
    
    print("Silver layer created successfully")
    
if __name__ == "__main__":
    main()
