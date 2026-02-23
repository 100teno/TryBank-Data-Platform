from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckGold").getOrCreate()

df = spark.read.parquet("data_lake/gold/customer_metrics").printSchema()

# df.printSchema()
# df.show(5)

# f9e4a96b-ca85-4571-9a28-d1846665af03



