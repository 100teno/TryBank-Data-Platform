from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def create_spark_session():
    return (
        SparkSession.builder
        .appName("FraudModelTraining")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()

    # Real Gold layer

    df = spark.read.parquet("data_lake/gold/transactions")

    feature_columns = [
        "amount",
        "customer_avg_amount",
        "amount_deviation_from_avg",
        "international_ratio",
        "customer_total_transactions"
    ]
    
    df.printSchema()


    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features"
    )

    df = assembler.transform(df)

    # training / test split

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    # creating model

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="fraud_flag" \
    )


    model = lr.fit(train_df)

    # make predictions

    predictions = model.transform(test_df)

    # evaluate

    evaluator = BinaryClassificationEvaluator(
        labelCol = "fraud_flag",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    auc = evaluator.evaluate(predictions)

    print(f"Model AUC: {auc}")

    # save model
    model.write().overwrite().save("models/fraud_model")

    print("Model trained and saved successfully")

    spark.stop()

if __name__ == "__main__":
    main()
