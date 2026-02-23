from fastapi import FastAPI
from pydantic import BaseModel
from uuid import uuid4
from datetime import datetime
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs as spark_abs, lit
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler

# Spark Session (Global)

spark = SparkSession.builder.appName("FraudAPI").getOrCreate()

app = FastAPI()

# Load Trained Model

model = LogisticRegressionModel.load("models/fraud_model")

feature_columns = [
    "amount",
    "customer_avg_amount",
    "amount_deviation_from_avg",
    "international_ratio",
    "customer_total_transactions"
]

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)



# Request Schema

class Transaction(BaseModel):
    customer_id: str
    card_id: str
    transaction_type: str
    amount: float
    merchant: str
    merchant_category: str
    country: str
    device_id: str
    is_international: bool

# Ingestion Endpoint (Bronze)

@app.post("/transactions")
def create_transaction(transaction: Transaction):
    os.makedirs("data_lake/bronze", exist_ok=True)

    record = transaction.dict()
    record["transaction_id"] = str(uuid4())
    record["timestamp"] = datetime.utcnow().isoformat()

    with open("data_lake/bronze/transactions.json", "a") as f:
        f.write(json.dumps(record) + "\n")

    return {"message": "Transaction ingested successfully"}


# Prediction Endpoint (ML)

@app.post("/predict")
def predict(transaction: Transaction):

    # Converter request para dict
    record = transaction.dict()

    # Criar DataFrame Spark
    df = spark.createDataFrame([record])

    # Simular histórico básico
    # (produção real buscaria no Gold layer)

    df = df.withColumn("customer_avg_amount", lit(500.0))
    df = df.withColumn("customer_total_transactions", lit(10))
    df = df.withColumn("international_ratio", lit(0.1))

    df = df.withColumn(
        "amount_deviation_from_avg",
        spark_abs(col("amount") - col("customer_avg_amount"))
    )

    # Feature Vector

    df = assembler.transform(df)


    # Prediction

    prediction = model.transform(df)

    result = prediction.select("probability", "prediction").collect()[0]

    return {
        "fraud_probability": float(result["probability"][1]),
        "fraud_prediction": int(result["prediction"])
    }