from fastapi import FastAPI
from pydantic import BaseModel
from uuid import uuid4
from datetime import datetime
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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

    record = transaction.dict()

    gold_df = spark.read.parquet("data_lake/gold/customer_metrics")

    customer_data = gold_df.filter(
        col("customer_id") == record["customer_id"]
    )

    # Cliente com histórico

    if customer_data.count() > 0:

        row = customer_data.collect()[0]

        customer_total_transactions = int(row["total_transactions"])
        total_amount = float(row["total_amount"])

        customer_avg_amount = (
            total_amount / customer_total_transactions
            if customer_total_transactions > 0 else 0.0
        )

        international_ratio = 0.0

    # Cold Start com média global

    else:

        global_agg = gold_df.agg(
            {"total_amount": "sum", "total_transactions": "sum"}
        ).collect()[0]

        global_total_amount = float(global_agg["sum(total_amount)"])
        global_total_transactions = float(global_agg["sum(total_transactions)"])

        customer_avg_amount = (
            global_total_amount / global_total_transactions
            if global_total_transactions > 0 else 0.0
        )

        customer_total_transactions = 0
        international_ratio = 0.0

    # Feature Engineering

    amount_deviation_from_avg = abs(
        record["amount"] - customer_avg_amount
    )

    feature_data = [{
        "amount": record["amount"],
        "customer_avg_amount": customer_avg_amount,
        "amount_deviation_from_avg": amount_deviation_from_avg,
        "international_ratio": international_ratio,
        "customer_total_transactions": customer_total_transactions
    }]

    df = spark.createDataFrame(feature_data)
    df = assembler.transform(df)

    prediction = model.transform(df)

    result = prediction.select("probability", "prediction").collect()[0]

    fraud_probability = float(result["probability"][1])
    fraud_prediction = int(result["prediction"])

    # Save Prediction (Monitoring)

    os.makedirs("data_lake/predictions", exist_ok=True)

    prediction_record = {
        "customer_id": record["customer_id"],
        "amount": record["amount"],
        "fraud_probability": fraud_probability,
        "fraud_prediction": fraud_prediction,
        "timestamp": datetime.utcnow().isoformat()
    }

    with open("data_lake/predictions/predictions.json", "a") as f:
        f.write(json.dumps(prediction_record) + "\n")

    return {
        "fraud_probability": fraud_probability,
        "fraud_prediction": fraud_prediction
    }