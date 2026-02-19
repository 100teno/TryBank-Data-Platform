from fastapi import FastAPI
from pydantic import BaseModel
from uuid import uuid4
from datetime import datetime
import json
import os

app = FastAPI()

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


@app.post("/transactions")
def create_transaction(transaction: Transaction):
    os.makedirs("data_lake/bronze", exist_ok=True)

    record = transaction.dict()
    record["transaction_id"] = str(uuid4())
    record["timestamp"] = datetime.utcnow().isoformat()

    with open("data_lake/bronze/transactions.json", "a") as f:
        f.write(json.dumps(record) + "\n")

    return {"message": "Transaction ingested successfully"}