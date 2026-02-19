import random
from uuid import uuid4
from datetime import datetime
from faker import Faker
import json
import os

fake = Faker()

MERCHANTS = [
    ("Amazon", "ecommerce"),
    ("Uber", "transport"),
    ("iFood", "food"),
    ("Netflix", "subscription"),
    ("Apple Store", "electronics"),
    ("Shell", "gas"),
]

COUNTRIES = ["BR", "US", "AR", "FR", "DE"]


def generate_transaction():
    merchant, category = random.choice(MERCHANTS)

    transaction = {
        "transaction_id": str(uuid4()),
        "customer_id": str(uuid4()),
        "card_id": str(uuid4()),
        "transaction_type": random.choice(["debit", "credit"]),
        "amount": round(random.uniform(5, 5000), 2),
        "merchant": merchant,
        "merchant_category": category,
        "country": random.choice(COUNTRIES),
        "timestamp": datetime.utcnow().isoformat(),
        "device_id": str(uuid4()),
        "is_international": random.choice([True, False])
    }

    return transaction


def generate_batch(n=10000):
    os.makedirs("data_lake/bronze", exist_ok=True)

    with open("data_lake/bronze/transactions.json", "w") as f:
        for _ in range(n):
            transaction = generate_transaction()
            f.write(json.dumps(transaction) + "\n")


if __name__ == "__main__":
    generate_batch()
    print("Bronze batch generated successfully.")
