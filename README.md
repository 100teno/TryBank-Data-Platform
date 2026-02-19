TryBank Data Platform
Overview

TryBank Data Platform is a simulated end-to-end data engineering project that models a simplified digital banking architecture. The project demonstrates how transaction data can be ingested, processed, enriched with fraud risk logic, and stored using a layered Data Lake approach.

The goal is to showcase practical data engineering concepts including batch processing, data modeling, fraud risk scoring, and API-based ingestion using modern tools such as Spark and FastAPI.

Architecture

The platform follows a Medallion Architecture pattern:

Bronze Layer – Raw transaction data (JSON)

Silver Layer – Cleaned and typed data (Parquet)

Gold Layer – Aggregated metrics and fraud scoring

Data Flow

Transactions are generated or sent via API.

Data is written to the Bronze layer in JSON format.

Spark processes Bronze data into structured Silver tables.

Gold layer applies fraud scoring logic and aggregates customer metrics.

Project Structure
trybank-data-platform/
│
├── api/                  # FastAPI ingestion service
├── simulator/            # Transaction data generator
├── jobs/                 # Spark batch processing jobs
├── data_lake/
│   ├── bronze/           # Raw JSON data
│   ├── silver/           # Cleaned Parquet data
│   └── gold/             # Aggregated and enriched datasets
├── models/               # Future ML models (planned)
├── docs/                 # Documentation and architecture diagrams
├── requirements.txt
└── README.md

Technologies Used

Python 3.12

PySpark

FastAPI

Uvicorn

Faker

Parquet

Git

WSL (Linux environment on Windows)

Fraud Risk Scoring

The Gold layer implements a rule-based fraud scoring system. Each transaction receives a fraud score between 0.0 and 1.0 based on risk signals such as:

High transaction amount

International transaction

High-risk merchant category

Transactions exceeding a defined threshold are flagged as potential fraud.

This structure allows future evolution into a machine learning-based fraud detection model.

Running the Project
1. Setup Virtual Environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

2. Generate Bronze Data
python simulator/generator.py

3. Process Bronze → Silver
python jobs/bronze_to_silver.py

4. Process Silver → Gold
python jobs/silver_to_gold.py

5. Run the API
uvicorn api.main:app --reload


Open:

http://127.0.0.1:8000/docs

Roadmap

Planned improvements:

Structured Streaming ingestion

Automated orchestration

Feature store abstraction

Machine learning fraud model

Dockerization

CI/CD integration

Purpose

This project is designed as a practical portfolio piece to demonstrate real-world data engineering architecture and implementation skills in a fintech-like environment.