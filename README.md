# TryBank Data Platform

## Overview

TryBank Data Platform is an end-to-end data engineering project that simulates the core data architecture of a digital banking system.

The platform demonstrates how transaction data can be ingested, processed, enriched with fraud risk logic, and structured using a layered Data Lake architecture. It combines API ingestion, batch processing with Spark, and risk scoring logic to simulate a realistic fintech data workflow.

This project was designed to showcase practical data engineering concepts in a production-inspired environment.

---

## Architecture

The platform follows a **Medallion Architecture** pattern:

- **Bronze Layer** — Raw transaction data (JSON, append-only)
- **Silver Layer** — Cleaned, typed, and structured data (Parquet)
- **Gold Layer** — Aggregated metrics and fraud risk scoring

### Data Flow

1. Transactions are generated via simulator or sent through an API.
2. Data is appended to the Bronze layer in JSON format.
3. Spark processes Bronze data into structured Silver tables.
4. Gold layer applies fraud scoring and generates customer-level metrics.

---

## Project Structure

```text
trybank-data-platform/
│
├── api/
│   └── main.py                  # FastAPI ingestion service
│
├── simulator/
│   └── generator.py             # Synthetic transaction generator
│
├── jobs/
│   ├── bronze_to_silver.py      # Spark job: JSON → Parquet
│   └── silver_to_gold.py        # Spark job: Risk scoring & aggregations
│
├── data_lake/
│   ├── bronze/                  # Raw transaction data (JSON)
│   ├── silver/                  # Cleaned structured data (Parquet)
│   └── gold/                    # Enriched datasets and metrics
│
├── models/                      # Reserved for ML fraud models (future)
├── docs/                        # Architecture documentation
│
├── requirements.txt
└── README.md
```

---

## Technology Stack

- Python 3
- PySpark
- FastAPI
- Uvicorn
- Faker
- Parquet
- Git
- Linux (WSL environment)

---

## Fraud Risk Scoring

The Gold layer implements a rule-based fraud scoring system.

Each transaction receives a `fraud_score` between `0.0` and `1.0` based on risk signals such as:

- High transaction amount
- International transaction
- High-risk merchant category

Transactions exceeding a defined threshold are flagged as potential fraud.

The design is modular and can evolve into a supervised machine learning model in future versions.

---

## Running the Project

### 1. Environment Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Generate Bronze Data

```bash
python simulator/generator.py
```

### 3. Process Bronze → Silver

```bash
python jobs/bronze_to_silver.py
```

### 4. Process Silver → Gold

```bash
python jobs/silver_to_gold.py
```

### 5. Run the Ingestion API

```bash
uvicorn api.main:app --reload
```

API documentation available at:

```
http://127.0.0.1:8000/docs
```

---

## Data Layers Explained

### Bronze Layer
- Raw, append-only JSON
- No transformation applied
- Represents the source of truth

### Silver Layer
- Structured and typed
- Converted to Parquet format
- Ready for analytics and transformation

### Gold Layer
- Fraud scoring applied
- Customer-level aggregations generated
- Optimized for BI or ML consumption

---

## Future Improvements

Planned enhancements include:

- Structured Streaming ingestion
- Machine learning fraud model
- Orchestration layer
- Docker containerization
- CI/CD integration
- Monitoring and observability
- Data validation layer

---

## Purpose

This project serves as a portfolio-grade demonstration of modern data engineering architecture in a fintech-inspired scenario.

It reflects real-world patterns such as layered data lakes, feature engineering, batch processing, and API-based ingestion.
