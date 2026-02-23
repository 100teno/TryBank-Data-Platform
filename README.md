# TryBank Data Platform

## Overview

TryBank Data Platform is an end-to-end data engineering and machine learning project that simulates the core data architecture of a digital banking system with fraud detection capabilities.

The platform demonstrates how transaction data can be ingested, processed, enriched with behavioral features, and used to train a supervised fraud detection model using a layered Data Lake architecture.

It combines API ingestion, batch processing with Spark, feature engineering, machine learning training, and real-time inference to simulate a realistic fintech data workflow.

This project was designed to showcase production-inspired data engineering and ML concepts in an integrated environment.

---

## Architecture

The platform follows a **Medallion Architecture** pattern:

- **Bronze Layer** — Raw transaction data (JSON, append-only)
- **Silver Layer** — Cleaned, typed, and structured data (Parquet)
- **Gold Layer** — Aggregated behavioral features for fraud modeling
- **Model Layer** — Supervised ML training and evaluation
- **API Layer** — Real-time fraud probability inference

### Data Flow

1. Transactions are generated via simulator or sent through the API.
2. Data is appended to the Bronze layer in JSON format.
3. Spark processes Bronze data into structured Silver tables.
4. Gold layer computes customer-level behavioral features.
5. A supervised Logistic Regression model is trained using Spark MLlib.
6. The trained model is persisted locally.
7. The API loads the model and performs real-time fraud inference.

---

## Project Structure

```text
trybank-data-platform/
│
├── api/
│   └── main.py                     # FastAPI ingestion and prediction service
│
├── simulator/
│   └── generator.py                # Synthetic transaction generator
│
├── jobs/
│   ├── bronze_to_silver.py         # Spark job: JSON → Parquet
│   ├── silver_to_gold.py           # Spark job: Feature engineering
│   └── train_model.py              # ML training pipeline (Spark MLlib)
│
├── pipeline.py                     # Orchestration script
│
├── data_lake/
│   ├── bronze/                     # Raw transaction data (JSON)
│   ├── silver/                     # Cleaned structured data (Parquet)
│   ├── gold/                       # Behavioral feature datasets
│   └── predictions/                # Prediction logs (future improvement)
│
├── models/
│   └── fraud_model/                # Persisted Spark ML model
│
├── docs/                           # Architecture documentation
│
├── requirements.txt
└── README.md
```

---

## Technology Stack

- Python 3  
- PySpark (Batch processing & MLlib)  
- FastAPI  
- Uvicorn  
- Faker  
- Parquet  
- Git  
- Linux (WSL environment)

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-MLlib-FDEE21?logo=apachespark&logoColor=black)
![FastAPI](https://img.shields.io/badge/FastAPI-Inference%20API-009688?logo=fastapi&logoColor=white)
![Data Lake](https://img.shields.io/badge/Data%20Architecture-Medallion-6A5ACD)
![Status](https://img.shields.io/badge/Project-Active-2ECC71)

---

## Fraud Detection Model

The Gold layer generates behavioral features used to train a supervised Logistic Regression model.

Engineered features include:

- Transaction amount
- Customer average transaction amount
- Deviation from historical average
- International transaction ratio
- Total customer transaction volume

The model is evaluated using a Train/Test split and ROC AUC metric.

Current AUC: ~0.76 (realistic evaluation without data leakage).

The trained model is saved and loaded by the API for real-time fraud probability inference.

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

### 3. Run Data Pipeline

```bash
python pipeline.py
```

Or run jobs individually:

```bash
python jobs/bronze_to_silver.py
python jobs/silver_to_gold.py
python jobs/train_model.py
```

### 4. Run the API

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
- Cleaned and ready for transformations

### Gold Layer
- Behavioral feature engineering applied
- Customer-level aggregations generated
- Optimized for machine learning consumption

---

## Future Improvements

Planned enhancements include:

- Real-time feature lookup inside the API
- Prediction logging layer
- Model monitoring metrics
- Docker containerization
- CI/CD integration
- Data validation layer
- Drift detection

---

## Purpose

This project serves as a portfolio-grade demonstration of modern data engineering and machine learning architecture in a fintech-inspired scenario.

It reflects real-world patterns such as layered data lakes, behavioral feature engineering, supervised model training, batch processing, and API-based inference deployment.