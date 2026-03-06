# Setup Guide

## Prerequisites
- Python 3.10+
- Docker Desktop
- Azure Account (free tier works)
- PayPal Developer Account (free)

## Step 1 - Clone & Setup
```bash
git clone https://github.com/yourusername/paypal-data-pipeline.git
cd paypal-data-pipeline
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

## Step 2 - Configure Environment
```bash
cp .env.example .env
# Edit .env with your PayPal sandbox + Azure credentials
```

## Step 3 - Start Services
```bash
cd docker
docker-compose up -d
# Airflow UI: http://localhost:8080 (user: airflow / pass: airflow)
```

## Step 4 - Run Pipeline
```bash
# Test PayPal connection
python -m ingestion.paypal_auth

# Run full ingestion
python -m ingestion.paypal_client
```
