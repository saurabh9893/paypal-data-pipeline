# 💳 PayPal E-Commerce Data Pipeline on Azure

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![Azure](https://img.shields.io/badge/Azure-Data%20Lake-0078D4?logo=microsoftazure)
![Apache Spark](https://img.shields.io/badge/Apache-Spark-E25A1C?logo=apachespark)
![Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE?logo=apacheairflow)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker)
![Kafka](https://img.shields.io/badge/Apache-Kafka-231F20?logo=apachekafka)

> End-to-end cloud data pipeline ingesting PayPal transaction data via REST API, transforming with PySpark using medallion architecture, orchestrated by Airflow on Docker, with a Kafka-based real-time streaming layer — all on Azure.

---

## 🏗️ Architecture

```
PayPal Sandbox API (REST + Webhooks)
            │
            ▼
  ┌─────────────────────┐
  │  Python Ingestion   │  ← OAuth2, pagination, error handling
  └─────────┬───────────┘
            │
            ▼
  ┌─────────────────────┐
  │  Azure Data Lake    │  ← Bronze Layer (raw JSON, partitioned)
  │     (ADLS Gen2)     │
  └─────────┬───────────┘
            │
            ▼
  ┌─────────────────────┐
  │  PySpark Transform  │  ← Bronze → Silver → Gold
  └─────────┬───────────┘
            │
            ▼
  ┌─────────────────────┐
  │   Azure SQL DB      │  ← Gold Layer (analytics-ready)
  └─────────┬───────────┘
            │
            ▼
  ┌─────────────────────┐
  │  Power BI/Metabase  │  ← Revenue, refund, payment dashboards
  └─────────────────────┘

  ┌─────────────────────────────────┐
  │  Kafka (Real-time layer)        │
  │  PayPal Webhooks → Kafka Topic  │
  │  Consumer → ADLS Bronze         │
  └─────────────────────────────────┘

  ┌─────────────────────────────────┐
  │  Airflow (Orchestration)        │
  │  Schedules & monitors all tasks │
  └─────────────────────────────────┘
```

---

## 🧰 Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| **Source** | PayPal REST API | Transaction & payment data |
| **Ingestion** | Python + Requests | OAuth2, API calls, pagination |
| **Storage** | Azure Data Lake Gen2 | Raw + processed data lake |
| **Processing** | PySpark | Large-scale data transformation |
| **Orchestration** | Apache Airflow | Pipeline scheduling & monitoring |
| **Serving** | Azure SQL Database | Analytics-ready structured data |
| **Streaming** | Apache Kafka | Real-time webhook event processing |
| **Containers** | Docker | Reproducible local environment |
| **Dashboard** | Power BI / Metabase | Business intelligence layer |

---

## 📁 Project Structure

```
paypal-data-pipeline/
├── config/                    # Config & Azure connection helpers
├── ingestion/                 # PayPal OAuth2, API client, ADLS upload
├── dags/                      # Airflow DAG definitions
├── transformations/           # PySpark Bronze→Silver→Gold scripts
├── kafka/                     # Kafka producer & consumer
├── sql/                       # Azure SQL schema & analytics queries
├── docker/                    # docker-compose for Airflow + Kafka
├── dashboard/                 # Power BI / Metabase setup guide
├── tests/                     # Unit tests
├── docs/                      # Architecture diagrams, setup guide
├── .env.example               # Environment variable template
└── requirements.txt
```

---

## 🚀 Quick Start

### 1. Clone & Setup
```bash
git clone https://github.com/yourusername/paypal-data-pipeline.git
cd paypal-data-pipeline

python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2. Configure Credentials
```bash
cp .env.example .env
# Fill in your PayPal Sandbox + Azure credentials in .env
```

### 3. Start Services (Docker)
```bash
cd docker
docker-compose up -d

# Airflow UI → http://localhost:8080
# Default login: airflow / airflow
```

### 4. Test PayPal Connection
```bash
python -m ingestion.paypal_auth
```

### 5. Run Pipeline
```bash
# Manual ingestion run
python -m ingestion.paypal_client

# Or trigger via Airflow UI → paypal_data_pipeline DAG
```

---

## 📊 Data Model (Gold Layer)

```
transactions_gold         daily_revenue            payment_method_summary
─────────────────         ─────────────            ──────────────────────
transaction_id (PK)       report_date (PK)         payment_method (PK)
transaction_date          total_transactions        transaction_count
amount                    total_revenue             total_amount
currency_code             total_refunds             created_at
transaction_status        net_revenue
payment_method            created_at
payer_email
merchant_name
```

---

## 📅 Build Roadmap

- [x] Day 1  — Project setup, Azure + PayPal sandbox config
- [x] Day 2  — PayPal OAuth2 + transaction API client
- [x] Day 3  — Upload raw data to ADLS Bronze layer
- [ ] Day 4  — Airflow setup in Docker
- [ ] Day 5  — Airflow DAG for scheduled ingestion
- [ ] Day 6  — PySpark Bronze → Silver transformation
- [ ] Day 7  — Write Silver Parquet back to ADLS
- [ ] Day 8  — Gold layer aggregations
- [ ] Day 9  — Load Gold → Azure SQL
- [ ] Day 10 — Dashboard (Power BI / Metabase)
- [ ] Day 11 — Data quality checks
- [ ] Day 12 — Dockerize + polish GitHub
- [ ] Day 13 — Kafka setup in Docker
- [ ] Day 14 — Kafka Producer (simulate webhooks)
- [ ] Day 15 — Kafka Consumer → ADLS

---

## 👤 Author

**Your Name**
- GitHub: [@yourusername](https://github.com/yourusername)
- LinkedIn: [linkedin.com/in/yourprofile](https://linkedin.com/in/yourprofile)
