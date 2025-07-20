
# Retail Sales Analytics ETL Pipeline

This project is an end-to-end retail sales analytics pipeline built using modern data engineering tools. It simulates real-time sales data, processes it using Apache Spark, and visualizes insights using Apache Superset.

## 🧱 Project Structure

```
retail_analytics_etl/
├── dags/                         # Airflow DAGs
│   └── retail_sales_pipeline.py
├── docker/                       # Dockerfiles for services
│   ├── airflow/
│   ├── superset/
│   └── spark/
├── jars/                         # Spark dependencies (Kafka, Delta, PostgreSQL)
├── logs/                         # Airflow logs
├── requirements.txt              # Python dependencies
├── docker-compose.yml            # Docker orchestration
├── Makefile                      # Quick start commands
└── README.md                     # Project documentation
```

## 🧩 Components

- **Apache Kafka**: Simulates customer, product, and sales event streams.
- **Apache Spark**: Consumes and transforms Kafka streams; saves clean data in Delta format.
- **Airflow**: Orchestrates ingestion, transformation, and export jobs.
- **PostgreSQL**: Stores transformed sales data.
- **Apache Superset**: Dashboards for visual analytics.

## ⚙️ Setup Instructions

```bash
# Step 1: Clean any previous state
make down

# Step 2: Initialize Airflow DB & create admin user
make init

# Step 3: Build and start services
make up
```

## 🗺 DAG Tasks Overview

- `simulate_kafka_events`: Produces product/customer/sales data to Kafka topics.
- `process_customers`: Reads customer stream and stores in Delta Lake.
- `process_products`: Reads product stream and stores in Delta Lake.
- `run_spark_job`: Processes all streams and prepares final report.
- `export_to_postgres`: Writes final output to PostgreSQL.
- `bootstrap_superset`: Adds DB connection and imports dashboards.

## 📊 Superset Access

Visit [http://localhost:8088](http://localhost:8088)  
- **User**: admin@example.com  
- **Password**: admin

## 📎 Notes

- Built-in `.env` file contains credentials and DB config.
- `delta-spark` is used for efficient incremental processing.
- All data is simulated—ideal for demos, testing, or extending to real retail datasets.

---

© 2025 Retail Analytics Platform — All rights reserved.
