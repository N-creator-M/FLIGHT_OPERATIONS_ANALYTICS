# Flight Operations Analytics

## Overview

This project is an end-to-end data engineering pipeline designed to process and analyze flight operations data using a modern medallion architecture (**Bronze / Silver / Gold**).

The goal is to transform raw flight data into structured analytical datasets that can support operational monitoring, KPI reporting, and business intelligence use cases.

The pipeline is orchestrated with Apache Airflow, containerized with Docker, and integrated with Snowflake for cloud data warehousing.

---

## Project Architecture

```text
Raw CSV Files
     ↓
Bronze Layer   → Raw ingestion
     ↓
Silver Layer   → Cleaned and standardized data
     ↓
Gold Layer     → Aggregated analytics-ready datasets
     ↓
Snowflake      → Reporting / Dashboarding
```

---

## Tech Stack

* Python
* Apache Airflow
* Docker & Docker Compose
* Snowflake

---

## Pipeline Workflow

### 1. Bronze Layer

Raw flight source files are ingested with minimal transformation.

### 2. Silver Layer

Data quality checks, cleaning, type casting, and normalization are applied.

### 3. Gold Layer

Business metrics and aggregated datasets are created for analytics consumption.

### 4. Snowflake Load

Gold datasets are loaded into Snowflake tables for reporting.

---

## Example Use Cases

* Delay trend analysis
* Flight volume monitoring
* Route performance tracking
* Operational KPI dashboards
* Airline efficiency analysis

---

## How to Run

### Start Services

```bash
docker-compose up -d
```

### Launch Airflow

Open Airflow UI:

```text
http://localhost:8080
```

### Run Pipeline

Trigger the DAG from the Airflow interface.

---

## Future Improvements

* Add automated data validation tests
* Implement CI/CD deployment
* Connect BI dashboards (Power BI / Tableau)
* Add real-time streaming ingestion
* Monitoring & alerting

---

## Author

Created by** Nassima Maarouf **
