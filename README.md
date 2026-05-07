# Nashville Urban Operations Intelligence Platform

An end-to-end cloud-native data engineering platform designed to ingest, process, transform, orchestrate, and visualize urban operations data for the City of Nashville.

---

# Project Overview

This platform collects operational and housing-related public datasets from Nashville Open Data APIs and transforms them into analytics-ready datasets using a modern data engineering stack.

The project demonstrates:

- API data ingestion
- Cloud storage architecture
- Data warehouse modeling
- ETL/ELT processing
- Workflow orchestration
- Data transformation with dbt
- Dashboard analytics
- Production-style pipeline design

---

# Architecture Diagram

![Architecture Diagram](charts/nashville_architecture_diagram_minimal.png)

---

# Technology Stack

| Layer | Technology |
|---|---|
| Programming | Python |
| Cloud Storage | AWS S3 |
| Data Warehouse | Snowflake |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Dashboarding | Streamlit |
| Containerization | Docker |
| Version Control | Git + GitHub |

---

# Data Sources

The platform ingests data from multiple Nashville public APIs.

## Nashville 311 Service Requests
Tracks citizen service requests and operational issues.

## Nashville Housing Property Data
Contains parcel-level housing and property valuation information.

## Nashville Property Violations
Tracks code violations and property-related operational activity.

---

# End-to-End Pipeline Flow

```text
Nashville APIs
      ↓
Python Data Ingestion
      ↓
AWS S3 Data Lake
      ↓
Snowflake RAW Layer
      ↓
Snowflake SILVER Layer
      ↓
Snowflake GOLD Layer
      ↓
dbt Transformations
      ↓
Apache Airflow Orchestration
      ↓
Streamlit Dashboard






Data Architecture
RAW Layer

Stores semi-structured JSON data directly from APIs.

SILVER Layer

Contains cleaned and standardized datasets with typed columns.

GOLD Layer

Contains business-ready aggregated analytics tables optimized for dashboard consumption.

dbt Transformations

dbt is used to:

Clean and standardize raw data
Build Silver and Gold models
Create analytics-ready datasets
Implement data quality testing

Example dbt tests:

not_null
unique
accepted_values
Airflow Orchestration

Apache Airflow orchestrates the complete pipeline:

API ingestion
S3 uploads
Snowflake loading
Bronze → Silver transformations
Silver → Gold transformations
dbt execution
Pipeline monitoring
Dashboard Features

The Streamlit dashboard provides:

City-level filtering
311 request analytics
Property violation analysis
Housing and property insights
Operational trend visualization
Project Structure
Nashville-urban-operations-intelligence-platform/
│
├── api_data_ingestion/
├── airflow_orchestration/
├── charts/
├── local_data/
├── nashville_data_transformation/
├── snowflake_data_warehouse/
├── streamlit_dashboard/
├── .env
├── README.md
└── requirements.txt


