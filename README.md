# eCommerce Customer Churn – PySpark Medallion Pipeline
<img width="750" height="500" alt="image" src="https://github.com/user-attachments/assets/17d1c35b-25b3-4388-aa31-479b612adec3" />

An ETL pipeline built with Apache Airflow, PySpark and PostgreSQL using the Medallion architecture (Bronze → Silver → Gold). It ingests eCommerce-dataset.csv (customer behavior data with a Churned label), cleans it with Spark, and builds a star schema plus summary tables for churn analysis.

- Dataset Source: https://www.kaggle.com/datasets/srisyra02/ecommerce-customer-behavior-dataset

# Medallion Architecture (Bronze -> Silver -> Gold)
<img width="1835" height="1068" alt="image" src="https://github.com/user-attachments/assets/aadf5693-a0de-42da-8ddc-6618f2020086" />

# DAG Architecture
<img width="1672" height="784" alt="image" src="https://github.com/user-attachments/assets/7d1b2046-ad20-4d32-9a6c-15132800418e" />

# Quick Start
- Modify the following string in the x-airflow-common function within docker-compose.yaml

```
environment:
  _PIP_ADDITIONAL_REQUIREMENTS: "pandas sqlalchemy psycopg2-binary openpyxl"
  DATA_DIR: /opt/airflow/data
  PG_HOST: host.docker.internal
  PG_PORT: "5432"
  PG_DB: postgres
  PG_USER: postgres
  PG_PASSWORD: ${PG_PASSWORD}
volumes:
  - "<csv/xlsx file address>:/opt/airflow/data"
```
- run

```
docker compose up -d
```

- Access http://localhost:8080 and trigger the data_pipeline_for_food_delivery_orders DAG

