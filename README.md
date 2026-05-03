# nyc_collisions
Built an end-to-end data pipeline using Apache Airflow to ingest, transform, and load NYC motor vehicle collision data into PostgreSQL, leveraging Parquet-based intermediate storage and SQLAlchemy for database integration.

<img width="793" height="516" alt="image" src="https://github.com/user-attachments/assets/b7110673-561e-417c-acc2-a7f21edcde33" />

**System Design and Architecture**



<img width="1303" height="596" alt="image" src="https://github.com/user-attachments/assets/6bcfb10d-74b3-4364-b7c8-30906d4c9705" />

**Database Queries**


<img width="315" height="312" alt="image" src="https://github.com/user-attachments/assets/bde26cc0-b0b2-4d90-b0a1-c52e87c38d03" />
<img width="1016" height="276" alt="image" src="https://github.com/user-attachments/assets/3d5d4bf8-d8bf-472c-b56f-c00971ca9bb3" />


**Ensure Authentication Correct for load_to_postgres**

Please be aware: Apache Airflow will not be able to load data in database if password or connection engine is setup incorrectly. 

## Project Overview

This project builds a production-style data pipeline that:

* Extracts raw NYC collisions data from a public API
* Transforms and cleans the data into analytical datasets
* Stores processed data in Parquet format (data lake style)
* Loads curated datasets into PostgreSQL for downstream analytics
* Orchestrates the entire workflow using Apache Airflow

---

## Tech Stack

* **Orchestration**: Apache Airflow
* **Data Processing**: Python, Pandas
* **Storage (Intermediate)**: Parquet
* **Database**: PostgreSQL
* **Workflow Environment**: WSL (Windows Subsystem for Linux)
* **Database UI**: pgAdmin
* **API Source**: NYC Open Data

---

## Project Structure

```
nyc_collisions/
│
├── dags/
│   └── nyc_collisions_dag.py
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── create_gold_tables.py
│   └── load_postgres.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── gold/
│
├── airflow_env/
├── requirements.txt
└── README.md
```

---

## Pipeline Workflow

### 1. Extract

* Pulls NYC collisions data in batches from API
* Saves raw data locally

### 2. Transform

* Cleans null values and formats columns
* Standardizes schema

### 3. Create Gold Tables

* Aggregates data into analytics-ready tables:

  * crashes by borough
  * monthly trends
  * crashes by hour
  * top contributing factors

### 4. Load to PostgreSQL

* Writes final tables to PostgreSQL using SQLAlchemy

---

## Setup Instructions

### 1. Clone Repository

```bash
git clone https://github.com/yourusername/nyc-collisions-pipeline.git
cd nyc-collisions-pipeline
```

---

### 2. Create Virtual Environment

```bash
python -m venv airflow_env
source airflow_env/bin/activate
```

---

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Install Required Packages

```bash
pip install pyarrow psycopg2-binary apache-airflow
```

---

### 5. Start Airflow

```bash
airflow standalone
```

Access UI:

```
http://localhost:8080
```

---

### 6. Set Up PostgreSQL Connection in Airflow

In Airflow UI:

**Admin → Connections → Add**

| Field    | Value           |
| -------- | --------------- |
| Conn Id  | postgres_nyc    |
| Type     | Postgres        |
| Host     | YOUR_WINDOWS_IP |
| Database | nyc_collisions  |
| Login    | postgres        |
| Password | YOUR_PASSWORD   |
| Port     | 5432            |

---

### 7. Run the Pipeline

1. Enable DAG: `nyc_collisions_pipeline`
2. Click **Trigger DAG**
3. Monitor execution in Graph View

---

## Output Tables

The pipeline produces the following tables in PostgreSQL:

* `crashes_by_borough`
* `monthly_trends`
* `crash_by_hour`
* `top_factors`

---

## Lessons Learned

### 1. WSL Networking is Non-Trivial

* `localhost` in WSL ≠ Windows localhost
* Must use Windows host IP (from `/etc/resolv.conf`)

---

### 2. Airflow Environment Isolation

* Environment variables don’t reliably propagate
* Airflow Connections are the correct way to manage credentials

---

### 3. Parquet Requires External Engines

* Pandas does not natively support Parquet
* Requires:

  * `pyarrow` OR
  * `fastparquet`

---

### 4. Debugging Airflow Requires Logs First

* Always check:

  ```
  Task → Logs
  ```
* UI alone is not enough

---

### 5. SQLAlchemy vs Raw Connections

* Pandas `.to_sql()` expects SQLAlchemy engine
* Passing incorrect connection types causes runtime errors

---

### 6. Incremental Debugging is Critical

* Fix one stage at a time:

  * Extract → Transform → Load
* Don’t debug evert stage at once

---

## Future Improvements

* Dockerize entire pipeline
* Add data validation (Great Expectations)
* Implement incremental loads instead of full refresh
* Add monitoring & alerting
* Deploy to cloud AWS/GCP/Azure

