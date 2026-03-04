Brewery Data Pipeline (AB-InBev)
This project implements an automated ETL pipeline that fetches data from the Open Brewery DB API, processes it through a Medallion Architecture, and stores it in a PostgreSQL Datalake using Apache Airflow for orchestration.

## 🏗️ Architecture Overview
The pipeline is divided into three distinct layers to ensure data quality and scalability:

- Bronze Layer: Raw data ingestion. Data is fetched from the API and stored as JSONB with partitioning by ingestion date (anomesdia).
- Silver Layer: Data cleaning and transformation. It handles deduplication, type casting, and schema enforcement.
- Gold Layer: Data aggregation. Provides business-ready insights, such as the total count of breweries per state.

## 🛠️ Tech Stack
- Orchestrator: Apache Airflow 2.8.1
- Database: PostgreSQL 15 (Separate instances for Airflow metadata and the Datalake)
- Containerization: Docker & Docker Compose
- Language: Python 3.10
- Visualization/Management: pgAdmin 4

## 🚀 Getting Started
Prerequisites:
- Docker installed
- Docker Compose installed
- Git

Installation & Setup
Clone the repository:
```
git clone https://github.com/descomplicandodados/ab-inbev.git
```

- Open the project folder
```
cd ab-inbev
```

Configure Environment Variables:
Copy the example environment file and fill in any necessary secrets (though the project comes with defaults for local development):
cp .env.example .env

Spin up the containers:
The following command will build the custom Airflow image, initialize the database, and start all services (Postgres, Airflow Webserver, Scheduler, and pgAdmin):
```
docker-compose up -d
```

Access the interfaces:
Airflow UI: http://localhost:8080 
pgAdmin: http://localhost:5050 

## 📈 Pipeline Details
The DAG brewery_full_pipeline runs daily and consists of three main tasks:
bronze_ingestion:
- Connects to https://api.openbrewerydb.org/v1/breweries.
-Iterates through API pagination (200 records per page).
- Saves raw data into breweries_bronze.

silver_transformation:
- Reads from Bronze.
- Transforms JSON fields into structured columns.

gold_aggregation:
- Aggregates the number of breweries by state and date.
- Saves the results into breweries_gold.

Observability
All layers log execution metrics (total records processed and duration) into the pipeline_metrics table within the Postgres Datalake.

## 📁 Project Structure
├── airflow/dags/      # Airflow DAG definitions
├── src/               # ETL Scripts (bronze.py, silver.py, gold.py)
├── tests/             # Unit and integration tests
├── Dockerfile         # Custom Airflow image with dependencies
├── docker-compose.yml # Infrastructure definition
└── .env               # Environment configuration

## 🧪 Running Tests
To run the test suite, ensure your environment is active and run:
pytest