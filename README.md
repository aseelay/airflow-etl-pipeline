# Airflow ETL Pipeline

## Overview

This project implements an ETL pipeline using Apache Airflow to extract data from Google Sheets, transform it, and load it into an AWS Data Lake using Parquet format.

The pipeline supports both full and incremental data loading.

---

## Architecture

```
Google Sheets
      |
      v
Apache Airflow
      |
      v
Data Transformation
      |
      v
AWS S3 (Data Lake)
      |
      v
AWS Glue Catalog
      |
      v
Amazon Athena
```

---

## Technologies Used

- Apache Airflow
- Docker & Docker Compose
- Python
- Google Sheets API
- AWS S3
- AWS Glue
- Amazon Athena
- Parquet Data Format

---

## Project Structure

```
airflow-pipeline/

├── dags/
│   ├── full_excel_load.py
│   └── event_based_load.py
│
├── config/
│   └── config.example.yaml
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Pipeline Features

### Full Load

- Extracts all data from Google Sheets.
- Transforms the extracted data.
- Converts data into Parquet format.
- Stores the output files in AWS S3.

### Incremental Load

- Runs automatically based on Airflow scheduling.
- Tracks processed rows using Airflow Variables.
- Loads only new or updated records.

---

## Configuration

The project uses a configuration file to manage connections and settings.

Create your local configuration file:

```
config/config.yaml
```

Use the example configuration:

```
config/config.example.yaml
```

The configuration includes:

- AWS S3 settings
- Google Sheets information
- Pipeline parameters

Sensitive information such as AWS credentials and service account files should not be uploaded to GitHub.

---

## Running the Project

### Start Airflow Services

```bash
docker compose up -d
```

### Check Running Containers

```bash
docker ps
```

### Stop Services

```bash
docker compose down
```

---

## Airflow DAGs

### Full Load DAG

`full_excel_load.py`

This DAG performs a complete extraction from Google Sheets and loads the data into AWS S3.

---

### Incremental Load DAG

`event_based_load.py`

This DAG performs incremental extraction by processing only new data based on the stored watermark.

---

## Data Storage

Processed data is stored in:

```
AWS S3
```

using:

```
Parquet format
```

The stored data can be queried using:

```
Amazon Athena
```

---

## Future Improvements

- Add data quality checks.
- Add monitoring and alerting.
- Improve pipeline scalability.
- Automate deployment.

---

## Author

Aseel