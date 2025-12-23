# Air Quality ETL & Analytics Pipeline

An end-to-end automated data engineering pipeline that ingests, cleans, and analyzes air pollution data from multiple sources using **Apache Airflow** and **PySpark**.

## 🚀 Project Overview
This project demonstrates a production-grade ETL (Extract, Transform, Load) workflow for environmental data. It automates the collection of pollutant metrics (PM2.5, NO2, SO2, etc.) from diverse file formats, applies complex transformations for data quality, and generates analytical insights including Air Quality Index (AQI) classifications.

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow
* **Data Processing:** PySpark (Spark SQL & DataFrames)
* **Programming:** Python (Pandas, OpenPyXL)
* **Data Sources:** Heterogeneous datasets (CSV and Excel/XLSX)

## 🏗️ Pipeline Architecture
The pipeline is orchestrated as a Directed Acyclic Graph (DAG) in Airflow with three primary stages:

1.  **Ingest (Merge):** Merges raw data from CSV and Excel sources into a unified Spark DataFrame, handling schema alignment and data type casting.
2.  **Transform:** Executes data cleaning tasks including deduplication, handling null values, and noise removal. It also calculates the **AQI** and categorizes it into health risk levels (Good, Satisfactory, Poor, etc.) based on CPCB standards.
3.  **Analyze & Visualize:** Performs city-wise statistical analysis, identifies the most polluted regions, and generates a final summary report in CSV format.

## 🏗️ Pipeline Architecture

![Airflow DAG Graph](Screenshot%202025-12-23%20110914.png)

The pipeline is orchestrated as a Directed Acyclic Graph (DAG) in Airflow with three primary stages:
* **merge_raw_data**: Ingests and unifies CSV and Excel datasets.
* **transform_data**: Cleans data and calculates the AQI.
* **analyze_and_visualize**: Generates final reports and summaries.
  
## 📂 Project Structure
```text
├── dags/
│   └── air_pollution_dag.py     # Airflow DAG defining task dependencies
├── spark/
│   ├── merge.py                 # Data ingestion & schema unification
│   ├── transform.py             # Data cleaning & AQI calculation logic
│   └── analysis_viz.py          # Summary report generation & analytics
└── data/
    ├── raw/                     # Original CSV and Excel datasets
    ├── processed/               # Cleaned parquet/csv output
    └── analysis/                # Final summary reports



