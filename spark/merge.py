from pyspark.sql import SparkSession
import pandas as pd
import os

# Start Spark
spark = SparkSession.builder \
    .appName("AirPollutionMerge") \
    .getOrCreate()

# -----------------------------
# 1. Read CSV (Spark) - Using Absolute Path
# -----------------------------
csv_df_raw = spark.read.option("header", True).csv(
    "/opt/airflow/data/raw/air_pollution.csv"
)

csv_df_raw = csv_df_raw.withColumn(
    "pollutant_avg",
    csv_df_raw["pollutant_avg"].cast("double")
)

# Pivot CSV
csv_df = csv_df_raw.groupBy("city", "last_update") \
    .pivot("pollutant_id") \
    .avg("pollutant_avg") \
    .selectExpr(
        "city as City",
        "last_update as Date",
        "`PM2.5` as PM2_5",
        "PM10",
        "NO2",
        "SO2"
    )

# -----------------------------
# 2. Read Excel (Pandas → Spark) - Using Absolute Path
# -----------------------------
# Added engine="openpyxl" for compatibility
excel_pd = pd.read_excel("/opt/airflow/data/raw/Dataset_23-4.xlsx", engine="openpyxl")

excel_df = spark.createDataFrame(excel_pd) \
    .selectExpr(
        "City",
        "Time as Date",
        "`PM2.5` as PM2_5",
        "PM10",
        "NO2",
        "SO2"
    )

# -----------------------------
# 3. Merge
# -----------------------------
merged_df = csv_df.unionByName(excel_df)

merged_df.show(10)

# -----------------------------
# 4. Save output - Using Absolute Path
# -----------------------------
output_path = "/opt/airflow/data/processed/air_pollution_merged"
merged_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()