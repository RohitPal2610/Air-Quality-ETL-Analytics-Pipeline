from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("AirPollutionAnalysis") \
    .getOrCreate()

# ---------------------------------------------------------
# 2. Load the Transformed Data - Using Absolute Path
# ---------------------------------------------------------
# Points to the output folder of your transform.py script
input_path = "/opt/airflow/data/processed/new"
df = spark.read.option("header", True).csv(input_path)

# Convert columns to double for calculation
num_cols = ["PM2_5", "PM10", "NO2", "SO2", "AQI"]
for col in num_cols:
    df = df.withColumn(col, F.col(col).cast("double"))

# --- ANALYSIS 1: AQI Category Count ---
print("\n" + "="*30)
print("AQI CATEGORY DISTRIBUTION")
print("="*30)
df.groupBy("AQI_Category").count().orderBy(F.desc("count")).show()

# --- ANALYSIS 2: Top 10 Most Polluted Cities ---
print("\n" + "="*30)
print("TOP 10 MOST POLLUTED CITIES")
print("="*30)
df.select("City", "AQI", "AQI_Category") \
  .orderBy(F.desc("AQI")).limit(10).show()

# --- ANALYSIS 3: Pollutant Summary Statistics ---
print("\n" + "="*30)
print("POLLUTANT SUMMARY STATISTICS")
print("="*30)
df.select(num_cols).describe().show()

# --- ANALYSIS 4: City-Wise Average Pollution ---
print("\n" + "="*30)
print("CITY-WISE AVERAGE AQI (Top 5)")
print("="*30)
df.groupBy("City").agg(F.avg("AQI").alias("Avg_AQI")) \
  .orderBy(F.desc("Avg_AQI")).limit(5).show()

# ---------------------------------------------------------
# 3. Export Summary - Using Absolute Path
# ---------------------------------------------------------
output_path = "/opt/airflow/data/analysis/summary_report"
df.groupBy("AQI_Category").count().coalesce(1).write.mode("overwrite") \
    .option("header", True).csv(output_path)

spark.stop()