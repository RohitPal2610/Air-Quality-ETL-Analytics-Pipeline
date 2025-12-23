from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Start Spark
spark = SparkSession.builder \
    .appName("AirPollutiontransform") \
    .getOrCreate()

# ---------------------------------------------------------
# 1. Read Data - Using Absolute Path from Container
# ---------------------------------------------------------
# Path points to the output of your previous 'merge' task
input_path = "/opt/airflow/data/processed/air_pollution_merged"
data = spark.read.option("header", True).csv(input_path)

# Convert Date column to timestamp
transformed_df = data.withColumn(
    "Date",
    F.to_timestamp("Date", "dd-MM-yyyy HH:mm:ss")
)

# ---------------------------------------------------------
# 2. Data Cleaning
# ---------------------------------------------------------
# Remove bad / duplicate data
transformed_df = (transformed_df.dropna(subset=["City", "Date"])
                  .dropDuplicates(["City", "Date"]))

# Clean numeric columns (treat negative values as null)
pollutants = ["PM2_5", "PM10", "NO2", "SO2"]
for col in pollutants:
    # Ensure columns are numeric before processing
    transformed_df = transformed_df.withColumn(col, F.col(col).cast("double"))
    
    transformed_df = transformed_df.withColumn(
        col,
        F.when(F.col(col) < 0, None).otherwise(F.col(col))
    )

# Round values (noise removal)
for col in pollutants:
    transformed_df = transformed_df.withColumn(
        col,
        F.round(F.col(col), 2)
    )

# ---------------------------------------------------------
# 3. AQI Calculation & Categorization
# ---------------------------------------------------------
# AQI Calculation (simple version: max of all pollutants)
transformed_df = transformed_df.withColumn(
    "AQI",
    F.greatest("PM2_5", "PM10", "NO2", "SO2")
)

# AQI Category (CPCB-style)
transformed_df = transformed_df.withColumn(
    "AQI_Category",
    F.when(F.col("AQI") <= 50, "Good")
     .when(F.col("AQI") <= 100, "Satisfactory")
     .when(F.col("AQI") <= 200, "Moderate")
     .when(F.col("AQI") <= 300, "Poor")
     .when(F.col("AQI") <= 400, "Very Poor")
     .otherwise("Severe")
)

# ---------------------------------------------------------
# 4. Save Final Output - Using Absolute Path
# ---------------------------------------------------------
final_df = transformed_df.select(
    "City", "Date", "PM2_5", "PM10", "NO2", "SO2", "AQI", "AQI_Category"
)

final_df.printSchema()
final_df.show(10, truncate=False)

output_path = "/opt/airflow/data/processed/clean_air_pollution_data"
final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()