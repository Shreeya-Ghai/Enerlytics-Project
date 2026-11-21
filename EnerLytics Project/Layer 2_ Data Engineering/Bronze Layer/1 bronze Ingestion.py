# Databricks notebook source
# DBTITLE 1,Bronze Ingestion — Overview
# MAGIC %md
# MAGIC # EnerLytics — Bronze Layer: 01 Bronze Ingestion
# MAGIC
# MAGIC Store the raw ingested electricity data as Delta tables in `workspace.default`:
# MAGIC
# MAGIC - `bronze_energy`
# MAGIC - `bronze_peak_demand`
# MAGIC - `bronze_unmet_demand`
# MAGIC
# MAGIC Data is read as-is (no cleaning) from `/Volumes/workspace/default/enerlyticsrawdata/`, preserving the original working implementation.

# COMMAND ----------

# DBTITLE 1,Load raw datasets
# Load electricity datasets using Apache Spark (raw, as-is)

energy_df = spark.read.csv(
    "/Volumes/workspace/default/enerlyticsrawdata/corrected_daily_energy_met_MU (HOW MUCH ELECTRICITY WAS SUPPLIED).csv",
    header=True,
    inferSchema=True
)

peak_df = spark.read.csv(
    "/Volumes/workspace/default/enerlyticsrawdata/corrected_peak_met_MW.csv",
    header=True,
    inferSchema=True
)

unmet_df = spark.read.csv(
    "/Volumes/workspace/default/enerlyticsrawdata/peak_unmet_MW.csv",
    header=True,
    inferSchema=True
)

print("Energy:", energy_df.count())
print("Peak:", peak_df.count())
print("Unmet:", unmet_df.count())

# COMMAND ----------

# DBTITLE 1,Write Bronze Delta tables
# STEP 3 - RAW DATA STORAGE

energy_df.write.format("delta") \
    .option("delta.columnMapping.mode", "name") \
    .option("delta.minReaderVersion", "2") \
    .option("delta.minWriterVersion", "5") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_energy")

peak_df.write.format("delta") \
    .option("delta.columnMapping.mode", "name") \
    .option("delta.minReaderVersion", "2") \
    .option("delta.minWriterVersion", "5") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_peak_demand")

unmet_df.write.format("delta") \
    .option("delta.columnMapping.mode", "name") \
    .option("delta.minReaderVersion", "2") \
    .option("delta.minWriterVersion", "5") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_unmet_demand")

print("All three tables created successfully!")

# COMMAND ----------

# DBTITLE 1,Verify Bronze tables
# Verify Bronze tables

bronze_tables = [
    "bronze_energy",
    "bronze_peak_demand",
    "bronze_unmet_demand"
]

for table in bronze_tables:
    df = spark.table(f"workspace.default.{table}")
    print(table, ":", df.count(), "rows")

# COMMAND ----------

