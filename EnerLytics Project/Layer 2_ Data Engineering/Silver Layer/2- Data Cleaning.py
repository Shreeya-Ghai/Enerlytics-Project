# Databricks notebook source
# DBTITLE 1,Load raw datasets
# Load electricity datasets using Apache Spark

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

# DBTITLE 1,Clean the three datasets
# Clean each dataset (dedup, drop empty columns, average conflicting records per date)

from pyspark.sql.functions import col, avg

def clean_dataset(df, name):

    # Remove exact duplicate rows
    df = df.dropDuplicates()

    # Identify numerical columns
    numeric_cols = [
        field.name
        for field in df.schema.fields
        if field.name != "Date"
        and field.dataType.typeName() in
        ["integer", "long", "float", "double", "decimal", "short"]
    ]

    # Remove completely empty columns
    valid_cols = [
        c for c in numeric_cols
        if df.filter(col(c).isNotNull()).limit(1).count() > 0
    ]

    # Average conflicting records for the same date
    cleaned = df.groupBy("Date").agg(
        *[avg(col(c)).alias(c) for c in valid_cols]
    )

    print("\n", name)
    print("Cleaned rows:", cleaned.count())
    print("Removed empty columns:", 
          set(df.columns) - set(["Date"] + valid_cols))

    return cleaned


energy_clean = clean_dataset(energy_df, "ENERGY")
peak_clean = clean_dataset(peak_df, "PEAK DEMAND")
unmet_clean = clean_dataset(unmet_df, "UNMET DEMAND")

# COMMAND ----------

# DBTITLE 1,Write Silver Delta tables
# STEP 4 - Store Cleaned Data

energy_clean.write.format("delta").option(
    "delta.columnMapping.mode", "name"
).option(
    "delta.minReaderVersion", "2"
).option(
    "delta.minWriterVersion", "5"
).mode("overwrite").saveAsTable(
    "workspace.default.silver_energy"
)

peak_clean.write.format("delta").option(
    "delta.columnMapping.mode", "name"
).option(
    "delta.minReaderVersion", "2"
).option(
    "delta.minWriterVersion", "5"
).mode("overwrite").saveAsTable(
    "workspace.default.silver_peak_demand"
)

unmet_clean.write.format("delta").option(
    "delta.columnMapping.mode", "name"
).option(
    "delta.minReaderVersion", "2"
).option(
    "delta.minWriterVersion", "5"
).mode("overwrite").saveAsTable(
    "workspace.default.silver_unmet_demand"
)

print("All three tables created successfully!")

# COMMAND ----------

# DBTITLE 1,Verify Silver tables
# Verify Silver tables

silver_tables = [
    "silver_energy",
    "silver_peak_demand",
    "silver_unmet_demand"
]

for table in silver_tables:
    df = spark.table(f"workspace.default.{table}")
    print(table, ":", df.count(), "rows")

# COMMAND ----------

