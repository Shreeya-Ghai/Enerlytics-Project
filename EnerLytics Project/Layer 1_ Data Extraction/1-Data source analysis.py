# Databricks notebook source
# DBTITLE 1,List uploaded datasets
# List all uploaded electricity datasets

files = dbutils.fs.ls("/Volumes/workspace/default/enerlyticsrawdata/")

for file in files:
    print(file.name)

# COMMAND ----------

# DBTITLE 1,Load datasets and count rows
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

print("ENERGY DATA:", energy_df.count(), "rows")
print("PEAK DEMAND DATA:", peak_df.count(), "rows")
print("UNMET DEMAND DATA:", unmet_df.count(), "rows")

# COMMAND ----------

# DBTITLE 1,Inspect schemas
# Inspect schemas and data types
print("ENERGY DATA COLUMNS")
energy_df.printSchema()

print("PEAK DEMAND COLUMNS")
peak_df.printSchema()

print("UNMET DEMAND COLUMNS")
unmet_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Inspect column names
print("ENERGY:", energy_df.columns)
print("\nPEAK:", peak_df.columns)
print("\nUNMET:", unmet_df.columns)

# COMMAND ----------

# DBTITLE 1,Preview first 5 rows
# Preview the first 5 rows of each dataset
print("ENERGY DATA")
display(energy_df.limit(5))

print("PEAK DEMAND DATA")
display(peak_df.limit(5))

print("UNMET DEMAND DATA")
display(unmet_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Identify missing values
# To identify missing values

def show_missing(df, name):
    from pyspark.sql.functions import col, sum, when

    result = df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).first().asDict()

    print("\n" + name)
    for column, count in result.items():
        if count > 0:
            print(column, ":", count)

show_missing(energy_df, "ENERGY")
show_missing(peak_df, "PEAK DEMAND")
show_missing(unmet_df, "UNMET DEMAND")

# COMMAND ----------

# DBTITLE 1,Missing value findings
# MAGIC %md
# MAGIC **Missing-value findings (from the original analysis)**
# MAGIC
# MAGIC - Andaman and Nicobar Islands and Lakshadweep: entirely empty. We'll exclude these columns from analytical calculations.
# MAGIC - Dadra and Nagar Haveli and Daman and Diu: 695 missing values each. We'll investigate before filling.
# MAGIC - Telangana and AMNSIL: retain the available records and investigate missing dates.
# MAGIC - Small isolated gaps: investigate before deciding whether to impute or leave null.

# COMMAND ----------

# DBTITLE 1,Check duplicate dates
# To check duplicates

print("ENERGY DUPLICATE DATES:",
      energy_df.count() - energy_df.select("Date").distinct().count())

print("PEAK DUPLICATE DATES:",
      peak_df.count() - peak_df.select("Date").distinct().count())

print("UNMET DUPLICATE DATES:",
      unmet_df.count() - unmet_df.select("Date").distinct().count())

# COMMAND ----------

# DBTITLE 1,Duplicate date occurrences
from pyspark.sql.functions import col, count

print("ENERGY DUPLICATE DATES")
energy_df.groupBy("Date") \
    .agg(count("*").alias("Occurrences")) \
    .filter(col("Occurrences") > 1) \
    .orderBy("Date") \
    .show(100, False)

# COMMAND ----------

# DBTITLE 1,Investigate duplicated date
# Investigate the duplicated date found in the raw data
print("ENERGY DATA")
energy_df.filter(col("Date") == "2013-06-26").show(truncate=False)

print("PEAK DEMAND DATA")
peak_df.filter(col("Date") == "2013-06-26").show(truncate=False)

print("UNMET DEMAND DATA")
unmet_df.filter(col("Date") == "2013-06-26").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Check exact duplicate rows
# Check for exact duplicate rows

datasets = {
    "ENERGY": energy_df,
    "PEAK DEMAND": peak_df,
    "UNMET DEMAND": unmet_df
}

for name, df in datasets.items():
    total_rows = df.count()
    unique_rows = df.dropDuplicates().count()

    print("\n", name)
    print("Total rows:", total_rows)
    print("Unique rows:", unique_rows)
    print("Exact duplicate rows:", total_rows - unique_rows)

# COMMAND ----------

# DBTITLE 1,Identify conflicting records
# Identify conflicting records for the same date

from pyspark.sql.functions import count, countDistinct, struct, col

for name, df in datasets.items():

    print("\n", name)

    duplicates = (
        df.groupBy("Date")
        .agg(
            count("*").alias("records"),
            countDistinct(struct(*df.columns)).alias("unique_records")
        )
        .filter(
            (col("records") > 1) &
            (col("unique_records") > 1)
        )
        .orderBy("Date")
    )

    duplicates.show(100, False)

# COMMAND ----------

# DBTITLE 1,Data source analysis summary
# MAGIC %md
# MAGIC **Summary of data source analysis**
# MAGIC
# MAGIC - All three datasets are wide-format daily tables: 3,955 rows, one `Date` column and one column per Indian state / region, plus `AMNSIL`, `Damodar Valley Corporation` and `Total`.
# MAGIC - The duplicated date `2013-06-26` contains conflicting values; the Silver layer resolves it by averaging (see `02_data_cleaning`).
# MAGIC - Two columns are entirely empty (Andaman and Nicobar Islands, Lakshadweep) and are dropped during cleaning.

# COMMAND ----------

