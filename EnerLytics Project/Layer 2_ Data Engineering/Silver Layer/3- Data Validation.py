# Databricks notebook source
# DBTITLE 1,Data Validation — Overview
# MAGIC %md
# MAGIC # EnerLytics — Silver Layer: 03 Data Validation
# MAGIC
# MAGIC Validate the Bronze and Silver Delta tables:
# MAGIC
# MAGIC - Row counts per table
# MAGIC - Date consistency across the three Silver tables
# MAGIC - No duplicate dates remain after cleaning
# MAGIC - Remaining missing values (isolated gaps are intentionally left null)

# COMMAND ----------

# DBTITLE 1,Row counts
# Row counts for Bronze and Silver tables

tables = [
    "bronze_energy",
    "bronze_peak_demand",
    "bronze_unmet_demand",
    "silver_energy",
    "silver_peak_demand",
    "silver_unmet_demand"
]

for table in tables:
    df = spark.table(f"workspace.default.{table}")
    print(table, ":", df.count(), "rows")

# COMMAND ----------

# DBTITLE 1,Date consistency
# Date consistency across the Silver tables

energy_dates = spark.table("workspace.default.silver_energy").select("Date")
peak_dates = spark.table("workspace.default.silver_peak_demand").select("Date")
unmet_dates = spark.table("workspace.default.silver_unmet_demand").select("Date")

print("Energy dates:", energy_dates.distinct().count())
print("Peak dates:", peak_dates.distinct().count())
print("Unmet dates:", unmet_dates.distinct().count())

print("Energy vs Peak differences:", energy_dates.subtract(peak_dates).count())
print("Energy vs Unmet differences:", energy_dates.subtract(unmet_dates).count())

# COMMAND ----------

# DBTITLE 1,Check for duplicate dates in Silver
# No duplicate dates remain after cleaning

energy_clean = spark.table("workspace.default.silver_energy")
peak_clean = spark.table("workspace.default.silver_peak_demand")
unmet_clean = spark.table("workspace.default.silver_unmet_demand")

print("ENERGY DUPLICATE DATES:",
      energy_clean.count() - energy_clean.select("Date").distinct().count())

print("PEAK DUPLICATE DATES:",
      peak_clean.count() - peak_clean.select("Date").distinct().count())

print("UNMET DUPLICATE DATES:",
      unmet_clean.count() - unmet_clean.select("Date").distinct().count())

# COMMAND ----------

# DBTITLE 1,Remaining missing values in Silver
# Remaining missing values in Silver (isolated gaps are intentionally left null)

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

show_missing(energy_clean, "SILVER ENERGY")
show_missing(peak_clean, "SILVER PEAK DEMAND")
show_missing(unmet_clean, "SILVER UNMET DEMAND")

# COMMAND ----------

# DBTITLE 1,Validation expectations
# MAGIC %md
# MAGIC **Expected validation results**
# MAGIC
# MAGIC - Bronze tables hold the raw 3,955 rows per dataset; Silver tables hold one row per unique date after deduplication and averaging of conflicting records.
# MAGIC - The three Silver tables contain identical date sets, so inner joins in the Gold layer lose no dates.
# MAGIC - Entirely empty columns (Andaman and Nicobar Islands, Lakshadweep) are absent from Silver.
# MAGIC - Isolated missing values (e.g. Dadra and Nagar Haveli, Daman and Diu, Telangana, AMNSIL) remain null, per the original cleaning decision.

# COMMAND ----------

