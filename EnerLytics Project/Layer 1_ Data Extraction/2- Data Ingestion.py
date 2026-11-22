# Databricks notebook source
# DBTITLE 1,Check uploaded files
# Check uploaded files

display(dbutils.fs.ls("/Volumes/workspace/default/enerlyticsrawdata/"))

# COMMAND ----------

# DBTITLE 1,Load the three datasets
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

# DBTITLE 1,Preview loaded data
# Preview the loaded data
print("ENERGY DATA")
display(energy_df.limit(5))

print("PEAK DEMAND DATA")
display(peak_df.limit(5))

print("UNMET DEMAND DATA")
display(unmet_df.limit(5))

# COMMAND ----------

