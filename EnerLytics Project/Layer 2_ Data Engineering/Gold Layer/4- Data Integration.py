# Databricks notebook source
# DBTITLE 1,Date consistency check
# Confirm the Silver tables share the same dates before joining

energy_dates = spark.table("workspace.default.silver_energy").select("Date")
peak_dates = spark.table("workspace.default.silver_peak_demand").select("Date")
unmet_dates = spark.table("workspace.default.silver_unmet_demand").select("Date")

print("Energy dates:", energy_dates.distinct().count())
print("Peak dates:", peak_dates.distinct().count())
print("Unmet dates:", unmet_dates.distinct().count())

print("Energy vs Peak differences:", energy_dates.subtract(peak_dates).count())
print("Energy vs Unmet differences:", energy_dates.subtract(unmet_dates).count())

# COMMAND ----------

# DBTITLE 1,Join datasets on Date
# Join the electricity datasets on Date and rename columns by source

energy = spark.table("workspace.default.silver_energy")
peak = spark.table("workspace.default.silver_peak_demand")
unmet = spark.table("workspace.default.silver_unmet_demand")

joined = energy.alias("e") \
    .join(peak.alias("p"), "Date", "inner") \
    .join(unmet.alias("u"), "Date", "inner")

from pyspark.sql.functions import col

selected_columns = [col("Date")]

for c in energy.columns:
    if c != "Date":
        selected_columns.append(col(f"e.`{c}`").alias(f"{c}_energy"))

for c in peak.columns:
    if c != "Date":
        selected_columns.append(col(f"p.`{c}`").alias(f"{c}_peak_MW"))

for c in unmet.columns:
    if c != "Date":
        selected_columns.append(col(f"u.`{c}`").alias(f"{c}_unmet_MW"))

gold_df = joined.select(*selected_columns)

print("Integrated dataset created successfully!")
print("Rows:", gold_df.count())
print("Columns:", len(gold_df.columns))

display(gold_df.orderBy("Date").limit(5))

# COMMAND ----------

# DBTITLE 1,Save Gold table
# Save integrated dataset as a Gold Delta table

gold_df.write.format("delta") \
    .option("delta.columnMapping.mode", "name") \
    .option("delta.minReaderVersion", "2") \
    .option("delta.minWriterVersion", "5") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_electricity_analysis")

print("Gold table created successfully!")

# COMMAND ----------

# DBTITLE 1,Verify Gold table
# Verify the Gold table

gold_check = spark.table("workspace.default.gold_electricity_analysis")
gold_check.agg(
    __import__("pyspark").sql.functions.count("*").alias("Rows"),
    __import__("pyspark").sql.functions.min("Date").alias("First_Date"),
    __import__("pyspark").sql.functions.max("Date").alias("Last_Date")
).show()

# COMMAND ----------

