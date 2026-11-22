# Databricks notebook source
# DBTITLE 1,Load Gold and Silver tables
# Load the integrated Gold dataset and the Silver tables (for column lists)

gold_df = spark.table("workspace.default.gold_electricity_analysis")

energy_clean = spark.table("workspace.default.silver_energy")
peak_clean = spark.table("workspace.default.silver_peak_demand")
unmet_clean = spark.table("workspace.default.silver_unmet_demand")

print("Gold rows:", gold_df.count())

# COMMAND ----------

# DBTITLE 1,State-wise performance
# State-wise electricity performance

from pyspark.sql.functions import sum, avg, col

state_columns = [
    c for c in energy_clean.columns
    if c != "Date" and c in peak_clean.columns and c in unmet_clean.columns
]

state_results = []

for state in state_columns:
    result = gold_df.agg(
        sum(col(f"`{state}_energy`")).alias("Total_Supply_MU"),
        avg(col(f"`{state}_peak_MW`")).alias("Avg_Peak_MW"),
        avg(col(f"`{state}_unmet_MW`")).alias("Avg_Unmet_MW")
    ).withColumn("State", __import__("pyspark").sql.functions.lit(state))

    state_results.append(result)

from functools import reduce

state_analysis = reduce(lambda a, b: a.unionByName(b), state_results)

display(state_analysis.orderBy(col("Avg_Unmet_MW").desc()))

# COMMAND ----------

# DBTITLE 1,Yearly trends
# Year-wise electricity trends

from pyspark.sql.functions import year, sum, avg, col

yearly_analysis = gold_df.groupBy(
    year("Date").alias("Year")
).agg(
    sum("Total_energy").alias("Total_Supply_MU"),
    avg("Total_peak_MW").alias("Avg_Peak_Demand_MW"),
    avg("Total_unmet_MW").alias("Avg_Unmet_Demand_MW")
).orderBy("Year")

display(yearly_analysis)

# COMMAND ----------

# DBTITLE 1,Monthly trends
# Monthly electricity trends

from pyspark.sql.functions import month, year, avg, col

monthly_analysis = gold_df.groupBy(
    year("Date").alias("Year"),
    month("Date").alias("Month")
).agg(
    avg("Total_energy").alias("Avg_Supply_MU"),
    avg("Total_peak_MW").alias("Avg_Peak_MW"),
    avg("Total_unmet_MW").alias("Avg_Unmet_MW")
).orderBy("Year", "Month")

display(monthly_analysis)

# COMMAND ----------

# DBTITLE 1,Top 10 shortage states
# Identifying top 10 states by unmet demand

top_shortage_states = state_analysis.orderBy(
    col("Avg_Unmet_MW").desc()
).limit(10)

display(top_shortage_states)

# COMMAND ----------

# DBTITLE 1,Save analytical Gold tables
# Save analytical Gold tables

from pyspark.sql.functions import col

state_analysis_clean = state_analysis.filter(
    ~col("State").isin(
        "Total",
        "AMNSIL",
        "Damodar Valley Corporation"
    )
)

state_analysis_clean.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.default.gold_state_analysis"
)

yearly_analysis.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.default.gold_yearly_trends"
)

monthly_analysis.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.default.gold_monthly_trends"
)

print("All analytical Gold tables saved successfully!")

# COMMAND ----------

# DBTITLE 1,KPI summary
# KPI Summary
from pyspark.sql.functions import sum, avg, countDistinct

dashboard_kpis = gold_df.agg(
    sum("Total_energy").alias("Total Electricity Supplied (MU)"),
    avg("Total_peak_MW").alias("Average Peak Demand (MW)"),
    avg("Total_unmet_MW").alias("Average Unmet Demand (MW)"),
    countDistinct("Date").alias("Days Analyzed")
)

display(dashboard_kpis)

# COMMAND ----------

