# Databricks notebook source
# DBTITLE 1,SQL Views — Overview
# MAGIC %md
# MAGIC # EnerLytics — Gold Layer: 06 SQL Views
# MAGIC
# MAGIC Create the SQL views needed for analysis and Power BI. The original project used temporary views (`createOrReplaceTempView`); this notebook additionally creates **persistent** views in `workspace.default` over the Gold tables so they can be queried by Power BI without rerunning the pipeline:
# MAGIC
# MAGIC - `v_electricity_analysis` → `gold_electricity_analysis` (integrated daily dataset)
# MAGIC - `v_state_analysis` → `gold_state_analysis` (state-wise performance)
# MAGIC - `v_yearly_trends` → `gold_yearly_trends` (yearly trends)
# MAGIC - `v_monthly_trends` → `gold_monthly_trends` (monthly trends)
# MAGIC
# MAGIC It then runs the original SQL analysis queries against these views.

# COMMAND ----------

# DBTITLE 1,Create persistent views
# Create persistent SQL views over the Gold tables

views = {
    "v_electricity_analysis": "gold_electricity_analysis",
    "v_state_analysis": "gold_state_analysis",
    "v_yearly_trends": "gold_yearly_trends",
    "v_monthly_trends": "gold_monthly_trends"
}

for view, table in views.items():
    spark.sql(
        f"CREATE OR REPLACE VIEW workspace.default.{view} "
        f"AS SELECT * FROM workspace.default.{table}"
    )
    print(f"Created view workspace.default.{view}")

# COMMAND ----------

# DBTITLE 1,Run analytical SQL queries
# SQL ANALYSIS - Electricity insights (original queries, run against the views)

spark.sql("""
SELECT State, Avg_Unmet_MW
FROM workspace.default.v_state_analysis
ORDER BY Avg_Unmet_MW DESC
LIMIT 10
""").show()

spark.sql("""
SELECT Year, Total_Supply_MU, Avg_Peak_Demand_MW,
       Avg_Unmet_Demand_MW
FROM workspace.default.v_yearly_trends
ORDER BY Year
""").show()

spark.sql("""
SELECT Date, Total_unmet_MW
FROM workspace.default.v_electricity_analysis
ORDER BY Total_unmet_MW DESC
LIMIT 10
""").show()

# COMMAND ----------

# DBTITLE 1,Verify views
# Verify the views

display(spark.sql("SHOW VIEWS IN workspace.default"))

for view in views:
    count = spark.table(f"workspace.default.{view}").count()
    print(view, ":", count, "rows")

# COMMAND ----------

