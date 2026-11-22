EnerLytics
Big Data Engineering and Analytics Pipeline for Indian Electricity Consumption
Technologies: Python, Apache Spark ,PySpark, Databricks, Delta Lake, SQL, Microsoft Power BI
Team Members – Shreeya Ghai (2401730030)
                                Harshdeep Singh Hothi (2401730219)
                                Riya Goyal (2401730215)

1. Introduction
EnerLytics is a Big Data engineering and business intelligence project designed to analyse electricity supply and demand patterns across Indian states.
The project focuses on three key electricity indicators:
•	Electricity Supply (Million Units – MU)
•	Peak Demand Met (Megawatts – MW)
•	Peak Demand Unmet (Megawatts – MW)
OBJECTIVE- To develop an end-to-end data pipeline that transforms raw electricity datasets into structured, validated and analysis-ready data, followed by an interactive business intelligence dashboard.
The architecture is divided into three major layers:
1.	Data Extraction
2.	Data Engineering
3.	Data Visualization
The project follows the Medallion Architecture (Bronze, Silver and Gold) for organising data processing and storage.

2. Problem Statement -  
Energy – Smart Electricity Consumption Analytics: Analyze electricity consumption, peak demand, regional energy usage and consumption patterns.
Electricity supply and demand fluctuate across geographical regions and time periods. Analysing these variations requires combining multiple datasets, handling inconsistent data structures and processing large volumes of records.
Raw datasets alone do not provide an efficient way to perform meaningful comparisons or identify trends.
EnerLytics addresses this challenge by developing a structured data engineering pipeline that integrates electricity supply and demand information, performs data quality checks, and produces analytical datasets for visualisation.



3. Project Architecture
Stage 1: Data Acquisition and Extraction
•	Data Source Identification: Identify publicly available electricity datasets containing daily statewise electricity supply, peak demand met and peak demand unmet information.
•	Data Extraction: Obtain the datasets from the external data source in CSV format.
•	Data Ingestion: Import the source files into the Databricks environment for processing using Apache Spark.
•	Raw Data Preservation: Retain the original datasets as the source of truth before applying transformations.
Stage 2: Distributed Data Processing Using Apache Spark
•	Spark Initialization: Establish a SparkSession in Databricks to access Spark's distributed processing capabilities.
•	Data Loading: Read the three CSV datasets into PySpark DataFrames with appropriate headers and inferred or explicitly defined schemas.
•	Schema Inspection: Examine column names, data types, record counts and dataset structure to understand the incoming data.
•	Distributed Processing: Use Spark DataFrame operations to perform transformations across data partitions, enabling scalable processing.
•	Transformation Execution: Apply filtering, column operations, type conversions, aggregations and joins as required by the analytical workflow.
Stage 3: Data Engineering Using the Medallion Architecture
Data quality layers using Delta Lake.
 Bronze Layer - Raw Data Storage
 Silver Layer - Data Cleaning and Standardization
 Gold Layer - Data Integration and Analytical Modelling

Stage 4: SQL-Based Analytical Processing
•	SQL Integration: Access Gold-layer Delta tables through Databricks SQL.
•	Data Aggregation: Apply aggregate functions such as SUM, AVG, MIN and MAX to calculate electricity indicators.
•	Analytical Grouping: Perform statewise, monthly and yearly grouping to identify regional and temporal patterns.
•	SQL Views: Create reusable views to simplify access to processed analytical datasets.
•	Query Validation: Verify the results and ensure the analytical outputs are consistent with the underlying Gold tables.
Stage 5: Microsoft Power BI Dashboard
•	Data Connectivity: Establish a connection between Microsoft Power BI and the Databricks SQL Warehouse.
•	Gold Data Consumption: Import or query the curated Gold-layer tables and SQL views.
•	Data Modelling: Organize the analytical datasets for reporting and establish relationships where required.
•	DAX Measures: Develop calculated measures for key electricity indicators where needed.
•	Dashboard Development: Design interactive KPI cards, trend charts, statewise comparisons and analytical summaries.
•	Interactive Filtering: Implement year and state slicers to allow users to explore electricity patterns dynamically.
Stage 6: Validation and Final Output

4. Layer 1 – Data Extraction
4.1 Concept
Data extraction is the first stage of a data engineering pipeline. It involves collecting information from external sources and transferring it into an environment where it can be processed.
4.2 EnerLytics Implementation
EnerLytics uses publicly available electricity dataset containing daily statewise information.
Three source files are used:
Dataset	 Description
Daily Electricity Supply	 Electricity supplied/met across Indian states
Peak Demand Met	Peak electricity demand successfully met
Peak Demand Unmet	Electricity demand that remained unmet during peak periods
The datasets are ingested into Databricks for subsequent processing.
Data source: https://zenodo.org/records/14983362

5. Layer 2 – Data Engineering
5.1 Concept
Data engineering involves designing and implementing systems that collect, transform, organise and prepare data for analytical use.
EnerLytics uses an ETL (Extract, Transform, Load) workflow.
•	Extract: Read source datasets into the processing environment.
•	Transform: Clean, validate, standardise and integrate the data.
•	Load: Store processed outputs in structured Delta tables.
5.2 Apache Spark and PySpark
Apache Spark is the primary data processing framework used in EnerLytics to process and integrate the three electricity datasets.
The project uses PySpark, Spark's Python API, to perform data processing operations within the Databricks environment.
Implementation in EnerLytics
1. SparkSession Initialization
The SparkSession is used as the entry point for executing Spark operations within Databricks notebooks. It provides access to Spark DataFrame functionality and enables the loading and processing of the electricity datasets.
2. CSV Data Loading
The three source datasets containing electricity supply, peak demand met and peak demand unmet information are loaded into Spark DataFrames using Spark's CSV reader.
Header recognition and schema inference are used to interpret the structure and data types of the source files.
3. DataFrame Operations
PySpark DataFrames are used to inspect and manipulate the datasets. Operations include:
•	Retrieving dataset dimensions and column information.
•	Inspecting schemas and data types.
•	Displaying sample records.
•	Selecting, renaming and removing columns.
•	Handling missing and unnecessary data.
•	Preparing datasets for integration.
4. Data Cleaning and Transformation
Spark DataFrame transformations are used to standardise the source datasets, remove unnecessary empty state columns and prepare the electricity records for further processing.
5. Dataset Integration
The electricity supply, peak demand met and peak demand unmet datasets are integrated using their common date and state-level attributes to create a consolidated analytical dataset.
The initial integration produced a dataset containing 3,900 records and 112 columns.
6. Data Validation
Spark-based checks are performed to compare record counts and date consistency across the three source datasets. Initial validation confirmed 3,900 matching dates across the datasets, with zero date mismatches.
Apache Spark therefore provides the processing foundation for the EnerLytics ETL workflow, enabling structured transformations and integration of multiple electricity datasets.

5.3 Databricks
Databricks serves as the primary development and data engineering environment for EnerLytics. It provides the workspace in which the source datasets are loaded, processed through PySpark notebooks, stored as analytical tables and queried using SQL.
Implementation in EnerLytics

1. Catalog and Schema Management
The Databricks Unity Catalog environment is used to organise and access project data through catalogs and schemas. The project uses the workspace default namespace for its existing analytical tables, subject to the final project configuration.
2. Delta Table Management
Processed electricity data is stored and managed through Delta tables, making structured datasets available for subsequent analytical operations.
5. SQL Warehouse Integration
Databricks SQL Warehouse provides the SQL execution environment used to query analytical tables and expose processed data for external applications such as Microsoft Power BI.

5.4 Delta Lake
Delta Lake is the storage layer used in EnerLytics to organise processed electricity datasets into structured tables.
The project adopts the Medallion Architecture, using Bronze, Silver and Gold layers to progressively organise and prepare data for analysis.

6. Medallion Architecture
The Medallion Architecture is a data organisation pattern that divides data into three progressive quality layers: Bronze, Silver and Gold.
6.1 Bronze Layer – Raw Data
The Bronze layer is the initial storage layer.
Its purpose is to preserve source data in its original or minimally processed form.
Operations:
•	Ingest source CSV files.
•	Preserve original records and source structure.
•	Store the ingested data as Delta tables.
•	Maintain a reliable foundation for subsequent processing.
EnerLytics: Separate raw Delta tables for electricity supply, peak demand met and peak demand unmet.
6.2 Silver Layer – Cleaned Data
The Silver layer contains cleaned, standardised and validated datasets.
This stage improves data quality and consistency.
Operations:
•	Handle missing and null values.
•	Standardise column names.
•	Convert columns into appropriate data types.
•	Identify and handle duplicate records.
•	Remove unnecessary empty columns.
•	Validate dates and data consistency.
•	Store the cleaned datasets in Delta format.
EnerLytics: The three electricity datasets are prepared for reliable integration and analysis.
6.3 Gold Layer – Business-Level Data
The Gold layer contains curated, integrated and analysis-ready data.
It is designed to support reporting, analytical queries and business intelligence.
Operations:
•	Integrate electricity supply and demand datasets.
•	Generate statewise analytical tables.
•	Create yearly and monthly summaries.
•	Calculate relevant analytical metrics.
•	Create SQL views for downstream consumption.
EnerLytics: Gold-layer outputs provide the data foundation for the Power BI dashboard.
7. SQL and Analytical Processing
Structured Query Language (SQL) is used to query, aggregate and analyse structured data.
Important SQL concepts used in analytical systems include:
•	SELECT and WHERE for data retrieval and filtering.
•	GROUP BY for aggregating records.
•	JOIN for combining related datasets.
•	Aggregate functions such as SUM, AVG, MIN and MAX.
•	Views for reusable analytical queries.
EnerLytics uses SQL queries and views to make the Gold-layer datasets accessible for analytical reporting.
8. Layer 3 – Data Visualization
8.1 Microsoft Power BI
Microsoft Power BI is a business intelligence platform used to connect, model, analyse and visualise data.
Key components include:
Power Query: Used for connecting to data sources and performing data preparation.
Data Modelling: Organises relationships between tables and defines how data is analysed.
DAX (Data Analysis Expressions): A formula language used to create calculated columns, measures and analytical calculations.
Visualizations: Includes bar charts, line charts, tables, cards, maps and other graphical representations.
Slicers: Allow users to interactively filter dashboard data.

8.2 EnerLytics Dashboard
The dashboard focuses on analysing electricity supply and demand patterns.
Planned analytical components include:
•	Total Electricity Supply.
•	Average Peak Demand.
•	Average Unmet Demand.
•	Year-wise electricity trends.
•	Statewise electricity comparisons.
•	Interactive year and state filters.
The dashboard uses Gold-layer analytical data to present processed information in an accessible format.
9. Data Processing and Validation
Data quality is an important part of any data engineering pipeline.
EnerLytics performs validation checks to ensure that the three datasets can be integrated reliably.
Initial data preparation results:
Validation Metric	Result
Processed daily records	3,900
Integrated dataset columns	112
Date mismatches across source datasets	0
Empty state columns identified and removed	2
The initial validation confirmed consistent dates across the three source datasets.
10. Technology Stack
Technology	Technical Role
Python	Programming language for data processing
Apache Spark	Distributed processing framework
PySpark	Python interface for Spark
Databricks	Cloud-based data engineering platform
Delta Lake	Transactional data storage layer
SQL	Data querying and analytical processing
Microsoft Power BI	Business intelligence and visualisation
GitHub	Source code management and project documentation


11. Conclusion
EnerLytics demonstrates the application of Big Data engineering concepts to the energy sector.
The project combines data extraction, distributed processing, transactional storage, analytical querying and business intelligence into a single workflow.
By applying Apache Spark, PySpark, Databricks and Delta Lake, the project demonstrates how raw datasets can be transformed into structured and reliable analytical information.
The final Power BI dashboard provides an interactive interface for exploring electricity supply, peak demand and unmet demand patterns across Indian states.


