# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: raw ingestion
# MAGIC Bronze stores the data exactly as received (every column as text). We only add audit columns.

# COMMAND ----------
from pyspark.sql import functions as F

SCHEMA = "workspace.dq_pipeline"
VOLUME = "/Volumes/workspace/dq_pipeline/raw_files"

# COMMAND ----------
# One id for this whole pipeline run, such as 20261127101530 (year, month, day, hour, minute, second)
run_id = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMddHHmmss')").first()[0]

# Read the CSV with every column as text (inferSchema = False)
raw = (spark.read
       .option("header", True)
       .option("inferSchema", False)
       .csv(f"{VOLUME}/online_retail.csv"))

# Add audit columns (they start with an underscore so they are easy to tell apart)
bronze = (raw
    .withColumn("_bronze_row_id", F.monotonically_increasing_id())
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.lit("online_retail.csv"))
    .withColumn("_run_id", F.lit(run_id)))

# Save as a Delta table inside Unity Catalog
(bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA}.bronze_retail"))

# Save the small reference list as a table too
(spark.read.option("header", True).csv(f"{VOLUME}/ref_country.csv")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA}.ref_country"))

print("run_id:", run_id)
