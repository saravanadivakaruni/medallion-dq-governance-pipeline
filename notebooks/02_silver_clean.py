# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver (step 1): remove exact duplicates, standardize, convert types
# MAGIC Output: `silver_staged` (cleaned but not yet validated) and one row in `pipeline_run_log`.

# COMMAND ----------
from pyspark.sql import functions as F

SCHEMA = "workspace.dq_pipeline"

BUSINESS_COLS = ["InvoiceNo", "StockCode", "Description", "Quantity",
                 "InvoiceDate", "UnitPrice", "CustomerID", "Country"]

# COMMAND ----------
bronze = spark.table(f"{SCHEMA}.bronze_retail")
run_id = bronze.select("_run_id").first()[0]
raw_rows = bronze.count()

# Remove rows that are identical in all 8 business columns, keeping one copy of each
deduped = bronze.dropDuplicates(BUSINESS_COLS)
rows_after_dedupe = deduped.count()
exact_duplicates_removed = raw_rows - rows_after_dedupe

print(f"raw rows: {raw_rows:,} | after removing exact duplicates: {rows_after_dedupe:,} | removed: {exact_duplicates_removed:,}")

# COMMAND ----------
def clean_str(col):
    """Collapse repeated spaces, trim the ends, turn empty text into NULL."""
    t = F.trim(F.regexp_replace(F.col(col), r"\s+", " "))
    return F.when(t == "", None).otherwise(t)

# try_cast / try_to_timestamp return NULL for bad values instead of crashing (serverless uses ANSI mode)
typed = (deduped
    .withColumn("_raw_qty",   clean_str("Quantity"))
    .withColumn("_raw_date",  clean_str("InvoiceDate"))
    .withColumn("_raw_price", clean_str("UnitPrice"))
    .select(
        clean_str("InvoiceNo").alias("InvoiceNo"),
        F.upper(clean_str("StockCode")).alias("StockCode"),
        clean_str("Description").alias("Description"),
        F.expr("try_cast(_raw_qty as int)").alias("Quantity"),
        F.expr("try_to_timestamp(_raw_date, 'yyyy-MM-dd HH:mm:ss')").alias("InvoiceDate"),
        F.expr("try_cast(_raw_price as double)").alias("UnitPrice"),
        F.regexp_replace(clean_str("CustomerID"), r"\.0$", "").alias("CustomerID"),
        clean_str("Country").alias("Country"),
        "_raw_qty", "_raw_date", "_raw_price", "_bronze_row_id", "_run_id"))

# Replace known alternative country names with the official name
COUNTRY_MAP = {"EIRE": "Ireland", "USA": "United States", "RSA": "South Africa"}
country_expr = F.col("Country")
for old, new in COUNTRY_MAP.items():
    country_expr = F.when(F.col("Country") == old, F.lit(new)).otherwise(country_expr)
typed = typed.withColumn("Country", country_expr)

# Flag 1: a value was present in the raw data but could not be converted to its type
typed = typed.withColumn("_parse_ok", ~(
      (F.col("_raw_qty").isNotNull()   & F.col("Quantity").isNull())
    | (F.col("_raw_date").isNotNull()  & F.col("InvoiceDate").isNull())
    | (F.col("_raw_price").isNotNull() & F.col("UnitPrice").isNull())))

# Flag 2: sales have Quantity > 0, cancellations (InvoiceNo starts with C) have Quantity < 0
is_cancel = F.col("InvoiceNo").startswith("C")
typed = typed.withColumn("_qty_sign_ok", F.coalesce(
    ((F.col("Quantity") > 0) & ~is_cancel) | ((F.col("Quantity") < 0) & is_cancel), F.lit(True)))

staged = typed.drop("_raw_qty", "_raw_date", "_raw_price")

# COMMAND ----------
(staged.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA}.silver_staged"))

(spark.createDataFrame(
        [(run_id, raw_rows, rows_after_dedupe, exact_duplicates_removed)],
        "run_id string, raw_rows long, rows_after_dedupe long, exact_duplicates_removed long")
    .withColumn("logged_ts", F.current_timestamp())
    .write.format("delta").mode("append").saveAsTable(f"{SCHEMA}.pipeline_run_log"))

print("saved silver_staged and pipeline_run_log for run", run_id)
