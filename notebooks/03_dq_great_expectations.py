# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Data quality engine 1: Great Expectations
# MAGIC Defines the rule registry (`dq_rules`) and measures every rule with Great Expectations.
# MAGIC Results go to `dq_check_results` (engine = `great_expectations`). No rows are moved or deleted here.

# COMMAND ----------
# MAGIC %pip install great_expectations==1.25.0

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import great_expectations as gx
import great_expectations.expectations as gxe
from datetime import datetime
from pyspark.sql import functions as F

SCHEMA = "workspace.dq_pipeline"
STOCK_PATTERN = r"^[0-9]{5}[A-Z]{0,2}$"
DATE_MIN = datetime(2010, 12, 1)
DATE_MAX = datetime(2011, 12, 31, 23, 59, 59)

staged = spark.table(f"{SCHEMA}.silver_staged")
run_id = staged.select("_run_id").first()[0]
valid_countries = [r[0] for r in spark.table(f"{SCHEMA}.ref_country").collect()]

# Great Expectations runs on a pandas table (about 540,000 rows fits in memory)
pdf = staged.toPandas()
print("run_id:", run_id, "| rows loaded:", len(pdf), "| countries in reference list:", len(valid_countries))

# COMMAND ----------
# Rule registry: one place that defines every rule's name, dimension, severity and threshold.
RULES = [
    # (check_name, dimension, severity, threshold_pct, description)
    ("values_parseable", "Validity", "critical", 99.0, "Quantity, InvoiceDate and UnitPrice can be converted to their data types"),
    ("invoice_no_not_null", "Completeness", "critical", 99.0, "InvoiceNo is present"),
    ("unit_price_positive", "Validity", "critical", 99.0, "UnitPrice is greater than 0"),
    ("invoice_date_in_range", "Validity", "critical", 99.0, "InvoiceDate is between 2010-12-01 and 2011-12-31"),
    ("quantity_sign_matches_invoice_type", "Consistency", "critical", 99.0, "Sales have Quantity > 0; cancellations (C invoices) have Quantity < 0"),
    ("country_in_reference", "Reference Data", "critical", 99.0, "Country exists in ref_country"),
    ("customer_id_not_null", "Completeness", "warning", 70.0, "CustomerID is present (guest checkouts are legitimate)"),
    ("description_not_null", "Completeness", "warning", 99.0, "Description is present"),
    ("stock_code_pattern", "Consistency", "warning", 98.0, "StockCode matches the product code pattern"),
    ("invoice_stock_key_unique", "Uniqueness", "warning", 95.0, "(InvoiceNo, StockCode) is unique"),
]

(spark.createDataFrame(
        RULES,
        "check_name string, dimension string, severity string, threshold_pct double, description string")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA}.dq_rules"))

# COMMAND ----------
# One Great Expectations "expectation" per rule
EXPECTATIONS = {
    "values_parseable": gxe.ExpectColumnValuesToBeInSet(column="_parse_ok", value_set=[True]),
    "invoice_no_not_null": gxe.ExpectColumnValuesToNotBeNull(column="InvoiceNo"),
    "unit_price_positive": gxe.ExpectColumnValuesToBeBetween(column="UnitPrice", min_value=0, strict_min=True),
    "invoice_date_in_range": gxe.ExpectColumnValuesToBeBetween(column="InvoiceDate", min_value=DATE_MIN, max_value=DATE_MAX),
    "quantity_sign_matches_invoice_type": gxe.ExpectColumnValuesToBeInSet(column="_qty_sign_ok", value_set=[True]),
    "country_in_reference": gxe.ExpectColumnValuesToBeInSet(column="Country", value_set=valid_countries),
    "customer_id_not_null": gxe.ExpectColumnValuesToNotBeNull(column="CustomerID"),
    "description_not_null": gxe.ExpectColumnValuesToNotBeNull(column="Description"),
    "stock_code_pattern": gxe.ExpectColumnValuesToMatchRegex(column="StockCode", regex=STOCK_PATTERN),
    "invoice_stock_key_unique": gxe.ExpectCompoundColumnsToBeUnique(column_list=["InvoiceNo", "StockCode"]),
}

# COMMAND ----------
# Give Great Expectations the table, then check every rule
context = gx.get_context(mode="ephemeral")
data_source = context.data_sources.add_pandas("retail_silver_staged")
data_asset = data_source.add_dataframe_asset(name="silver_staged")
batch_definition = data_asset.add_batch_definition_whole_dataframe("whole_table")
batch = batch_definition.get_batch(batch_parameters={"dataframe": pdf})

results = []
for check_name, dimension, severity, threshold, _ in RULES:
    outcome = batch.validate(EXPECTATIONS[check_name]).result
    checked = int(outcome["element_count"])
    failed = int(outcome["unexpected_count"])
    results.append((run_id, "great_expectations", dimension, check_name, severity, checked, failed, threshold))
    pass_rate = 100 * (checked - failed) / checked
    status = "PASS" if pass_rate >= threshold else "FAIL"
    print(f"{check_name:38s} checked={checked:>8,} failed={failed:>8,} pass={pass_rate:8.3f}%  [{severity}] {status}")

# COMMAND ----------
# Save the results. If this notebook is re-run for the same pipeline run, replace the old rows.
RESULTS_TABLE = f"{SCHEMA}.dq_check_results"
if spark.catalog.tableExists(RESULTS_TABLE):
    spark.sql(f"DELETE FROM {RESULTS_TABLE} WHERE run_id = '{run_id}' AND engine = 'great_expectations'")

(spark.createDataFrame(
        results,
        "run_id string, engine string, dimension string, check_name string, severity string, "
        "records_checked long, records_failed long, threshold_pct double")
    .withColumn("run_ts", F.current_timestamp())
    .write.format("delta").mode("append").saveAsTable(RESULTS_TABLE))

print("saved", len(results), "results for run", run_id)
