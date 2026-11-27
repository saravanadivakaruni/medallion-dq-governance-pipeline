# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Data quality engine 2: Soda Core
# MAGIC Runs the same rules as SodaCL checks (checks.yml in the volume) on DuckDB, over a copy of `silver_staged`.
# MAGIC Results go to `dq_check_results` (engine = `soda`). No rows are moved or deleted here.

# COMMAND ----------
# MAGIC %pip install soda-core-duckdb==3.5.6

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import duckdb
from soda.scan import Scan
from pyspark.sql import functions as F

SCHEMA = "workspace.dq_pipeline"
VOLUME = "/Volumes/workspace/dq_pipeline/raw_files"

staged = spark.table(f"{SCHEMA}.silver_staged")
run_id = staged.select("_run_id").first()[0]
rules = {r.check_name: r for r in spark.table(f"{SCHEMA}.dq_rules").collect()}

pdf = staged.toPandas()
ref = spark.table(f"{SCHEMA}.ref_country").toPandas()

# DuckDB reads plain text (object) columns reliably, so convert any newer pandas "string" columns
def to_plain_text(frame):
    return frame.astype({c: object for c in frame.columns if str(frame[c].dtype) in ("string", "str")})

pdf, ref = to_plain_text(pdf), to_plain_text(ref)
print("run_id:", run_id, "| rows loaded:", len(pdf), "| reference rows:", len(ref))

# COMMAND ----------
# Give DuckDB the two tables, then let Soda run every check in checks.yml
con = duckdb.connect()
con.register("silver_staged", pdf)
con.register("ref_country", ref)

scan = Scan()
scan.set_scan_definition_name("silver_staged_scan")
scan.set_data_source_name("duckdb")
scan.add_duckdb_connection(duckdb_connection=con, data_source_name="duckdb")
scan.add_sodacl_yaml_file(f"{VOLUME}/checks.yml")
scan.execute()

if scan.has_error_logs():
    raise Exception(scan.get_error_logs_text())

checks = scan.get_scan_results()["checks"]
total_rows = int(next(c for c in checks if c["name"] == "row_count")["diagnostics"]["value"])

results = []
for c in checks:
    name = c["name"]
    if name == "row_count":
        continue
    failed = int(c["diagnostics"]["value"])
    rule = rules[name]
    results.append((run_id, "soda", rule.dimension, name, rule.severity, total_rows, failed, rule.threshold_pct))
    pass_rate = 100 * (total_rows - failed) / total_rows
    status = "PASS" if pass_rate >= rule.threshold_pct else "FAIL"
    print(f"{name:38s} checked={total_rows:>8,} failed={failed:>8,} pass={pass_rate:8.3f}%  [{rule.severity}] {status}")

# COMMAND ----------
# Save the results. If this notebook is re-run for the same pipeline run, replace the old rows.
RESULTS_TABLE = f"{SCHEMA}.dq_check_results"
if spark.catalog.tableExists(RESULTS_TABLE):
    spark.sql(f"DELETE FROM {RESULTS_TABLE} WHERE run_id = '{run_id}' AND engine = 'soda'")

(spark.createDataFrame(
        results,
        "run_id string, engine string, dimension string, check_name string, severity string, "
        "records_checked long, records_failed long, threshold_pct double")
    .withColumn("run_ts", F.current_timestamp())
    .write.format("delta").mode("append").saveAsTable(RESULTS_TABLE))

print("saved", len(results), "results for run", run_id)
