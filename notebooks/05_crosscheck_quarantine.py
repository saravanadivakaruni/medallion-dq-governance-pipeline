# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Preflight, cross-check of the data quality engines, and quarantine
# MAGIC 1. Preflight: confirm that steps 1 to 8 left everything this notebook needs.
# MAGIC 2. Compare failure counts: Great Expectations vs Soda vs plain PySpark.
# MAGIC 3. Stop if the engines disagree on a critical rule.
# MAGIC 4. Move rows that fail a critical rule to `quarantine_retail` (with reasons); the rest go to `silver_retail`.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SCHEMA = "workspace.dq_pipeline"
C = F.col
STOCK_PATTERN = r"^[0-9]{5}[A-Z]{0,2}$"
DATE_MIN = "2010-12-01 00:00:00"
DATE_MAX = "2011-12-31 23:59:59"

# COMMAND ----------
# PREFLIGHT: stop early, with a clear message, if an earlier step is missing something
hard_problems = []

def report(label, ok, detail="", hard=True):
    status = "PASS" if ok else ("FAIL" if hard else "WARN")
    print(f"{status}  {label}" + ("" if ok or not detail else f"  -> {detail}"))
    if not ok and hard:
        hard_problems.append(label)

for table in ["silver_staged", "dq_rules", "dq_check_results", "ref_country", "pipeline_run_log"]:
    report(f"table {table} exists", spark.catalog.tableExists(f"{SCHEMA}.{table}"))
if hard_problems:
    raise Exception(f"Missing tables, go back to the earlier steps: {hard_problems}")

staged = spark.table(f"{SCHEMA}.silver_staged")
run_id = staged.select("_run_id").first()[0]
staged_rows = staged.count()
log = spark.table(f"{SCHEMA}.pipeline_run_log").filter(C("run_id") == run_id).first()
report(f"silver_staged rows ({staged_rows:,}) equal rows_after_dedupe in pipeline_run_log",
       log is not None and staged_rows == log.rows_after_dedupe,
       f"log says {None if log is None else log.rows_after_dedupe}")

per_engine = {r["engine"]: r["n"] for r in (spark.table(f"{SCHEMA}.dq_check_results")
              .filter(C("run_id") == run_id).groupBy("engine").agg(F.count("*").alias("n")).collect())}
report(f"Great Expectations results for run {run_id}: {per_engine.get('great_expectations', 0)} rules (need 10)",
       per_engine.get("great_expectations", 0) == 10)
report(f"Soda results for run {run_id}: {per_engine.get('soda', 0)} rules (need 10)",
       per_engine.get("soda", 0) == 10, "run notebook 04 (Soda) before continuing", hard=False)
if hard_problems:
    raise Exception(f"Preflight failed: {hard_problems}")

valid_countries = [r[0] for r in spark.table(f"{SCHEMA}.ref_country").collect()]
rules = {r.check_name: r for r in spark.table(f"{SCHEMA}.dq_rules").collect()}
print("\nPreflight finished. run_id:", run_id)

# COMMAND ----------
# The same 10 rules written a third time, in plain PySpark. Each condition is TRUE for a row that BREAKS the rule.
CONDITIONS = {
    "values_parseable":      ~C("_parse_ok"),
    "invoice_no_not_null":   C("InvoiceNo").isNull(),
    "unit_price_positive":   C("UnitPrice").isNotNull() & (C("UnitPrice") <= 0),
    "invoice_date_in_range": C("InvoiceDate").isNotNull() & ~C("InvoiceDate").between(
                                 F.lit(DATE_MIN).cast("timestamp"), F.lit(DATE_MAX).cast("timestamp")),
    "quantity_sign_matches_invoice_type": ~C("_qty_sign_ok"),
    "country_in_reference":  C("Country").isNotNull() & ~C("Country").isin(valid_countries),
    "customer_id_not_null":  C("CustomerID").isNull(),
    "description_not_null":  C("Description").isNull(),
    "stock_code_pattern":    C("StockCode").isNotNull() & ~C("StockCode").rlike(STOCK_PATTERN),
}

# Count the failing rows of every rule in one pass over the table
row_counts = staged.agg(*[F.sum(F.when(cond, 1).otherwise(0)).alias(name)
                          for name, cond in CONDITIONS.items()]).first().asDict()
spark_counts = {name: int(n) for name, n in row_counts.items()}

# The uniqueness rule needs a window: count the rows that share (InvoiceNo, StockCode) with another row
key_window = Window.partitionBy("InvoiceNo", "StockCode")
spark_counts["invoice_stock_key_unique"] = (staged
    .withColumn("_key_rows", F.count(F.lit(1)).over(key_window))
    .filter(C("_key_rows") > 1).count())

assert set(spark_counts) == set(rules), "dq_rules and the PySpark conditions have different rule names"
print("PySpark failing-row counts:")
for name, n in spark_counts.items():
    print(f"  {name:38s} {n:>8,}")

# COMMAND ----------
# Compare the three engines for every rule
wide = (spark.table(f"{SCHEMA}.dq_check_results").filter(C("run_id") == run_id)
        .groupBy("check_name").pivot("engine", ["great_expectations", "soda"])
        .agg(F.max("records_failed")).collect())
gx_soda = {r["check_name"]: (r["great_expectations"], r["soda"]) for r in wide}

comparison, critical_mismatch, warning_mismatch = [], [], []
for name, rule in rules.items():
    gx_n, soda_n = gx_soda.get(name, (None, None))
    sp_n = spark_counts[name]
    known = [v for v in (gx_n, soda_n, sp_n) if v is not None]
    agree = len(set(known)) == 1
    comparison.append((run_id, name, rule.severity, gx_n, soda_n, sp_n, agree))
    if not agree:
        (critical_mismatch if rule.severity == "critical" else warning_mismatch).append((name, gx_n, soda_n, sp_n))

COMPARISON_TABLE = f"{SCHEMA}.dq_engine_comparison"
if spark.catalog.tableExists(COMPARISON_TABLE):
    spark.sql(f"DELETE FROM {COMPARISON_TABLE} WHERE run_id = '{run_id}'")
(spark.createDataFrame(comparison,
        "run_id string, check_name string, severity string, gx_failed long, soda_failed long, "
        "spark_failed long, all_agree boolean")
    .withColumn("compared_ts", F.current_timestamp())
    .write.format("delta").mode("append").saveAsTable(COMPARISON_TABLE))

display(spark.table(COMPARISON_TABLE).filter(C("run_id") == run_id).orderBy("severity", "check_name"))

if per_engine.get("soda", 0) == 0:
    print("NOTE: no Soda results for this run, so only Great Expectations and PySpark were compared.")
for item in warning_mismatch:
    print("NOTE (warning-level rule, engines differ): name, gx, soda, spark =", item)
if critical_mismatch:
    raise Exception(f"Engines disagree on CRITICAL rules (name, gx, soda, spark): {critical_mismatch}")
print("All engines agree on every critical rule." if not warning_mismatch else
      "All engines agree on every critical rule (see notes above for warning rules).")

# COMMAND ----------
# Quarantine: rows that break any CRITICAL rule are set aside with the reasons written next to them
critical = []
for name, rule in rules.items():
    if rule.severity == "critical":
        if name not in CONDITIONS:
            raise Exception(f"{name} cannot be critical in this notebook (it needs a window calculation)")
        critical.append((name, CONDITIONS[name]))

flagged = staged.withColumn(
    "fail_reasons", F.concat_ws(",", *[F.when(cond, F.lit(name)) for name, cond in critical]))

helper_cols = ["_parse_ok", "_qty_sign_ok"]
quarantine = (flagged.filter(C("fail_reasons") != "")
              .drop(*helper_cols)
              .withColumn("_quarantined_ts", F.current_timestamp()))
clean = flagged.filter(C("fail_reasons") == "").drop("fail_reasons", *helper_cols)

(quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA}.quarantine_retail"))
(clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA}.silver_retail"))

n_quarantine = spark.table(f"{SCHEMA}.quarantine_retail").count()
n_clean = spark.table(f"{SCHEMA}.silver_retail").count()
assert n_quarantine + n_clean == staged_rows, "rows were lost or duplicated between staged, quarantine and clean"
print(f"staged={staged_rows:,} | quarantined={n_quarantine:,} | clean={n_clean:,}")

# A row can break several rules, so count each reason separately and compare with the rule counts
reasons = (spark.table(f"{SCHEMA}.quarantine_retail")
           .select(F.explode(F.split("fail_reasons", ",")).alias("reason"))
           .groupBy("reason").count().orderBy("reason"))
display(reasons)
reason_counts = {r["reason"]: r["count"] for r in reasons.collect()}
for name, _ in critical:
    assert reason_counts.get(name, 0) == spark_counts[name], f"reason count for {name} does not match the rule count"
print("Quarantine reasons match the critical rule counts.")
