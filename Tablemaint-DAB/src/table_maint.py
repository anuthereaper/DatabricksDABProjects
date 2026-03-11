# Databricks notebook source
# MAGIC %pip install -U https://github.com/alexott/cyber-spark-data-connectors/releases/download/v0.0.4/cyber_spark_data_connectors-0.0.4-py3-none-any.whl

# COMMAND ----------

from pyspark.sql import functions as F, types as T
# import uuid
import datetime
from collections import defaultdict
# import traceback

from common.utils import now_utc, quote_part, parse_full_name, fq_table_name, is_due, log_event, show_tables_in_schema, show_schemas_in_catalog, table_exists, run_sql_with_retry
# COMMAND ----------

# ------------ Widgets ------------
dbutils.widgets.text("ctrl_table_full_name", "dev.maint.table_maint_ctrl", "Control table (full name or in current schema)")
dbutils.widgets.text("log_table_full_name", "dev.maint.table_maint_logs", "Log table (full name or in current schema)")
dbutils.widgets.dropdown("ops_to_run", "OPTIMIZE,VACUUM,ANALYZE",
                         ["OPTIMIZE", "VACUUM", "ANALYZE", "OPTIMIZE,VACUUM", "OPTIMIZE,ANALYZE", "VACUUM,ANALYZE", "OPTIMIZE,VACUUM,ANALYZE"],
                         "Operations to run")
dbutils.widgets.dropdown("dry_run", "Y", ["Y", "N"], "Dry run (no execution)")
dbutils.widgets.text("weekly_run_day", "MONDAY", "Weekly run day (MONDAY..SUNDAY)")
dbutils.widgets.text("monthly_run_dom", "1", "Monthly run day-of-month (1..31)")
dbutils.widgets.text("default_vacuum_hours", "168", "Default VACUUM retention hours if null (min 168 hrs)")
dbutils.widgets.dropdown("allow_low_retention", "false", ["true", "false"], "Allow VACUUM < 168 hours (dangerous)")
dbutils.widgets.text("max_retries", "0", "Retries per operation on failure (integer)")

dbutils.widgets.text("job_id", "")
dbutils.widgets.text("job_name", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_name", "")
dbutils.widgets.text("task_run_id", "")

# ------------ Resolve widget values ------------
CTRL_TBL = dbutils.widgets.get("ctrl_table_full_name").strip().upper()
LOG_TBL = dbutils.widgets.get("log_table_full_name").strip().upper()
OPS_TO_RUN = [x.strip().upper() for x in dbutils.widgets.get("ops_to_run").split(",") if x.strip()]
DRY_RUN = dbutils.widgets.get("dry_run") == "true"
WEEKLY_RUN_DAY = dbutils.widgets.get("weekly_run_day").strip().upper()
MONTHLY_RUN_DOM = int(dbutils.widgets.get("monthly_run_dom"))
DEFAULT_VAC_HOURS = int(dbutils.widgets.get("default_vacuum_hours"))
MAX_RETRIES = int(dbutils.widgets.get("max_retries"))
job_id = dbutils.widgets.get("job_id")
job_name = dbutils.widgets.get("job_name")
job_run_id = dbutils.widgets.get("job_run_id")
task_name = dbutils.widgets.get("task_name")
task_run_id = dbutils.widgets.get("task_run_id")
run_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.datetime.utcnow()

# print(f"Run ID: {RUN_ID}")
print(f"Ops: {OPS_TO_RUN}, Dry-run={DRY_RUN}, WeeklyDay={WEEKLY_RUN_DAY}, MonthlyDOM={MONTHLY_RUN_DOM}, MaxRetries={MAX_RETRIES}")

# Low-retention safety
# if not ALLOW_LOW_RETENTION:
#     spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
# else:
#     spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")


# COMMAND ----------


# ----------------- Load and expand control entries -----------------

ctrl_df = spark.table(CTRL_TBL).where("tmc_active_flag = 'Y'")

# Parse tmc_name and explode rules
parsed = (ctrl_df
          .withColumn("tmc_name", F.trim(F.col("tmc_name")))
          .withColumn("tmc_freq", F.upper(F.col("tmc_freq")))
          .withColumn("tmc_inc_excl_flag", F.upper(F.col("tmc_inc_excl_flag")))
          .withColumn("object_type", F.when(F.size(F.split("tmc_name", "\\.")) == 1, F.lit("CATALOG"))
                                      .when(F.size(F.split("tmc_name", "\\.")) == 2, F.lit("SCHEMA"))
                                      .when(F.size(F.split("tmc_name", "\\.")) == 3, F.lit("TABLE"))
                                      .otherwise(F.lit("INVALID"))))

invalid = parsed.filter("object_type = 'INVALID'")
if invalid.count() > 0:
    raise ValueError("Found invalid tmc_name entries (must be catalog | catalog.schema | catalog.schema.table). Fix and rerun.")

rows = parsed.collect()

# Expand to actual tables
expanded = []  # list of dict: {tmc_id,catalog,schema,table,object_type,freq,inc_excl,zorder,retention,desc}

for r in rows:
    tmc_id = r["tmc_id"]
    name = r["tmc_name"]
    inc_excl = r["tmc_inc_excl_flag"]
    freq = r["tmc_freq"]
    zcols = r["tmc_opt_zorder_cols"]
    retention = r["tmc_vac_retention_hours"] if r["tmc_vac_retention_hours"] is not None else DEFAULT_VAC_HOURS
    desc = r["tmc_description"]
    obj_type, catalog, schema, table = parse_full_name(name)

    if obj_type == "TABLE":
        expanded.append(dict(tmc_id=tmc_id, catalog=catalog, schema=schema, table=table,
                             object_type=obj_type, freq=freq, inc_excl=inc_excl,
                             zorder=zcols, retention=retention, description=desc, details="direct table"))
    elif obj_type == "SCHEMA":
        # Show all tables in schema
        try:
            df = show_tables_in_schema(spark, catalog, schema)
            for row in df.collect():
                tname = row["tableName"]
                expanded.append(dict(tmc_id=tmc_id, catalog=catalog, schema=schema, table=tname,
                                     object_type="TABLE", freq=freq, inc_excl=inc_excl,
                                     zorder=zcols, retention=retention, description=desc, details=f"expanded from schema {catalog}.{schema}"))
        except Exception as e:
            # log and continue
            log_event(spark, "PLAN", f"{catalog}.{schema}", "FAILED", now_utc(), now_utc(), f"Schema expansion failed: {e}. SHOW TABLES failed", LOG_TBL, job_id, job_name, job_run_id, task_run_id, task_name, run_by, 0, 0)
    elif obj_type == "CATALOG":
        # List schemas, then list tables
        try:
            schemas = show_schemas_in_catalog(spark, catalog).collect()
            for sc in schemas:
                sc_name = sc["schema_name"]
                try:
                    df = show_tables_in_schema(spark, catalog, sc_name)
                    for row in df.collect():
                        tname = row["tableName"]
                        expanded.append(dict(tmc_id=tmc_id, catalog=catalog, schema=sc_name, table=tname,
                                        object_type="TABLE", freq=freq, inc_excl=inc_excl,
                                        zorder=zcols, retention=retention, description=desc, details=f"expanded from catalog {catalog}"))
                except Exception as e:
                    log_event(spark, "PLAN", f"{catalog}.{sc_name}", "FAILED", now_utc(), now_utc(), f"Schema expansion failed: {e}. SHOW TABLES failed", LOG_TBL, job_id, job_name, job_run_id, task_run_id, task_name, run_by, 0, 0)
        except Exception as e:
            log_event(spark, "PLAN", f"{catalog}", "FAILED", now_utc(), now_utc(), f"Catalog expansion failed: {e}. SHOW SCHEMAS failed", LOG_TBL, job_id, job_name, job_run_id, task_run_id, task_name, run_by, 0, 0)

# Resolve include/exclude where exclude wins; prefer table specificity
# Build maps: table_key -> most-specific decision (E over I)

# Group all rules by table fully-qualified key
rules_by_table = defaultdict(list)
for it in expanded:
    key = f"{it['catalog']}.{it['schema']}.{it['table']}".lower()
    rules_by_table[key].append(it)

final_targets = []
today = now_utc()

for key, items in rules_by_table.items():
    # Prefer exclusion if any E; collect merged properties (freq/zorder/retention)
    # If multiple freq values, we keep the most frequent run (DAILY > WEEKLY > MONTHLY > MANUAL)
    order = {"DAILY": 3, "WEEKLY": 2, "MONTHLY": 1, "MANUAL": 0}
    inc = any(i["inc_excl"] == "I" for i in items)
    exc = any(i["inc_excl"] == "E" for i in items)
    if exc:
        # log_event(spark, "PLAN", f"{items[key]['catalog']}.items[key]['schema'].items[key]['table']", "SKIPPED", now_utc(), now_utc(), "Table has been excluded", LOG_TBL, job_id, job_name, job_run_id, task_run_id, task_name, run_by, 0, 0)
        continue  # exclude wins

    if not inc:
        continue  # no include -> not selected

    # Merge: pick the "highest" frequency present
    chosen = max(items, key=lambda x: order.get(x["freq"], -1))
    final_targets.append(chosen)

print(f"Expanded rules -> candidate tables: {len(final_targets)}")

# Compute due today
due_targets = []
for t in final_targets:
    if is_due(t["freq"], today, WEEKLY_RUN_DAY, MONTHLY_RUN_DOM):
        due_targets.append(t)

print(f"Tables due today: {len(due_targets)}")


# COMMAND ----------

# ----------------- Operations -----------------

def run_optimize(fq_name: str, zorder_cols):
    if zorder_cols and len(zorder_cols) > 0:
        cols = ", ".join([quote_part(c) for c in zorder_cols])
        sql = f"OPTIMIZE {fq_name} ZORDER BY ({cols})"
    else:
        sql = f"OPTIMIZE {fq_name}"
    return sql


def run_vacuum(fq_name: str, retention_hours: int):
    # Safety: disallow low retention unless explicitly allowed
    if (retention_hours or 0) < 168:  # and not ALLOW_LOW_RETENTION:
        return None, f"Retention {retention_hours} < 168 hours not allowed (set allow_low_retention=true to override)"
    rh = retention_hours if retention_hours is not None else DEFAULT_VAC_HOURS
    sql = f"VACUUM {fq_name} RETAIN {int(rh)} HOURS"
    return sql, None


def run_analyze(fq_name: str):
    # Table-level stats (you can extend to column stats if needed)
    sql = f"ANALYZE TABLE {fq_name} COMPUTE STATISTICS"
    return sql


# Execute for each table due
for t in due_targets:
    catalog, schema, table = t["catalog"], t["schema"], t["table"]
    fq = fq_table_name(catalog, schema, table)
    freq = t["freq"]
    zcols = t["zorder"]
    retention = t["retention"]

    # Verify table exists (avoid failures if dropped after planning)
    if not table_exists(spark, catalog, schema, table):
        log_event(spark, "PLAN", f"{catalog}.{schema}.{table}", "SKIPPED", now_utc(), now_utc(), "Table no longer exists, Existence check failed", LOG_TBL, job_id, job_name, job_run_id, task_run_id, task_name, run_by, 0, 0)
        continue

    # OPTIMIZE
    if "OPTIMIZE" in OPS_TO_RUN:
        op = "OPTIMIZE"
        start = now_utc()
        sql_text = run_optimize(fq, zcols)
        status = "SUCCESS"
        err = None
        if DRY_RUN:
            status = "SKIPPED"
            err = "Dry run: command not executed"
        else:
            ok, err = run_sql_with_retry(spark, sql_text, MAX_RETRIES, DRY_RUN)
            status = "SUCCESS" if ok else "FAILED"
        log_event(spark, op, f"{catalog}.{schema}.{table}", status, start, now_utc(), "Table no longer exists, Existence check failed", LOG_TBL, job_id, job_name, job_run_id, task_run_id, task_name, run_by, 0, 0)

    # VACUUM
    if "VACUUM" in OPS_TO_RUN:
        op = "VACUUM"
        start = now_utc()
        sql_text, pre_err = run_vacuum(fq, retention)
        if pre_err:
            # Log skip due to safety
            log_event(spark, op, f"{catalog}.{schema}.{table}", "SKIPPED", start, now_utc(), f"{pre_err}. Safety check", LOG_TBL, job_id, job_name, job_run_id, task_run_id, task_name, run_by, 0, 0)
        else:
            status = "SUCCESS"
            err = None
            if DRY_RUN:
                status = "SKIPPED"
                err = "Dry run: command not executed"
            else:
                ok, err = run_sql_with_retry(spark, sql_text, MAX_RETRIES, DRY_RUN)
                status = "SUCCESS" if ok else "FAILED"
            log_event(spark, op, f"{catalog}.{schema}.{table}", status, start, now_utc(), f"{err}.{sql_text}", LOG_TBL, job_id, job_name, job_run_id, task_run_id, task_name, run_by, 0, 0)

    # ANALYZE
    if "ANALYZE" in OPS_TO_RUN:
        op = "ANALYZE"
        start = now_utc()
        sql_text = run_analyze(fq)
        status = "SUCCESS"
        err = None
        if DRY_RUN:
            status = "SKIPPED"
            err_msg = f"Dry run: command not executed.{sql_text}"
        else:
            ok, err = run_sql_with_retry(spark, sql_text, MAX_RETRIES, DRY_RUN)
            if ok:
                status = "SUCCESS"
                err_msg = ""
            else:
                status = "FAILED"
                err_msg = f"{err}.{sql_text}"
        log_event(spark, op, f"{catalog}.{schema}.{table}", status, start, now_utc(), err_msg, LOG_TBL, job_id, job_name, job_run_id, task_run_id, task_name, run_by, 0, 0)

# COMMAND ----------

summary_df = spark.sql(f"SELECT * FROM {LOG_TBL} WHERE tml_job_run_id = '{job_run_id}' AND  tml_task_run_id = '{task_run_id};'")
display(summary_df.orderBy(F.col("tml_timestamp")))

# COMMAND ----------
