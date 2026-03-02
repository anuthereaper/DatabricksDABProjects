# Databricks notebook source
# COMMAND ----------
from common.utils import (
    read_etl_config,
    format_sql_stmt,
    validate_variant_data,
    func_load_sql_stmts,
    func_override_parms,
    func_update_status_loading_log,
    func_execute_notebook,
    get_metrics_data
)

import json
from datetime import datetime
# import time
# import re
from pyspark.sql import Row
from pyspark.sql.types import StringType, LongType, StructField, StructType, DoubleType
from pyspark.sql.functions import to_date, col

# COMMAND ----------

dbutils.widgets.text("etl_date", "YYYY-MM-DD")
dbutils.widgets.text("interface", "AUTOLOAD1")
dbutils.widgets.text("target_layer", "BRONZE")
dbutils.widgets.text("config_table", "dev.etl_meta.etl_meta_config")
dbutils.widgets.text("parms_ovrd", "")
dbutils.widgets.text("jobid", "")
dbutils.widgets.text("jobname", "")
dbutils.widgets.text("jobrunid", "")
dbutils.widgets.text("taskname", "")
dbutils.widgets.text("taskrunid", "")
dbutils.widgets.text("sleeptime", "60")
dbutils.widgets.text("log_tbl_load_status", "Y")
dbutils.widgets.text("notebook_timeout", "60")
dbutils.widgets.text("logging_level", "INFO")

# COMMAND ----------

etl_date = dbutils.widgets.get("etl_date")
interface = dbutils.widgets.get("interface")
target_layer = dbutils.widgets.get("target_layer")
config_table = dbutils.widgets.get("config_table")
parms_ovrd = dbutils.widgets.get("parms_ovrd")
jobid = dbutils.widgets.get("jobid")
jobname = dbutils.widgets.get("jobname")
jobrunid = dbutils.widgets.get("jobrunid")
taskname = dbutils.widgets.get("taskname")
taskrunid = dbutils.widgets.get("taskrunid")
log_tbl_load_status = dbutils.widgets.get("log_tbl_load_status")
logging_level = dbutils.widgets.get("logging_level")
notebook_timeout = int(dbutils.widgets.get("notebook_timeout"))

table_dtls = config_table.split('.')
etl_config_schema = table_dtls[0] + "." + table_dtls[1]

# COMMAND ----------


class bcolors:
    ENDC = '\033[0m'
    orange = '\033[33m'
    purple = '\033[35m'
    OKBLUE = '\0033[94m]'
    CRITICALRED = '\0033[91m]'


# Get the user id who is running this notebook
userid = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# Get the notebook name for logging purposes
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]

# spark.udf.register("fuzzrapid", fuzzyrapid, DoubleType())

# Populate the common variable dictionary for later use
common_var_dict = {}
common_var_dict["userid"] = userid
common_var_dict["notebook_name"] = notebook_name
common_var_dict["jobname"] = jobname
common_var_dict["jobrunid"] = jobrunid
common_var_dict["jobid"] = jobid
common_var_dict["taskname"] = taskname
common_var_dict["taskrunid"] = taskrunid
common_var_dict["etl_date"] = etl_date
common_var_dict["Interface"] = interface

# Check existence of the config table.
if not spark.catalog.tableExists(f"{config_table}"):
    raise ValueError(f"Config table {config_table} not present. Please create")

if logging_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
    raise ValueError(f"Incorrect logging level error : {logging_level}")

# COMMAND ----------

# FETCH THE NECESSARY ROWS FROM CONFIG TABLE
try:
    print(f"{bcolors.OKBLUE}{datetime.now()}: Parameters passed --> interface :{interface}: target_layer: {target_layer}, etl_date: {etl_date}; config_table: {config_table}; parm_ovrd:{parms_ovrd}")
    tran_pandas_df = read_etl_config(interface.upper(), target_layer.upper(), spark, config_table)
    config_row_count = tran_pandas_df.shape[0]
    print(f"{bcolors.OKBLUE}{datetime.now()}: Config retrieved for Interface id {interface} and row_count = {config_row_count}")
except Exception as e:
    raise Exception(f"Error while retrieving config data : {e}")

# COMMAND ----------

# MAIN TRANSFORMATION LOGIC LOOP
sql_stmt_dict = {}
for index, row in tran_pandas_df.iterrows():
    emc_func_type = row['emc_func_type'].upper()
    emc_func_dtls = row['emc_func_dtls']
    emc_target = row['emc_target']
    emc_out_mode = row['emc_out_mode']
    emc_seq = row['emc_seq']

    results = []
    result = {}

    try:
        print(f"{bcolors.OKBLUE}{datetime.now()}: Executing transformation {interface}, seq - {emc_seq} with target : {emc_target} and mode {emc_func_type}")

        func_dtls_dict = json.loads(str(emc_func_dtls))
        validate_variant_data(emc_func_type, func_dtls_dict)

        match emc_func_type.upper():
            case "SQL":
                # Section for SQL option. To execute a specific SQL statement governed by the sql_var within the dtls column in the metadata config.
                sql_stmt = ""
                sql_stmt = format_sql_stmt(sql_stmt_dict[func_dtls_dict["sql_var"]], common_var_dict, func_dtls_dict["sql_parms"])
                print(f"{bcolors.OKBLUE}{datetime.now()}: SQL statement formatted to : \"{sql_stmt}\"")

                if emc_target != "":
                    version_df = spark.sql(f"SELECT version from (DESCRIBE HISTORY {emc_target}) LIMIT 1")
                    prev_version = version_df.collect()[0][0]

                spark.sql(sql_stmt)
                if emc_target != "":
                    version_df = spark.sql(f"SELECT version from (DESCRIBE HISTORY {emc_target}) LIMIT 1")
                    new_version = version_df.collect()[0][0]

                    rows_inserted, rows_updated, rows_deleted = get_metrics_data(spark, emc_target, new_version)

                    func_update_status_loading_log(common_var_dict, emc_target, rows_inserted, rows_updated, rows_deleted, spark, prev_version, new_version, "SUCCESS", datetime.now().isoformat(), etl_config_schema)
                print(f"{bcolors.OKBLUE}{datetime.now()}: SQL statement executed successfully : \"{sql_stmt}\"")

            case "NOTEBOOK":
                # Section for NOTEBOOK option. To execute a notebook. If the notebook is loading a table, it is recommended to pass the rows _inserted, prev
                sql_notebook_parms = func_override_parms(parms_ovrd, func_dtls_dict["parms"])
                notebook_path = func_dtls_dict["notebook_path"]
                if not func_dtls_dict["timeout"]:
                    timeout = notebook_timeout
                else:
                    timeout = func_dtls_dict["timeout"]
                if emc_target != "":
                    version_df = spark.sql(f"SELECT version from (DESCRIBE HISTORY {emc_target}) LIMIT 1")
                    prev_version = version_df.collect()[0][0]

                func_execute_notebook(sql_notebook_parms, notebook_path, spark, timeout, common_var_dict)
                print(f"Target is {emc_target}")
                if emc_target != "":
                    version_df = spark.sql(f"SELECT version from (DESCRIBE HISTORY {emc_target}) LIMIT 1")
                    new_version = version_df.collect()[0][0]

                    rows_inserted, rows_updated, rows_deleted = get_metrics_data(spark, emc_target, new_version)

                    func_update_status_loading_log(common_var_dict, emc_target, rows_inserted, rows_updated, rows_deleted, spark, prev_version, new_version, "SUCCESS", datetime.now().isoformat(), etl_config_schema)
                    print("loaded status table")

                print(f"{bcolors.OKBLUE}{datetime.now()}: External notebook {notebook_path} executed successfully")

            case "SQLLOAD":
                sql_stmt_dict = func_load_sql_stmts(func_dtls_dict["sql_path"])
                print(f"{bcolors.OKBLUE}{datetime.now()}: SQL file {func_dtls_dict['sql_path']} loaded successfully")

            case _:
                raise Exception("Out type : {out_type} is incorrect. Please check ")

    except Exception as e:

        print(f"{bcolors.CRITICALRED}{datetime.now()}: CRITICAL ERROR. PLEASE CHECK TASK OUTPUT. interface {interface} seq {emc_seq} mode : {emc_func_type}")
        if emc_target != "":
            new_version = 0
            prev_version = 0
            status = "FAILED"
            func_update_status_loading_log(common_var_dict, emc_target, 0, 0, 0, spark, prev_version, new_version, status, datetime.now().isoformat(), etl_config_schema)

        raise Exception(f"CRITICAL ERROR Z001. Please check {e}")
    print(f"{bcolors.OKBLUE}{datetime.now()}: Completed execution of interface {interface} and sql: {emc_seq}")

print(f"{bcolors.OKBLUE}{datetime.now()}: Completed execution of interface {interface} and layer {target_layer}")
