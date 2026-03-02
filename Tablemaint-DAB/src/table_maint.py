# Databricks notebook source
# MAGIC %pip install -U https://github.com/alexott/cyber-spark-data-connectors/releases/download/v0.0.4/cyber_spark_data_connectors-0.0.4-py3-none-any.whl

# COMMAND ----------

from common.utils import get_included_tables

# COMMAND ----------

dbutils.widgets.text("exec_mode", "", "Execution mode")

# COMMAND ----------
# Execution mode has the following values :
# I : Only process the included(tmic_inc_excl_flag = 'I') tables
# E : Process all tables but exclude the excluded(tmic_inc_excl_flag = 'E') tables
# B : Include the included(tmic_inc_excl_flag = 'I') tables but exclude the excluded(tmic_inc_excl_flag = 'E') tables. Exclude gets precedence
exec_mode = dbutils.widgets.get("exec_mode").upper()

# Validate exec_mode
if exec_mode not in ["I", "E", "B"]:
    raise ValueError(f"Invalid exec_mode: {exec_mode}. Allowed values are 'I', 'E', or 'B'.")

# COMMAND ----------

table_list = spark.sql("SELECT * FROM system.information_schema.tables WHERE data_source_format = 'DELTA';")

# COMMAND ----------
get_included_tables()
