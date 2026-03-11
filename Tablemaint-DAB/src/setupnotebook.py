# Databricks notebook source
dbutils.widgets.text("catalog_name", "", "Catalog name")
dbutils.widgets.text("schema_name", "", "Schema name")

# COMMAND ----------
# MAGIC %md
# MAGIC
# MAGIC Create the catalog and schema if not already present

# COMMAND ----------
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

query = f"""
CREATE CATALOG IF NOT EXISTS {catalog_name};
"""

spark.sql(query)

query = f"""
CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};
"""

spark.sql(query)

# COMMAND ----------
query = f"""
USE CATALOG {catalog_name};
"""

spark.sql(query)

query = f"""
USE SCHEMA {schema_name};
"""

spark.sql(query)

# COMMAND ----------
# MAGIC %md
# MAGIC
# MAGIC Create or replace the table maintenance config table

# COMMAND ----------
query = """
CREATE OR REPLACE TABLE table_maint_ctrl
  (tmc_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY COMMENT "Table id primary key to identify a single row",
  tmc_group_id INT COMMENT "Group id. Records with the same id can be fetched to be maintenance for a specific lakeflow run. Frequence is hence governed by the frequency of the lakeflow",
  tmc_inc_excl_flag STRING NOT NULL CHECK (tmc_inc_excl_flag IN ('I', 'E')) COMMENT "I = include for maintenance, E = exclude from maintenance",
  tmc_freq STRING NOT NULL CHECK (tmc_freq IN ('DAILY', 'WEEKLY', 'MONTHLY')) COMMENT "Frequency of the maintenance",
  tmc_name STRING NOT NULL COMMENT "Fully qualified name: catalog.schema.table; or catalog.schema; or catalog",
  tmc_opt_zorder_cols ARRAY<STRING> COMMENT "Columns to ZORDER by during OPTIMIZE",
  tmc_vac_retention_hours INT COMMENT "Retention hours for VACUUM",
  tmc_description STRING COMMENT "Free text description of this maintenance configuration",
  tmc_active_flag STRING CHECK (tmc_active_flag IN ('Y', 'N')) COMMENT "Y= active configuration, N = inactive"
  )
  COMMENT "Configuration for table maintenance with include and/or exclude flag across catalog, schema, or table"
"""

spark.sql(query)

# COMMAND ----------
# MAGIC %md
# MAGIC
# MAGIC Create a logging table

# COMMAND ----------
query = """
CREATE OR REPLACE TABLE dev.maint.table_maint_logs
  (tml_uuid STRING COMMENT "primary key to identify a single row",
  tml_timestamp TIMESTAMP COMMENT "Log Timestamp",
  tml_operation STRING COMMENT "OPTIMIZE | VACUUM | ANALYZE ",
  tml_catalog_name STRING COMMENT "Catalog name",
  tml_schema_name STRING COMMENT "Schema name",
  tml_table_name STRING COMMENT "Table name",
  tml_status STRING COMMENT "SUCCESS | FAILED | SKIPPED",
  tml_error_message STRING COMMENT "Error message incase of failure",
  tml_run_by STRING COMMENT "Operation run by",
  tml_start_ts TIMESTAMP COMMENT "Maintenance start Timestamp",
  tml_end_ts TIMESTAMP COMMENT "Maintenance End Timestamp",
  tml_job_id STRING NOT NULL COMMENT "Workflow id",
  tml_job_name STRING NOT NULL COMMENT "Workflow name",
  tml_job_run_id STRING NOT NULL COMMENT "Run id",
  tml_task_run_id STRING NOT NULL COMMENT "Task id",
  tml_task_name STRING NOT NULL COMMENT "Task name",
  tml_file_count_before INT,
  tml_file_count_after INT
  )
  COMMENT "Delta maintenance execution log (all operations and outcomes)"
"""

spark.sql(query)

# COMMAND ----------
