import pytest

try:
    from databricks.connect import DatabricksSession as SparkSession
except ImportError:
    from pyspark.sql import SparkSession as SparkSession

# from pyspark.sql import DataFrame
# from ps_test_blueprint.nyctaxi_functions import *
from src.common.utils import read_etl_config, func_override_parms, format_sql_stmt, func_load_sql_stmts, func_update_status_loading_log, func_execute_notebook, dummy_func
from databricks.sdk.service.jobs import RunResultState


@pytest.fixture
def job_id(ws, request):
    job_name = request.config.getoption("--job-name")
    job_id = next((job.job_id for job in ws.jobs.list() if job.settings.name == job_name), None)

    if job_id is None:
        raise ValueError(f"Job '{job_name}' not found.")

    return job_id


@pytest.fixture(scope="function")
def test_tables(spark, ws, make_schema):
    """
    Creates a temporary schema (via make_schema) and a couple of test tables,
    then drops them after the test.

    Returns a dict with catalog/schema/table names you can use in your test.
    """
    catalog_name = "dev_etl"
    schema_name = make_schema(catalog_name=catalog_name).name # already unique from your make_schema
    print(f"myschema name is  : {schema_name}")
    # If make_schema doesn't switch context, do it explicitly:
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"USE SCHEMA {schema_name}")

    # Create a couple of Delta tables for tests
    test_config_tbl = "etl_meta_config"
    test_load_status_tbl = "etl_table_load_status"

    # Idempotent cleanup before creation (useful when debugging failed runs)
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.{test_config_tbl}")
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.{test_load_status_tbl}")

    job_name = "ETL meta transformation setup job"
    job_id = next((job.job_id for job in ws.jobs.list() if job.settings.name == job_name), None)
    if job_id is None:
        raise ValueError(f"Job '{job_name}' not found.")

    # Create integration test config and log tables
    run_wait = ws.jobs.run_now(job_id=job_id, job_parameters={"catalog_name": catalog_name, "schema_name": schema_name})
    run_result = run_wait.result()
    result_status = run_result.state.result_state
    if result_status != RunResultState.SUCCESS:
        raise ValueError(f"Job '{job_name}' ended with status {result_status}.")
    else:
        print(f"Test config tables have been created successfully.")

    # Seed some small test data (replace with values your notebooks expect)
    #spark.sql(f"""
    #    INSERT INTO {catalog_name}.{schema_name}.{trips_tbl} VALUES
    #        (1001, 1, timestamp('2024-01-01 10:00:00'), timestamp('2024-01-01 10:12:00'), 12.5),
    #        (1002, 2, timestamp('2024-01-01 11:00:00'), timestamp('2024-01-01 11:22:00'), 20.0)
    #""")

    resources = {
        "catalog": catalog_name,
        "schema": schema_name,
        "tables": {
            "config": test_config_tbl,
            "log_status": test_load_status_tbl
        }
    }

    try:
        yield resources
    finally:
        # Teardown: drop tables; optionally drop schema if your make_schema
        # doesn’t already handle cleanup.
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.{test_config_tbl}")
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.{test_load_status_tbl}")
        # If you want to drop the schema as well (and it's disposable):
        # spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")


@pytest.mark.integration_test
# def test_get_nyctaxi_trips():
#  df = get_nyctaxi_trips()
#  assert df.count() > 0
def test_dummy_func():
    assert dummy_func("Anupam") == "Hi Anupam"


@pytest.mark.integration_test
def test_job(spark, make_schema, ws, job_id, test_tables):
    catalog_name = test_tables["catalog"]
    schema_name = test_tables["schema"]
    # schema_name = make_schema(catalog_name=catalog_name).name
    # print(f"myschema name is  : {schema_name}")
    run_wait = ws.jobs.run_now(job_id=job_id, job_parameters={"catalog_name": catalog_name, "schema_name": schema_name})
    run_result = run_wait.result()
    result_status = run_result.state.result_state
    assert result_status == RunResultState.SUCCESS

    # Any additional tests can be written here
    # df = spark.read.table(f"{catalog_name}.{schema_name}.avg_distance")
    # assert df.count()>0
