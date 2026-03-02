import json
from pyspark.sql import SparkSession
import re
from pyspark.dbutils import DBUtils
import pandas as pd
from typing import Any, Dict, Mapping, Optional


def read_etl_config(task_id: str, target_layer, spark: SparkSession, etl_config_table: str) -> pd.DataFrame:
    """Reads ETL configuration from a specified table using Spark SQL and returns the result as a Pandas DataFrame.

    Args:
        task_id (str): The task id for fetch all rows to be executed for a particular task in a workflow.
        target_layer (str): The target layer (e.g., bronze, silver, gold) to filter the configuration.
        spark (SparkSession): The active Spark session used to execute the query.
        etl_config_table (str): The fully qualified name of the ETL configuration table. Can be a different table for a view. To be passed with catalog and schema.
        exec_mode (str): Execution mode (not used in this function but may be relevant for future extensions).

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the filtered ETL configuration, sorted by 'xfm_dtl_seq'.

    """
    query = f"SELECT * FROM {etl_config_table} WHERE emc_task_id = \"{task_id}\" AND emc_target_layer = \"{target_layer}\" AND emc_active = \"Y\";"

    print(f"query is {query}")
    tran_df = spark.sql(query)
    tran_pandas_df = tran_df.toPandas().sort_values('emc_seq')

    return tran_pandas_df


def validate_variant_data(func_type: str, func_dtls: dict) -> bool:
    """
    Validate the contents of `func_dtls` based on the provided `func_type`.

    This function checks that all mandatory keys exist and have valid values
    for different function types used in ETL metadata configuration.

    Supported func_type values and their validations:
    - BRONZE:
        * Requires keys: 'format', 'source_path', 'checkpoint'
        * 'format' must be one of: CSV, JSON, PARQUET
    - SQL:
        * Requires key: 'sql_var'
    - SQLLOAD:
        * Requires key: 'sql_path'
    - NOTEBOOK:
        * Requires key: 'notebook_path'

    Args:
        func_type (str): The function type (e.g., BRONZE, SQL, SQLLOAD, NOTEBOOK).
        func_dtls (dict): Dictionary containing function details to validate.

    Returns:
        bool: True if validation passes, otherwise raises ValueError.

    Raises:
        ValueError: If any required key is missing or contains an invalid value.
    """

    match func_type.upper():
        # case "AUTOLOAD":
        #    format = func_dtls.get("format", "").strip().upper()
        #    source_path = func_dtls.get("source_path", "").strip()
        #    checkpoint = func_dtls.get("checkpoint", "").strip()
        #    header = func_dtls.get("header", "").strip()
        #    if not format:
        #        raise ValueError("No format detected in emc_func_dtls. This is mandatory for AUTOLOAD func_type. Please check")
        #    else:
        #        if format not in ("CSV", "JSON", "PARQUET"):
        #            raise ValueError("Valid values of format is CSV, JSON or PARQUET. Please check")
        #    if not source_path:
        #        raise ValueError("No source_path detected in emc_func_dtls. This is mandatory for AUTOLOAD func_type. Please check")
        #    if not checkpoint:
        #        raise ValueError("No checkpoint detected in emc_func_dtls. This is mandatory for AUTOLOAD func_type. Please check")
        #    if not header:
        #        raise ValueError("No header option detected in emc_func_dtls. This is mandatory for AUTOLOAD func_type. Valid values are 'false' or 'true'. Please check")
        #    return True
        case "SQL":
            sql_var = func_dtls.get("sql_var", "").strip()
            if not sql_var:
                raise ValueError("No sql_var detected in emc_func_dtls. This is mandatory for SQL func_type. Please check")
            # sql_parms = func_dtls.get("sql_parms", "").strip()
            if "sql_parms" not in func_dtls:
                raise ValueError("No sql_parms detected in emc_func_dtls. This is mandatory for SQL func_type. Please check")
            return True
        case "SQLLOAD":
            sql_path = func_dtls.get("sql_path", "").strip()
            if not sql_path:
                raise ValueError("No sql_path detected in emc_func_dtls. This is mandatory for SQLLOAD func_type. Please check")
            return True
        case "NOTEBOOK":
            notebook_path = func_dtls.get("notebook_path", "").strip()
            if not notebook_path:
                raise ValueError("No notebook_path detected in emc_func_dtls. This is mandatory for NOTEBOOK func_type. Please check")
            return True
        case _:
            raise ValueError(f"{func_type} : Incorrect emc_func_type detected. Please check.")


def format_sql_stmt(sql_stmt: str, common_var_dict: dict, sql_parms: str) -> str:
    """
    Formats an SQL statement by replacing placeholders with actual values.

    This function takes an SQL template string and substitutes placeholders
    such as `{etl_date}`, `{jobrunid}`, and `{taskrunid}` with values from
    `common_var_dict`. Additionally, if `sql_parms` contains a JSON string
    of key-value pairs, those keys are also replaced in the SQL statement.

    Args:
        sql_stmt (str): The SQL statement template containing placeholders.
        common_var_dict (dict): Dictionary with common variables like
            'etl_date', 'jobrunid', and 'taskrunid'.
        sql_parms (str): A JSON-formatted string of additional parameters
            to replace in the SQL statement. If empty, no extra replacements
            are performed.

    Returns:
        str: The formatted SQL statement with all placeholders replaced.

    Example:
        >>> sql = "SELECT * FROM table WHERE date = '{etl_date}' AND id = '{jobrunid}'"
        >>> common_vars = {"etl_date": "2022-01-01", "jobrunid": "123", "taskrunid": "456"}
        >>> extra_parms = '{"customer_id": "789"}'
        >>> format_sql_stmt(sql, common_vars, extra_parms)
        "SELECT * FROM table WHERE date = '2022-01-01' AND id = '123'"
    """

    sql_stmt = sql_stmt.replace("{etl_date}", common_var_dict["etl_date"])
    sql_stmt = sql_stmt.replace("{jobrunid}", common_var_dict["jobrunid"])
    sql_stmt = sql_stmt.replace("{taskrunid}", common_var_dict["taskrunid"])

    if sql_parms != "":
        json_object = json.loads(sql_parms)
        for key in json_object:
            sql_stmt = sql_stmt.replace("{" + key + "}", str(json_object[key]))

    return sql_stmt


def func_override_parms(parms_ovrd: str, xfm_dtl_sql_notebook_parms: str) -> str:
    """
    Merge or override default SQL notebook parameters with user-provided overrides.

    This function takes two JSON-formatted strings:
    - `parms_ovrd`: A JSON string containing parameter overrides.
    - `xfm_dtl_sql_notebook_parms`: A JSON string containing default parameters.

    Behavior:
    - If `parms_ovrd` is not empty, it parses both JSON strings and updates the default
      parameters with the override values.
    - If `parms_ovrd` is empty, it returns the default parameters as-is.
    - If the resulting parameter set is empty, it returns an empty JSON object (`{}`).

    Args:
        parms_ovrd (str): JSON string of override parameters. Can be empty.
        xfm_dtl_sql_notebook_parms (str): JSON string of default parameters. Can be empty.

    Returns:
        str: A JSON string representing the merged parameter set.

    Example:
        >>> func_override_parms('{"param1": "value1"}', '{"param2": "value2"}')
        '{"param2": "value2", "param1": "value1"}'
    """

    sql_notebook_parms = "{}"
    if xfm_dtl_sql_notebook_parms == "":
        xfm_dtl_sql_notebook_parms = "{}"

    if parms_ovrd != "":
        sql_notebook_parms = json.loads(sql_notebook_parms)
        # default_parms_json = json.loads(xfm_dtl_sql_notebook_parms)
        default_parms_json = xfm_dtl_sql_notebook_parms
        parms_ovrd_json = json.loads(parms_ovrd)
        for key in parms_ovrd_json:
            default_parms_json[key] = parms_ovrd_json[key]

        sql_notebook_parms = json.dumps(default_parms_json)
    else:
        sql_notebook_parms = json.dumps(xfm_dtl_sql_notebook_parms)

    if sql_notebook_parms == "":
        sql_notebook_parms = "{}"

    return sql_notebook_parms


def func_load_sql_stmts(xfm_dtl_notebook_path: str) -> dict:
    """
    Loads and parses SQL statements from a given file into a dictionary.

    This function reads a file containing SQL statements, removes comments
    and unnecessary formatting, and splits the content into individual SQL
    statements. Each statement is expected to follow the format:
        variable_name = SQL_QUERY;

    The function returns a dictionary where keys are variable names and
    values are the corresponding SQL queries.

    Processing steps:
        1. Read the file content.
        2. Remove comment lines starting with '#' and ending with a newline.
        3. Remove triple quotes and strip newline characters.
        4. Split the content by semicolons (';') to separate statements.
        5. Build a dictionary mapping variable names to SQL queries.

    Args:
        xfm_dtl_notebook_path (str): Path to the file containing SQL statements.

    Returns:
        dict: A dictionary where keys are variable names and values are SQL statements.

    Example:
        File content:
            # This is a comment
            query1 = SELECT * FROM table1;
            query2 = SELECT * FROM table2;

        >>> func_load_sql_stmts("queries.txt")
        {'query1': 'SELECT * FROM table1', 'query2': 'SELECT * FROM table2'}
    """

    with open(xfm_dtl_notebook_path) as queryFile:
        s = queryFile.read()

        # Remove all comment lines starting with # and ending woth \n
        sql_stmt_str = re.sub(r'#.*?\n', r'', s)

        # Remove all sets of 3 quotations and strip any new line characters
        sql_stmt_str = sql_stmt_str.replace("\"\"\"", "").strip("\n")
        sql_strings = sql_stmt_str.split(";")
        if sql_strings[-1] == "":
            sql_strings = sql_strings[0:-1]

    # Build a dictionary of sql variables and corresponding sql statements
    sql_stmt_dict = {}
    for sql_string in sql_strings:
        sql = sql_string.split("=" , 1)
        sql_stmt_dict[sql[0].strip().strip("\n")] = sql[1].strip().strip("\n")
    return sql_stmt_dict


def func_update_status_loading_log(
    common_var_dict: dict,
    out_view: str,
    rows_inserted: int,
    rows_updated: int,
    rows_deleted: int,
    spark: SparkSession,
    prev_version: int,
    new_version: int,
    status: str,
    start_time: str,
    etl_config_schema: str
):
    """
    Update the ETL load status log table with details of a transformation run.

    This function builds and executes an SQL `INSERT` statement to record
    metadata and statistics about a data load operation in the
    `test_v2.xfm_table_load_status` table. It captures information such as:

    - ETL date
    - Target table name
    - Row counts (inserted, updated, deleted)
    - Previous and new Delta table version numbers
    - Pipeline and job identifiers
    - Task identifiers
    - User and notebook details
    - Start and end timestamps
    - Status of the operation (e.g., SUCCESS, FAILED)

    Args:
        common_var_dict (dict): Dictionary containing common variables such as:
            'etl_date', 'jobname', 'jobrunid', 'jobid',
            'taskrunid', 'taskname', 'userid', and 'notebook_name'.
        out_view (str): Name of the output view or table being processed.
        rows_inserted (int): Number of rows inserted during the operation.
        rows_updated (int): Number of rows updated during the operation.
        rows_deleted (int): Number of rows deleted during the operation.
        spark (SparkSession): Active Spark session used to execute the SQL.
        prev_version (int): Previous version number of the Delta table.
        new_version (int): New version number of the Delta table.
        status (str): Status of the operation (e.g., 'SUCCESS', 'FAILED').
        start_time (str): Timestamp when the operation started.
        etl_config_schema (str): Schema name where the ETL configuration table resides.

    Returns:
        None: Executes an SQL statement and prints a confirmation message.

    Example:
        func_update_status_loading_log(
            common_var_dict,
            "customer_table",
            100, 50, 10,
            spark,
            1, 2,
            "SUCCESS",
            "2025-11-11 08:00:00",
            "etl_config"
        )
        # Output:
        # Table load status update complete for customer_table
    """
    table_dtls = out_view.split(".")
    load_sql_stmt = f"""
                Insert into {etl_config_schema}.etl_table_load_status by name
                select uuid() as etls_log_id,
                \"{common_var_dict["etl_date"]}\" as etls_load_date,
                \"{table_dtls[0]}\" as etls_catalog,
                \"{table_dtls[1]}\" as etls_schema,
                \"{table_dtls[2]}\" as etls_table_name,
                {rows_inserted} as etls_rows_inserted,
                {rows_updated} as etls_rows_updated,
                {rows_deleted} as etls_rows_deleted,
                {prev_version} as etls_prev_ver,
                nullif({new_version},0) as etls_new_ver,
                \"{common_var_dict["jobname"]}\" as etls_jobname,
                \"{common_var_dict["jobrunid"]}\" as etls_jobrunid,
                \"{common_var_dict["jobid"]}\" as etls_jobid,
                \"{common_var_dict["taskrunid"]}\" as etls_taskrunid,
                \"{common_var_dict["taskname"]}\" as etls_taskname,
                \"{common_var_dict["userid"]}\" as etls_run_by,
                \"{common_var_dict["notebook_name"]}\" as etls_notebook,
                CURRENT_TIMESTAMP as etls_start_timestamp,
                '{start_time}' AS etls_end_timestamp,
                \"{status}" as etls_status;
                """
    spark.sql(load_sql_stmt)
    print(f"Table load status update complete for {out_view}")


def func_execute_notebook(
    sql_notebook_parms: Dict[str, Any], #Mapping[str, Any],
    notebook_path: str,
    spark: SparkSession,
    notebook_timeout: int = 120,
    common_var_dict: Optional[Dict[str, Any]] = None,
) -> Any:

    """
    Executes a Databricks notebook with specified parameters and returns execution results.

    This function uses the Databricks `dbutils.notebook.run()` API to run a notebook
    located at `xfm_dtl_notebook_path`. It merges common variables from `common_var_dict`
    with additional parameters provided in `sql_notebook_parms` (JSON string), then
    passes them to the notebook as input arguments. After execution, it parses the
    notebook's JSON response to extract row count and version details.

    Args:
        xfm_dtl_notebook_path (str): Path to the Databricks notebook to execute.
        sql_notebook_parms (str): JSON-formatted string of additional parameters for the notebook.
        spark (SparkSession): Active Spark session used to initialize DBUtils.
        xfm_dtl_notebook_timeout (int, optional): Timeout in seconds for notebook execution.
            Defaults to 120.
        common_var_dict (dict): Dictionary of common variables such as 'etl_date',
            'jobname', 'jobrunid', 'jobid', 'taskrunid', 'taskname', 'userid',
            and 'Interface'.

    Returns:
        tuple: A tuple containing:
            - row_count (int): Number of rows processed by the notebook.
            - prev_version (int): Previous version number of the data.
            - new_version (int): New version number of the data.

    Example:
        >>> common_vars = {
        ...     "etl_date": "2025-11-11",
        ...     "jobname": "ETL Job",
        ...     "jobrunid": "12345",
        ...     "jobid": "67890",
        ...     "taskrunid": "111",
        ...     "taskname": "silver_load",
        ...     "userid": "user@example.com",
        ...     "Interface": "cust_bronze_to_silver"
        ... }
        >>> sql_parms = '{"param1": "value1"}'
        >>> func_execute_notebook("/Shared/my_notebook", sql_parms, spark, 300, common_vars)
        (100, 1, 2)
    """

    dbutils = DBUtils(spark)

    # notebook_parms = json.loads(sql_notebook_parms)
    notebook_parms = json.loads(sql_notebook_parms)
    print(f"Notebook parms before : {notebook_parms}")
    if notebook_parms == "":
        notebook_parms = {}
    notebook_parms["etl_date"] = common_var_dict["etl_date"]
    notebook_parms["jobname"] = common_var_dict["jobname"]
    notebook_parms["jobrunid"] = common_var_dict["jobrunid"]
    notebook_parms["jobid"] = common_var_dict["jobid"]
    notebook_parms["taskrunid"] = common_var_dict["taskrunid"]
    notebook_parms["taskname"] = common_var_dict["taskname"]
    notebook_parms["userid"] = common_var_dict["userid"]
    notebook_parms["Interface"] = common_var_dict["Interface"]
    print(f"Notebook parms after : {notebook_parms}")
    # notebooktimeout = int(xfm_dtl_notebook_timeout)

    dbutils.notebook.run(notebook_path, notebook_timeout, notebook_parms)
    print("Notebook completed successfully")


def get_metrics_data(spark: SparkSession, emc_target: str, new_version: int) -> tuple[int, int, int]:
    """
    Retrieve Delta Lake operation metrics for a specific table version.

    Args:
        spark (SparkSession): The active Spark session.
        emc_target (str): The fully qualified Delta table name (e.g., 'dev.bronze.test').
        new_version (int): The version number of the Delta table to inspect.

    Returns:
        tuple: A tuple of three integers:
            - rows_inserted (int): Number of rows inserted in the operation.
            - rows_updated (int): Number of rows updated in the operation.
            - rows_deleted (int): Number of rows deleted in the operation.

    Raises:
        Exception: If the operation type is unknown or not handled.
    """

    metric_df = spark.sql(f"SELECT operation, operationMetrics FROM (DESCRIBE HISTORY {emc_target}) WHERE version = {new_version};")
    operation = metric_df.collect()[0][0]
    metrics = metric_df.collect()[0][1]
    print(f"Operation : {operation} ; metrics {metrics}")
    match operation:
        case "STREAMING UPDATE":
            rows_inserted = metrics["numOutputRows"]
            rows_updated = 0
            rows_deleted = 0
        case "WRITE":
            rows_inserted = metrics["numOutputRows"]
            rows_updated = 0
            rows_deleted = 0
        case "DELETE":
            rows_inserted = 0
            rows_updated = 0
            rows_deleted = metrics["numDeletedRows"]
        case "UPDATE":
            rows_inserted = 0
            rows_updated = metrics["numUpdatedRows"]
            rows_deleted = 0
        case "MERGE":
            rows_inserted = metrics["numTargetRowsInserted"]
            rows_updated = metrics["numTargetRowsUpdated"]
            rows_deleted = metrics["numTargetRowsDeleted"]
        case _:
            raise Exception("Unknown operation from HISTORY table : {operation}. Please check ")
    print(f"Metrics : {rows_inserted} {rows_updated} {rows_deleted}")
    return (rows_inserted, rows_updated, rows_deleted)


def dummy_func(name: str) -> str:
    return f"Hi {name}"
