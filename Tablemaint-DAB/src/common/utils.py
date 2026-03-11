import datetime
import uuid
from pyspark.sql import SparkSession, functions as F


def get_included_tables():  # (level: str, name: str, spark: Optional[SparkSession] = None) -> str:
    """Generates table name with catalog and schema if specified.

    Args:
        level (str): The level of the table (silver, gold, bronze, ...).
        name (str): The name of the table on the given level.
        spark (Optional[SparkSession], optional): Spark session. Defaults to None.

    Raises:
        Exception: ValueError if schema is not specified when catalog is specified.

    Returns:
        str: The fully qualified table name with catalog and schema.
    """
    print("hello")


# ----------------- Helpers -----------------

def now_utc():
    """
    Return the current UTC timestamp as an aware datetime object.

    Note:
        Using datetime.datetime.now(datetime.timezone.utc) returns a timezone-aware
        UTC datetime. The commented out utcnow() returns a naive datetime, which is
        less safe for comparisons.
    """
    # return datetime.datetime.utcnow()
    return datetime.datetime.now(datetime.timezone.utc)


def weekday_name(dt):
    return dt.strftime("%A").upper()


def quote_part(part: str) -> str:
    """
    Wrap a single identifier with backticks, escaping internal backticks.

    Args:
        part: The identifier to quote (e.g., catalog, schema, table).

    Returns:
        str: The identifier wrapped in backticks, with internal backticks escaped.

    Example:
        part='my`table' -> '`my``table`'
    """
    part = part.strip()
    part = part.replace("`", "``")
    return f"`{part}`"


def parse_full_name(name: str):
    """
    Parse a fully qualified name into its components.

    Accepts any of:
      - 'catalog.schema.table'
      - 'catalog.schema'
      - 'catalog'

    Returns:
        tuple: (object_type, catalog, schema, table)
            object_type in {'CATALOG', 'SCHEMA', 'TABLE'}

    Raises:
        ValueError: If name is None or has more than 3 dot-separated parts.
    """
    if name is None:
        raise ValueError("tmc_name is NULL")
    parts = [p for p in name.split(".") if p.strip() != ""]
    if len(parts) == 1:
        return ("CATALOG", parts[0], None, None)
    elif len(parts) == 2:
        return ("SCHEMA", parts[0], parts[1], None)
    elif len(parts) == 3:
        return ("TABLE", parts[0], parts[1], parts[2])
    else:
        raise ValueError(f"Invalid tmc_name format: '{name}'")


def fq_table_name(catalog, schema, table):
    """
    Build a fully-qualified table name with each part quoted.

    Args:
        catalog: Catalog name (str)
        schema: Schema name (str)
        table: Table name (str)

    Returns:
        str: A string like '`catalog`.`schema`.`table`'
    """
    return ".".join([quote_part(catalog), quote_part(schema), quote_part(table)])


def is_due(freq: str, today: datetime.datetime, weekly_day: str, monthly_dom: int):
    """
    Determine if a task is due based on its frequency and current date.

    Args:
        freq: Frequency string; expected values: 'DAILY', 'WEEKLY', 'MONTHLY', 'MANUAL'
        today: A datetime representing the current date (used for day/month checks)
        weekly_day: Uppercase weekday name (e.g., 'MONDAY') for WEEKLY frequency
        monthly_dom: Day-of-month integer (1-31) for MONTHLY frequency
        include_manual: If True, consider MANUAL tasks as due

    Returns:
        bool: True if due, else False
    """
    freq = (freq or "").upper()
    if freq == "DAILY":
        return True
    if freq == "WEEKLY":
        return weekday_name(today) == weekly_day
    if freq == "MONTHLY":
        return today.day == monthly_dom
    return False


def log_event(
    spark: SparkSession,
    op: str,
    full_name: str,
    status: str,
    start_ts,
    end_ts,
    err: str,
    LOG_TBL: str,
    job_id: str,
    job_name: str,
    job_run_id: str,
    task_run_id: str,
    task_name: str,
    run_by: str,
    file_cnt_before: int,
    file_cnt_after: int
):
    """
    Append a single operation log row into the specified log table.

    Args:
        op: Operation name (e.g., 'LOAD', 'MERGE')
        full_name: Target object full name like 'catalog.schema.table'
        status: Status string (e.g., 'SUCCESS', 'FAILED')
        start_ts: Start timestamp (datetime)
        end_ts: End timestamp (datetime)
        err: Error message (if any; can be None or empty)
        LOG_TBL: Fully-qualified log table name to write into
        job_id, job_name, run_id, task_id, task_name: Identifiers for orchestration context
        run_by: Who/what triggered the run
        file_cnt_before, file_cnt_after: File counts (int) for before/after metrics

    Behavior:
        - Parses full_name into catalog/schema/table.
        - Creates a one-row DataFrame with a new UUID and current UTC timestamp.
        - Appends the row to LOG_TBL using saveAsTable in append mode.
    """
    # Parse the full object name to extract catalog/schema/table
    tbl_parts = parse_full_name(full_name)
    catalog = tbl_parts[1]
    schema = tbl_parts[2]
    table = tbl_parts[3]

    # Create a one-row DataFrame containing the log record
    df = spark.createDataFrame(
        [
            (
                str(uuid.uuid4()),             # tml_uuid
                now_utc(),                     # tml_timestamp (UTC)
                op,                            # tml_operation
                catalog,                       # tml_catalog_name
                schema,                        # tml_schema_name
                table,                         # tml_table_name
                status,                        # tml_status
                err,                           # tml_error_message
                run_by,                        # tml_run_by
                start_ts,                      # tml_start_ts
                end_ts,                        # tml_end_ts
                job_id,                        # tml_job_id
                job_name,                      # tml_job_name
                job_run_id,                        # tml_run_id
                task_run_id,                       # tml_task_id
                task_name,                     # tml_task_name
                file_cnt_before,               # tml_file_count_before
                file_cnt_after                 # tml_file_count_after
            )
        ],
        schema=(
            "tml_uuid STRING, "
            "tml_timestamp TIMESTAMP, "
            "tml_operation STRING, "
            "tml_catalog_name STRING, "
            "tml_schema_name STRING, "
            "tml_table_name STRING, "
            "tml_status STRING, "
            "tml_error_message STRING, "
            "tml_run_by STRING, "
            "tml_start_ts TIMESTAMP, "
            "tml_end_ts TIMESTAMP, "
            "tml_job_id STRING, "
            "tml_job_name STRING, "
            "tml_job_run_id STRING, "
            "tml_task_run_id STRING, "
            "tml_task_name STRING, "
            "tml_file_count_before INT, "
            "tml_file_count_after INT"
        )
    )

    df.write.mode("append").saveAsTable(LOG_TBL)


def show_tables_in_schema(spark: SparkSession, catalog: str, schema: str):
    """
    List tables in a given catalog.schema.

    Returns:
        DataFrame with columns:
            - schema_name (aliased from 'database')
            - tableName
    """
    q = f"SHOW TABLES IN {quote_part(catalog)}.{quote_part(schema)}"
    return spark.sql(q).select(F.col("database").alias("schema_name"), "tableName")


def show_schemas_in_catalog(spark: SparkSession, catalog: str):
    """
    List schemas in a given catalog.

    Returns:
        DataFrame with a single column 'schema_name' (aliased from 'databaseName').
    """
    q = f"SHOW SCHEMAS IN {quote_part(catalog)}"
    return spark.sql(q).select(F.col("databaseName").alias("schema_name"))


def table_exists(spark: SparkSession, catalog: str, schema: str, table: str) -> bool:
    """
    Check if a table exists by attempting to reference it.

    Returns:
        True if spark.table() resolves successfully, otherwise False.
    """
    try:
        spark.table(fq_table_name(catalog, schema, table))
        return True
    except Exception:
        return False


def run_sql_with_retry(spark: SparkSession, sql_text: str, max_retries: int, DRY_RUN: bool):
    """
    Execute SQL with retries.

    Args:
        spark: SparkSession
        sql_text: SQL statement to execute
        max_retries: Maximum retry attempts on failure (integer >= 0)
        DRY_RUN: If True, do not execute; return early (see note below)

    Returns:
        (success: bool, error_message: str | None)
            - success=True and error_message=None if SQL executed successfully
            - success=False and error_message has the last exception message otherwise

    Note:
        The current implementation returns `None` immediately if DRY_RUN is True,
        which does not match the documented return type. Consider returning
        (True, None) or (False, 'DRY_RUN') for consistency.
    """
    attempt = 0
    last_err = None
    while attempt <= max_retries:
        try:
            if DRY_RUN:
                return None
            spark.sql(sql_text)
            return True, None
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)}"
            attempt += 1
            if attempt > max_retries:
                break
    return False, last_err
