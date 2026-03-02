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
