import pytest
from pyspark.sql import SparkSession
from src.common.utils import read_etl_config, func_override_parms, format_sql_stmt, func_load_sql_stmts, func_update_status_loading_log, func_execute_notebook, dummy_func
# from src.common.test_func import func_test


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


def test_dummy_func():
    assert dummy_func("Anupam") == "Hi Anupam"


def test_func_override_parms_positive():
    assert func_override_parms('{"key1": "value1"}', {"key1": "valuex", "key2": "valuey"}) == '{"key1": "value1", "key2": "valuey"}'


def test_func_override_parms_blank():    
    assert func_override_parms('', '') == '{}'


# def test_load_sql_stmt():
#     assert func_load_sql_stmts("./data/test_sql_stmt.txt") == []


# def test_test_func_invalid_input():
#    with pytest.raises(ValueError):
#        read_etl_config("invalid")
