import pytest
from pyspark.sql import SparkSession
from common.utils import read_etl_config, format_sql_stmt, validate_variant_data, func_load_sql_stmts, func_override_parms, func_update_status_loading_log, func_execute_notebook, get_metrics_data, dummy_func

# from src.common.test_func import func_test

from pathlib import Path


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


def test_dummy_func():
    assert dummy_func("Anupam") == "Hi Anupam"


# Test func_override_parms Function
# -------------------------------------------------
def test_func_override_parms_positive():
    # Test the replacing of parm override onto the xfm_dtl_sql_notebook_parms
    assert func_override_parms('{"key1": "value1"}', {"key1": "valuex", "key2": "valuey"}) == '{"key1": "value1", "key2": "valuey"}'


def test_func_override_parms_blank():
    # Test the function with empty inputs
    assert func_override_parms("", {}) == "{}"


def test_func_override_parms_blankovrd():
    # Test the replacing of empty parm override onto the xfm_dtl_sql_notebook_parms
    parms_ovrd = ""
    xfm_dtl_sql_notebook_parms = {"parm1": "oldvalue", "parm2": "value2"}
    assert func_override_parms(parms_ovrd, xfm_dtl_sql_notebook_parms) == '{"parm1": "oldvalue", "parm2": "value2"}'


# Test func_load_sql_stmts Function
# -------------------------------------------------
def test_load_sql_stmt():
    test_dir = Path(__file__).parent

    # Path to the data folder and file
    file_path = test_dir / "data" / "test_sql_stmt.txt"
    assert func_load_sql_stmts(file_path) == {
        'sql1': 'SELECT * from test_table\nWHERE col1=\'A\'',
        'sql2': 'SELECT * from test_table WHERE col1=\'A\''
    }


# Test format_sql_stmt Function
# -------------------------------------------------
def test_format_sql_stmt1():
    # Check if the format_sql_stmt function replaces the values from the sql parm and any system parms correctly into the sql_stmt
    sql_stmt = "SELECT * FROM A where a = '{sqlparm}', x='{etl_date}' and y='{jobrunid}' and z='{taskrunid}'"
    common_var_dict = {"etl_date": "2022-01-01", "jobrunid": "testjobrunid", "taskrunid": "testtaskrunid"}
    sql_parms = "{\"sqlparm\": \"abc\"}"
    assert format_sql_stmt(sql_stmt, common_var_dict, sql_parms) == "SELECT * FROM A where a = 'abc', x='2022-01-01' and y='testjobrunid' and z='testtaskrunid'"
