from common.test_func import func_load_sql_stmts, test_func
from pathlib import Path

file_path = Path(__file__).parent.parent / 'tests' / 'data' / 'test_sql_stmt.txt'
print(file_path)
x = func_load_sql_stmts(file_path)

print(x)

print(test_func("Anupam"))
