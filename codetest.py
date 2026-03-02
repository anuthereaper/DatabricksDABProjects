etl_config_table = "etl_config_table"
task_id = "task_id"
target_layer = "target_layer"

query = f"SELECT * FROM {etl_config_table} WHERE emc_task_id = \"{task_id}\" AND emc_target_layer = \"{target_layer}\" AND emc_active = \"Y\";"
print(query)
