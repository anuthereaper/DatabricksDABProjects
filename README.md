# DatabricksDABProjects
Multiple databricks projects that can be deployed using Databricks asset bundles


databricks auth login --host https://adb-560023734609075.15.azuredatabricks.net
databricks auth login --host dbc-9f02badd-3b21.cloud.databricks.com
databricks auth login --profile devfree

databricks bundle init default-python

databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev hello_job
databricks bundle destroy



Check databricks.yml configuration settings
https://docs.databricks.com/aws/en/dev-tools/bundles/settings



https://medium.com/@ysingh27/implementing-databricks-apps-with-databricks-asset-bundles-dabs-a-practical-guide-304d36dd61c2

Example github repo : https://github.com/databricks/databricks-asset-bundles-dais2023/blob/main/bundle.yml

To generate a DABs yml file from existing resources : databricks bundle generate app --existing-app-name your-existing-app-name


TESTING :
    UNIT : 
    To execute from tests folder, From the etl_meta_transformation folder and type : pytest tests/ -v
    To execute from src/test/unit and use the pyproject.toml file, From the etl_meta_transformation folder type 'pip install -e .' and then from root directory type 'pytest etl_meta_transformation/src/tests/unit -vv'

    INTEGRATION : (https://community.databricks.com/t5/technical-blog/integration-testing-for-lakeflow-jobs-with-pytest-and-databricks/ba-p/141683)
    ##Install python uv
    Install required Python dependencies using uv --> uv sync --only-group integration-tests
    Get the databriks host --> export DATABRICKS_HOST=<your-dev-workspace-url>   e.g. export DATABRICKS_HOST=https://dbc-9f02badd-3b21.cloud.databricks.com/
    Get the databricks cluster id : For serverless in the bash terminal -->  export DATABRICKS_SERVERLESS_COMPUTE_ID=auto and export DATABRICKS_WAREHOUSE_ID=8fabeaed0fb07237
    Run the integration tests : First go to etl_meta_transformation subfolder.
                                --> uv run python -m pytest -rsx -m integration_test --job-name="ETL meta transformation demo pipeline"
                                  uv run python -m pytest -rsx -m integration_test --job-name="workflow_test_automation_blueprint_job"
                                  uv run -- python -m pytest -rsx -m integration_test --job-name="ETL meta transformation demo pipeline"  





Deployment :
databricks bundle validate
databricks bundle deploy -t dev


## **Overview**
The **ETL Meta Transformation Setup Job** initializes all metadata structures required by the ETL meta‑driven transformation framework. It creates configuration tables and optionally loads demo data to help users understand the framework and run the **ETL Meta Transformation Demo Pipeline**.

When the `load_demo_data` parameter is set to `"Y"`, the job also loads demo configuration entries and creates sample objects (catalogs, schemas, tables, and sample files) used to demonstrate the framework end‑to‑end.

***

## **Parameters**

### **`load_demo_data`**

Specifies whether demo data and demo structures should be created and loaded.

| Value | Meaning                                                                           |
| ----- | --------------------------------------------------------------------------------- |
| `"Y"` | Loads demo data, creates schemas, tables, folders, and generates a demo CSV file. |
| `"N"` | Skips demo data loading. Only configuration tables are created.                   |

**Default:** Typically `"N"` unless overridden in job configuration.  
**Recommendation:** Set to `"Y"` when running the setup job for the first time in a dev/test workspace.

***

## **How to Run the Setup Job**

1.  Set the parameter:

        load_demo_data = "Y"

2.  Execute the workflow:

    **ETL meta transformation setup job**

This will run the setup notebooks in order.

***

## **Workflow Structure**

The workflow contains **two main tasks**, each running a dedicated notebook.

***

## **Task 1 — `setupnotebook`**

Creates the core configuration tables for the ETL meta transformation framework.

### **Tables Created**

#### **`etl_meta_config`**

*   Stores ETL transformation instructions and metadata.
*   Defines how source‑to‑target mappings are executed by the framework.

#### **`etl_table_load_status`**

*   Maintains execution logs for table loads.
*   Tracks status, timestamps, and processing metadata.

***

## **Task 2 — `loaddemodata`**

*Executed only when:*

    load_demo_data = "Y"

This notebook loads demo configuration and creates a mock development environment for demonstration purposes.

### **Actions Performed**

#### **1. Load Demo Configuration**

*   Inserts demonstration rows into `etl_meta_config`.

#### **2. Create Catalog**

Creates the development catalog:

    dev_etl

#### **3. Create Schemas Under `dev_etl`**

*   `bronze`
*   `silver`
*   `gold`
*   `volumes`

#### **4. Create Demo Tables**

The following sample tables are created:

*   `dev_etl.bronze.test`
*   `dev_etl.bronze.test1`
*   `dev_etl.silver.test`
*   `dev_etl.gold.test`
*   `dev_etl.gold.agg_age`

#### **5. Create Folder & Demo File**

*   Folder created:
        dev_etl.volumes.etl_autoloader_folder
*   A demo CSV file is generated inside the folder to simulate an autoloader source.

***

## **What to Do Next**

After the setup job finishes:

1.  Review data inside:
    *   `etl_meta_config`
    *   `etl_table_load_status`
2.  Inspect the created schemas and demo tables under the `dev_etl` catalog.
3.  Run the **ETL Meta Transformation Demo Pipeline** to see the framework in action.
4.  Review data inside:
    *   `etl_table_load_status`
    *   `dev_etl.silver.test`
    *   `dev_etl.gold.test`
    *   `dev_etl.gold.agg_age`
    *   `dev_etl.bronze.test`
    *   `dev_etl.bronze.test1`

***


