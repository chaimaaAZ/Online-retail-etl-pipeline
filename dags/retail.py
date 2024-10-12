from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from astro import sql as aql
from astro.files  import File 
from astro.sql.table import Table,Metadata
from astro.constants import FileType
from include.soda.check_function import check
from cosmos.config import ProjectConfig, RenderConfig
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

SNOWFLAKE_CONN_ID = 'online_retail_snowflake_conn'
conn = snowflake.connector.connect(
        user='',
        password='',
        account='',
        warehouse="online_retail_wh",
        database="online_retail_db",
        schema="ST_INTERNAL_STAGES"
    )

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONN_ID, 
        profile_args={"database": "online_retail_db", "schema": "online_retail_schema"},
    )
)
def ingest_data_to_snowflake(**context):
    
    cursor = conn.cursor()

    cursor.execute("USE ROLE st_online_retail")
    cursor.execute("USE DATABASE online_retail_db")
    cursor.execute("""
    CREATE OR REPLACE FILE FORMAT online_retail_schema.my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ',' 
        COMPRESSION = 'GZIP' 
        FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
        NULL_IF = ('NULL', 'null') 
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        skip_header = 1;
    """)

    cursor.execute("CREATE OR REPLACE STAGE ST_INTERNAL_STAGES.ST_DEMO_STAGE  DIRECTORY = (ENABLE=TRUE);")

    local_file_path = '/usr/local/airflow/include/dataset/online_retail.csv'
    cursor.execute(f"PUT file://{local_file_path} @ST_INTERNAL_STAGES.ST_DEMO_STAGE ")

    cursor.close()
    conn.close()


def load_raw_data_to_table(**context):
    
    cursor = conn.cursor()

    cursor.execute("USE ROLE st_online_retail")
    cursor.execute("USE DATABASE online_retail_db")
    cursor.execute("""
    CREATE OR REPLACE TABLE online_retail_schema.RAW_INVOICES (
                    InvoiceNo STRING,
                    StockCode STRING,
                    Description STRING,
                    Quantity INTEGER,
                    InvoiceDate STRING,
                    UnitPrice FLOAT,
                    CustomerId float,
                    Country STRING
                );
    """)

    cursor.execute("""
                COPY INTO online_retail_schema.RAW_INVOICES
                FROM @ONLINE_RETAIL_DB.ST_INTERNAL_STAGES.ST_DEMO_STAGE/online_retail.csv
                FILE_FORMAT = (FORMAT_NAME = online_retail_schema.my_csv_format)
                ON_ERROR = 'CONTINUE' 
                purge = True;

""")

    cursor.close()
    conn.close()

def check_raw_data(scan_name='check_load', check_subpath='sources'):
    return check(scan_name,check_subpath)


def check_dim_fact_data(scan_name='check_load', check_subpath='transform'):
    return check(scan_name,check_subpath)



with DAG(
    dag_id="retail",
    start_date=datetime(2023, 10, 10),
    schedule_interval=None,
    catchup=False,
    tags=['retail']
) as dag:

    load_file_to_staging = PythonOperator(
        task_id="Ingest_raw_data_in_snowflake",
        python_callable=ingest_data_to_snowflake,
        provide_context=True,
    )


    raw_to_table = PythonOperator(
        task_id="snowflake_raw_to_table",
        python_callable = load_raw_data_to_table,
        provide_context=True,
    )

    raw_data_soda_check = PythonOperator(
           task_id='soda_check_for_raw_data',
           python_callable = check_raw_data
    )

    dbt_create_dim_fact_tables_tag = DbtTaskGroup(
        group_id='transform_using_dbt',
        project_config=ProjectConfig(dbt_project_path="/usr/local/airflow/dags/dbt/online_retail_pipeline"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv2/bin/dbt"  # Ensure this path is correct
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )
    check_transform_data = PythonOperator(
           task_id='soda_check_transform_data',
           python_callable = check_dim_fact_data
    )
    create_report_tables_with_dbt = DbtTaskGroup(
        group_id='create_report_tables_with_dbt',
        project_config=ProjectConfig(dbt_project_path="/usr/local/airflow/dags/dbt/online_retail_pipeline"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv2/bin/dbt"  # Ensure this path is correct
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/reporting']
        )
    )


load_file_to_staging >> raw_to_table >> raw_data_soda_check >> dbt_create_dim_fact_tables_tag >> check_transform_data >> create_report_tables_with_dbt



