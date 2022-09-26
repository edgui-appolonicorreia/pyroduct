from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.utils.task_group import TaskGroup
from Airflow.framework.utils.data_inspect import DataInspect

from datetime import timedelta, datetime
import json
import logging

# Setting up logging
# Create and configure logger
logging.basicConfig(
    format="%(name)s -> %(levelname)s: %(message)s",
    filemode="a",
    level=logging.DEBUG
)

# Creating an object
logger = logging.getLogger()

# Setting some global vars
SILVER_BUCKET = "silver-test-edgui"
BRONZE_BUCKET = "bronze-test-edgui"
SCHEDULE_INTERVAL = "@hourly"

default_args = {
    "owner": "Edson G. A. Correia",
    "start_date": datetime(2022, 2, 20),
    "depends_on_past": False,
    "email": ["ed.guilherme.correia@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

with DAG(
    dag_id="oracle_hourly",
    schedule_interval=None,
    # schedule_interval=schedule_interval,
    dagrun_timeout=timedelta(minutes=55),
    tags=["pipeline", "oracle", "daily"],
    max_active_runs=1,
    max_active_tasks=2,
    default_args=default_args
) as dag:

    tasks_groups = []

    start_task = DummyOperator(task_id="start_task")

    # Getting the metadata from the tables
    oracle_metadata = Variable.get("oracle_metadata", default_var=False, deserialize_json=True)
    oracle_schemas = list(oracle_metadata.keys())

    # Getting the credentials
    oracle_credentials = Variable.get("oracle_credentials_secret", default_var=False, deserialize_json=True)
    redshift_credentials = Variable.get("redshift_credentials_secret", default_var=False, deserialize_json=True)

    for schema in oracle_schemas:

        with TaskGroup(group_id=schema) as db_tg:

            data_inspect = BranchPythonOperator(
                task_id=f"{schema}_data_inspect",
                python_callable=DataInspect.oracle_check_table_data,
                provide_context=True,
                depends_on_past=False,
                trigger_rule="none_failed",
                op_kwargs={
                    "ts": "{{ ts }}",
                    "schema": schema,
                    "oracle_credentials": oracle_credentials,
                    "oracle_schema_metadata": oracle_metadata[schema],
                    "interval": SCHEDULE_INTERVAL
                }
            )

            for table_metadata in oracle_metadata[schema]:

                table_name = list(table_metadata.keys())[0]
                functions = table_metadata[table_name]["functions"]
                upsert_col = table_metadata[table_name]["upsert_col"]
                constraint_conflict = table_metadata[table_name]["constraint_conflict"]

                extraction_task = AwsLambdaInvokeFunctionOperator(
                    task_id=f"{table_name}_extrac",
                    function_name=functions["extraction"],
                    trigger_rule="all_success",
                    payload=json.dumps({
                        "bucket": BRONZE_BUCKET,
                        "bucket_path": f"{schema}/{table_name}",
                        "schema": schema,
                        "table_name": table_name,
                        "dt_execution": "{{ ts }}",
                        "upsert_col": upsert_col,
                        "host": oracle_credentials["host"],
                        "port": oracle_credentials["port"],
                        "tns": oracle_credentials["tns"],
                        "user": oracle_credentials["user"],
                        "psswd": oracle_credentials["psswd"],
                        "interval": SCHEDULE_INTERVAL
                    })
                )

                transformation_task = AwsLambdaInvokeFunctionOperator(
                    task_id=f"{table_name}_transform",
                    function_name=functions["transformation"],
                    trigger_rule="all_success",
                    payload=json.dumps({
                        "source_bucket": BRONZE_BUCKET,
                        "target_bucket": SILVER_BUCKET,
                        "schema": schema,
                        "table_name": table_name,
                        "dt_execution": "{{ ts }}",
                        "interval": SCHEDULE_INTERVAL
                    })
                )

                load_task = AwsLambdaInvokeFunctionOperator(
                    task_id=f"{table_name}_transform",
                    function_name=functions["transformation"],
                    trigger_rule="all_success",
                    payload=json.dumps({
                        "source_bucket": SILVER_BUCKET,
                        "schema": schema,
                        "table_name": table_name,
                        "dt_execution": "{{ ts }}",
                        "constraint_conflict": constraint_conflict,
                        "interval": SCHEDULE_INTERVAL,
                        "host": redshift_credentials["host"],
                        "port": redshift_credentials["port"],
                        "db": redshift_credentials["db"],
                        "user": redshift_credentials["user"],
                        "psswd": redshift_credentials["psswd"]
                    })
                )

                start_task >> data_inspect >> extraction_task >> transformation_task >> load_task
